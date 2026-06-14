//! PostgreSQL logical-replication ingest.
//!
//! Owns the replication configuration, pgoutput decoding, and MDBX persistence
//! of replication commit batches.

pub mod ingest;
pub mod postgres;
pub mod runner;
pub mod runtime;
pub mod snapshot;

use std::{
    collections::HashMap,
    env,
    fmt::{self, Display},
};

use crate::config::load_config_from_env;

const SERVICE_MODE_ENV: &str = "POWERSYNC_RUST_SERVICE_MODE";
const REPLICATION_ENABLED_ENV: &str = "POWERSYNC_RUST_REPLICATION_ENABLED";
const REPLICATION_URI_ENV: &str = "POWERSYNC_RUST_POSTGRES_REPLICATION_URI";
const REPLICATION_URI_ENV_ALT: &str = "POWERSYNC_POSTGRES_REPLICATION_URI";
const SLOT_ENV: &str = "POWERSYNC_RUST_REPLICATION_SLOT";
const SLOT_ENV_ALT: &str = "POWERSYNC_POSTGRES_REPLICATION_SLOT";
const PUBLICATION_ENV: &str = "POWERSYNC_RUST_REPLICATION_PUBLICATION";
const PUBLICATION_ENV_ALT: &str = "POWERSYNC_POSTGRES_REPLICATION_PUBLICATION";
const GROUP_ENV: &str = "POWERSYNC_RUST_GROUP_ID";
const GROUP_ENV_ALT: &str = "POWERSYNC_GROUP_ID";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceMode {
    ApiOnly,
    ReplicationOnly,
    Unified,
}

impl ServiceMode {
    fn from_optional_str(value: Option<&str>) -> Result<Self, ReplicationConfigError> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None => Ok(Self::ApiOnly),
            Some(value) if matches_ci(value, &["api", "api-only", "serve", "serving"]) => {
                Ok(Self::ApiOnly)
            }
            Some(value)
                if matches_ci(value, &["replication", "replication-only", "cdc", "ingest"]) =>
            {
                Ok(Self::ReplicationOnly)
            }
            Some(value) if matches_ci(value, &["unified", "all", "fullstack"]) => Ok(Self::Unified),
            Some(other) => Err(ReplicationConfigError::InvalidServiceMode(other.to_owned())),
        }
    }

    pub fn runs_api(self) -> bool {
        matches!(self, Self::ApiOnly | Self::Unified)
    }

    pub fn runs_replication(self) -> bool {
        matches!(self, Self::ReplicationOnly | Self::Unified)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PostgresReplicationConfig {
    pub uri: String,
    pub slot_name: String,
    pub publication_name: String,
    pub group_id: String,
}

impl fmt::Debug for PostgresReplicationConfig {
    // Manual impl so the connection URI's password never reaches logs: `uri`
    // is the full `postgres://user:password@host/db` string and this struct is
    // logged at startup (directly and via `RuntimePlan`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresReplicationConfig")
            .field("uri", &redact_postgres_uri(&self.uri))
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("group_id", &self.group_id)
            .finish()
    }
}

/// Mask the password in a `scheme://user:password@host/...` URI for logging.
pub(crate) fn redact_postgres_uri(uri: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(uri) else {
        return "<redacted-invalid-postgres-uri>".to_owned();
    };
    if parsed.password().is_some() && parsed.set_password(Some("***")).is_err() {
        return "<redacted-invalid-postgres-uri>".to_owned();
    }
    let query_pairs = parsed
        .query_pairs()
        .map(|(key, value)| {
            let value = if key.eq_ignore_ascii_case("password") {
                "***".to_owned()
            } else {
                value.into_owned()
            };
            (key.into_owned(), value)
        })
        .collect::<Vec<_>>();
    parsed.set_query(None);
    if !query_pairs.is_empty() {
        parsed.query_pairs_mut().extend_pairs(query_pairs);
    }
    parsed.to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePlan {
    pub service_mode: ServiceMode,
    pub replication: Option<PostgresReplicationConfig>,
}

impl RuntimePlan {
    pub fn from_env() -> Result<Self, ReplicationConfigError> {
        let vars = env::vars().collect::<HashMap<_, _>>();
        let config = load_config_from_env().map_err(ReplicationConfigError::Config)?;
        Self::from_lookup_with_config(
            |name| vars.get(name).cloned(),
            config.as_ref().map(|loaded| loaded.config()),
            service_mode_from_args(),
        )
    }

    pub fn from_lookup<F>(lookup: F) -> Result<Self, ReplicationConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        Self::from_lookup_with_config(lookup, None, None)
    }

    pub fn from_lookup_with_config<F>(
        lookup: F,
        config: Option<&crate::config::PowerSyncConfig>,
        arg_service_mode: Option<String>,
    ) -> Result<Self, ReplicationConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let config_uri = config
            .and_then(|config| config.replication.as_ref())
            .and_then(|replication| replication.connections.first())
            .map(|connection| connection.uri.clone());

        let service_mode_value = lookup(SERVICE_MODE_ENV).or(arg_service_mode);
        let service_mode = ServiceMode::from_optional_str(service_mode_value.as_deref())?;
        let replication_enabled = lookup(REPLICATION_ENABLED_ENV)
            .as_deref()
            .map(parse_bool)
            .transpose()?
            .unwrap_or(false);

        let uri = first_present(
            &lookup,
            &[
                REPLICATION_URI_ENV,
                "PS_DATA_SOURCE_URI",
                REPLICATION_URI_ENV_ALT,
            ],
        )
        .or(config_uri);
        let slot_name = first_present(&lookup, &[SLOT_ENV, "PS_REPLICATION_SLOT", SLOT_ENV_ALT]);
        let publication_name = first_present(
            &lookup,
            &[PUBLICATION_ENV, "PS_PUBLICATION", PUBLICATION_ENV_ALT],
        );
        let group_id = first_present(&lookup, &[GROUP_ENV, GROUP_ENV_ALT]);

        let any_replication_env_present = [
            uri.as_ref(),
            slot_name.as_ref(),
            publication_name.as_ref(),
            group_id.as_ref(),
        ]
        .into_iter()
        .any(|value| value.is_some());
        let replication_required = replication_enabled || service_mode.runs_replication();

        let replication = if replication_required || any_replication_env_present {
            Some(PostgresReplicationConfig {
                uri: uri.ok_or(ReplicationConfigError::MissingRequiredEnv(
                    REPLICATION_URI_ENV,
                ))?,
                slot_name: slot_name.unwrap_or_else(|| "powersync_mdbx".to_owned()),
                publication_name: publication_name.unwrap_or_else(|| "powersync".to_owned()),
                group_id: group_id.unwrap_or_else(|| "default".to_owned()),
            })
        } else {
            None
        };

        Ok(Self {
            service_mode,
            replication,
        })
    }

    pub fn api_boot_required(&self) -> bool {
        self.service_mode.runs_api()
    }

    pub fn replication_boot_requested(&self) -> bool {
        self.service_mode.runs_replication()
    }

    pub fn ensure_supported_bootstrap(&self) -> Result<(), ReplicationConfigError> {
        if self.replication_boot_requested() && self.replication.is_none() {
            return Err(ReplicationConfigError::MissingRequiredEnv(
                REPLICATION_URI_ENV,
            ));
        }

        Ok(())
    }
}

impl PostgresReplicationConfig {
    #[cfg(test)] // vestigial: the live path drives the protocol via pgwire-replication
    pub fn start_replication_query(
        &self,
        start_lsn: postgres::PostgresLsn,
    ) -> Result<String, postgres::ReplicationQueryError> {
        postgres::start_replication_query(
            &self.slot_name,
            std::slice::from_ref(&self.publication_name),
            start_lsn,
        )
    }

    #[cfg(test)] // vestigial: the live path drives the protocol via pgwire-replication
    pub fn create_slot_query(&self, temporary_slot: bool) -> String {
        let mode = if temporary_slot {
            postgres::ReplicationSlotMode::Temporary
        } else {
            postgres::ReplicationSlotMode::Persistent
        };
        postgres::create_replication_slot_query(&self.slot_name, mode)
    }

    pub fn bootstrap(
        &self,
        start_lsn: postgres::PostgresLsn,
    ) -> Result<runtime::ReplicationBootstrap, runtime::ReplicationBootstrapError> {
        runtime::ReplicationBootstrap::from_config(self, start_lsn)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationConfigError {
    #[error("unsupported {SERVICE_MODE_ENV} value: {0} (expected api-only, replication-only, or unified)")]
    InvalidServiceMode(String),
    #[error("invalid boolean value for {name}: {value}")]
    InvalidBooleanEnv { name: &'static str, value: String },
    #[error("missing required replication env {0}")]
    MissingRequiredEnv(&'static str),
    #[error("replication bootstrap for {0} mode requires a valid replication configuration in powersync_mdbx")]
    ReplicationBootstrapNotImplemented(ServiceMode),
    #[error("{0}")]
    Config(String),
}

fn first_present<F>(lookup: &F, names: &[&str]) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    names.iter().find_map(|name| {
        lookup(name)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

fn parse_bool(value: &str) -> Result<bool, ReplicationConfigError> {
    let trimmed = value.trim();
    if matches_ci(trimmed, &["1", "true", "yes", "on"]) {
        Ok(true)
    } else if matches_ci(trimmed, &["0", "false", "no", "off"]) {
        Ok(false)
    } else {
        Err(ReplicationConfigError::InvalidBooleanEnv {
            name: REPLICATION_ENABLED_ENV,
            value: trimmed.to_owned(),
        })
    }
}

fn service_mode_from_args() -> Option<String> {
    let args = env::args().collect::<Vec<_>>();
    args.windows(2).find_map(|pair| {
        if pair[0] == "-r" || pair[0] == "--runtime" || pair[0] == "--service-mode" {
            Some(pair[1].clone())
        } else {
            None
        }
    })
}

fn matches_ci(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}

impl Display for ServiceMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::ApiOnly => "api-only",
            Self::ReplicationOnly => "replication-only",
            Self::Unified => "unified",
        };
        f.write_str(label)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{redact_postgres_uri, RuntimePlan, ServiceMode};
    use crate::replication::postgres::PostgresLsn;
    use std::str::FromStr;

    fn plan(vars: &[(&str, &str)]) -> RuntimePlan {
        let lookup = vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        RuntimePlan::from_lookup(|name| lookup.get(name).cloned()).expect("runtime plan")
    }

    #[test]
    fn postgres_uri_redaction_covers_userinfo_and_query_passwords() {
        let redacted = redact_postgres_uri(
            "postgres://replicator:userinfo-secret@db.example/app?sslmode=verify-full&password=query-secret",
        );

        assert!(!redacted.contains("userinfo-secret"));
        assert!(!redacted.contains("query-secret"));
        assert!(redacted.contains("replicator:***@"));
        assert!(redacted.contains("password=***"));
        assert_eq!(
            redact_postgres_uri("password=bare-secret host=db.example"),
            "<redacted-invalid-postgres-uri>"
        );
    }

    #[test]
    fn defaults_to_api_only_without_replication_config() {
        let plan = plan(&[]);
        assert_eq!(plan.service_mode, ServiceMode::ApiOnly);
        assert_eq!(plan.replication, None);
    }

    #[test]
    fn unified_mode_requires_and_loads_replication_config() {
        let plan = plan(&[
            ("POWERSYNC_RUST_SERVICE_MODE", "unified"),
            (
                "POWERSYNC_RUST_POSTGRES_REPLICATION_URI",
                "postgres://postgres:postgres@localhost/db",
            ),
            ("POWERSYNC_RUST_REPLICATION_SLOT", "slot_rust"),
            ("POWERSYNC_RUST_REPLICATION_PUBLICATION", "pub_rust"),
            ("POWERSYNC_RUST_GROUP_ID", "tenant-rust"),
        ]);

        assert_eq!(plan.service_mode, ServiceMode::Unified);
        let replication = plan.replication.expect("replication config");
        assert_eq!(replication.slot_name, "slot_rust");
        assert_eq!(replication.publication_name, "pub_rust");
        assert_eq!(replication.group_id, "tenant-rust");
    }

    #[test]
    fn explicit_replication_enable_supports_alias_envs() {
        let plan = plan(&[
            ("POWERSYNC_RUST_REPLICATION_ENABLED", "true"),
            (
                "POWERSYNC_POSTGRES_REPLICATION_URI",
                "postgres://postgres:postgres@localhost/db",
            ),
            ("POWERSYNC_POSTGRES_REPLICATION_SLOT", "slot_alias"),
            ("POWERSYNC_POSTGRES_REPLICATION_PUBLICATION", "pub_alias"),
        ]);

        let replication = plan.replication.expect("replication config");
        assert_eq!(replication.slot_name, "slot_alias");
        assert_eq!(replication.publication_name, "pub_alias");
        assert_eq!(replication.group_id, "default");
    }

    #[test]
    fn partial_replication_config_uses_powersync_defaults() {
        let lookup = HashMap::from([
            (
                "POWERSYNC_RUST_REPLICATION_ENABLED".to_owned(),
                "1".to_owned(),
            ),
            (
                "POWERSYNC_RUST_POSTGRES_REPLICATION_URI".to_owned(),
                "postgres://postgres:postgres@localhost/db".to_owned(),
            ),
        ]);

        let plan = RuntimePlan::from_lookup(|name| lookup.get(name).cloned()).expect("plan");
        let replication = plan.replication.expect("replication config");
        assert_eq!(replication.slot_name, "powersync_mdbx");
        assert_eq!(replication.publication_name, "powersync");
    }

    #[test]
    fn invalid_service_mode_is_rejected() {
        let lookup = HashMap::from([(
            "POWERSYNC_RUST_SERVICE_MODE".to_owned(),
            "mystery".to_owned(),
        )]);
        let error =
            RuntimePlan::from_lookup(|name| lookup.get(name).cloned()).expect_err("invalid mode");
        assert!(error
            .to_string()
            .contains("unsupported POWERSYNC_RUST_SERVICE_MODE value: mystery"));
    }

    #[test]
    fn api_only_bootstrap_is_supported() {
        let plan = plan(&[]);
        assert!(plan.api_boot_required());
        assert!(!plan.replication_boot_requested());
        plan.ensure_supported_bootstrap()
            .expect("api-only bootstrap");
    }

    #[test]
    fn unified_bootstrap_is_supported_once_replication_config_exists() {
        let plan = plan(&[
            ("POWERSYNC_RUST_SERVICE_MODE", "unified"),
            (
                "POWERSYNC_RUST_POSTGRES_REPLICATION_URI",
                "postgres://postgres:postgres@localhost/db",
            ),
            ("POWERSYNC_RUST_REPLICATION_SLOT", "slot_rust"),
            ("POWERSYNC_RUST_REPLICATION_PUBLICATION", "pub_rust"),
        ]);

        plan.ensure_supported_bootstrap()
            .expect("supported bootstrap");
    }

    #[test]
    fn replication_config_builds_pgoutput_queries() {
        let plan = plan(&[
            ("POWERSYNC_RUST_REPLICATION_ENABLED", "true"),
            (
                "POWERSYNC_RUST_POSTGRES_REPLICATION_URI",
                "postgres://postgres:postgres@localhost/db",
            ),
            ("POWERSYNC_RUST_REPLICATION_SLOT", "slot_rust"),
            ("POWERSYNC_RUST_REPLICATION_PUBLICATION", "pub_rust"),
        ]);

        let replication = plan.replication.expect("replication config");
        assert_eq!(
            replication.create_slot_query(false),
            "CREATE_REPLICATION_SLOT slot_rust LOGICAL pgoutput NOEXPORT_SNAPSHOT"
        );
        assert_eq!(
            replication
                .start_replication_query(PostgresLsn::from_str("0/16B6AF0").expect("lsn"))
                .expect("query"),
            "START_REPLICATION SLOT slot_rust LOGICAL 0/16B6AF0 (proto_version '1', publication_names 'pub_rust')"
        );
    }
}
