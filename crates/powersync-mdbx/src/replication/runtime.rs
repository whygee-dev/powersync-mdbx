use std::{collections::BTreeSet, env, path::PathBuf, str::FromStr, time::Duration};

use pgwire_replication::{lsn::Lsn, PgWireError, ReplicationClient, ReplicationConfig};
use tokio::task::JoinHandle;
use tokio_postgres::config::Host;
use tokio_postgres::{Client, NoTls};

use super::{postgres::PostgresLsn, PostgresReplicationConfig};
use crate::postgres_tls::{ParsedPostgresConnection, PostgresTlsPolicy};

const REPLICATION_STATUS_INTERVAL_MS_ENV: &str = "POWERSYNC_RUST_REPLICATION_STATUS_INTERVAL_MS";
const REPLICATION_IDLE_WAKEUP_INTERVAL_MS_ENV: &str =
    "POWERSYNC_RUST_REPLICATION_IDLE_WAKEUP_INTERVAL_MS";
// Product default, intentionally lower than pgwire-replication's idle wakeup
// default so a persisted idle-end commit is acknowledged upstream promptly.
const DEFAULT_REPLICATION_STATUS_INTERVAL_MS: u64 = 1_000;
const DEFAULT_REPLICATION_IDLE_WAKEUP_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Clone)]
pub struct ReplicationBootstrap {
    pub control_plane: tokio_postgres::Config,
    pub replication_plane: ReplicationConfig,
    tls_policy: PostgresTlsPolicy,
}

impl ReplicationBootstrap {
    pub async fn prepare(
        config: &PostgresReplicationConfig,
    ) -> Result<Self, ReplicationRuntimeError> {
        let bootstrap = Self::from_config(config, PostgresLsn(0))
            .map_err(ReplicationRuntimeError::Bootstrap)?;
        let control_plane = bootstrap.connect_control_plane().await?;

        bootstrap.ensure_publication(&control_plane.client).await?;
        bootstrap.ensure_slot(&control_plane.client).await?;
        let start_lsn = bootstrap.detect_start_lsn(&control_plane.client).await?;
        control_plane.shutdown().await?;

        Self::from_config(config, start_lsn).map_err(ReplicationRuntimeError::Bootstrap)
    }

    pub async fn prepare_existing(
        config: &PostgresReplicationConfig,
        durable_lsn: PostgresLsn,
    ) -> Result<Self, ReplicationRuntimeError> {
        let bootstrap =
            Self::from_config(config, durable_lsn).map_err(ReplicationRuntimeError::Bootstrap)?;
        let control_plane = bootstrap.connect_control_plane().await?;
        bootstrap.ensure_publication(&control_plane.client).await?;
        bootstrap
            .validate_existing_slot(&control_plane.client, durable_lsn)
            .await?;
        control_plane.shutdown().await?;
        Ok(bootstrap)
    }

    pub fn from_config(
        config: &PostgresReplicationConfig,
        start_lsn: PostgresLsn,
    ) -> Result<Self, ReplicationBootstrapError> {
        Self::from_config_with_feedback_lookup(config, start_lsn, |name| env::var(name).ok())
    }

    fn from_config_with_feedback_lookup<F>(
        config: &PostgresReplicationConfig,
        start_lsn: PostgresLsn,
        lookup: F,
    ) -> Result<Self, ReplicationBootstrapError>
    where
        F: Fn(&str) -> Option<String> + Copy,
    {
        let parsed = ParsedPostgresConnection::parse(&config.uri)
            .map_err(ReplicationBootstrapError::InvalidTlsPolicy)?;
        let control_plane = parsed.config.clone();
        let connection = PostgresConnectionInfo::from_tokio_config(&control_plane)?;

        let replication_plane = match connection.host {
            PostgresHost::Tcp(host) => ReplicationConfig::new(
                host,
                connection.user,
                connection.password,
                connection.database,
                config.slot_name.clone(),
                config.publication_name.clone(),
            )
            .with_port(connection.port)
            .with_start_lsn(Lsn(start_lsn.to_u64())),
            PostgresHost::Unix(socket_dir) => ReplicationConfig::unix(
                socket_dir.to_string_lossy().to_string(),
                connection.port,
                connection.user,
                connection.password,
                connection.database,
                config.slot_name.clone(),
                config.publication_name.clone(),
            )
            .with_start_lsn(Lsn(start_lsn.to_u64())),
        };
        let replication_plane = configure_replication_feedback_from_lookup(
            replication_plane.with_tls(parsed.replication_tls()),
            lookup,
        )?;

        Ok(Self {
            control_plane,
            replication_plane,
            tls_policy: parsed.tls,
        })
    }

    pub fn with_start_lsn(&self, start_lsn: PostgresLsn) -> Self {
        let mut replication_plane = self.replication_plane.clone();
        replication_plane.start_lsn = Lsn(start_lsn.to_u64());

        Self {
            control_plane: self.control_plane.clone(),
            replication_plane,
            tls_policy: self.tls_policy.clone(),
        }
    }

    pub async fn connect_control_plane(
        &self,
    ) -> Result<ControlPlaneConnection, ReplicationRuntimeError> {
        let (client, task) = match &self.tls_policy {
            PostgresTlsPolicy::Disabled => {
                let (client, connection) = self
                    .control_plane
                    .connect(NoTls)
                    .await
                    .map_err(ReplicationRuntimeError::ControlPlaneConnect)?;
                (client, tokio::spawn(connection))
            }
            PostgresTlsPolicy::VerifyFull { .. } => {
                let parsed = ParsedPostgresConnection {
                    config: self.control_plane.clone(),
                    tls: self.tls_policy.clone(),
                };
                let connector = parsed
                    .rustls_connector()
                    .map_err(ReplicationRuntimeError::TlsConfiguration)?;
                let (client, connection) = self
                    .control_plane
                    .connect(connector)
                    .await
                    .map_err(ReplicationRuntimeError::ControlPlaneConnect)?;
                (client, tokio::spawn(connection))
            }
        };

        Ok(ControlPlaneConnection { client, task })
    }

    pub async fn connect_replication(&self) -> Result<ReplicationClient, ReplicationRuntimeError> {
        ReplicationClient::connect(self.replication_plane.clone())
            .await
            .map_err(ReplicationRuntimeError::ReplicationConnect)
    }

    pub async fn detect_start_lsn(
        &self,
        client: &Client,
    ) -> Result<PostgresLsn, ReplicationRuntimeError> {
        let row = client
            .query_opt(slot_progress_query(), &[&self.replication_plane.slot])
            .await
            .map_err(ReplicationRuntimeError::ControlPlaneQuery)?;

        if let Some(row) = row {
            let confirmed: Option<String> = row.get(0);
            let restart: Option<String> = row.get(1);

            if let Some(lsn) = confirmed.or(restart) {
                return PostgresLsn::from_str(&lsn)
                    .map_err(ReplicationRuntimeError::InvalidStartLsn);
            }
        }

        let row = client
            .query_one(current_wal_lsn_query(), &[])
            .await
            .map_err(ReplicationRuntimeError::ControlPlaneQuery)?;
        let lsn: String = row.get(0);
        PostgresLsn::from_str(&lsn).map_err(ReplicationRuntimeError::InvalidStartLsn)
    }

    pub async fn ensure_publication(&self, client: &Client) -> Result<(), ReplicationRuntimeError> {
        ensure_publication(client, &self.replication_plane.publication).await
    }

    pub async fn ensure_publication_covers(
        &self,
        client: &Client,
        required_source_tables: &[&str],
    ) -> Result<(), ReplicationRuntimeError> {
        self.ensure_publication(client).await?;
        validate_publication_coverage(
            client,
            &self.replication_plane.publication,
            required_source_tables,
        )
        .await
    }

    pub async fn ensure_slot(&self, client: &Client) -> Result<(), ReplicationRuntimeError> {
        ensure_slot(client, &self.replication_plane.slot).await
    }

    async fn validate_existing_slot(
        &self,
        client: &Client,
        durable_lsn: PostgresLsn,
    ) -> Result<(), ReplicationRuntimeError> {
        let row = client
            .query_opt(
                "SELECT plugin, slot_type, database, active, confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.replication_plane.slot],
            )
            .await
            .map_err(ReplicationRuntimeError::ControlPlaneQuery)?
            .ok_or_else(|| {
                ReplicationRuntimeError::SlotContinuity(format!(
                    "replication slot {} is missing for MDBX durable LSN {durable_lsn}; refusing to create a new slot past persisted state",
                    self.replication_plane.slot
                ))
            })?;
        let plugin: Option<String> = row.get(0);
        let slot_type: String = row.get(1);
        let database: Option<String> = row.get(2);
        let active: bool = row.get(3);
        let confirmed: Option<String> = row.get(4);
        let restart: Option<String> = row.get(5);
        if plugin.as_deref() != Some("pgoutput")
            || slot_type != "logical"
            || database.as_deref() != Some(self.replication_plane.database.as_str())
        {
            return Err(ReplicationRuntimeError::SlotContinuity(format!(
                "replication slot {} does not match pgoutput/logical/database {}",
                self.replication_plane.slot, self.replication_plane.database
            )));
        }
        if active {
            return Err(ReplicationRuntimeError::SlotContinuity(format!(
                "replication slot {} is already active",
                self.replication_plane.slot
            )));
        }
        for (label, value) in [("confirmed_flush_lsn", confirmed), ("restart_lsn", restart)] {
            if let Some(value) = value {
                let slot_lsn = PostgresLsn::from_str(&value)
                    .map_err(ReplicationRuntimeError::InvalidStartLsn)?;
                if slot_lsn > durable_lsn {
                    return Err(ReplicationRuntimeError::SlotContinuity(format!(
                        "slot {label} {slot_lsn} is ahead of MDBX durable LSN {durable_lsn}"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn configure_replication_feedback_from_lookup<F>(
    config: ReplicationConfig,
    lookup: F,
) -> Result<ReplicationConfig, ReplicationBootstrapError>
where
    F: Fn(&str) -> Option<String> + Copy,
{
    Ok(config
        .with_status_interval(replication_feedback_duration_from_lookup(
            REPLICATION_STATUS_INTERVAL_MS_ENV,
            DEFAULT_REPLICATION_STATUS_INTERVAL_MS,
            lookup,
        )?)
        .with_wakeup_interval(replication_feedback_duration_from_lookup(
            REPLICATION_IDLE_WAKEUP_INTERVAL_MS_ENV,
            DEFAULT_REPLICATION_IDLE_WAKEUP_INTERVAL_MS,
            lookup,
        )?))
}

fn replication_feedback_duration_from_lookup<F>(
    name: &'static str,
    default_ms: u64,
    lookup: F,
) -> Result<Duration, ReplicationBootstrapError>
where
    F: Fn(&str) -> Option<String>,
{
    let Some(raw) = lookup(name)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(Duration::from_millis(default_ms));
    };

    let millis = raw
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(ReplicationBootstrapError::InvalidDurationEnv { name, value: raw })?;
    Ok(Duration::from_millis(millis))
}

pub struct ControlPlaneConnection {
    pub client: Client,
    pub task: JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl ControlPlaneConnection {
    pub async fn shutdown(self) -> Result<(), ReplicationRuntimeError> {
        let Self { client, task } = self;
        drop(client);
        task.await
            .map_err(ReplicationRuntimeError::ControlPlaneJoin)?
            .map_err(ReplicationRuntimeError::ControlPlaneConnection)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PostgresHost {
    Tcp(String),
    Unix(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PostgresConnectionInfo {
    host: PostgresHost,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl PostgresConnectionInfo {
    fn from_tokio_config(
        config: &tokio_postgres::Config,
    ) -> Result<Self, ReplicationBootstrapError> {
        let hosts = config.get_hosts();
        let host = match hosts {
            [] => return Err(ReplicationBootstrapError::MissingHost),
            [Host::Tcp(host)] => PostgresHost::Tcp(host.clone()),
            [Host::Unix(path)] => PostgresHost::Unix(path.clone()),
            _ => return Err(ReplicationBootstrapError::MultipleHostsUnsupported),
        };

        let port = config.get_ports().first().copied().unwrap_or(5432);
        let user = config
            .get_user()
            .ok_or(ReplicationBootstrapError::MissingUser)?
            .to_owned();
        let password = String::from_utf8(
            config
                .get_password()
                .ok_or(ReplicationBootstrapError::MissingPassword)?
                .to_vec(),
        )
        .map_err(|_| ReplicationBootstrapError::NonUtf8Password)?;
        let database = config
            .get_dbname()
            .ok_or(ReplicationBootstrapError::MissingDatabase)?
            .to_owned();

        Ok(Self {
            host,
            port,
            user,
            password,
            database,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationBootstrapError {
    #[error("invalid PostgreSQL connection URI: {0}")]
    InvalidConnectionUri(tokio_postgres::Error),
    #[error("invalid PostgreSQL TLS policy: {0}")]
    InvalidTlsPolicy(String),
    #[error("PostgreSQL connection URI must include a host or unix socket path")]
    MissingHost,
    #[error("multiple PostgreSQL hosts are not supported for the replication bootstrap")]
    MultipleHostsUnsupported,
    #[error("PostgreSQL connection URI must include a user")]
    MissingUser,
    #[error("PostgreSQL connection URI must include a password")]
    MissingPassword,
    #[error("PostgreSQL connection URI must include a database name")]
    MissingDatabase,
    #[error("PostgreSQL connection password must be valid UTF-8")]
    NonUtf8Password,
    #[error("invalid positive millisecond duration for {name}: {value}")]
    InvalidDurationEnv { name: &'static str, value: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationRuntimeError {
    #[error("failed to prepare replication bootstrap: {0}")]
    Bootstrap(ReplicationBootstrapError),
    #[error("invalid PostgreSQL TLS configuration: {0}")]
    TlsConfiguration(String),
    #[error("failed to connect control plane: {0}")]
    ControlPlaneConnect(tokio_postgres::Error),
    #[error("control-plane query failed: {0}")]
    ControlPlaneQuery(tokio_postgres::Error),
    #[error("control-plane simple query failed: {0}")]
    ControlPlaneSimpleQuery(tokio_postgres::Error),
    #[error("control-plane connection task failed: {0}")]
    ControlPlaneConnection(tokio_postgres::Error),
    #[error("control-plane task join failed: {0}")]
    ControlPlaneJoin(tokio::task::JoinError),
    #[error("failed to connect replication stream: {0}")]
    ReplicationConnect(PgWireError),
    #[error("replication stream failed: {0}")]
    ReplicationReceive(PgWireError),
    #[error("replication shutdown failed: {0}")]
    ReplicationShutdown(PgWireError),
    #[error("invalid start LSN returned by control plane: {0}")]
    InvalidStartLsn(super::postgres::PostgresLsnParseError),
    #[error("invalid PostgreSQL identifier for replication bootstrap: {0:?}")]
    InvalidIdentifier(String),
    #[error("invalid unsigned integer value for POWERSYNC_RUST_REPLICATION_MAX_EVENTS: {0}")]
    InvalidMaxEvents(String),
    #[error("replication slot {slot_name:?} already exists with unsupported shape (plugin={plugin:?}, slot_type={slot_type})")]
    UnsupportedExistingSlot {
        slot_name: String,
        plugin: Option<String>,
        slot_type: String,
    },
    #[error("unsafe replication slot continuity: {0}")]
    SlotContinuity(String),
    #[error("unsafe PostgreSQL publication coverage: {0}")]
    PublicationCoverage(String),
    #[error(
        "PostgreSQL 13 or newer is required for publication validation; server_version_num={0}"
    )]
    UnsupportedPostgresVersion(i32),
}

pub fn slot_progress_query() -> &'static str {
    "SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1"
}

pub fn current_wal_lsn_query() -> &'static str {
    "SELECT pg_current_wal_lsn()::text"
}

pub fn publication_exists_query() -> &'static str {
    "SELECT 1 FROM pg_publication WHERE pubname = $1"
}

pub fn postgres_server_version_query() -> &'static str {
    "SELECT current_setting('server_version_num')::integer"
}

pub fn publication_coverage_query(
    server_version_num: i32,
) -> Result<&'static str, ReplicationRuntimeError> {
    if server_version_num < 130_000 {
        return Err(ReplicationRuntimeError::UnsupportedPostgresVersion(
            server_version_num,
        ));
    }
    Ok(
        "SELECT p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, \
                p.pubviaroot, pt.schemaname, pt.tablename \
         FROM pg_publication AS p \
         LEFT JOIN pg_publication_tables AS pt \
           ON NOT p.puballtables AND pt.pubname = p.pubname \
         WHERE p.pubname = $1",
    )
}

pub fn publication_table_semantics_query(
    server_version_num: i32,
) -> Result<&'static str, ReplicationRuntimeError> {
    if server_version_num < 130_000 {
        return Err(ReplicationRuntimeError::UnsupportedPostgresVersion(
            server_version_num,
        ));
    }
    if server_version_num < 150_000 {
        return Ok("SELECT EXISTS (SELECT 1 FROM pg_attribute AS a \
                            WHERE a.attrelid = c.oid AND a.attnum > 0 \
                              AND NOT a.attisdropped AND a.attgenerated <> ''), \
                    FALSE, \
                    c.relkind = 'p' OR EXISTS (SELECT 1 FROM pg_inherits AS i WHERE i.inhparent = c.oid) \
             FROM pg_publication AS p \
             JOIN pg_namespace AS ns ON ns.nspname = $2 \
             JOIN pg_class AS c ON c.relnamespace = ns.oid AND c.relname = $3 \
             WHERE p.pubname = $1");
    }
    Ok("SELECT EXISTS (SELECT 1 FROM pg_attribute AS a \
                        WHERE a.attrelid = c.oid AND a.attnum > 0 \
                          AND NOT a.attisdropped \
                          AND NOT (a.attname = ANY(pt.attnames))), \
                pt.rowfilter IS NOT NULL, \
                c.relkind = 'p' OR EXISTS (SELECT 1 FROM pg_inherits AS i WHERE i.inhparent = c.oid) \
         FROM pg_publication AS p \
         JOIN pg_namespace AS ns ON ns.nspname = $2 \
         JOIN pg_class AS c ON c.relnamespace = ns.oid AND c.relname = $3 \
         LEFT JOIN pg_publication_tables AS pt \
           ON pt.pubname = p.pubname AND pt.schemaname = $2 AND pt.tablename = $3 \
         WHERE p.pubname = $1")
}

pub fn create_publication_query(publication_name: &str) -> Result<String, ReplicationRuntimeError> {
    Ok(format!(
        "CREATE PUBLICATION {} FOR ALL TABLES",
        quote_identifier(publication_name)?
    ))
}

pub fn quote_identifier(identifier: &str) -> Result<String, ReplicationRuntimeError> {
    let trimmed = identifier.trim();
    if trimmed.is_empty() || trimmed.bytes().any(|byte| byte == 0) {
        return Err(ReplicationRuntimeError::InvalidIdentifier(
            identifier.to_owned(),
        ));
    }

    Ok(format!("\"{}\"", trimmed.replace('"', "\"\"")))
}

pub fn slot_state_query() -> &'static str {
    "SELECT plugin, slot_type FROM pg_replication_slots WHERE slot_name = $1"
}

pub fn create_logical_replication_slot_query() -> &'static str {
    "SELECT 1 FROM pg_create_logical_replication_slot($1, 'pgoutput')"
}

async fn ensure_publication(
    client: &Client,
    publication_name: &str,
) -> Result<(), ReplicationRuntimeError> {
    let publication_exists = client
        .query_opt(publication_exists_query(), &[&publication_name])
        .await
        .map_err(ReplicationRuntimeError::ControlPlaneQuery)?
        .is_some();

    if !publication_exists {
        let statement = create_publication_query(publication_name)?;
        client
            .simple_query(&statement)
            .await
            .map_err(ReplicationRuntimeError::ControlPlaneSimpleQuery)?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicationCoverage {
    all_tables: bool,
    publishes_insert: bool,
    publishes_update: bool,
    publishes_delete: bool,
    publishes_truncate: bool,
    publishes_via_partition_root: bool,
    tables: BTreeSet<(String, String)>,
    tables_with_omitted_columns: BTreeSet<(String, String)>,
    tables_with_row_filters: BTreeSet<(String, String)>,
    parent_tables: BTreeSet<(String, String)>,
}

async fn validate_publication_coverage(
    client: &Client,
    publication_name: &str,
    required_source_tables: &[&str],
) -> Result<(), ReplicationRuntimeError> {
    let server_version_num: i32 = client
        .query_one(postgres_server_version_query(), &[])
        .await
        .map_err(ReplicationRuntimeError::ControlPlaneQuery)?
        .get(0);
    let coverage_query = publication_coverage_query(server_version_num)?;
    let rows = client
        .query(coverage_query, &[&publication_name])
        .await
        .map_err(ReplicationRuntimeError::ControlPlaneQuery)?;
    let Some(first) = rows.first() else {
        return Err(ReplicationRuntimeError::PublicationCoverage(format!(
            "publication {publication_name:?} does not exist after bootstrap"
        )));
    };

    let mut coverage = PublicationCoverage {
        all_tables: first.get(0),
        publishes_insert: first.get(1),
        publishes_update: first.get(2),
        publishes_delete: first.get(3),
        publishes_truncate: first.get(4),
        publishes_via_partition_root: first.get(5),
        tables: BTreeSet::new(),
        tables_with_omitted_columns: BTreeSet::new(),
        tables_with_row_filters: BTreeSet::new(),
        parent_tables: BTreeSet::new(),
    };
    for row in rows {
        let schema = row.get::<_, Option<String>>(6);
        let table = row.get::<_, Option<String>>(7);
        if let (Some(schema), Some(table)) = (schema, table) {
            coverage.tables.insert((schema, table));
        }
    }

    let semantics_query = publication_table_semantics_query(server_version_num)?;
    for source_table in required_source_tables {
        let (schema, table) = split_source_table(source_table);
        let table_identity = (schema.to_owned(), table.to_owned());
        if !coverage.all_tables && !coverage.tables.contains(&table_identity) {
            continue;
        }
        let semantics = client
            .query_opt(semantics_query, &[&publication_name, &schema, &table])
            .await
            .map_err(ReplicationRuntimeError::ControlPlaneQuery)?;
        let Some(semantics) = semantics else {
            continue;
        };
        if semantics.get::<_, bool>(0) {
            coverage
                .tables_with_omitted_columns
                .insert(table_identity.clone());
        }
        if semantics.get::<_, bool>(1) {
            coverage
                .tables_with_row_filters
                .insert(table_identity.clone());
        }
        if semantics.get::<_, bool>(2) {
            coverage.parent_tables.insert(table_identity);
        }
    }

    validate_publication_description(publication_name, required_source_tables, &coverage)
}

fn validate_publication_description(
    publication_name: &str,
    required_source_tables: &[&str],
    coverage: &PublicationCoverage,
) -> Result<(), ReplicationRuntimeError> {
    let mut missing_operations = Vec::new();
    if !coverage.publishes_insert {
        missing_operations.push("insert");
    }
    if !coverage.publishes_update {
        missing_operations.push("update");
    }
    if !coverage.publishes_delete {
        missing_operations.push("delete");
    }
    // TRUNCATE is unsupported, but it must reach the decoder so the service
    // fails closed instead of silently retaining rows removed at the source.
    if !coverage.publishes_truncate {
        missing_operations.push("truncate");
    }

    let required_tables = required_source_tables
        .iter()
        .map(|source_table| {
            let (schema, table) = split_source_table(source_table);
            (schema.to_owned(), table.to_owned())
        })
        .collect::<BTreeSet<_>>();
    let missing_tables = if coverage.all_tables {
        Vec::new()
    } else {
        required_tables
            .difference(&coverage.tables)
            .map(|(schema, table)| format!("{schema}.{table}"))
            .collect::<Vec<_>>()
    };
    let restricted_required_tables = |tables: &BTreeSet<(String, String)>| {
        required_tables
            .intersection(tables)
            .map(|(schema, table)| format!("{schema}.{table}"))
            .collect::<Vec<_>>()
    };
    let tables_with_omitted_columns =
        restricted_required_tables(&coverage.tables_with_omitted_columns);
    let tables_with_row_filters = restricted_required_tables(&coverage.tables_with_row_filters);
    let parent_tables = restricted_required_tables(&coverage.parent_tables);

    if missing_operations.is_empty()
        && missing_tables.is_empty()
        && tables_with_omitted_columns.is_empty()
        && tables_with_row_filters.is_empty()
        && parent_tables.is_empty()
        && !coverage.publishes_via_partition_root
    {
        return Ok(());
    }

    let mut problems = Vec::new();
    if !missing_operations.is_empty() {
        problems.push(format!(
            "does not publish required operations: {}",
            missing_operations.join(", ")
        ));
    }
    if !missing_tables.is_empty() {
        problems.push(format!(
            "is missing required source tables: {}",
            missing_tables.join(", ")
        ));
    }
    if !tables_with_omitted_columns.is_empty() {
        problems.push(format!(
            "omits columns from required source tables: {}",
            tables_with_omitted_columns.join(", ")
        ));
    }
    if !tables_with_row_filters.is_empty() {
        problems.push(format!(
            "uses row filters for required source tables: {}",
            tables_with_row_filters.join(", ")
        ));
    }
    if !parent_tables.is_empty() {
        problems.push(format!(
            "uses partition or inheritance parents as required source tables: {}",
            parent_tables.join(", ")
        ));
    }
    if coverage.publishes_via_partition_root {
        problems.push("enables publish_via_partition_root".to_owned());
    }
    Err(ReplicationRuntimeError::PublicationCoverage(format!(
        "publication {publication_name:?} {}",
        problems.join("; ")
    )))
}

fn split_source_table(source_table: &str) -> (&str, &str) {
    source_table
        .split_once('.')
        .unwrap_or(("public", source_table))
}

async fn ensure_slot(client: &Client, slot_name: &str) -> Result<(), ReplicationRuntimeError> {
    let slot_state = client
        .query_opt(slot_state_query(), &[&slot_name])
        .await
        .map_err(ReplicationRuntimeError::ControlPlaneQuery)?;

    if let Some(row) = slot_state {
        let plugin: Option<String> = row.get(0);
        let slot_type: String = row.get(1);

        if plugin.as_deref() != Some("pgoutput") || slot_type != "logical" {
            return Err(ReplicationRuntimeError::UnsupportedExistingSlot {
                slot_name: slot_name.to_owned(),
                plugin,
                slot_type,
            });
        }

        return Ok(());
    }

    client
        .query_one(create_logical_replication_slot_query(), &[&slot_name])
        .await
        .map_err(ReplicationRuntimeError::ControlPlaneQuery)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use pgwire_replication::ReplicationConfig;

    use super::{
        configure_replication_feedback_from_lookup, create_logical_replication_slot_query,
        create_publication_query, current_wal_lsn_query, postgres_server_version_query,
        publication_coverage_query, publication_exists_query, publication_table_semantics_query,
        quote_identifier, slot_progress_query, slot_state_query, validate_publication_description,
        PublicationCoverage, ReplicationBootstrap, ReplicationBootstrapError,
        ReplicationRuntimeError, REPLICATION_IDLE_WAKEUP_INTERVAL_MS_ENV,
        REPLICATION_STATUS_INTERVAL_MS_ENV,
    };
    use crate::replication::{postgres::PostgresLsn, PostgresReplicationConfig};

    #[test]
    fn builds_tcp_control_and_replication_configs_from_uri() {
        let config = PostgresReplicationConfig {
            uri: "postgres://replicator:secret@db.example.com:5544/powersync?sslmode=disable"
                .to_owned(),
            slot_name: "slot_rust".to_owned(),
            publication_name: "pub_rust".to_owned(),
            group_id: "default".to_owned(),
        };

        let bootstrap =
            ReplicationBootstrap::from_config(&config, PostgresLsn(42)).expect("bootstrap");

        assert_eq!(bootstrap.control_plane.get_hosts().len(), 1);
        assert_eq!(bootstrap.control_plane.get_ports(), &[5544]);
        assert_eq!(bootstrap.control_plane.get_user(), Some("replicator"));
        assert_eq!(bootstrap.control_plane.get_dbname(), Some("powersync"));
        assert_eq!(bootstrap.replication_plane.host, "db.example.com");
        assert_eq!(bootstrap.replication_plane.port, 5544);
        assert_eq!(bootstrap.replication_plane.user, "replicator");
        assert_eq!(bootstrap.replication_plane.password, "secret");
        assert_eq!(bootstrap.replication_plane.database, "powersync");
        assert_eq!(bootstrap.replication_plane.slot, "slot_rust");
        assert_eq!(bootstrap.replication_plane.publication, "pub_rust");
        assert_eq!(bootstrap.replication_plane.start_lsn.0, 42);
    }

    #[test]
    fn builds_unix_socket_replication_config_from_uri() {
        let config = PostgresReplicationConfig {
            uri: "postgresql://replicator:secret@%2Fvar%2Frun%2Fpostgresql/powersync".to_owned(),
            slot_name: "slot_rust".to_owned(),
            publication_name: "pub_rust".to_owned(),
            group_id: "default".to_owned(),
        };

        let bootstrap =
            ReplicationBootstrap::from_config(&config, PostgresLsn(0)).expect("bootstrap");
        assert!(bootstrap.replication_plane.is_unix_socket());
        assert_eq!(bootstrap.replication_plane.database, "powersync");
        assert_eq!(bootstrap.replication_plane.slot, "slot_rust");
    }

    #[test]
    fn rejects_uri_without_password() {
        let config = PostgresReplicationConfig {
            uri: "postgres://replicator@localhost/powersync?sslmode=disable".to_owned(),
            slot_name: "slot_rust".to_owned(),
            publication_name: "pub_rust".to_owned(),
            group_id: "default".to_owned(),
        };

        let error = ReplicationBootstrap::from_config(&config, PostgresLsn(0))
            .expect_err("missing password");
        assert!(matches!(error, ReplicationBootstrapError::MissingPassword));
    }

    #[test]
    fn replication_feedback_uses_product_default_one_second_intervals() {
        let config =
            configure_replication_feedback_from_lookup(base_replication_config(), |_| None)
                .expect("feedback config");

        assert_eq!(config.status_interval, Duration::from_millis(1_000));
        assert_eq!(config.idle_wakeup_interval, Duration::from_millis(1_000));
    }

    #[test]
    fn replication_feedback_uses_positive_millisecond_env_overrides() {
        let config = configure_replication_feedback_from_lookup(
            base_replication_config(),
            |name| match name {
                REPLICATION_STATUS_INTERVAL_MS_ENV => Some("250".to_owned()),
                REPLICATION_IDLE_WAKEUP_INTERVAL_MS_ENV => Some(" 500 ".to_owned()),
                _ => None,
            },
        )
        .expect("feedback config");

        assert_eq!(config.status_interval, Duration::from_millis(250));
        assert_eq!(config.idle_wakeup_interval, Duration::from_millis(500));
    }

    #[test]
    fn replication_bootstrap_from_config_applies_feedback_lookup() {
        let config = PostgresReplicationConfig {
            uri: "postgres://replicator:secret@db.example.com:5544/powersync?sslmode=disable"
                .to_owned(),
            slot_name: "slot_rust".to_owned(),
            publication_name: "pub_rust".to_owned(),
            group_id: "default".to_owned(),
        };

        let bootstrap = ReplicationBootstrap::from_config_with_feedback_lookup(
            &config,
            PostgresLsn(42),
            |name| match name {
                REPLICATION_STATUS_INTERVAL_MS_ENV => Some("125".to_owned()),
                REPLICATION_IDLE_WAKEUP_INTERVAL_MS_ENV => Some("250".to_owned()),
                _ => None,
            },
        )
        .expect("bootstrap");

        assert_eq!(bootstrap.replication_plane.start_lsn.0, 42);
        assert_eq!(
            bootstrap.replication_plane.status_interval,
            Duration::from_millis(125)
        );
        assert_eq!(
            bootstrap.replication_plane.idle_wakeup_interval,
            Duration::from_millis(250)
        );
    }

    #[test]
    fn replication_feedback_allows_one_mixed_env_override() {
        let config = configure_replication_feedback_from_lookup(
            base_replication_config(),
            |name| match name {
                REPLICATION_STATUS_INTERVAL_MS_ENV => Some("250".to_owned()),
                REPLICATION_IDLE_WAKEUP_INTERVAL_MS_ENV => Some(" ".to_owned()),
                _ => None,
            },
        )
        .expect("feedback config");

        assert_eq!(config.status_interval, Duration::from_millis(250));
        assert_eq!(config.idle_wakeup_interval, Duration::from_millis(1_000));
    }

    #[test]
    fn replication_feedback_rejects_invalid_duration_env() {
        let error = configure_replication_feedback_from_lookup(base_replication_config(), |name| {
            (name == REPLICATION_STATUS_INTERVAL_MS_ENV).then(|| "0".to_owned())
        })
        .expect_err("invalid feedback config");

        assert!(matches!(
            error,
            ReplicationBootstrapError::InvalidDurationEnv {
                name: REPLICATION_STATUS_INTERVAL_MS_ENV,
                ..
            }
        ));
    }

    #[test]
    fn slot_progress_query_matches_expected_projection() {
        assert_eq!(
            slot_progress_query(),
            "SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1"
        );
    }

    #[test]
    fn current_wal_lsn_query_matches_expected_projection() {
        assert_eq!(current_wal_lsn_query(), "SELECT pg_current_wal_lsn()::text");
    }

    #[test]
    fn publication_exists_query_matches_expected_projection() {
        assert_eq!(
            publication_exists_query(),
            "SELECT 1 FROM pg_publication WHERE pubname = $1"
        );
    }

    #[test]
    fn publication_coverage_query_avoids_expanding_all_tables_publications() {
        let query = publication_coverage_query(150_000).expect("PostgreSQL 15 query");
        assert!(query.contains("ON NOT p.puballtables"));
        assert!(query.contains("pt.schemaname, pt.tablename"));
        assert!(query.contains("p.pubviaroot"));
    }

    #[test]
    fn publication_coverage_query_supports_pre_filter_postgres_versions() {
        assert_eq!(
            postgres_server_version_query(),
            "SELECT current_setting('server_version_num')::integer"
        );
        assert!(matches!(
            publication_coverage_query(120_000),
            Err(ReplicationRuntimeError::UnsupportedPostgresVersion(120_000))
        ));
    }

    #[test]
    fn publication_table_semantics_queries_are_version_compatible() {
        let postgres_14 = publication_table_semantics_query(140_000).expect("PostgreSQL 14");
        assert!(postgres_14.contains("a.attgenerated <> ''"));
        assert!(postgres_14.contains("FALSE"));
        assert!(!postgres_14.contains("pt.attnames"));

        let postgres_15 = publication_table_semantics_query(150_000).expect("PostgreSQL 15");
        assert!(postgres_15.contains("a.attname = ANY(pt.attnames)"));
        assert!(postgres_15.contains("pt.rowfilter IS NOT NULL"));
        assert!(postgres_15.contains("i.inhparent = c.oid"));

        let postgres_18 = publication_table_semantics_query(180_000).expect("PostgreSQL 18");
        assert_eq!(postgres_18, postgres_15);
    }

    #[test]
    fn publication_coverage_accepts_all_tables_with_required_operations() {
        validate_publication_description(
            "pub_rust",
            &["tasks", "private.memberships"],
            &publication_coverage(true, true, true, true, true, &[]),
        )
        .expect("all-tables publication should cover every source table");
    }

    #[test]
    fn publication_coverage_accepts_explicit_required_tables() {
        validate_publication_description(
            "pub_rust",
            &["tasks", "private.memberships"],
            &publication_coverage(
                false,
                true,
                true,
                true,
                true,
                &[
                    ("public", "tasks"),
                    ("private", "memberships"),
                    ("public", "extra"),
                ],
            ),
        )
        .expect("explicit publication should cover all required source tables");
    }

    #[test]
    fn publication_coverage_rejects_missing_source_tables_deterministically() {
        let error = validate_publication_description(
            "pub_rust",
            &["tasks", "private.memberships", "public.comments"],
            &publication_coverage(false, true, true, true, true, &[("public", "tasks")]),
        )
        .expect_err("partial publication must fail closed");

        assert!(matches!(
            error,
            ReplicationRuntimeError::PublicationCoverage(_)
        ));
        assert_eq!(
            error.to_string(),
            "unsafe PostgreSQL publication coverage: publication \"pub_rust\" is missing required source tables: private.memberships, public.comments"
        );
    }

    #[test]
    fn publication_coverage_rejects_disabled_change_operations() {
        let error = validate_publication_description(
            "pub_rust",
            &["tasks"],
            &publication_coverage(true, true, false, false, false, &[]),
        )
        .expect_err("publication without update/delete must fail closed");

        assert_eq!(
            error.to_string(),
            "unsafe PostgreSQL publication coverage: publication \"pub_rust\" does not publish required operations: update, delete, truncate"
        );
    }

    #[test]
    fn publication_coverage_rejects_omitted_columns_filters_and_partition_root_mapping() {
        let mut coverage = publication_coverage(
            false,
            true,
            true,
            true,
            true,
            &[("public", "tasks"), ("private", "memberships")],
        );
        coverage
            .tables_with_omitted_columns
            .insert(("public".to_owned(), "tasks".to_owned()));
        coverage
            .tables_with_row_filters
            .insert(("private".to_owned(), "memberships".to_owned()));
        coverage.publishes_via_partition_root = true;

        let error = validate_publication_description(
            "pub_rust",
            &["tasks", "private.memberships"],
            &coverage,
        )
        .expect_err("publication transformations must fail closed");

        assert_eq!(
            error.to_string(),
            "unsafe PostgreSQL publication coverage: publication \"pub_rust\" omits columns from required source tables: public.tasks; uses row filters for required source tables: private.memberships; enables publish_via_partition_root"
        );
    }

    #[test]
    fn publication_coverage_ignores_restrictions_on_unreferenced_tables() {
        let mut coverage = publication_coverage(
            false,
            true,
            true,
            true,
            true,
            &[("public", "tasks"), ("public", "audit_log")],
        );
        coverage
            .tables_with_row_filters
            .insert(("public".to_owned(), "audit_log".to_owned()));

        validate_publication_description("pub_rust", &["tasks"], &coverage)
            .expect("unreferenced publication entries do not affect the active plan");
    }

    #[test]
    fn publication_coverage_rejects_omitted_generated_columns() {
        let mut coverage =
            publication_coverage(false, true, true, true, true, &[("public", "tasks")]);
        coverage
            .tables_with_omitted_columns
            .insert(("public".to_owned(), "tasks".to_owned()));

        let error = validate_publication_description("pub_rust", &["tasks"], &coverage)
            .expect_err("unpublished generated columns must fail closed");
        assert_eq!(
            error.to_string(),
            "unsafe PostgreSQL publication coverage: publication \"pub_rust\" omits columns from required source tables: public.tasks"
        );
    }

    #[test]
    fn publication_coverage_rejects_partition_and_inheritance_parents() {
        let mut coverage =
            publication_coverage(false, true, true, true, true, &[("public", "tasks")]);
        coverage
            .parent_tables
            .insert(("public".to_owned(), "tasks".to_owned()));

        let error = validate_publication_description("pub_rust", &["tasks"], &coverage)
            .expect_err("parent-table relation identities must fail closed");
        assert_eq!(
            error.to_string(),
            "unsafe PostgreSQL publication coverage: publication \"pub_rust\" uses partition or inheritance parents as required source tables: public.tasks"
        );
    }

    #[test]
    fn create_publication_query_quotes_identifier() {
        assert_eq!(
            create_publication_query("pub\"rust").expect("valid identifier"),
            "CREATE PUBLICATION \"pub\"\"rust\" FOR ALL TABLES"
        );
    }

    #[test]
    fn cloned_bootstrap_can_override_start_lsn() {
        let config = PostgresReplicationConfig {
            uri: "postgres://replicator:secret@db.example.com:5544/powersync?sslmode=disable"
                .to_owned(),
            slot_name: "slot_rust".to_owned(),
            publication_name: "pub_rust".to_owned(),
            group_id: "default".to_owned(),
        };

        let bootstrap =
            ReplicationBootstrap::from_config(&config, PostgresLsn(42)).expect("bootstrap");
        let updated = bootstrap.with_start_lsn(PostgresLsn(84));

        assert_eq!(bootstrap.replication_plane.start_lsn.0, 42);
        assert_eq!(updated.replication_plane.start_lsn.0, 84);
        assert_eq!(updated.replication_plane.slot, "slot_rust");
    }

    #[test]
    fn slot_state_query_matches_expected_projection() {
        assert_eq!(
            slot_state_query(),
            "SELECT plugin, slot_type FROM pg_replication_slots WHERE slot_name = $1"
        );
    }

    #[test]
    fn create_logical_replication_slot_query_matches_expected_projection() {
        assert_eq!(
            create_logical_replication_slot_query(),
            "SELECT 1 FROM pg_create_logical_replication_slot($1, 'pgoutput')"
        );
    }

    #[test]
    fn quote_identifier_rejects_blank_values() {
        let error = quote_identifier("   ").expect_err("blank identifier");
        assert_eq!(
            error.to_string(),
            "invalid PostgreSQL identifier for replication bootstrap: \"   \""
        );
    }

    fn base_replication_config() -> ReplicationConfig {
        ReplicationConfig::new(
            "db.example.com",
            "replicator",
            "secret",
            "powersync",
            "slot_rust",
            "pub_rust",
        )
    }

    fn publication_coverage(
        all_tables: bool,
        publishes_insert: bool,
        publishes_update: bool,
        publishes_delete: bool,
        publishes_truncate: bool,
        tables: &[(&str, &str)],
    ) -> PublicationCoverage {
        PublicationCoverage {
            all_tables,
            publishes_insert,
            publishes_update,
            publishes_delete,
            publishes_truncate,
            publishes_via_partition_root: false,
            tables: tables
                .iter()
                .map(|(schema, table)| ((*schema).to_owned(), (*table).to_owned()))
                .collect(),
            tables_with_omitted_columns: BTreeSet::new(),
            tables_with_row_filters: BTreeSet::new(),
            parent_tables: BTreeSet::new(),
        }
    }
}
