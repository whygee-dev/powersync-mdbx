use std::{
    collections::BTreeMap,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, StatusCode};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    auth::{ApiAuthConfig, AuthFailure, TokenPayload, UserAuthConfig},
    config::load_config_from_env,
    replication::{ingest::ReplicationMdbxStore, redact_postgres_uri},
    sync_rules::{
        canonical_storage_contract_id, compile_sync_rules_source, default_bucket_requests,
        load_runtime_sync_rules_source, lower_canonical_semantic_plan, CanonicalBinding,
        CanonicalStream, CompiledTablePlan, ResolvedSyncBucket, RustExecutionPlan, SyncRuleError,
    },
};

mod debug;
mod parameters;

pub use debug::{debug_sync_rules, SyncRulesDebugInfo};
pub use parameters::ResolvedParameterContext;

// Service-owned data lives under the working directory by default: a
// world-writable /tmp default would let any local user pre-create or replace
// state the service trusts at boot.
const DEFAULT_SYNC_RULES_STATE_PATH: &str = "./data/powersync-mdbx-sync-rules-state.json";
const SYNC_RULES_HISTORY_LIMIT: usize = 64;

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SyncRulesLifecycleStatus {
    Pending,
    Activating,
    #[default]
    Active,
    Failed,
    Retired,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct PersistedActivationMetadata {
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub layout_change: bool,
    #[serde(default)]
    pub storage_contract_id: String,
    #[serde(default)]
    pub activated_at_ms: u64,
    #[serde(default)]
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncRulesMutationOptions {
    #[serde(default)]
    pub base_version: Option<u64>,
    #[serde(default)]
    pub intent_token: Option<String>,
}

#[derive(Clone)]
pub struct ServiceContext {
    api_auth: ApiAuthConfig,
    user_auth: Option<UserAuthConfig>,
    allow_anonymous_sync: bool,
    source_connections: Vec<SourceConnection>,
    query_capability_enabled: bool,
    sync_rules_state: Arc<SyncRulesState>,
}

impl ServiceContext {
    pub fn from_env() -> Result<Self, String> {
        Self::with_state_path(sync_rules_state_path())
    }

    pub fn with_state_path(path: PathBuf) -> Result<Self, String> {
        let current_content = load_runtime_sync_rules_source()?;
        let sync_rules_state = Arc::new(SyncRulesState::load(path, current_content)?);
        let config = load_config_from_env()?;
        let user_auth =
            UserAuthConfig::from_env_with_config(config.as_ref().map(|loaded| loaded.config()))?;
        let allow_anonymous_sync = env_flag("POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC");
        if user_auth.is_none() {
            if allow_anonymous_sync {
                tracing::warn!(
                    "no user JWT key configured and POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC=1: \
                     /sync/stream will serve all bucket data without authentication"
                );
            } else {
                tracing::warn!(
                    "no user JWT key configured: /sync/stream will reject all requests; \
                     configure POWERSYNC_RUST_JWKS_JSON/POWERSYNC_RUST_JWKS_URL or set \
                     POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC=1 to opt into anonymous access"
                );
            }
        }
        let source_connections = parse_source_connections_from_env_with_config(
            config.as_ref().map(|loaded| loaded.config()),
        );
        let query_capability_enabled = env_flag("POWERSYNC_RUST_ENABLE_QUERY_CAPABILITY");
        Ok(Self {
            api_auth: ApiAuthConfig::from_env_with_config(
                config.as_ref().map(|loaded| loaded.config()),
            ),
            user_auth,
            allow_anonymous_sync,
            source_connections,
            query_capability_enabled,
            sync_rules_state,
        })
    }

    #[doc(hidden)] // test-support: public only for integration tests
    pub fn new_for_tests(
        path: PathBuf,
        api_tokens: Vec<String>,
        user_auth: Option<UserAuthConfig>,
        source_connections: Vec<SourceConnection>,
    ) -> Result<Self, String> {
        Self::new_for_tests_with_query_capability(
            path,
            api_tokens,
            user_auth,
            source_connections,
            false,
        )
    }

    #[doc(hidden)] // test-support: public only for integration tests
    pub fn new_for_tests_with_query_capability(
        path: PathBuf,
        api_tokens: Vec<String>,
        user_auth: Option<UserAuthConfig>,
        source_connections: Vec<SourceConnection>,
        query_capability_enabled: bool,
    ) -> Result<Self, String> {
        let current_content = load_runtime_sync_rules_source()?;
        let sync_rules_state = Arc::new(SyncRulesState::load(path, current_content)?);
        Ok(Self {
            api_auth: ApiAuthConfig::new(api_tokens),
            user_auth,
            allow_anonymous_sync: false,
            source_connections,
            query_capability_enabled,
            sync_rules_state,
        })
    }

    /// Test/benchmark escape hatch mirroring POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC.
    pub fn with_allow_anonymous_sync(mut self, allow: bool) -> Self {
        self.allow_anonymous_sync = allow;
        self
    }

    pub(crate) fn allows_anonymous_sync(&self) -> bool {
        self.allow_anonymous_sync
    }

    pub fn authorize_api(&self, headers: &HeaderMap) -> Result<(), AuthFailure> {
        self.api_auth.authorize_headers(headers)
    }

    pub fn authorize_user(&self, headers: &HeaderMap) -> Result<Option<TokenPayload>, AuthFailure> {
        match &self.user_auth {
            Some(config) => config.authorize_headers(headers).map(Some),
            // Fail closed: serving bucket data without any configured key is
            // an explicit opt-in, never a configuration accident.
            None if self.allow_anonymous_sync => Ok(None),
            None => Err(AuthFailure::disabled()),
        }
    }

    /// Diagnostics are open when the admin API is disabled (local dev/tests)
    /// and require an API token once one is configured.
    pub fn authorize_diagnostics(&self, headers: &HeaderMap) -> Result<(), AuthFailure> {
        if self.api_auth.is_configured() {
            self.api_auth.authorize_headers(headers)
        } else {
            Ok(())
        }
    }

    pub fn sync_rules_state(&self) -> &Arc<SyncRulesState> {
        &self.sync_rules_state
    }

    pub fn active_plan(&self) -> Arc<RustExecutionPlan> {
        self.sync_rules_state.active_plan()
    }

    pub fn default_bucket_requests(&self) -> Vec<ResolvedSyncBucket> {
        self.active_plan().default_bucket_requests()
    }

    pub fn stream(&self, name: &str) -> Option<CanonicalStream> {
        self.active_plan().stream(name).cloned()
    }

    pub fn resolve_bucket_request(&self, name: &str) -> Option<ResolvedSyncBucket> {
        self.active_plan().resolve_bucket_request(name)
    }

    pub fn table_plan(&self, source_table: &str) -> Option<CompiledTablePlan> {
        self.active_plan().table_plan(source_table).cloned()
    }

    pub fn storage_contract_id(&self) -> String {
        canonical_storage_contract_id(self.active_plan().canonical())
    }

    pub fn diagnostics_payload(&self, include_content: bool) -> Value {
        let snapshot = self.sync_rules_state.snapshot();
        json!({
            "connections": self.connection_payloads(),
            "active_sync_rules": self.sync_rules_status_payload(&snapshot.current, include_content, true),
            "deploying_sync_rules": snapshot.next.as_ref().map(|entry| self.sync_rules_status_payload(entry, include_content, false)).unwrap_or_else(|| json!({"errors": [], "connections": []})),
            "history": snapshot.history.iter().map(|entry| self.sync_rules_debug_payload(entry)).collect::<Vec<_>>(),
            "lifecycle": self.sync_rules_lifecycle_payload(&snapshot),
        })
    }

    pub fn schema_payload(&self) -> Value {
        let snapshot = self.sync_rules_state.snapshot();
        let info = debug_sync_rules(&snapshot.current.content);
        let tables = info
            .as_ref()
            .map(|details| details.schema_tables())
            .unwrap_or_default();
        json!({
            "connections": self.source_connections.iter().map(|connection| json!({
                "id": connection.id,
                "tag": connection.tag,
                "schemas": [{
                    "name": "public",
                    "tables": tables,
                }]
            })).collect::<Vec<_>>(),
            "defaultConnectionTag": self.source_connections.first().map(|connection| connection.tag.clone()).unwrap_or_else(|| "postgresql".to_owned()),
            "defaultSchema": "public"
        })
    }

    pub fn sync_rules_current_payload(&self) -> Value {
        let snapshot = self.sync_rules_state.snapshot();
        json!({
            "data": {
                "current": self.sync_rules_debug_payload(&snapshot.current),
                "next": snapshot.next.as_ref().map(|entry| self.sync_rules_debug_payload(entry)),
                "history": snapshot.history.iter().map(|entry| self.sync_rules_debug_payload(entry)).collect::<Vec<_>>(),
                "lifecycle": self.sync_rules_lifecycle_payload(&snapshot),
            }
        })
    }

    pub fn sync_rules_validate_payload(&self, content: &str) -> Value {
        match debug_sync_rules(content) {
            Ok(info) => info.as_validate_payload(),
            Err(error) => json!({
                "valid": false,
                "errors": [error.to_string()],
                "bucket_definitions": [],
                "source_tables": [],
                "data_tables": {}
            }),
        }
    }

    pub fn admin_validate_payload(&self, content: &str) -> Value {
        match debug_sync_rules(content) {
            Ok(info) => json!({
                "connections": self.source_connections.iter().map(|connection| json!({
                    "id": connection.id,
                    "tag": connection.tag,
                    "slot_name": "",
                    "initial_replication_done": false,
                    "tables": info.source_tables,
                })).collect::<Vec<_>>(),
                "errors": [],
            }),
            Err(error) => json!({
                "connections": [],
                "errors": [{"level": "fatal", "message": error.to_string()}]
            }),
        }
    }

    pub fn deploy_sync_rules(
        &self,
        content: &str,
        options: SyncRulesMutationOptions,
    ) -> Result<Value, ControlPlaneError> {
        let previous = self.sync_rules_state.snapshot().current;
        let outcome = self.sync_rules_state.deploy(content, &options)?;
        self.handle_mutation_outcome("deploy", previous, outcome)
    }

    pub fn reprocess_sync_rules(
        &self,
        _options: SyncRulesMutationOptions,
    ) -> Result<Value, ControlPlaneError> {
        Err(ControlPlaneError::conflict(
            "Sync-rule reprocessing is disabled until the service can build and atomically activate a complete storage generation",
        ))
    }

    pub fn admin_reprocess(
        &self,
        options: SyncRulesMutationOptions,
    ) -> Result<Value, ControlPlaneError> {
        self.reprocess_sync_rules(options)
    }

    pub fn execute_sql_out_of_scope_payload(&self) -> Value {
        json!({
            "success": false,
            "out_of_scope": true,
            "prototype_scope": "excluded_from_powersync_mdbx_scope",
            "query_capability_enabled_ignored": self.query_capability_enabled,
            "results": {"columns": [], "rows": []},
            "error": "execute-sql is out of scope for powersync-mdbx"
        })
    }

    fn handle_mutation_outcome(
        &self,
        operation: &str,
        previous: PersistedSyncRulesEntry,
        outcome: SyncRulesMutationOutcome,
    ) -> Result<Value, ControlPlaneError> {
        match outcome {
            SyncRulesMutationOutcome::Pending(entry) => {
                self.activate_pending_sync_rules(operation, previous, entry)
            }
            SyncRulesMutationOutcome::AlreadyApplied(current) => {
                Ok(self.mutation_payload(operation, previous, current, false, true))
            }
        }
    }

    fn activate_pending_sync_rules(
        &self,
        operation: &str,
        previous: PersistedSyncRulesEntry,
        pending: PersistedSyncRulesEntry,
    ) -> Result<Value, ControlPlaneError> {
        let pending_plan = self
            .sync_rules_state
            .pending_plan()
            .ok_or_else(|| ControlPlaneError::conflict("No pending sync-rules candidate exists"))?;
        let current_contract_id = self.storage_contract_id();
        let pending_contract_id = canonical_storage_contract_id(pending_plan.canonical());
        let layout_change = pending_contract_id != current_contract_id;
        self.sync_rules_state.mark_activating(
            pending.version,
            operation,
            layout_change,
            pending_contract_id.clone(),
        )?;
        if layout_change {
            let reason = "Layout-changing sync-rule activation is disabled until the service can build and atomically activate a complete storage generation";
            self.sync_rules_state.mark_failed(
                pending.version,
                reason.to_owned(),
                true,
                pending_contract_id,
            )?;
            return Err(ControlPlaneError::conflict(reason));
        }
        let ingest_store = ReplicationMdbxStore::shared_from_env()
            .map_err(|error| ControlPlaneError::internal(error.to_string()))?;
        if let Err(error) = ingest_store.reset_for_layout_version(&pending_contract_id) {
            let reason = error.to_string();
            let _ = self.sync_rules_state.mark_failed(
                pending.version,
                reason.clone(),
                layout_change,
                pending_contract_id,
            );
            return Err(ControlPlaneError::internal(reason));
        }
        let current = match self.sync_rules_state.activate_pending(
            pending.version,
            layout_change,
            pending_contract_id.clone(),
        ) {
            Ok(current) => current,
            Err(error) => {
                let reason = error
                    .body()
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("failed to promote pending sync rules")
                    .to_owned();
                let _ = self.sync_rules_state.mark_failed(
                    pending.version,
                    reason,
                    layout_change,
                    pending_contract_id,
                );
                return Err(error);
            }
        };
        Ok(self.mutation_payload(operation, previous, current, true, false))
    }

    fn mutation_payload(
        &self,
        operation: &str,
        previous: PersistedSyncRulesEntry,
        current: PersistedSyncRulesEntry,
        activated: bool,
        idempotent: bool,
    ) -> Value {
        let snapshot = self.sync_rules_state.snapshot();
        let activation = current.activation.clone().unwrap_or_default();
        json!({
            "data": {
                "operation": operation,
                "activated": activated,
                "idempotent": idempotent,
                "plan_version": current.version,
                "base_version": current.base_version,
                "intent_hash": current.intent_hash,
                "layout_change": activation.layout_change,
                "failure_reason": activation.failure_reason,
                "previous": self.sync_rules_debug_payload(&previous),
                "current": self.sync_rules_debug_payload(&current),
                "pending": snapshot.next.as_ref().map(|entry| self.sync_rules_debug_payload(entry)),
                "history": snapshot.history.iter().map(|entry| self.sync_rules_debug_payload(entry)).collect::<Vec<_>>(),
                "lifecycle": self.sync_rules_lifecycle_payload(&snapshot),
            }
        })
    }

    fn connection_payloads(&self) -> Vec<Value> {
        if self.source_connections.is_empty() {
            return Vec::new();
        }
        self.source_connections
            .iter()
            .map(|connection| {
                json!({
                    "id": connection.id,
                    "tag": connection.tag,
                    "postgres_uri": redact_postgres_uri(&connection.uri),
                    "connected": false,
                    "errors": [],
                })
            })
            .collect()
    }

    fn sync_rules_lifecycle_payload(&self, snapshot: &SyncRulesStateSnapshot) -> Value {
        json!({
            "allowed_states": ["pending", "activating", "active", "failed", "retired"],
            "current_status": snapshot.current.status,
            "current_version": snapshot.current.version,
            "pending_version": snapshot.next.as_ref().map(|entry| entry.version),
            "history": snapshot.history.iter().map(sync_rules_lifecycle_event_payload).collect::<Vec<_>>(),
        })
    }

    fn sync_rules_status_payload(
        &self,
        entry: &PersistedSyncRulesEntry,
        include_content: bool,
        initial_replication_done: bool,
    ) -> Value {
        let activation = entry.activation.clone().unwrap_or_default();
        match debug_sync_rules(&entry.content) {
            Ok(info) => json!({
                "version": entry.version,
                "status": entry.status,
                "base_version": entry.base_version,
                "intent_hash": entry.intent_hash,
                "layout_change": activation.layout_change,
                "failure_reason": activation.failure_reason,
                "content": include_content.then_some(entry.content.clone()),
                "connections": self.source_connections.iter().map(|connection| json!({
                    "id": connection.id,
                    "tag": connection.tag,
                    "slot_name": entry.slot_name.clone().unwrap_or_default(),
                    "initial_replication_done": initial_replication_done,
                    "tables": info.source_tables,
                })).collect::<Vec<_>>(),
                "errors": [],
            }),
            Err(error) => json!({
                "version": entry.version,
                "status": entry.status,
                "base_version": entry.base_version,
                "intent_hash": entry.intent_hash,
                "layout_change": activation.layout_change,
                "failure_reason": activation.failure_reason,
                "content": include_content.then_some(entry.content.clone()),
                "connections": [],
                "errors": [{"level": "fatal", "message": error.to_string()}],
            }),
        }
    }

    fn sync_rules_debug_payload(&self, entry: &PersistedSyncRulesEntry) -> Value {
        let activation = entry.activation.clone().unwrap_or_default();
        match debug_sync_rules(&entry.content) {
            Ok(info) => json!({
                "version": entry.version,
                "status": entry.status,
                "base_version": entry.base_version,
                "intent_hash": entry.intent_hash,
                "created_at_ms": entry.created_at_ms,
                "updated_at_ms": entry.updated_at_ms,
                "activation": activation,
                "slot_name": entry.slot_name,
                "content": entry.content,
                "valid": true,
                "bucket_definitions": info.bucket_definitions,
                "source_tables": info.source_table_patterns,
                "data_tables": info.data_tables,
            }),
            Err(error) => json!({
                "version": entry.version,
                "status": entry.status,
                "base_version": entry.base_version,
                "intent_hash": entry.intent_hash,
                "created_at_ms": entry.created_at_ms,
                "updated_at_ms": entry.updated_at_ms,
                "activation": activation,
                "slot_name": entry.slot_name,
                "content": entry.content,
                "valid": false,
                "bucket_definitions": [],
                "source_tables": [],
                "data_tables": {},
                "errors": [error.to_string()],
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncRulesStateSnapshot {
    pub current: PersistedSyncRulesEntry,
    pub next: Option<PersistedSyncRulesEntry>,
    pub history: Vec<PersistedSyncRulesEntry>,
}

#[derive(Debug, Clone)]
enum SyncRulesMutationOutcome {
    Pending(PersistedSyncRulesEntry),
    AlreadyApplied(PersistedSyncRulesEntry),
}

#[derive(Debug)]
pub struct SyncRulesState {
    path: PathBuf,
    inner: Mutex<RuntimeSyncRulesState>,
}

impl SyncRulesState {
    fn load(path: PathBuf, current_content: String) -> Result<Self, String> {
        let persisted = if path.exists() {
            let raw = fs::read_to_string(&path).map_err(|error| {
                format!(
                    "failed to read sync-rules state {}: {error}",
                    path.display()
                )
            })?;
            serde_json::from_str::<PersistedSyncRulesState>(&raw).map_err(|error| {
                format!(
                    "failed to decode sync-rules state {}: {error}",
                    path.display()
                )
            })?
        } else {
            PersistedSyncRulesState {
                current: initial_sync_rules_entry(current_content),
                next: None,
                history: Vec::new(),
                revision: 0,
            }
        };
        Ok(Self {
            path,
            inner: Mutex::new(RuntimeSyncRulesState::from_persisted(persisted)?),
        })
    }

    pub fn snapshot(&self) -> SyncRulesStateSnapshot {
        let guard = self
            .inner
            .lock()
            .expect("sync rules state mutex should not be poisoned");
        SyncRulesStateSnapshot {
            current: guard.persisted.current.clone(),
            next: guard.persisted.next.clone(),
            history: guard.persisted.history.clone(),
        }
    }

    pub fn active_plan(&self) -> Arc<RustExecutionPlan> {
        self.inner
            .lock()
            .expect("sync rules state mutex should not be poisoned")
            .current_plan
            .clone()
    }

    pub fn pending_plan(&self) -> Option<Arc<RustExecutionPlan>> {
        self.inner
            .lock()
            .expect("sync rules state mutex should not be poisoned")
            .next_plan
            .clone()
    }

    fn deploy(
        &self,
        content: &str,
        options: &SyncRulesMutationOptions,
    ) -> Result<SyncRulesMutationOutcome, ControlPlaneError> {
        let next_plan =
            compile_execution_plan(content).map_err(ControlPlaneError::invalid_sync_rules)?;
        self.stage_candidate(content.to_owned(), next_plan, "deploy", options)
    }

    pub fn mark_activating(
        &self,
        version: u64,
        operation: &str,
        layout_change: bool,
        storage_contract_id: String,
    ) -> Result<PersistedSyncRulesEntry, ControlPlaneError> {
        let mut guard = self
            .inner
            .lock()
            .expect("sync rules state mutex should not be poisoned");
        let next =
            guard.persisted.next.as_mut().ok_or_else(|| {
                ControlPlaneError::conflict("No pending sync-rules candidate exists")
            })?;
        if next.version != version {
            return Err(ControlPlaneError::conflict_details(
                "Pending sync-rules candidate version mismatch",
                SyncRulesConflictDetails {
                    operation,
                    plan_version: Some(version),
                    base_version: next.base_version,
                    blocking: Some(next),
                    intent_hash: next.intent_hash.clone(),
                    layout_change: Some(layout_change),
                    failure_reason: None,
                },
            ));
        }
        next.status = SyncRulesLifecycleStatus::Activating;
        next.updated_at_ms = now_epoch_ms();
        next.activation = Some(PersistedActivationMetadata {
            operation: operation.to_owned(),
            layout_change,
            storage_contract_id,
            activated_at_ms: 0,
            failure_reason: None,
        });
        let entry = next.clone();
        push_history(&mut guard.persisted.history, entry.clone());
        self.persist(&guard.persisted)?;
        Ok(entry)
    }

    pub fn activate_pending(
        &self,
        version: u64,
        layout_change: bool,
        storage_contract_id: String,
    ) -> Result<PersistedSyncRulesEntry, ControlPlaneError> {
        let mut guard = self
            .inner
            .lock()
            .expect("sync rules state mutex should not be poisoned");
        let next =
            guard.persisted.next.take().ok_or_else(|| {
                ControlPlaneError::conflict("No pending sync-rules candidate exists")
            })?;
        if next.version != version {
            return Err(ControlPlaneError::conflict_details(
                "Pending sync-rules candidate version mismatch",
                SyncRulesConflictDetails {
                    operation: "activate",
                    plan_version: Some(version),
                    base_version: next.base_version,
                    blocking: Some(&next),
                    intent_hash: next.intent_hash.clone(),
                    layout_change: Some(layout_change),
                    failure_reason: None,
                },
            ));
        }
        let next_plan = guard.next_plan.take().ok_or_else(|| {
            ControlPlaneError::internal(
                "pending sync-rules plan is missing compiled state".to_owned(),
            )
        })?;
        let mut retired = guard.persisted.current.clone();
        retired.status = SyncRulesLifecycleStatus::Retired;
        retired.updated_at_ms = now_epoch_ms();
        push_history(&mut guard.persisted.history, retired);

        let mut current = next.clone();
        current.status = SyncRulesLifecycleStatus::Active;
        current.updated_at_ms = now_epoch_ms();
        current.activation = Some(PersistedActivationMetadata {
            operation: current
                .activation
                .as_ref()
                .map(|metadata| metadata.operation.clone())
                .unwrap_or_else(|| "activate".to_owned()),
            layout_change,
            storage_contract_id,
            activated_at_ms: now_epoch_ms(),
            failure_reason: None,
        });
        guard.persisted.current = current.clone();
        guard.current_plan = next_plan;
        push_history(&mut guard.persisted.history, current.clone());
        self.persist(&guard.persisted)?;
        Ok(current)
    }

    pub fn mark_failed(
        &self,
        version: u64,
        failure_reason: String,
        layout_change: bool,
        storage_contract_id: String,
    ) -> Result<PersistedSyncRulesEntry, ControlPlaneError> {
        let mut guard = self
            .inner
            .lock()
            .expect("sync rules state mutex should not be poisoned");
        let mut next =
            guard.persisted.next.take().ok_or_else(|| {
                ControlPlaneError::conflict("No pending sync-rules candidate exists")
            })?;
        if next.version != version {
            return Err(ControlPlaneError::conflict_details(
                "Pending sync-rules candidate version mismatch",
                SyncRulesConflictDetails {
                    operation: "activate",
                    plan_version: Some(version),
                    base_version: next.base_version,
                    blocking: Some(&next),
                    intent_hash: next.intent_hash.clone(),
                    layout_change: Some(layout_change),
                    failure_reason: Some(failure_reason),
                },
            ));
        }
        guard.next_plan.take();
        next.status = SyncRulesLifecycleStatus::Failed;
        next.updated_at_ms = now_epoch_ms();
        next.activation = Some(PersistedActivationMetadata {
            operation: next
                .activation
                .as_ref()
                .map(|metadata| metadata.operation.clone())
                .unwrap_or_else(|| "activate".to_owned()),
            layout_change,
            storage_contract_id,
            activated_at_ms: 0,
            failure_reason: Some(failure_reason),
        });
        push_history(&mut guard.persisted.history, next.clone());
        self.persist(&guard.persisted)?;
        Ok(next)
    }

    fn persist(&self, state: &PersistedSyncRulesState) -> Result<(), ControlPlaneError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(ControlPlaneError::io)?;
        }
        let encoded = serde_json::to_string_pretty(state).map_err(ControlPlaneError::serialize)?;
        let temp_path =
            self.path
                .with_extension(format!("tmp-{}-{}", std::process::id(), now_epoch_ms()));
        let write_result = (|| {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .map_err(ControlPlaneError::io)?;
            file.write_all(encoded.as_bytes())
                .map_err(ControlPlaneError::io)?;
            file.sync_all().map_err(ControlPlaneError::io)?;
            fs::rename(&temp_path, &self.path).map_err(ControlPlaneError::io)
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        write_result
    }

    fn stage_candidate(
        &self,
        content: String,
        next_plan: Arc<RustExecutionPlan>,
        operation: &str,
        options: &SyncRulesMutationOptions,
    ) -> Result<SyncRulesMutationOutcome, ControlPlaneError> {
        let mut guard = self
            .inner
            .lock()
            .expect("sync rules state mutex should not be poisoned");
        let requested_base_version = options
            .base_version
            .or(Some(guard.persisted.current.version));
        let intent_hash = compute_intent_hash(
            operation,
            requested_base_version,
            &content,
            options.intent_token.as_deref(),
        );
        let next_version = guard.persisted.revision + 1;

        if let Some(next) = guard.persisted.next.as_ref() {
            if next.intent_hash.as_deref() == Some(intent_hash.as_str())
                && next.base_version == requested_base_version
            {
                return Ok(SyncRulesMutationOutcome::Pending(next.clone()));
            }
            return Err(ControlPlaneError::conflict_details(
                format!("Busy processing sync rules - cannot {operation}"),
                SyncRulesConflictDetails {
                    operation,
                    plan_version: Some(next_version),
                    base_version: requested_base_version,
                    blocking: Some(next),
                    intent_hash: Some(intent_hash),
                    layout_change: next
                        .activation
                        .as_ref()
                        .map(|metadata| metadata.layout_change),
                    failure_reason: next
                        .activation
                        .as_ref()
                        .and_then(|metadata| metadata.failure_reason.clone()),
                },
            ));
        }

        if let Some(base_version) = requested_base_version {
            if guard.persisted.current.version != base_version {
                if guard.persisted.current.intent_hash.as_deref() == Some(intent_hash.as_str())
                    && guard.persisted.current.base_version == Some(base_version)
                {
                    return Ok(SyncRulesMutationOutcome::AlreadyApplied(
                        guard.persisted.current.clone(),
                    ));
                }
                return Err(ControlPlaneError::conflict_details(
                    "Stale base version for sync-rules mutation",
                    SyncRulesConflictDetails {
                        operation,
                        plan_version: Some(next_version),
                        base_version: Some(base_version),
                        blocking: Some(&guard.persisted.current),
                        intent_hash: Some(intent_hash),
                        layout_change: guard
                            .persisted
                            .current
                            .activation
                            .as_ref()
                            .map(|metadata| metadata.layout_change),
                        failure_reason: guard
                            .persisted
                            .current
                            .activation
                            .as_ref()
                            .and_then(|metadata| metadata.failure_reason.clone()),
                    },
                ));
            }
        }

        if guard.persisted.current.intent_hash.as_deref() == Some(intent_hash.as_str())
            && guard.persisted.current.base_version == requested_base_version
        {
            return Ok(SyncRulesMutationOutcome::AlreadyApplied(
                guard.persisted.current.clone(),
            ));
        }

        guard.persisted.revision = next_version;
        let now = now_epoch_ms();
        let entry = PersistedSyncRulesEntry {
            version: next_version,
            content,
            slot_name: Some(format!("powersync_mdbx_sync_rules_{next_version}")),
            status: SyncRulesLifecycleStatus::Pending,
            base_version: requested_base_version,
            intent_hash: Some(intent_hash),
            created_at_ms: now,
            updated_at_ms: now,
            activation: Some(PersistedActivationMetadata {
                operation: operation.to_owned(),
                ..Default::default()
            }),
        };
        guard.persisted.next = Some(entry.clone());
        guard.next_plan = Some(next_plan);
        push_history(&mut guard.persisted.history, entry.clone());
        self.persist(&guard.persisted)?;
        Ok(SyncRulesMutationOutcome::Pending(entry))
    }
}

#[derive(Debug)]
struct RuntimeSyncRulesState {
    persisted: PersistedSyncRulesState,
    current_plan: Arc<RustExecutionPlan>,
    next_plan: Option<Arc<RustExecutionPlan>>,
}

impl RuntimeSyncRulesState {
    fn from_persisted(persisted: PersistedSyncRulesState) -> Result<Self, String> {
        let persisted = normalize_persisted_state(persisted);
        let current_plan = compile_execution_plan(&persisted.current.content)
            .map_err(|error| error.to_string())?;
        let next_plan = persisted
            .next
            .as_ref()
            .map(|entry| compile_execution_plan(&entry.content).map_err(|error| error.to_string()))
            .transpose()?;
        Ok(Self {
            persisted,
            current_plan,
            next_plan,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSyncRulesState {
    current: PersistedSyncRulesEntry,
    next: Option<PersistedSyncRulesEntry>,
    #[serde(default)]
    history: Vec<PersistedSyncRulesEntry>,
    revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSyncRulesEntry {
    #[serde(default)]
    pub version: u64,
    pub content: String,
    pub slot_name: Option<String>,
    #[serde(default)]
    pub status: SyncRulesLifecycleStatus,
    #[serde(default)]
    pub base_version: Option<u64>,
    #[serde(default)]
    pub intent_hash: Option<String>,
    #[serde(default)]
    pub created_at_ms: u64,
    #[serde(default)]
    pub updated_at_ms: u64,
    #[serde(default)]
    pub activation: Option<PersistedActivationMetadata>,
}

fn compile_execution_plan(content: &str) -> Result<Arc<RustExecutionPlan>, SyncRuleError> {
    let canonical = compile_sync_rules_source(content)?;
    let plan = lower_canonical_semantic_plan(canonical)?;
    Ok(Arc::new(plan))
}

#[derive(Debug, Clone)]
pub struct SourceConnection {
    pub id: String,
    pub tag: String,
    pub uri: String,
}

#[derive(Debug)]
pub struct ControlPlaneError {
    status: StatusCode,
    body: Value,
}

struct SyncRulesConflictDetails<'a> {
    operation: &'a str,
    plan_version: Option<u64>,
    base_version: Option<u64>,
    blocking: Option<&'a PersistedSyncRulesEntry>,
    intent_hash: Option<String>,
    layout_change: Option<bool>,
    failure_reason: Option<String>,
}

impl ControlPlaneError {
    fn invalid_sync_rules(error: SyncRuleError) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: json!({"error": error.to_string()}),
        }
    }

    fn conflict(message: &str) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            body: json!({"error": message}),
        }
    }

    fn conflict_details(message: impl Into<String>, details: SyncRulesConflictDetails<'_>) -> Self {
        let message = message.into();
        Self {
            status: StatusCode::CONFLICT,
            body: json!({
                "error": message,
                "operation": details.operation,
                "plan_version": details.plan_version,
                "base_version": details.base_version,
                "blocking_version": details.blocking.map(|entry| entry.version),
                "blocking_state": details.blocking.map(|entry| entry.status.clone()),
                "intent_hash": details.intent_hash,
                "layout_change": details.layout_change,
                "failure_reason": details.failure_reason,
            }),
        }
    }

    fn internal(message: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: json!({"error": message}),
        }
    }

    fn io(error: std::io::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: json!({"error": error.to_string()}),
        }
    }

    fn serialize(error: serde_json::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: json!({"error": error.to_string()}),
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn body(&self) -> &Value {
        &self.body
    }
}

fn sync_rules_lifecycle_event_payload(entry: &PersistedSyncRulesEntry) -> Value {
    let activation = entry.activation.clone().unwrap_or_default();
    json!({
        "version": entry.version,
        "status": entry.status,
        "operation": activation.operation,
        "layout_change": activation.layout_change,
        "storage_contract_id": activation.storage_contract_id,
        "activated_at_ms": activation.activated_at_ms,
        "failure_reason": activation.failure_reason,
        "created_at_ms": entry.created_at_ms,
        "updated_at_ms": entry.updated_at_ms,
    })
}

fn initial_sync_rules_entry(content: String) -> PersistedSyncRulesEntry {
    let now = now_epoch_ms();
    PersistedSyncRulesEntry {
        version: 0,
        content,
        slot_name: None,
        status: SyncRulesLifecycleStatus::Active,
        base_version: None,
        intent_hash: None,
        created_at_ms: now,
        updated_at_ms: now,
        activation: Some(PersistedActivationMetadata {
            operation: "bootstrap".to_owned(),
            layout_change: false,
            storage_contract_id: String::new(),
            activated_at_ms: now,
            failure_reason: None,
        }),
    }
}

fn normalize_persisted_state(mut persisted: PersistedSyncRulesState) -> PersistedSyncRulesState {
    if persisted.current.updated_at_ms == 0 {
        persisted.current.updated_at_ms = persisted.current.created_at_ms;
    }
    if persisted.current.created_at_ms == 0 {
        persisted.current.created_at_ms = persisted.current.updated_at_ms;
    }
    if persisted.current.activation.is_none() {
        persisted.current.activation = Some(PersistedActivationMetadata {
            operation: "bootstrap".to_owned(),
            layout_change: false,
            storage_contract_id: String::new(),
            activated_at_ms: persisted.current.updated_at_ms,
            failure_reason: None,
        });
    }
    if persisted.current.status != SyncRulesLifecycleStatus::Active {
        persisted.current.status = SyncRulesLifecycleStatus::Active;
    }
    if let Some(next) = persisted.next.as_mut() {
        if next.status == SyncRulesLifecycleStatus::Active {
            next.status = SyncRulesLifecycleStatus::Pending;
        }
        if next.version == 0 {
            next.version = persisted.revision.max(persisted.current.version + 1);
        }
        if next.updated_at_ms == 0 {
            next.updated_at_ms = next.created_at_ms;
        }
        if next.created_at_ms == 0 {
            next.created_at_ms = next.updated_at_ms;
        }
    }
    if persisted.history.len() > SYNC_RULES_HISTORY_LIMIT {
        let drain = persisted.history.len() - SYNC_RULES_HISTORY_LIMIT;
        persisted.history.drain(0..drain);
    }
    persisted
}

fn push_history(history: &mut Vec<PersistedSyncRulesEntry>, entry: PersistedSyncRulesEntry) {
    history.push(entry);
    if history.len() > SYNC_RULES_HISTORY_LIMIT {
        let drain = history.len() - SYNC_RULES_HISTORY_LIMIT;
        history.drain(0..drain);
    }
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn compute_intent_hash(
    operation: &str,
    base_version: Option<u64>,
    content: &str,
    intent_token: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(operation.as_bytes());
    hasher.update(b":");
    hasher.update(base_version.unwrap_or_default().to_string().as_bytes());
    hasher.update(b":");
    if let Some(intent_token) = intent_token {
        hasher.update(intent_token.as_bytes());
    }
    hasher.update(b":");
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(super) fn binding_sql(binding: &CanonicalBinding) -> String {
    match binding {
        CanonicalBinding::AuthParameter { name } => format!("auth.parameter('{name}')"),
        CanonicalBinding::SubscriptionParameter { name } => {
            format!("subscription.parameter('{name}')")
        }
        CanonicalBinding::RequestUserId => "request.user_id()".to_owned(),
        CanonicalBinding::RequestJwt { claim } => format!("request.jwt() ->> '{claim}'"),
        CanonicalBinding::RequestParameter { name } => {
            format!("request.parameters() ->> '{name}'")
        }
        CanonicalBinding::RequestParameterArray { name } => {
            format!("json_each(request.parameters() ->> '{name}')")
        }
        CanonicalBinding::ParameterQueryColumn { name, lookup } => {
            format!(
                "parameter_query_column('{}','{}')",
                URL_SAFE_NO_PAD.encode(&lookup.raw_query),
                name.replace('\'', "''")
            )
        }
        CanonicalBinding::BucketParameter { name } => format!("bucket.{name}"),
    }
}

pub(super) fn flatten_json_map(
    prefix: &str,
    map: &serde_json::Map<String, Value>,
    out: &mut BTreeMap<String, String>,
) {
    for (key, value) in map {
        let next = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        flatten_json_value(&next, value, out);
    }
}

fn flatten_json_value(path: &str, value: &Value, out: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(map) => flatten_json_map(path, map, out),
        Value::Null => {}
        Value::String(text) => {
            out.insert(path.to_owned(), text.clone());
        }
        Value::Number(number) => {
            out.insert(path.to_owned(), number.to_string());
        }
        Value::Bool(boolean) => {
            out.insert(path.to_owned(), boolean.to_string());
        }
        Value::Array(array) => {
            out.insert(path.to_owned(), Value::Array(array.clone()).to_string());
        }
    }
}

fn parse_source_connections_from_env_with_config(
    config: Option<&crate::config::PowerSyncConfig>,
) -> Vec<SourceConnection> {
    let raw = env::var("POWERSYNC_RUST_SOURCE_CONNECTIONS_JSON")
        .ok()
        .or_else(|| env::var("POWERSYNC_RUST_SOURCE_CONNECTIONS").ok())
        .or_else(|| env::var("PS_DATA_SOURCE_URI").ok())
        .or_else(|| env::var("POWERSYNC_RUST_POSTGRES_REPLICATION_URI").ok())
        .or_else(|| env::var("POWERSYNC_POSTGRES_REPLICATION_URI").ok());
    let Some(raw) = raw else {
        return config
            .and_then(|config| config.replication.as_ref())
            .map(|replication| {
                replication
                    .connections
                    .iter()
                    .enumerate()
                    .map(|(index, connection)| SourceConnection {
                        id: connection
                            .id
                            .clone()
                            .unwrap_or_else(|| format!("db-{}", index + 1)),
                        tag: connection
                            .tag
                            .clone()
                            .or_else(|| connection.connection_type.clone())
                            .unwrap_or_else(|| "postgres".to_owned()),
                        uri: connection.uri.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
    };
    if raw.trim_start().starts_with('[') {
        serde_json::from_str::<Vec<Value>>(&raw)
            .unwrap_or_default()
            .into_iter()
            .filter_map(parse_source_connection)
            .collect()
    } else {
        raw.split(',')
            .enumerate()
            .filter_map(|(index, uri)| {
                let uri = uri.trim();
                if uri.is_empty() {
                    None
                } else {
                    Some(SourceConnection {
                        id: format!("db-{}", index + 1),
                        tag: "postgres".to_owned(),
                        uri: uri.to_owned(),
                    })
                }
            })
            .collect()
    }
}

fn parse_source_connection(value: Value) -> Option<SourceConnection> {
    let object = value.as_object()?;
    Some(SourceConnection {
        id: object
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("postgresql")
            .to_owned(),
        tag: object
            .get("tag")
            .and_then(Value::as_str)
            .unwrap_or("postgres")
            .to_owned(),
        uri: object.get("uri").and_then(Value::as_str)?.to_owned(),
    })
}

fn sync_rules_state_path() -> PathBuf {
    env::var("POWERSYNC_RUST_SYNC_RULES_STATE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_SYNC_RULES_STATE_PATH))
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

pub fn extract_string_map(
    map: &Option<serde_json::Map<String, Value>>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(map) = map {
        flatten_json_map("", map, &mut out);
    }
    out
}

#[doc(hidden)] // test-support: public only for integration tests
pub fn state_path_for_testing(path: impl AsRef<Path>) {
    unsafe {
        env::set_var("POWERSYNC_RUST_SYNC_RULES_STATE_PATH", path.as_ref());
    }
}

#[doc(hidden)] // test-support: public only for integration tests
pub fn clear_state_path_for_testing() {
    unsafe {
        env::remove_var("POWERSYNC_RUST_SYNC_RULES_STATE_PATH");
    }
}

pub fn default_stream_bucket_names() -> Vec<String> {
    default_bucket_requests()
        .into_iter()
        .map(|bucket| bucket.bucket_name().to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn request_parameter_context_prefers_auth_values_for_auth_parameter_binding() {
        let token = TokenPayload::new_for_tests(
            json!({"org_id": "org-auth", "sub": "user-1"}),
            Some("user-1".to_owned()),
        );
        let request_parameters = serde_json::Map::from_iter([(
            "org_id".to_owned(),
            Value::String("org-request".to_owned()),
        )]);
        let context = ResolvedParameterContext::from_request(Some(&token), &request_parameters);
        let subscription_parameters = BTreeMap::from([("org_id".to_owned(), "org-sub".to_owned())]);
        assert_eq!(
            context.binding_value(
                &CanonicalBinding::AuthParameter {
                    name: "org_id".to_owned(),
                },
                &subscription_parameters,
            ),
            Some("org-auth".to_owned())
        );
    }

    #[test]
    fn request_parameter_context_does_not_treat_client_parameters_as_auth_claims() {
        let token =
            TokenPayload::new_for_tests(json!({"sub": "user-1"}), Some("user-1".to_owned()));
        let request_parameters = serde_json::Map::from_iter([(
            "org_id".to_owned(),
            Value::String("org-request".to_owned()),
        )]);
        let context = ResolvedParameterContext::from_request(Some(&token), &request_parameters);
        let subscription_parameters = BTreeMap::from([("org_id".to_owned(), "org-sub".to_owned())]);

        assert_eq!(
            context.binding_value(
                &CanonicalBinding::AuthParameter {
                    name: "org_id".to_owned(),
                },
                &subscription_parameters,
            ),
            None
        );
        assert_eq!(
            context.binding_value(
                &CanonicalBinding::RequestParameter {
                    name: "org_id".to_owned(),
                },
                &subscription_parameters,
            ),
            Some("org-request".to_owned())
        );
        assert_eq!(
            context.binding_value(
                &CanonicalBinding::SubscriptionParameter {
                    name: "org_id".to_owned(),
                },
                &subscription_parameters,
            ),
            Some("org-sub".to_owned())
        );
    }

    #[test]
    fn sync_rules_state_persists_deployed_next_rules() {
        let temp = TempDir::new().expect("temp dir should exist");
        let path = temp.path().join("sync-rules-state.json");
        let state = SyncRulesState::load(
            path.clone(),
            load_runtime_sync_rules_source().expect("load runtime sync rules"),
        )
        .expect("state should load");
        let entry = state
            .deploy(
                "edition: 3\ncompatibility_version: 1\nstorage_version: 1\nstreams:\n  tasks:\n    auto_subscribe: true\n    query: SELECT * FROM public.tasks\n",
                &SyncRulesMutationOptions::default(),
            )
            .expect("deploy should succeed");
        let entry = match entry {
            SyncRulesMutationOutcome::Pending(entry)
            | SyncRulesMutationOutcome::AlreadyApplied(entry) => entry,
        };
        assert!(entry.slot_name.is_some());
        let reloaded = SyncRulesState::load(
            path,
            load_runtime_sync_rules_source().expect("load runtime sync rules"),
        )
        .expect("state should reload");
        assert!(reloaded.snapshot().next.is_some());
    }
}
