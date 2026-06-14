use std::{env, fs, sync::OnceLock};

use super::lowering::{compile_sync_rules_source, lower_canonical_semantic_plan};
use super::model::{
    CanonicalSemanticPlan, CanonicalStream, CompiledTablePlan, ResolvedSyncBucket,
    RustExecutionPlan, SyncBucketDescriptor, SyncRuleError,
};

pub const DEFAULT_TASKS_BUCKET_NAME: &str = "1#tasks|0[]";
pub const DEFAULT_TASKS_STREAM_NAME: &str = "tasks";
pub const TASKS_BY_PROJECT_STREAM_NAME: &str = "tasks_by_project";
pub const TASKS_BY_ORG_STREAM_NAME: &str = "tasks_by_org";
pub const PROJECTS_BY_ORG_STREAM_NAME: &str = "projects_by_org";
pub const PROJECTS_BY_OWNER_STREAM_NAME: &str = "projects_by_owner";
pub const COMMENTS_BY_TASK_STREAM_NAME: &str = "comments_by_task";
pub const COMMENTS_BY_ORG_STREAM_NAME: &str = "comments_by_org";
pub const MEMBERSHIPS_BY_ORG_STREAM_NAME: &str = "memberships_by_org";
pub const ORGANIZATIONS_BY_REGION_STREAM_NAME: &str = "organizations_by_region";

pub(super) const STREAM_BUCKET_PREFIX: &str = "1#";
pub(super) const CANONICAL_PLAN_VERSION: u32 = 1;
const RUST_INGEST_STORAGE_LAYOUT_VERSION: &str = "wire-mdbx-ingest-v3";
pub(super) const SUPPORTED_EDITION: u32 = 3;
pub(super) const SUPPORTED_COMPATIBILITY_VERSION: u32 = 1;
pub(super) const SUPPORTED_STORAGE_VERSION: u32 = 1;
const SYNC_RULES_TEXT_ENV: &str = "POWERSYNC_RUST_SYNC_RULES";
const SYNC_RULES_PATH_ENV: &str = "POWERSYNC_RUST_SYNC_RULES_PATH";
const BUILTIN_SYNC_RULES_SOURCE: &str =
    include_str!("../../tests/fixtures/sync_plan/benchmark_streams.sync-rules");

pub(super) fn contract_fingerprint(contract_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(contract_id.as_bytes());
    digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn execution_plan() -> &'static RustExecutionPlan {
    static EXECUTION_PLAN: OnceLock<RustExecutionPlan> = OnceLock::new();
    EXECUTION_PLAN.get_or_init(|| {
        let source =
            runtime_sync_rules_source().expect("runtime sync rules source should be readable");
        let canonical = compile_sync_rules_source(&source)
            .expect("runtime sync rules should compile into a canonical semantic plan");
        lower_canonical_semantic_plan(canonical)
            .expect("runtime canonical semantic plan should lower into an execution plan")
    })
}

pub fn bucket_catalog() -> &'static [SyncBucketDescriptor] {
    static BUCKET_CATALOG: OnceLock<Vec<SyncBucketDescriptor>> = OnceLock::new();
    BUCKET_CATALOG.get_or_init(|| {
        default_bucket_requests()
            .into_iter()
            .map(|bucket| SyncBucketDescriptor {
                bucket_name: bucket.bucket_name().to_owned(),
                stream_name: bucket.stream_name().to_owned(),
                is_default: bucket.is_default(),
            })
            .collect()
    })
}

pub fn find_bucket_descriptor(name: &str) -> Option<&'static SyncBucketDescriptor> {
    bucket_catalog()
        .iter()
        .find(|descriptor| descriptor.bucket_name == name)
}

pub fn is_supported_bucket(name: &str) -> bool {
    resolve_bucket_request(name).is_some()
}

pub fn default_bucket_requests() -> Vec<ResolvedSyncBucket> {
    execution_plan().default_bucket_requests()
}

pub fn default_bucket_request() -> ResolvedSyncBucket {
    default_bucket_requests()
        .into_iter()
        .next()
        .expect("execution plan should expose at least one default bucket request")
}

pub fn storage_contract_id() -> String {
    execution_plan().storage_contract_id().to_owned()
}

pub fn project_tasks_bucket_name(project_id: &str) -> String {
    bucket_name_for_stream(TASKS_BY_PROJECT_STREAM_NAME, &[project_id])
}

pub fn org_tasks_bucket_name(org_id: &str) -> String {
    bucket_name_for_stream(TASKS_BY_ORG_STREAM_NAME, &[org_id])
}

pub fn org_projects_bucket_name(org_id: &str) -> String {
    bucket_name_for_stream(PROJECTS_BY_ORG_STREAM_NAME, &[org_id])
}

pub fn owner_projects_bucket_name(owner_id: &str) -> String {
    bucket_name_for_stream(PROJECTS_BY_OWNER_STREAM_NAME, &[owner_id])
}

pub fn task_comments_bucket_name(task_id: &str) -> String {
    bucket_name_for_stream(COMMENTS_BY_TASK_STREAM_NAME, &[task_id])
}

pub fn org_comments_bucket_name(org_id: &str) -> String {
    bucket_name_for_stream(COMMENTS_BY_ORG_STREAM_NAME, &[org_id])
}

pub fn org_memberships_bucket_name(org_id: &str) -> String {
    bucket_name_for_stream(MEMBERSHIPS_BY_ORG_STREAM_NAME, &[org_id])
}

pub fn region_organizations_bucket_name(region: &str) -> String {
    bucket_name_for_stream(ORGANIZATIONS_BY_REGION_STREAM_NAME, &[region])
}

pub fn resolve_bucket_request(name: &str) -> Option<ResolvedSyncBucket> {
    execution_plan().resolve_bucket_request(name)
}

pub fn table_plan(source_table: &str) -> Option<&'static CompiledTablePlan> {
    execution_plan().table_plan(source_table)
}

pub fn stream(name: &str) -> Option<&'static CanonicalStream> {
    execution_plan().stream(name)
}

pub fn load_runtime_sync_rules_source() -> Result<String, String> {
    runtime_sync_rules_source()
}

pub fn canonical_storage_contract_id(canonical: &CanonicalSemanticPlan) -> String {
    format!(
        "layout={}:version={}:edition={}:compat={}:storage={}:canonical={}",
        RUST_INGEST_STORAGE_LAYOUT_VERSION,
        canonical.version,
        canonical.edition,
        canonical.compatibility_version,
        canonical.storage_version,
        serde_json::to_string(canonical).expect("canonical plan should serialize"),
    )
}

fn runtime_sync_rules_source() -> Result<String, String> {
    if let Ok(source) = env::var(SYNC_RULES_TEXT_ENV) {
        return Ok(source);
    }
    if let Ok(path) = env::var(SYNC_RULES_PATH_ENV) {
        return fs::read_to_string(&path)
            .map_err(|error| format!("failed to read sync rules from {path}: {error}"));
    }
    match crate::config::load_config_from_env() {
        Ok(Some(config)) => {
            if let Some(path) = config
                .config()
                .sync_rules
                .as_ref()
                .and_then(|sync_rules| sync_rules.path.as_deref())
            {
                let resolved = config.resolve_path(path);
                return fs::read_to_string(&resolved).map_err(|error| {
                    format!(
                        "failed to read sync rules from PowerSync config path {}: {error}",
                        resolved.display()
                    )
                });
            }
        }
        Ok(None) => {}
        Err(error) => return Err(error),
    }
    Ok(BUILTIN_SYNC_RULES_SOURCE.to_owned())
}

fn bucket_name_for_stream(stream_name: &str, values: &[&str]) -> String {
    bucket_name_for_stream_group(stream_name, 0, values)
}

pub(super) fn bucket_name_for_stream_group(
    stream_name: &str,
    group_index: usize,
    values: &[&str],
) -> String {
    format!(
        "{STREAM_BUCKET_PREFIX}{stream_name}|{group_index}{}",
        serde_json::to_string(values).expect("bucket parameter values should serialize")
    )
}

pub fn bucket_name_for_stream_values(stream_name: &str, values: &[String]) -> String {
    let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
    bucket_name_for_stream(stream_name, &borrowed)
}

pub fn bucket_name_for_stream_group_values(
    stream_name: &str,
    group_index: usize,
    values: &[String],
) -> String {
    let borrowed = values.iter().map(String::as_str).collect::<Vec<_>>();
    bucket_name_for_stream_group(stream_name, group_index, &borrowed)
}

pub(super) fn parse_bucket_values(encoded_values: &str) -> Result<Vec<String>, SyncRuleError> {
    serde_json::from_str(encoded_values).map_err(|error| {
        SyncRuleError(format!(
            "failed to parse bucket parameter array {}: {error}",
            encoded_values
        ))
    })
}
