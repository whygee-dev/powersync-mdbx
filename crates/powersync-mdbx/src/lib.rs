pub mod auth;
pub mod config;
pub mod control_plane;
pub mod http;
pub(crate) mod postgres_tls;
pub mod protocol;
pub mod replication;
pub mod server;
pub mod storage;
pub mod sync_rules;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::Router;
use control_plane::ServiceContext;
use storage::{build_storage, Storage, StorageBackend};

pub type SharedStorage = Arc<dyn Storage>;

pub fn build_app() -> Router {
    build_app_with_storage(build_storage(StorageBackend::from_env()))
}

pub fn build_app_with_storage(storage: SharedStorage) -> Router {
    let service_context =
        ServiceContext::from_env().expect("powersync_mdbx service context should load");
    build_app_with_storage_and_context(storage, service_context)
}

pub fn build_app_with_storage_and_context(
    storage: SharedStorage,
    service_context: ServiceContext,
) -> Router {
    http::router(storage, service_context)
}

pub fn build_app_with_storage_context_and_runtime_readiness(
    storage: SharedStorage,
    service_context: ServiceContext,
    runtime_readiness: Arc<AtomicBool>,
) -> Router {
    http::router_with_runtime_readiness(storage, service_context, Some(runtime_readiness))
}
