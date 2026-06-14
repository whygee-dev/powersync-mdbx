pub mod management;
pub mod sync_stream;

use axum::{
    extract::DefaultBodyLimit,
    extract::Request,
    extract::State,
    http::{header::HeaderName, HeaderValue},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{control_plane::ServiceContext, SharedStorage};

#[derive(Clone)]
pub struct AppState {
    storage: SharedStorage,
    service_context: ServiceContext,
    startup: StartupInfo,
    sync_read_admission: Arc<Semaphore>,
    sync_read_admission_timeout: Duration,
    runtime_readiness: Option<Arc<AtomicBool>>,
}

#[derive(Clone, Copy)]
pub struct StartupInfo {
    boot_instant: Instant,
    boot_unix_ms: u64,
}

impl AppState {
    pub fn new(storage: SharedStorage, service_context: ServiceContext) -> Self {
        Self::new_with_runtime_readiness(storage, service_context, None)
    }

    fn new_with_runtime_readiness(
        storage: SharedStorage,
        service_context: ServiceContext,
        runtime_readiness: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            storage,
            service_context,
            startup: StartupInfo {
                boot_instant: Instant::now(),
                boot_unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            },
            sync_read_admission: Arc::new(Semaphore::new(positive_env_usize(
                "POWERSYNC_RUST_MAX_CONCURRENT_SYNC_READS",
                8,
            ))),
            sync_read_admission_timeout: Duration::from_millis(positive_env_u64(
                "POWERSYNC_RUST_SYNC_READ_ADMISSION_TIMEOUT_MS",
                2_000,
            )),
            runtime_readiness,
        }
    }

    pub fn storage(&self) -> &SharedStorage {
        &self.storage
    }

    pub fn service_context(&self) -> &ServiceContext {
        &self.service_context
    }

    pub fn startup(&self) -> StartupInfo {
        self.startup
    }

    pub fn is_ready(&self) -> bool {
        self.runtime_readiness
            .as_ref()
            .is_none_or(|ready| ready.load(Ordering::Acquire))
            && self.storage.is_ready().unwrap_or(false)
    }

    pub async fn acquire_sync_read(&self) -> Result<OwnedSemaphorePermit, ()> {
        tokio::time::timeout(
            self.sync_read_admission_timeout,
            Arc::clone(&self.sync_read_admission).acquire_owned(),
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
    }
}

fn positive_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn positive_env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[derive(Clone, Copy)]
pub struct AdmissionStartedAt(pub Instant);

pub fn router(storage: SharedStorage, service_context: ServiceContext) -> Router {
    router_with_runtime_readiness(storage, service_context, None)
}

pub fn router_with_runtime_readiness(
    storage: SharedStorage,
    service_context: ServiceContext,
    runtime_readiness: Option<Arc<AtomicBool>>,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/debug/metrics", get(debug_metrics))
        .route("/probes/liveness", get(healthz))
        .route("/probes/readiness", get(readiness))
        .route(
            "/sync/stream",
            post(sync_stream::sync_stream).options(cors_preflight),
        )
        .route(
            "/api/sync-rules/v1/current",
            get(management::current_sync_rules),
        )
        .route(
            "/api/sync-rules/v1/validate",
            post(management::validate_sync_rules),
        )
        .route(
            "/api/sync-rules/v1/deploy",
            post(management::deploy_sync_rules),
        )
        .route(
            "/api/sync-rules/v1/reprocess",
            post(management::reprocess_sync_rules),
        )
        .route("/api/admin/v1/diagnostics", post(management::diagnostics))
        .route("/api/admin/v1/schema", post(management::schema))
        .route("/api/admin/v1/validate", post(management::validate_admin))
        .route("/api/admin/v1/reprocess", post(management::reprocess_admin))
        .route("/api/admin/v1/execute-sql", post(management::execute_sql))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .layer(from_fn(stamp_request_admission))
        .with_state(AppState::new_with_runtime_readiness(
            storage,
            service_context,
            runtime_readiness,
        ))
}

async fn healthz(State(state): State<AppState>) -> Response {
    const UPTIME_US_HEADER: HeaderName = HeaderName::from_static("x-powersync-uptime-us");
    const BOOT_UNIX_MS_HEADER: HeaderName = HeaderName::from_static("x-powersync-boot-unix-ms");

    let startup = state.startup();
    let mut response = "ok".into_response();
    response.headers_mut().insert(
        UPTIME_US_HEADER,
        HeaderValue::from_str(&startup.boot_instant.elapsed().as_micros().to_string())
            .expect("uptime header value should always be valid"),
    );
    response.headers_mut().insert(
        BOOT_UNIX_MS_HEADER,
        HeaderValue::from_str(&startup.boot_unix_ms.to_string())
            .expect("boot unix header value should always be valid"),
    );
    response
}

async fn readiness(State(state): State<AppState>) -> Response {
    let ready = state.is_ready();
    let mut response = healthz(State(state)).await;
    if !ready {
        *response.status_mut() = axum::http::StatusCode::SERVICE_UNAVAILABLE;
        *response.body_mut() = axum::body::Body::from("not ready");
    }
    response
}

async fn debug_metrics(State(state): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    if let Err(error) = state.service_context().authorize_diagnostics(&headers) {
        return (
            error.status,
            [(
                axum::http::header::CONTENT_TYPE,
                "application/json; charset=utf-8",
            )],
            error.body.to_string(),
        )
            .into_response();
    }
    Json(
        state
            .storage()
            .diagnostics_json()
            .unwrap_or_else(|| serde_json::json!({ "backend": "unknown", "metrics": null })),
    )
    .into_response()
}

async fn stamp_request_admission(mut request: Request, next: Next) -> Response {
    request
        .extensions_mut()
        .insert(AdmissionStartedAt(Instant::now()));
    let mut response = next.run(request).await;
    add_cors_headers(&mut response);
    response
}

async fn cors_preflight() -> Response {
    let mut response = ().into_response();
    add_cors_headers(&mut response);
    response
}

fn add_cors_headers(response: &mut Response) {
    const ALLOW_ORIGIN: HeaderName = HeaderName::from_static("access-control-allow-origin");
    const ALLOW_METHODS: HeaderName = HeaderName::from_static("access-control-allow-methods");
    const ALLOW_HEADERS: HeaderName = HeaderName::from_static("access-control-allow-headers");
    const EXPOSE_HEADERS: HeaderName = HeaderName::from_static("access-control-expose-headers");

    let headers = response.headers_mut();
    headers.insert(ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(ALLOW_METHODS, HeaderValue::from_static("GET,POST,OPTIONS"));
    headers.insert(
        ALLOW_HEADERS,
        HeaderValue::from_static("authorization,content-type,x-user-agent,accept"),
    );
    headers.insert(EXPOSE_HEADERS, HeaderValue::from_static("content-type"));
}
