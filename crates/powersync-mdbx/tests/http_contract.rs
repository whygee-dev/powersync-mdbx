use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bson::Document;
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use pg_walstream::{ChangeEvent, ColumnValue, Lsn, ReplicaIdentity, RowData};
use powersync_mdbx::{
    protocol::messages::{
        protocol_checksum_i32, put_checksum, remove_checksum, source_subkey_for_object,
    },
    replication::{
        ingest::{ReplicationCommitBatch, ReplicationMdbxStore},
        postgres::PostgresLsn,
    },
    storage::WireMdbxStorage,
    sync_rules::{
        org_comments_bucket_name, org_memberships_bucket_name, org_tasks_bucket_name,
        owner_projects_bucket_name, project_tasks_bucket_name, region_organizations_bucket_name,
        task_comments_bucket_name,
    },
};
use serde_json::Value;
use sha2::Sha256;
use std::{
    io::Cursor,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tempfile::TempDir;
use tower::ServiceExt;

const TICKETS_SYNC_RULES_YAML: &str =
    "edition: 3\ncompatibility_version: 1\nstorage_version: 1\nstreams:\n  tickets:\n    auto_subscribe: true\n    query: SELECT * FROM public.tickets\n";
fn app() -> axum::Router {
    app_with_storage(powersync_mdbx::storage::build_storage(
        powersync_mdbx::storage::StorageBackend::SyncEdge,
    ))
}

/// Anonymous-sync test app: user auth fails closed by default, so tests that
/// exercise the protocol without JWTs opt into anonymous access explicitly.
fn app_with_storage(storage: powersync_mdbx::SharedStorage) -> axum::Router {
    let temp = TempDir::new().expect("temp dir should exist");
    let context = powersync_mdbx::control_plane::ServiceContext::new_for_tests(
        temp.path().join("sync-rules-state.json"),
        Vec::new(),
        None,
        Vec::new(),
    )
    .expect("test service context should build")
    .with_allow_anonymous_sync(true);
    // Keep the state file alive for the rest of the test process.
    std::mem::forget(temp);
    powersync_mdbx::build_app_with_storage_and_context(storage, context)
}

fn app_with_context(
    storage: powersync_mdbx::SharedStorage,
    api_tokens: Vec<String>,
    user_auth: Option<powersync_mdbx::auth::UserAuthConfig>,
    source_connections: Vec<powersync_mdbx::control_plane::SourceConnection>,
) -> (axum::Router, TempDir) {
    let temp = TempDir::new().expect("temp dir should exist");
    let context = powersync_mdbx::control_plane::ServiceContext::new_for_tests(
        temp.path().join("sync-rules-state.json"),
        api_tokens,
        user_auth,
        source_connections,
    )
    .expect("test service context should build");
    (
        powersync_mdbx::build_app_with_storage_and_context(storage, context),
        temp,
    )
}

type HmacSha256 = Hmac<Sha256>;

fn management_app() -> (axum::Router, TempDir) {
    management_app_with_query_capability(false)
}

fn management_app_with_query_capability(query_capability_enabled: bool) -> (axum::Router, TempDir) {
    let temp = TempDir::new().expect("temp dir should exist");
    management_app_with_query_connection_inner(
        temp,
        query_capability_enabled,
        "postgres://postgres:secret@db:5432/app",
    )
}

fn management_app_with_query_connection(
    query_capability_enabled: bool,
    connection_uri: &str,
) -> (axum::Router, TempDir) {
    let temp = TempDir::new().expect("temp dir should exist");
    management_app_with_query_connection_inner(temp, query_capability_enabled, connection_uri)
}

fn management_app_with_query_connection_inner(
    temp: TempDir,
    query_capability_enabled: bool,
    connection_uri: &str,
) -> (axum::Router, TempDir) {
    let context =
        powersync_mdbx::control_plane::ServiceContext::new_for_tests_with_query_capability(
            temp.path().join("sync-rules-state.json"),
            vec!["admin-token".to_owned()],
            None,
            vec![powersync_mdbx::control_plane::SourceConnection {
                id: "db".to_owned(),
                tag: "postgres".to_owned(),
                uri: connection_uri.to_owned(),
            }],
            query_capability_enabled,
        )
        .expect("test service context should build");
    (
        powersync_mdbx::build_app_with_storage_and_context(
            powersync_mdbx::storage::build_storage(
                powersync_mdbx::storage::StorageBackend::SyncEdge,
            ),
            context,
        ),
        temp,
    )
}

#[tokio::test]
async fn healthz_returns_ok() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_healthz_startup_headers(response.headers());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"ok");
}

#[tokio::test]
async fn probe_aliases_return_ok() {
    for path in ["/probes/liveness", "/probes/readiness"] {
        let response = app()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "path {path}");
        assert_healthz_startup_headers(response.headers());
    }
}

#[tokio::test]
async fn unified_runtime_readiness_tracks_replication_connection() {
    let state_dir = TempDir::new().expect("state directory");
    let context = powersync_mdbx::control_plane::ServiceContext::new_for_tests(
        state_dir.path().join("sync-rules-state.json"),
        Vec::new(),
        None,
        Vec::new(),
    )
    .expect("service context");
    let readiness = Arc::new(AtomicBool::new(false));
    let app = powersync_mdbx::build_app_with_storage_context_and_runtime_readiness(
        powersync_mdbx::storage::build_storage(powersync_mdbx::storage::StorageBackend::SyncEdge),
        context,
        Arc::clone(&readiness),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/probes/readiness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    readiness.store(true, Ordering::Release);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/probes/readiness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    readiness.store(false, Ordering::Release);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/probes/readiness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn sync_stream_returns_ndjson_with_compatibility_headers() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"buckets":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/x-ndjson"
    );
    assert_eq!(response.headers().get("x-accel-buffering").unwrap(), "no");
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    assert_eq!(
        response.headers().get("x-powersync-emission-path").unwrap(),
        "preframed-body"
    );
    assert_debug_timing_headers(response.headers());

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_text = std::str::from_utf8(&body).unwrap();
    let lines = body_text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();

    assert!(lines.len() >= 3);
    assert!(lines[0].get("checkpoint").is_some());
    assert!(lines[1..lines.len() - 1]
        .iter()
        .all(|line| line.get("data").is_some()));
    assert!(lines[lines.len() - 1].get("checkpoint_complete").is_some());
}

#[tokio::test]
async fn sync_stream_rejects_malformed_after_cursor_instead_of_dropping_bucket() {
    let app = app();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"buckets":[{"name":"tasks","after":"not-a-number"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice::<Value>(&body).unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("invalid after cursor"));
}

#[tokio::test]
async fn sync_stream_rejects_malformed_json_instead_of_serving_default_buckets() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"buckets":["#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice::<Value>(&body).unwrap();
    assert_eq!(body["error"], "invalid sync request JSON");
}

#[tokio::test]
async fn sync_stream_fails_closed_when_no_user_auth_is_configured() {
    let (app, _temp) = app_with_context(
        powersync_mdbx::storage::build_storage(powersync_mdbx::storage::StorageBackend::SyncEdge),
        Vec::new(),
        None,
        Vec::new(),
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"buckets":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        serde_json::json!({"error": "Authentication disabled", "code": "PSYNC_S2106"})
    );
}

#[tokio::test]
async fn debug_metrics_requires_api_token_once_configured() {
    let (app, _temp) = app_with_context(
        powersync_mdbx::storage::build_storage(powersync_mdbx::storage::StorageBackend::SyncEdge),
        vec!["admin-token".to_owned()],
        None,
        Vec::new(),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/debug/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/debug/metrics")
                .header(header::AUTHORIZATION, "Bearer admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn sync_stream_requires_auth_when_user_auth_is_configured() {
    let (app, _temp) = app_with_context(
        powersync_mdbx::storage::build_storage(powersync_mdbx::storage::StorageBackend::SyncEdge),
        Vec::new(),
        Some(
            powersync_mdbx::auth::UserAuthConfig::from_hs256_secrets(
                vec![(Some("kid-1".to_owned()), b"super-secret".to_vec())],
                vec!["powersync".to_owned()],
                vec!["https://issuer.example".to_owned()],
            )
            .expect("valid auth policy"),
        ),
        Vec::new(),
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"buckets":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        serde_json::json!({"error": "Authentication required", "code": "PSYNC_S2106"})
    );
}

#[tokio::test]
async fn sync_stream_accepts_valid_hs256_bearer_token() {
    let (app, _temp) = app_with_context(
        powersync_mdbx::storage::build_storage(powersync_mdbx::storage::StorageBackend::SyncEdge),
        Vec::new(),
        Some(
            powersync_mdbx::auth::UserAuthConfig::from_hs256_secrets(
                vec![(Some("kid-1".to_owned()), b"super-secret".to_vec())],
                vec!["powersync".to_owned()],
                vec!["https://issuer.example".to_owned()],
            )
            .expect("valid auth policy"),
        ),
        Vec::new(),
    );
    let token = signed_hs256_token(
        b"super-secret",
        Some("kid-1"),
        serde_json::json!({
            "sub": "user-1",
            "aud": "powersync",
            "iss": "https://issuer.example",
            "exp": unix_now() + 300,
        }),
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{"buckets":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn management_routes_require_api_token_auth() {
    let (app, _temp) = app_with_context(
        powersync_mdbx::storage::build_storage(powersync_mdbx::storage::StorageBackend::SyncEdge),
        vec!["admin-token".to_owned()],
        None,
        Vec::new(),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/sync-rules/v1/current")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        serde_json::json!({"error": "Authentication required", "code": "PSYNC_S2106"})
    );
}

#[tokio::test]
async fn management_routes_expose_current_validate_deploy_reprocess_and_schema() {
    let (app, _temp) = management_app();

    let current = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/sync-rules/v1/current")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(current.status(), StatusCode::OK);
    let current_body =
        serde_json::from_slice::<Value>(&current.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(current_body["data"]["current"]["valid"], Value::Bool(true));

    let validate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sync-rules/v1/validate")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/yaml")
                .body(Body::from(TICKETS_SYNC_RULES_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(validate.status(), StatusCode::OK);

    let deploy = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sync-rules/v1/deploy")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/yaml")
                .body(Body::from(TICKETS_SYNC_RULES_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deploy.status(), StatusCode::CONFLICT);
    let deploy_body =
        serde_json::from_slice::<Value>(&deploy.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert!(deploy_body["error"]
        .as_str()
        .expect("deployment error")
        .contains("Layout-changing sync-rule activation is disabled"));

    let current_after_deploy = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/sync-rules/v1/current")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(current_after_deploy.status(), StatusCode::OK);
    let current_after_deploy_body = serde_json::from_slice::<Value>(
        &current_after_deploy
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap();
    assert_ne!(
        current_after_deploy_body["data"]["current"]["content"],
        TICKETS_SYNC_RULES_YAML
    );
    assert_eq!(
        current_after_deploy_body["data"]["current"]["status"],
        "active"
    );
    assert_eq!(current_after_deploy_body["data"]["next"], Value::Null);
    assert_eq!(
        current_after_deploy_body["data"]["history"]
            .as_array()
            .expect("history should be present")
            .len(),
        3
    );

    let diagnostics = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/v1/diagnostics")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"sync_rules_content":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(diagnostics.status(), StatusCode::OK);
    let diagnostics_body = serde_json::from_slice::<Value>(
        &diagnostics.into_body().collect().await.unwrap().to_bytes(),
    )
    .unwrap();
    assert_eq!(
        diagnostics_body["connections"][0]["postgres_uri"],
        Value::String("postgres://postgres:***@db:5432/app".to_owned())
    );
    assert!(diagnostics_body["deploying_sync_rules"]["content"].is_null());
    assert_eq!(diagnostics_body["active_sync_rules"]["status"], "active");
    assert_eq!(diagnostics_body["lifecycle"]["current_status"], "active");
    assert_eq!(
        diagnostics_body["history"]
            .as_array()
            .expect("history should be exposed")
            .last()
            .and_then(|entry| entry["status"].as_str()),
        Some("failed")
    );

    let schema = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/v1/schema")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(schema.status(), StatusCode::OK);
    let schema_body =
        serde_json::from_slice::<Value>(&schema.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(
        schema_body["defaultSchema"],
        Value::String("public".to_owned())
    );

    let execute_sql = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/v1/execute-sql")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "sql": {
                            "query": "SELECT 1",
                            "args": [],
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(execute_sql.status(), StatusCode::OK);
    let execute_sql_body = serde_json::from_slice::<Value>(
        &execute_sql.into_body().collect().await.unwrap().to_bytes(),
    )
    .unwrap();
    assert_eq!(execute_sql_body["success"], Value::Bool(false));
    assert_eq!(execute_sql_body["out_of_scope"], Value::Bool(true));
    assert_eq!(
        execute_sql_body["prototype_scope"],
        Value::String("excluded_from_powersync_mdbx_scope".to_owned())
    );
    assert_eq!(
        execute_sql_body["error"],
        Value::String("execute-sql is out of scope for powersync-mdbx".to_owned())
    );

    let reprocess = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sync-rules/v1/reprocess")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reprocess.status(), StatusCode::CONFLICT);
    let reprocess_body =
        serde_json::from_slice::<Value>(&reprocess.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert!(reprocess_body["error"]
        .as_str()
        .expect("reprocessing error")
        .contains("Sync-rule reprocessing is disabled"));

    let admin_validate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/v1/validate")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "sync_rules": TICKETS_SYNC_RULES_YAML,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin_validate.status(), StatusCode::OK);

    let admin_reprocess = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/v1/reprocess")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin_reprocess.status(), StatusCode::CONFLICT);
    let admin_reprocess_body = serde_json::from_slice::<Value>(
        &admin_reprocess
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap();
    assert!(admin_reprocess_body["error"]
        .as_str()
        .expect("admin reprocessing error")
        .contains("Sync-rule reprocessing is disabled"));
}

#[tokio::test]
async fn management_layout_changing_deploys_fail_closed_without_changing_current_rules() {
    let (app, _temp) = management_app();

    let first_deploy = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sync-rules/v1/deploy")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "content": TICKETS_SYNC_RULES_YAML,
                        "base_version": 0,
                        "intent_token": "deploy-tickets-v1",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_deploy.status(), StatusCode::CONFLICT);
    let first_body = serde_json::from_slice::<Value>(
        &first_deploy.into_body().collect().await.unwrap().to_bytes(),
    )
    .unwrap();
    assert!(first_body["error"]
        .as_str()
        .expect("deployment error")
        .contains("Layout-changing sync-rule activation is disabled"));

    let retry = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sync-rules/v1/deploy")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "content": TICKETS_SYNC_RULES_YAML,
                        "base_version": 0,
                        "intent_token": "deploy-tickets-v1",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retry.status(), StatusCode::CONFLICT);

    let current = app
        .oneshot(
            Request::builder()
                .uri("/api/sync-rules/v1/current")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(current.status(), StatusCode::OK);
    let current_body =
        serde_json::from_slice::<Value>(&current.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(current_body["data"]["current"]["version"], Value::from(0));
    assert_ne!(
        current_body["data"]["current"]["content"],
        TICKETS_SYNC_RULES_YAML
    );
    assert_eq!(current_body["data"]["next"], Value::Null);
}

#[tokio::test]
async fn management_reprocess_fails_closed_without_mutating_current_rules() {
    let (app, _temp) = management_app();

    for uri in [
        "/api/sync-rules/v1/reprocess",
        "/api/admin/v1/reprocess",
        "/api/sync-rules/v1/reprocess",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(header::AUTHORIZATION, "Token admin-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "base_version": 0,
                            "intent_token": "reprocess-current-v1",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = serde_json::from_slice::<Value>(
            &response.into_body().collect().await.unwrap().to_bytes(),
        )
        .unwrap();
        assert!(body["error"]
            .as_str()
            .expect("reprocessing error")
            .contains("Sync-rule reprocessing is disabled"));
    }

    let current = app
        .oneshot(
            Request::builder()
                .uri("/api/sync-rules/v1/current")
                .header(header::AUTHORIZATION, "Token admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(current.status(), StatusCode::OK);
    let current_body =
        serde_json::from_slice::<Value>(&current.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(current_body["data"]["current"]["version"], Value::from(0));
    assert_eq!(current_body["data"]["current"]["status"], "active");
    assert_eq!(current_body["data"]["next"], Value::Null);
    assert!(current_body["data"]["history"]
        .as_array()
        .expect("history should be present")
        .is_empty());
}

#[tokio::test]
async fn execute_sql_remains_out_of_scope_even_when_query_capability_is_enabled() {
    let (app, _temp) = management_app_with_query_connection(
        true,
        "postgres://invalid:invalid@127.0.0.1:1/nowhere",
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/v1/execute-sql")
                .header(header::AUTHORIZATION, "Token admin-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "sql": {
                            "query": "SELECT $1::text AS greeting",
                            "args": ["hello"],
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body =
        serde_json::from_slice::<Value>(&response.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(body["success"], Value::Bool(false));
    assert_eq!(body["out_of_scope"], Value::Bool(true));
    assert_eq!(body["query_capability_enabled_ignored"], Value::Bool(true));
    assert_eq!(body["results"]["columns"], serde_json::json!([]));
    assert_eq!(body["results"]["rows"], serde_json::json!([]));
    assert_eq!(
        body["error"],
        Value::String("execute-sql is out of scope for powersync-mdbx".to_owned())
    );
}

#[tokio::test]
async fn sync_stream_rejects_unknown_explicit_bucket_requests() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"buckets":[{"name":"unknown","after":"7"},{"name":"1#tasks|0[]","after":"0"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        serde_json::json!({"error": "unknown bucket unknown"})
    );
}

#[tokio::test]
async fn sync_stream_returns_500_without_leaking_detail_when_storage_read_fails() {
    use powersync_mdbx::{
        storage::{Storage, StorageError, StreamEncoding, SyncBucketCursors, SyncChunkSource},
        sync_rules::RustExecutionPlan,
    };

    struct FailingStorage;

    impl Storage for FailingStorage {
        fn sync_chunk_source_for_buckets_with_plan(
            &self,
            _buckets: &SyncBucketCursors,
            _plan: &RustExecutionPlan,
            _encoding: StreamEncoding,
        ) -> Result<SyncChunkSource, StorageError> {
            Err(StorageError(
                "mdbx read failed: simulated-internal-detail".to_owned(),
            ))
        }
    }

    let response = app_with_storage(Arc::new(FailingStorage))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"buckets":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_text = std::str::from_utf8(&body).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(body_text).unwrap(),
        serde_json::json!({"error": "internal storage error"})
    );
    assert!(
        !body_text.contains("simulated-internal-detail"),
        "internal storage error detail must not leak to clients"
    );
}

/// Builds a single-commit replication batch of text-column INSERTs for `table`.
fn text_insert_commit_batch(
    transaction_id: u32,
    lsn_base: u64,
    commit_time_base: i64,
    table: &str,
    rows: &[&[(&str, &str)]],
) -> ReplicationCommitBatch {
    ReplicationCommitBatch {
        transaction_id,
        begin_final_lsn: PostgresLsn(lsn_base),
        begin_commit_time_micros: commit_time_base,
        commit_lsn: PostgresLsn(lsn_base + 1),
        end_lsn: PostgresLsn(lsn_base + 2),
        commit_time_micros: commit_time_base + 1,
        column_types_by_table: std::collections::BTreeMap::new(),
        changes: rows
            .iter()
            .map(|row| {
                ChangeEvent::insert("public", table, 1, text_row(row), Lsn::from(lsn_base + 2))
            })
            .collect(),
    }
}

fn text_row(pairs: &[(&str, &str)]) -> RowData {
    RowData::from_pairs(
        pairs
            .iter()
            .map(|&(column, value)| (column, ColumnValue::text(value)))
            .collect(),
    )
}

fn single_bucket_sync_request(bucket: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/sync/stream")
        .header(header::ACCEPT, "application/x-ndjson")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({
                "buckets": [
                    {
                        "name": bucket,
                        "after": "0"
                    }
                ]
            })
            .to_string(),
        ))
        .unwrap()
}

async fn read_body_text(response: axum::response::Response) -> String {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    std::str::from_utf8(&body).unwrap().to_owned()
}

fn parse_ndjson_lines(body_text: &str) -> Vec<Value> {
    body_text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

fn data_entries(lines: &[Value]) -> Vec<&Value> {
    lines
        .iter()
        .filter_map(|line| line.get("data"))
        .flat_map(|payload| {
            payload
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .collect()
}

fn checkpoint_has_stream(lines: &[Value], stream_name: &str) -> bool {
    lines.iter().any(|line| {
        line.get("checkpoint")
            .and_then(|checkpoint| checkpoint.get("streams"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|stream| stream.get("name").and_then(Value::as_str) == Some(stream_name))
    })
}

fn checkpoint_bucket<'a>(lines: &'a [Value], bucket_name: &str) -> &'a Value {
    lines
        .iter()
        .filter_map(|line| line.get("checkpoint"))
        .flat_map(|checkpoint| {
            checkpoint
                .get("buckets")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .find(|bucket| bucket.get("bucket").and_then(Value::as_str) == Some(bucket_name))
        .unwrap_or_else(|| panic!("checkpoint should describe bucket {bucket_name}"))
}

fn mark_ingest_ready(store: &ReplicationMdbxStore) {
    let lsn = store
        .last_persisted_end_lsn()
        .expect("read persisted LSN")
        .expect("persisted batch should have an LSN");
    store
        .persist_initial_snapshot_marker_with_plan(
            lsn,
            powersync_mdbx::sync_rules::execution_plan(),
            "http-contract-test-source",
        )
        .expect("mark test snapshot complete");
}

/// Shared skeleton for the scoped-bucket `/sync/stream` scenarios: seed rows
/// through the live replication ingest store, request a single bucket with
/// `after: "0"`, and assert the bucket serves only the matching row.
///
/// Returns the parsed NDJSON lines so individual tests can layer extra
/// per-scenario assertions on top.
struct ScopedBucketScenario<'a> {
    transaction_id: u32,
    lsn_base: u64,
    table: &'a str,
    rows: &'a [&'a [(&'a str, &'a str)]],
    bucket: String,
    stream_name: &'a str,
    served_object_id: &'a str,
    filtered_object_id: &'a str,
}

async fn assert_sync_stream_serves_scoped_bucket(scenario: ScopedBucketScenario<'_>) -> Vec<Value> {
    let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
    let tail_dir = TempDir::new().expect("tail temp directory should exist");
    let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
    let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");

    ingest_store
        .persist_batch(&text_insert_commit_batch(
            scenario.transaction_id,
            scenario.lsn_base,
            1,
            scenario.table,
            scenario.rows,
        ))
        .expect("persist runtime batch");
    mark_ingest_ready(&ingest_store);

    let storage: powersync_mdbx::SharedStorage = Arc::new(WireMdbxStorage::new_with_ingest(
        snapshot_dir.path(),
        tail_dir.path(),
        ingest_dir.path(),
    ));

    let response = app_with_storage(storage)
        .oneshot(single_bucket_sync_request(&scenario.bucket))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_text = read_body_text(response).await;
    let lines = parse_ndjson_lines(&body_text);
    let entries = data_entries(&lines);

    assert!(checkpoint_has_stream(&lines, scenario.stream_name));
    assert!(entries.iter().any(|entry| {
        entry.get("object_id").and_then(Value::as_str) == Some(scenario.served_object_id)
    }));
    assert!(entries.iter().all(|entry| {
        entry.get("object_id").and_then(Value::as_str) != Some(scenario.filtered_object_id)
    }));

    lines
}

#[tokio::test]
async fn sync_stream_serves_project_scoped_bucket_from_live_ingest_state() {
    let lines = assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 92,
        lsn_base: 910,
        table: "tasks",
        rows: &[
            &[
                ("id", "task-http-project-a"),
                ("org_id", "org-runtime"),
                ("project_id", "project-http-a"),
                ("title", "HTTP Project A"),
                ("status", "todo"),
                ("priority", "1"),
                ("assignee_id", "user-runtime"),
                ("story_points", "2"),
                ("updated_at", "2026-04-11T00:00:00Z"),
                ("summary", "runtime:http:project:a"),
            ],
            &[
                ("id", "task-http-project-b"),
                ("org_id", "org-runtime"),
                ("project_id", "project-http-b"),
                ("title", "HTTP Project B"),
                ("status", "todo"),
                ("priority", "2"),
                ("assignee_id", "user-runtime"),
                ("story_points", "3"),
                ("updated_at", "2026-04-11T00:00:00Z"),
                ("summary", "runtime:http:project:b"),
            ],
        ],
        bucket: project_tasks_bucket_name("project-http-b"),
        stream_name: "tasks_by_project",
        served_object_id: "task-http-project-b",
        filtered_object_id: "task-http-project-a",
    })
    .await;

    // Checkpoint pin: the project bucket only contains the single matching
    // row, so its checkpoint checksum must equal that row's PUT checksum and
    // its count must be exactly one.
    let entries = data_entries(&lines);
    let served = entries
        .iter()
        .find(|entry| entry.get("object_id").and_then(Value::as_str) == Some("task-http-project-b"))
        .expect("served bucket entry should exist");
    let served_data = served
        .get("data")
        .and_then(Value::as_str)
        .expect("served bucket entry should carry data");
    let expected_checksum = put_checksum("tasks", "task-http-project-b", served_data);
    assert_eq!(
        served.get("checksum").and_then(Value::as_u64),
        Some(u64::from(expected_checksum))
    );
    let bucket = checkpoint_bucket(&lines, &project_tasks_bucket_name("project-http-b"));
    assert_eq!(
        bucket.get("checksum").and_then(Value::as_i64),
        Some(i64::from(protocol_checksum_i32(expected_checksum)))
    );
    assert_eq!(bucket.get("count").and_then(Value::as_u64), Some(1));
}

#[tokio::test]
async fn sync_stream_serves_task_scoped_comments_bucket_from_live_ingest_state() {
    assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 93,
        lsn_base: 920,
        table: "comments",
        rows: &[
            &[
                ("id", "comment-http-task-a"),
                ("org_id", "org-runtime"),
                ("task_id", "task-http-a"),
                ("owner_id", "user-runtime"),
                ("author_id", "author-a"),
                ("body", "HTTP Comment A"),
                ("created_at", "2026-04-12T00:00:00Z"),
                ("updated_at", "2026-04-12T00:00:01Z"),
            ],
            &[
                ("id", "comment-http-task-b"),
                ("org_id", "org-runtime"),
                ("task_id", "task-http-b"),
                ("owner_id", "user-runtime"),
                ("author_id", "author-b"),
                ("body", "HTTP Comment B"),
                ("created_at", "2026-04-12T00:00:02Z"),
                ("updated_at", "2026-04-12T00:00:03Z"),
            ],
        ],
        bucket: task_comments_bucket_name("task-http-b"),
        stream_name: "comments_by_task",
        served_object_id: "comment-http-task-b",
        filtered_object_id: "comment-http-task-a",
    })
    .await;
}

#[tokio::test]
async fn sync_stream_serves_org_scoped_memberships_bucket_from_live_ingest_state() {
    assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 94,
        lsn_base: 930,
        table: "memberships",
        rows: &[
            &[
                ("id", "membership-http-org-a"),
                ("org_id", "org-http-a"),
                ("user_id", "user-a"),
                ("owner_id", "owner-a"),
                ("role", "member"),
                ("display_name", "HTTP Member A"),
                ("email", "a@example.com"),
                ("updated_at", "2026-04-12T00:00:00Z"),
            ],
            &[
                ("id", "membership-http-org-b"),
                ("org_id", "org-http-b"),
                ("user_id", "user-b"),
                ("owner_id", "owner-b"),
                ("role", "admin"),
                ("display_name", "HTTP Member B"),
                ("email", "b@example.com"),
                ("updated_at", "2026-04-12T00:00:01Z"),
            ],
        ],
        bucket: org_memberships_bucket_name("org-http-b"),
        stream_name: "memberships_by_org",
        served_object_id: "membership-http-org-b",
        filtered_object_id: "membership-http-org-a",
    })
    .await;
}

#[tokio::test]
async fn sync_stream_serves_org_scoped_tasks_bucket_from_live_ingest_state() {
    assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 95,
        lsn_base: 940,
        table: "tasks",
        rows: &[
            &[
                ("id", "task-http-org-a"),
                ("org_id", "org-http-a"),
                ("project_id", "project-http-a"),
                ("title", "HTTP Org A"),
                ("status", "todo"),
                ("priority", "1"),
                ("assignee_id", "user-a"),
                ("story_points", "2"),
                ("updated_at", "2026-04-12T00:00:00Z"),
                ("summary", "runtime:http:org:a"),
            ],
            &[
                ("id", "task-http-org-b"),
                ("org_id", "org-http-b"),
                ("project_id", "project-http-b"),
                ("title", "HTTP Org B"),
                ("status", "doing"),
                ("priority", "2"),
                ("assignee_id", "user-b"),
                ("story_points", "3"),
                ("updated_at", "2026-04-12T00:00:01Z"),
                ("summary", "runtime:http:org:b"),
            ],
        ],
        bucket: org_tasks_bucket_name("org-http-b"),
        stream_name: "tasks_by_org",
        served_object_id: "task-http-org-b",
        filtered_object_id: "task-http-org-a",
    })
    .await;
}

#[tokio::test]
async fn sync_stream_serves_owner_scoped_projects_bucket_from_live_ingest_state() {
    assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 96,
        lsn_base: 950,
        table: "projects",
        rows: &[
            &[
                ("id", "project-http-owner-a"),
                ("org_id", "org-http-a"),
                ("code", "PRJ-A"),
                ("name", "HTTP Owner A"),
                ("status", "active"),
                ("priority", "1"),
                ("owner_id", "owner-http-a"),
                ("updated_at", "2026-04-12T00:00:00Z"),
                ("summary", "runtime:http:owner:a"),
            ],
            &[
                ("id", "project-http-owner-b"),
                ("org_id", "org-http-b"),
                ("code", "PRJ-B"),
                ("name", "HTTP Owner B"),
                ("status", "active"),
                ("priority", "2"),
                ("owner_id", "owner-http-b"),
                ("updated_at", "2026-04-12T00:00:01Z"),
                ("summary", "runtime:http:owner:b"),
            ],
        ],
        bucket: owner_projects_bucket_name("owner-http-b"),
        stream_name: "projects_by_owner",
        served_object_id: "project-http-owner-b",
        filtered_object_id: "project-http-owner-a",
    })
    .await;
}

#[tokio::test]
async fn sync_stream_serves_org_scoped_comments_bucket_from_live_ingest_state() {
    assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 97,
        lsn_base: 960,
        table: "comments",
        rows: &[
            &[
                ("id", "comment-http-org-a"),
                ("org_id", "org-http-a"),
                ("task_id", "task-http-a"),
                ("owner_id", "owner-http-a"),
                ("author_id", "author-http-a"),
                ("body", "HTTP Org Comment A"),
                ("created_at", "2026-04-12T00:00:00Z"),
                ("updated_at", "2026-04-12T00:00:01Z"),
            ],
            &[
                ("id", "comment-http-org-b"),
                ("org_id", "org-http-b"),
                ("task_id", "task-http-b"),
                ("owner_id", "owner-http-b"),
                ("author_id", "author-http-b"),
                ("body", "HTTP Org Comment B"),
                ("created_at", "2026-04-12T00:00:02Z"),
                ("updated_at", "2026-04-12T00:00:03Z"),
            ],
        ],
        bucket: org_comments_bucket_name("org-http-b"),
        stream_name: "comments_by_org",
        served_object_id: "comment-http-org-b",
        filtered_object_id: "comment-http-org-a",
    })
    .await;
}

#[tokio::test]
async fn sync_stream_serves_region_scoped_organizations_bucket_from_live_ingest_state() {
    assert_sync_stream_serves_scoped_bucket(ScopedBucketScenario {
        transaction_id: 98,
        lsn_base: 970,
        table: "organizations",
        rows: &[
            &[
                ("id", "org-http-region-a"),
                ("name", "HTTP Region A"),
                ("owner_id", "owner-region-a"),
                ("plan", "starter"),
                ("region", "eu-west-1"),
                ("updated_at", "2026-04-12T00:00:00Z"),
            ],
            &[
                ("id", "org-http-region-b"),
                ("name", "HTTP Region B"),
                ("owner_id", "owner-region-b"),
                ("plan", "enterprise"),
                ("region", "us-east-1"),
                ("updated_at", "2026-04-12T00:00:01Z"),
            ],
        ],
        bucket: region_organizations_bucket_name("us-east-1"),
        stream_name: "organizations_by_region",
        served_object_id: "org-http-region-b",
        filtered_object_id: "org-http-region-a",
    })
    .await;
}

#[tokio::test]
async fn sync_stream_returns_bson_stream_when_requested() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/vnd.powersync.bson-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"buckets":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/vnd.powersync.bson-stream"
    );
    assert_eq!(
        response.headers().get("x-powersync-emission-path").unwrap(),
        "preframed-body"
    );
    assert_debug_timing_headers(response.headers());

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let docs = decode_concatenated_bson_docs(&body);

    assert!(docs.len() >= 3);
    assert!(docs[0].contains_key("checkpoint"));
    assert!(docs[1..docs.len() - 1]
        .iter()
        .all(|doc| doc.contains_key("data")));
    assert!(docs[docs.len() - 1].contains_key("checkpoint_complete"));
}

#[tokio::test]
async fn sync_stream_serves_ingest_derived_task_tail_over_http() {
    let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
    let tail_dir = TempDir::new().expect("tail temp directory should exist");
    let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
    let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");

    let batch = text_insert_commit_batch(
        91,
        900,
        1,
        "tasks",
        &[&[
            ("id", "task-runtime-http"),
            ("org_id", "org-runtime"),
            ("project_id", "project-runtime"),
            ("title", "Runtime HTTP Insert"),
            ("status", "todo"),
            ("priority", "3"),
            ("assignee_id", "user-runtime"),
            ("story_points", "5"),
            ("updated_at", "2026-04-11T00:00:00Z"),
            ("summary", "runtime:http:insert"),
        ]],
    );
    ingest_store
        .persist_batch(&batch)
        .expect("persist runtime batch");
    mark_ingest_ready(&ingest_store);

    let storage: powersync_mdbx::SharedStorage = Arc::new(WireMdbxStorage::new_with_ingest(
        snapshot_dir.path(),
        tail_dir.path(),
        ingest_dir.path(),
    ));

    let response = app_with_storage(storage)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"buckets":[{"name":"1#tasks|0[]","after":"0"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-powersync-emission-path").unwrap(),
        "chunk-stream"
    );

    let body_text = read_body_text(response).await;
    let lines = parse_ndjson_lines(&body_text);
    let entries = data_entries(&lines);

    assert!(entries.iter().any(|entry| {
        entry.get("object_id").and_then(Value::as_str) == Some("task-runtime-http")
            && entry.get("op").and_then(Value::as_str) == Some("PUT")
    }));
    assert!(
        !body_text.contains("task-org-001-0001-0001"),
        "router-level full snapshot should be sourced from ingest state once available"
    );

    // Checkpoint checksum pin (initial snapshot): a single inserted row means
    // the checkpoint checksum is exactly the row's PUT checksum (no tail
    // remainder yet) and the count is one. Recompute the expectation with the
    // crate's own protocol helpers from the served oplog payload.
    let put_entry = entries
        .iter()
        .find(|entry| entry.get("object_id").and_then(Value::as_str) == Some("task-runtime-http"))
        .expect("inserted task should be served as a PUT entry");
    let put_data = put_entry
        .get("data")
        .and_then(Value::as_str)
        .expect("PUT entry should carry the row payload");
    let expected_put_checksum = put_checksum("tasks", "task-runtime-http", put_data);
    assert_eq!(
        put_entry.get("checksum").and_then(Value::as_u64),
        Some(u64::from(expected_put_checksum)),
        "served oplog entry checksum should match put_checksum over its payload"
    );
    let bucket = checkpoint_bucket(&lines, "1#tasks|0[]");
    assert_eq!(
        bucket.get("checksum").and_then(Value::as_i64),
        Some(i64::from(protocol_checksum_i32(expected_put_checksum))),
        "checkpoint checksum should equal the single PUT checksum as a wrapped i32"
    );
    assert_eq!(bucket.get("count").and_then(Value::as_u64), Some(1));
}

#[tokio::test]
async fn sync_stream_full_snapshot_uses_current_ingest_state_over_http() {
    let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
    let tail_dir = TempDir::new().expect("tail temp directory should exist");
    let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
    let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");

    let insert_batch = text_insert_commit_batch(
        92,
        920,
        1,
        "tasks",
        &[
            &[
                ("id", "task-runtime-http-snapshot"),
                ("org_id", "org-runtime"),
                ("project_id", "project-runtime"),
                ("title", "Runtime HTTP Snapshot"),
                ("status", "todo"),
                ("priority", "3"),
                ("assignee_id", "user-runtime"),
                ("story_points", "5"),
                ("updated_at", "2026-04-11T00:00:00Z"),
                ("summary", "runtime:http:snapshot"),
            ],
            &[
                ("id", "task-runtime-http-delete"),
                ("org_id", "org-runtime"),
                ("project_id", "project-runtime"),
                ("title", "Runtime HTTP Delete"),
                ("status", "todo"),
                ("priority", "1"),
                ("assignee_id", "user-runtime"),
                ("story_points", "1"),
                ("updated_at", "2026-04-11T00:00:00Z"),
                ("summary", "runtime:http:delete"),
            ],
        ],
    );
    ingest_store
        .persist_batch(&insert_batch)
        .expect("persist insert batch");
    mark_ingest_ready(&ingest_store);

    let storage: powersync_mdbx::SharedStorage = Arc::new(WireMdbxStorage::new_with_ingest(
        snapshot_dir.path(),
        tail_dir.path(),
        ingest_dir.path(),
    ));
    let app = app_with_storage(storage);

    // First request, before the update/delete batch: capture the v1 row
    // payloads and pin the initial-snapshot checkpoint math. With no tail
    // remainder yet, the checksum is the wrapping sum of both PUT checksums
    // and the count is the number of live rows.
    let initial_response = app
        .clone()
        .oneshot(single_bucket_sync_request("1#tasks|0[]"))
        .await
        .unwrap();
    assert_eq!(initial_response.status(), StatusCode::OK);
    let initial_lines = parse_ndjson_lines(&read_body_text(initial_response).await);
    let initial_entries = data_entries(&initial_lines);
    let initial_data_for = |object_id: &str| -> String {
        initial_entries
            .iter()
            .find(|entry| entry.get("object_id").and_then(Value::as_str) == Some(object_id))
            .and_then(|entry| entry.get("data"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("initial snapshot should serve {object_id}"))
            .to_owned()
    };
    let snapshot_v1_checksum = put_checksum(
        "tasks",
        "task-runtime-http-snapshot",
        &initial_data_for("task-runtime-http-snapshot"),
    );
    let delete_v1_checksum = put_checksum(
        "tasks",
        "task-runtime-http-delete",
        &initial_data_for("task-runtime-http-delete"),
    );
    let initial_bucket = checkpoint_bucket(&initial_lines, "1#tasks|0[]");
    assert_eq!(
        initial_bucket.get("checksum").and_then(Value::as_i64),
        Some(i64::from(protocol_checksum_i32(
            snapshot_v1_checksum.wrapping_add(delete_v1_checksum)
        ))),
        "initial checkpoint checksum should be the wrapping sum of both PUT checksums"
    );
    assert_eq!(initial_bucket.get("count").and_then(Value::as_u64), Some(2));

    let update_delete_batch = ReplicationCommitBatch {
        transaction_id: 93,
        begin_final_lsn: PostgresLsn(923),
        begin_commit_time_micros: 3,
        commit_lsn: PostgresLsn(924),
        end_lsn: PostgresLsn(925),
        commit_time_micros: 4,
        column_types_by_table: std::collections::BTreeMap::new(),
        changes: vec![
            ChangeEvent::update(
                "public",
                "tasks",
                1,
                Some(RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-http-snapshot")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime HTTP Snapshot")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("3")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("5")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:http:snapshot")),
                ])),
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-http-snapshot")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime HTTP Snapshot Updated")),
                    ("status", ColumnValue::text("done")),
                    ("priority", ColumnValue::text("4")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("8")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:01:00Z")),
                    (
                        "summary",
                        ColumnValue::text("runtime:http:snapshot:updated"),
                    ),
                ]),
                ReplicaIdentity::Default,
                Vec::new(),
                Lsn::from(925_u64),
            ),
            ChangeEvent::delete(
                "public",
                "tasks",
                1,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-http-delete")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime HTTP Delete")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("1")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("1")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:http:delete")),
                ]),
                ReplicaIdentity::Default,
                Vec::new(),
                Lsn::from(925_u64),
            ),
        ],
    };
    ingest_store
        .persist_batch(&update_delete_batch)
        .expect("persist update/delete batch");

    let response = app
        .oneshot(single_bucket_sync_request("1#tasks|0[]"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_text = read_body_text(response).await;
    let lines = parse_ndjson_lines(&body_text);
    let entries = data_entries(&lines);

    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.get("object_id").and_then(Value::as_str)
                == Some("task-runtime-http-snapshot"))
            .count(),
        1,
    );
    assert!(entries.iter().any(|entry| {
        entry.get("object_id").and_then(Value::as_str) == Some("task-runtime-http-snapshot")
            && entry
                .get("data")
                .and_then(Value::as_str)
                .is_some_and(|json| json.contains("Runtime HTTP Snapshot Updated"))
            && entry
                .get("data")
                .and_then(Value::as_str)
                .is_some_and(|json| json.contains("\"status\":\"done\""))
    }));
    assert!(entries.iter().all(|entry| {
        entry.get("object_id").and_then(Value::as_str) != Some("task-runtime-http-delete")
    }));
    assert!(
        !body_text.contains("task-org-001-0001-0001"),
        "router-level full snapshot should be sourced from current ingest state once available"
    );

    // Checkpoint checksum pin (after update + delete): the read path combines
    // the current-state accumulator (the surviving row's updated PUT) with the
    // persisted tail remainder. The ingest tail models the update as
    // REMOVE(old) + PUT(new), so the remainder accumulates the superseded v1
    // PUT checksum and a REMOVE tombstone checksum for each of the two
    // retired row versions (updated and deleted). Recompute both sides with
    // the crate's own helpers and wrapping u32/i32 arithmetic.
    let updated_data = entries
        .iter()
        .find(|entry| {
            entry.get("object_id").and_then(Value::as_str) == Some("task-runtime-http-snapshot")
        })
        .and_then(|entry| entry.get("data"))
        .and_then(Value::as_str)
        .expect("updated snapshot row should carry data");
    let updated_checksum = put_checksum("tasks", "task-runtime-http-snapshot", updated_data);
    let tail_remainder_checksum = snapshot_v1_checksum
        .wrapping_add(remove_checksum(&source_subkey_for_object(
            "tasks",
            "task-runtime-http-snapshot",
        )))
        .wrapping_add(delete_v1_checksum)
        .wrapping_add(remove_checksum(&source_subkey_for_object(
            "tasks",
            "task-runtime-http-delete",
        )));
    let bucket = checkpoint_bucket(&lines, "1#tasks|0[]");
    assert_eq!(
        bucket.get("checksum").and_then(Value::as_i64),
        Some(i64::from(protocol_checksum_i32(
            updated_checksum.wrapping_add(tail_remainder_checksum)
        ))),
        "checkpoint checksum should combine the current accumulator with the tail remainder"
    );
    assert_eq!(
        bucket.get("count").and_then(Value::as_u64),
        Some(5),
        "checkpoint count should cover the live PUT plus the four superseded tail ops"
    );
    // The synthetic CLEAR entry carries the tail remainder so client-side
    // checksum validation over the served ops matches the checkpoint.
    let clear_entry = entries
        .iter()
        .find(|entry| entry.get("op").and_then(Value::as_str) == Some("CLEAR"))
        .expect("full snapshot after compaction should emit a CLEAR entry");
    assert_eq!(
        clear_entry.get("checksum").and_then(Value::as_u64),
        Some(u64::from(tail_remainder_checksum)),
        "CLEAR entry checksum should equal the persisted tail remainder"
    );
}

#[tokio::test]
async fn hold_open_sync_stream_emits_follow_up_task_updates() {
    let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
    let tail_dir = TempDir::new().expect("tail temp directory should exist");
    let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
    let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");

    let initial_batch = text_insert_commit_batch(
        101,
        1_000,
        1,
        "tasks",
        &[&[
            ("id", "task-runtime-baseline"),
            ("org_id", "org-runtime"),
            ("project_id", "project-runtime"),
            ("title", "Runtime Baseline"),
            ("status", "todo"),
            ("priority", "2"),
            ("assignee_id", "user-runtime"),
            ("story_points", "3"),
            ("updated_at", "2026-04-11T00:00:00Z"),
            ("summary", "runtime:baseline"),
        ]],
    );
    ingest_store
        .persist_batch(&initial_batch)
        .expect("persist initial runtime batch");
    mark_ingest_ready(&ingest_store);
    let current_after = ingest_store
        .task_tail_last_op_id()
        .expect("tail last op id read should succeed")
        .expect("tail last op id should exist");

    let storage: powersync_mdbx::SharedStorage = Arc::new(WireMdbxStorage::new_with_ingest(
        snapshot_dir.path(),
        tail_dir.path(),
        ingest_dir.path(),
    ));

    let response = app_with_storage(storage)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"raw_data":true,"client_id":"benchmark-client","buckets":[{{"name":"1#tasks|0[]","after":"{current_after}"}}]}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-powersync-emission-path").unwrap(),
        "chunk-stream"
    );

    let mut body = response.into_body();
    let initial_checkpoint = tokio::time::timeout(Duration::from_millis(250), body.frame())
        .await
        .expect("initial checkpoint frame should arrive")
        .expect("initial checkpoint frame should exist")
        .expect("initial checkpoint frame should succeed")
        .into_data()
        .expect("initial checkpoint frame should contain data");
    assert!(
        std::str::from_utf8(&initial_checkpoint)
            .expect("initial checkpoint should be utf8")
            .contains("\"checkpoint\""),
        "hold-open stream should emit a checkpoint immediately"
    );

    let initial_complete = tokio::time::timeout(Duration::from_millis(250), body.frame())
        .await
        .expect("initial checkpoint_complete frame should arrive")
        .expect("initial checkpoint_complete frame should exist")
        .expect("initial checkpoint_complete frame should succeed")
        .into_data()
        .expect("initial checkpoint_complete frame should contain data");
    assert!(
        std::str::from_utf8(&initial_complete)
            .expect("initial checkpoint_complete should be utf8")
            .contains("\"checkpoint_complete\""),
        "hold-open stream should complete the initial checkpoint before waiting"
    );

    let update_batch = text_insert_commit_batch(
        102,
        1_010,
        3,
        "tasks",
        &[&[
            ("id", "task-runtime-hold-open"),
            ("org_id", "org-runtime"),
            ("project_id", "project-runtime"),
            ("title", "Runtime Hold Open Insert"),
            ("status", "todo"),
            ("priority", "4"),
            ("assignee_id", "user-runtime"),
            ("story_points", "8"),
            ("updated_at", "2026-04-11T00:00:01Z"),
            ("summary", "runtime:hold-open"),
        ]],
    );
    ingest_store
        .persist_batch(&update_batch)
        .expect("persist update runtime batch");

    let mut observed_update_payloads = Vec::new();
    while observed_update_payloads.len() < 6 {
        let next_frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("follow-up frame should arrive after update")
            .expect("follow-up frame should exist")
            .expect("follow-up frame should succeed");
        let bytes = next_frame
            .into_data()
            .expect("follow-up frame should contain data");
        let text = std::str::from_utf8(&bytes)
            .expect("follow-up frame should be utf8")
            .to_owned();
        observed_update_payloads.push(text.clone());
        let combined = observed_update_payloads.join("");
        if combined.contains("task-runtime-hold-open")
            && combined.contains("\"checkpoint_complete\"")
        {
            break;
        }
    }

    let combined = observed_update_payloads.join("");
    assert!(
        combined.contains("\"checkpoint\""),
        "follow-up stream should emit a fresh checkpoint when new data arrive"
    );
    assert!(
        combined.contains("task-runtime-hold-open"),
        "follow-up stream should emit the newly ingested task row"
    );
    assert!(
        combined.contains("\"checkpoint_complete\""),
        "follow-up stream should complete the new checkpoint after sending task data"
    );
}

fn decode_concatenated_bson_docs(bytes: &[u8]) -> Vec<Document> {
    let mut docs = Vec::new();
    let mut offset = 0usize;

    while offset < bytes.len() {
        let len_bytes: [u8; 4] = bytes[offset..offset + 4]
            .try_into()
            .expect("bson length prefix should exist");
        let doc_len = i32::from_le_bytes(len_bytes) as usize;
        let doc_end = offset + doc_len;
        let mut cursor = Cursor::new(&bytes[offset..doc_end]);
        let doc = Document::from_reader(&mut cursor).expect("valid bson document");
        docs.push(doc);
        offset = doc_end;
    }

    docs
}

fn assert_debug_timing_headers(headers: &axum::http::HeaderMap) {
    for name in [
        "x-powersync-request-ms",
        "x-powersync-total-request-ms",
        "x-powersync-pre-handler-ms",
        "x-powersync-request-us",
        "x-powersync-total-request-us",
        "x-powersync-pre-handler-us",
    ] {
        let value = headers
            .get(name)
            .unwrap_or_else(|| panic!("missing debug timing header {name}"))
            .to_str()
            .unwrap_or_else(|_| panic!("header {name} should be valid utf-8"));
        value
            .parse::<u128>()
            .unwrap_or_else(|_| panic!("header {name} should be an unsigned integer"));
    }
}

fn assert_healthz_startup_headers(headers: &axum::http::HeaderMap) {
    for name in ["x-powersync-uptime-us", "x-powersync-boot-unix-ms"] {
        let value = headers
            .get(name)
            .unwrap_or_else(|| panic!("missing startup header {name}"))
            .to_str()
            .unwrap_or_else(|_| panic!("header {name} should be valid utf-8"));
        value
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("header {name} should be an unsigned integer"));
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn signed_hs256_token(secret: &[u8], kid: Option<&str>, payload: Value) -> String {
    let header = if let Some(kid) = kid {
        serde_json::json!({"alg": "HS256", "typ": "JWT", "kid": kid})
    } else {
        serde_json::json!({"alg": "HS256", "typ": "JWT"})
    };
    let header_segment = URL_SAFE_NO_PAD.encode(header.to_string());
    let payload_segment = URL_SAFE_NO_PAD.encode(payload.to_string());
    let mut mac = HmacSha256::new_from_slice(secret).expect("secret should be valid");
    mac.update(format!("{header_segment}.{payload_segment}").as_bytes());
    let signature_segment = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{header_segment}.{payload_segment}.{signature_segment}")
}
