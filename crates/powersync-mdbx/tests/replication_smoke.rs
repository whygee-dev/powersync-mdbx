use std::{
    env,
    sync::{Arc, OnceLock},
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    http::{header, Request},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use pg_walstream::{ColumnValue, RowData};
use powersync_mdbx::{
    auth::{TokenPayload, UserAuthConfig},
    build_app_with_storage_and_context,
    control_plane::{ResolvedParameterContext, ServiceContext},
    replication::{
        ingest::ReplicationMdbxStore,
        postgres::PostgresLsn,
        runner::{
            run_replication_ingest_with_store, ReplicationRunnerOptions, ReplicationStream,
            ReplicationStreamEvent,
        },
        runtime::ReplicationBootstrap,
        PostgresReplicationConfig,
    },
    storage::WireMdbxStorage,
    sync_rules::{compile_sync_rules_source, lower_canonical_semantic_plan},
};
use sha2::Sha256;
use tempfile::TempDir;
use tokio::{
    sync::{Mutex, MutexGuard},
    task::JoinHandle,
    time::{sleep, timeout, Instant},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;

const LIVE_TEST_URI: &str = "POWERSYNC_RUST_LIVE_TEST_POSTGRES_URI";
const DEFAULT_LIVE_TEST_URI: &str =
    "postgres://postgres:postgres@127.0.0.1:5432/powersync_benchmark_test?sslmode=disable";
const DEFAULT_TASKS_BUCKET_REQUEST_BODY: &str =
    r#"{"buckets":[{"name":"1#tasks|0[]","after":"0"}]}"#;

#[tokio::test]
#[ignore = "requires a live PostgreSQL; run via cargo test --test replication_smoke -- --ignored"]
async fn streams_a_unique_logical_message_from_postgres() {
    let _guard = live_test_guard().await;
    let context = LiveReplicationContext::new("rust_smoke", None).await;
    let prefix = format!("powersync.rust.smoke.{}", context.suffix);
    let payload = format!("payload-{}", context.suffix);

    let mut stream = ReplicationStream::start(&context.config)
        .await
        .expect("start replication stream");
    context
        .client
        .execute(
            "SELECT pg_logical_emit_message(false, $1, $2)",
            &[&prefix, &payload],
        )
        .await
        .expect("emit logical message");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut found = false;

    while Instant::now() < deadline {
        let event = match timeout(Duration::from_millis(500), stream.recv()).await {
            Ok(Ok(Some(event))) => event,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => panic!("replication stream failed: {error}"),
            Err(_) => continue,
        };

        if let Some(durable_lsn) = event.durable_lsn() {
            stream.update_applied_lsn(durable_lsn);
        }

        if let ReplicationStreamEvent::Message {
            transactional,
            prefix: seen_prefix,
            content,
            ..
        } = event
        {
            if !transactional && seen_prefix == prefix && content == payload {
                found = true;
                break;
            }
        }
    }

    stream
        .shutdown()
        .await
        .expect("shutdown replication stream");
    context.finish().await;

    assert!(found, "expected to receive the emitted logical message");
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL; run via cargo test --test replication_smoke -- --ignored"]
async fn validates_explicit_publication_shape_from_postgres_catalogs() {
    let _guard = live_test_guard().await;
    let context = LiveReplicationContext::new("rust_publication", Some("tasks")).await;
    context
        .client
        .batch_execute(&format!(
            "CREATE PUBLICATION \"{}\" FOR TABLE tasks",
            context.publication_name
        ))
        .await
        .expect("create unrestricted explicit publication");

    let bootstrap = ReplicationBootstrap::from_config(&context.config, PostgresLsn(0))
        .expect("replication bootstrap");
    let control_plane = bootstrap
        .connect_control_plane()
        .await
        .expect("connect publication validator");
    bootstrap
        .ensure_publication_covers(&control_plane.client, &["tasks"])
        .await
        .expect("unrestricted explicit publication should be accepted");

    context
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION \"{}\" SET TABLE tasks WHERE (status = 'active')",
            context.publication_name
        ))
        .await
        .expect("add publication row filter");
    let error = bootstrap
        .ensure_publication_covers(&control_plane.client, &["tasks"])
        .await
        .expect_err("filtered publication should be rejected");
    assert!(error
        .to_string()
        .contains("uses row filters for required source tables: public.tasks"));

    context
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION \"{}\" SET TABLE tasks (id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary)",
            context.publication_name
        ))
        .await
        .expect("replace row filter with explicit column list");
    bootstrap
        .ensure_publication_covers(&control_plane.client, &["tasks"])
        .await
        .expect("a complete explicit column list should be accepted");

    context
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION \"{}\" SET TABLE tasks (id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at)",
            context.publication_name
        ))
        .await
        .expect("omit one publication column");
    let error = bootstrap
        .ensure_publication_covers(&control_plane.client, &["tasks"])
        .await
        .expect_err("an incomplete publication column list should be rejected");
    assert!(error
        .to_string()
        .contains("omits columns from required source tables: public.tasks"));

    context
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION \"{}\" SET TABLE tasks; \
             ALTER TABLE tasks ADD COLUMN title_length integer GENERATED ALWAYS AS (length(title)) STORED",
            context.publication_name
        ))
        .await
        .expect("add generated source column");
    let error = bootstrap
        .ensure_publication_covers(&control_plane.client, &["tasks"])
        .await
        .expect_err("unpublished generated column should be rejected");
    assert!(error
        .to_string()
        .contains("omits columns from required source tables: public.tasks"));

    context
        .client
        .batch_execute(&format!(
            "CREATE TABLE partitioned_tasks (id text, region integer) PARTITION BY LIST (region); \
             CREATE TABLE partitioned_tasks_1 PARTITION OF partitioned_tasks FOR VALUES IN (1); \
             DROP PUBLICATION \"{}\"; \
             CREATE PUBLICATION \"{}\" FOR ALL TABLES",
            context.publication_name, context.publication_name
        ))
        .await
        .expect("publish all tables including partitioned parent");
    let error = bootstrap
        .ensure_publication_covers(&control_plane.client, &["partitioned_tasks"])
        .await
        .expect_err("partitioned source parent should be rejected");
    assert!(
        error.to_string().contains(
            "uses partition or inheritance parents as required source tables: public.partitioned_tasks"
        ),
        "unexpected publication validation error: {error}"
    );
    context
        .client
        .batch_execute("DROP TABLE partitioned_tasks")
        .await
        .expect("drop partitioned test table");

    control_plane
        .shutdown()
        .await
        .expect("close publication validator");
    context.finish().await;
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL; run via cargo test --test replication_smoke -- --ignored"]
async fn ingests_task_insert_and_serves_it_via_wire_mdbx_http() {
    let _guard = live_test_guard().await;
    let harness = LiveIngestHarness::new("rust_ingest").await;
    let runtime_task_id = format!("task-runtime-live-{}", harness.context.suffix);
    let runtime_title = format!("Runtime live insert {}", harness.context.suffix);
    let before_insert = harness
        .ingest_store
        .task_tail_last_op_id()
        .expect("tail cursor")
        .unwrap_or(0);

    insert_task(
        &harness.context.client,
        &runtime_task_id,
        &runtime_title,
        "runtime:live:insert",
    )
    .await;
    wait_until_task_tail_exceeds(&harness.ingest_store, before_insert).await;

    let response = harness
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(DEFAULT_TASKS_BUCKET_REQUEST_BODY))
                .unwrap(),
        )
        .await
        .expect("sync request should succeed");

    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body should collect")
        .to_bytes();
    let body_text = std::str::from_utf8(&body).expect("response body should be utf-8 ndjson");

    assert!(body_text.contains(&runtime_task_id));
    assert!(body_text.contains(&runtime_title));
    assert!(
        !body_text.contains("task-org-001-0001-0001"),
        "live full snapshot should come from ingest-produced state, not fixture seed rows"
    );

    harness.finish().await;
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL; run via cargo test --test replication_smoke -- --ignored"]
async fn hold_open_sync_stream_emits_follow_up_task_updates_via_live_replication() {
    let _guard = live_test_guard().await;
    let harness = LiveIngestHarness::new("rust_hold_open").await;
    let baseline_task_id = format!("task-runtime-baseline-{}", harness.context.suffix);
    let follow_up_task_id = format!("task-runtime-follow-up-{}", harness.context.suffix);
    let before_baseline_insert = harness
        .ingest_store
        .task_tail_last_op_id()
        .expect("tail cursor")
        .unwrap_or(0);

    insert_task(
        &harness.context.client,
        &baseline_task_id,
        "Runtime Baseline",
        "runtime:baseline",
    )
    .await;
    wait_until_task_tail_exceeds(&harness.ingest_store, before_baseline_insert).await;
    let current_after = harness
        .ingest_store
        .task_tail_last_op_id()
        .expect("tail last op id read should succeed")
        .expect("tail last op id should exist");

    let response = harness
        .app()
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
        .expect("hold-open sync request should succeed");

    let mut body = response.into_body();
    let initial_checkpoint = next_body_frame(&mut body, Duration::from_millis(250)).await;
    assert!(
        std::str::from_utf8(&initial_checkpoint)
            .expect("initial checkpoint should be utf8")
            .contains("\"checkpoint\""),
        "hold-open stream should emit a checkpoint immediately"
    );

    let initial_complete = next_body_frame(&mut body, Duration::from_millis(250)).await;
    assert!(
        std::str::from_utf8(&initial_complete)
            .expect("initial checkpoint_complete should be utf8")
            .contains("\"checkpoint_complete\""),
        "hold-open stream should complete the initial checkpoint before waiting"
    );

    insert_task(
        &harness.context.client,
        &follow_up_task_id,
        "Runtime Hold Open Insert",
        "runtime:hold-open",
    )
    .await;
    wait_until_task_tail_exceeds(&harness.ingest_store, current_after).await;

    let mut observed_update_payloads = Vec::new();
    while observed_update_payloads.len() < 6 {
        let next_frame = next_body_frame(&mut body, Duration::from_secs(1)).await;
        let text = std::str::from_utf8(&next_frame)
            .expect("follow-up frame should be utf8")
            .to_owned();
        observed_update_payloads.push(text.clone());
        let combined = observed_update_payloads.join("");
        if combined.contains(&follow_up_task_id) && combined.contains("\"checkpoint_complete\"") {
            break;
        }
    }

    let combined = observed_update_payloads.join("");
    assert!(
        combined.contains("\"checkpoint\""),
        "follow-up stream should emit a fresh checkpoint when new data arrive"
    );
    assert!(
        combined.contains(&follow_up_task_id),
        "follow-up stream should emit the newly replicated task row"
    );
    assert!(
        combined.contains("\"checkpoint_complete\""),
        "follow-up stream should complete the new checkpoint after sending task data"
    );

    harness.finish().await;
}

#[tokio::test]
async fn resolves_fixture_style_lookup_rows_from_mdbx() {
    let plan = lower_canonical_semantic_plan(
        compile_sync_rules_source(parameter_sync_rules()).expect("parameter rules should compile"),
    )
    .expect("parameter rules should lower");
    let lookup = &plan
        .lookup_table_plan("Membership")
        .expect("Membership lookup table plan")
        .lookups[0];
    let token = TokenPayload::new_for_tests(
        serde_json::json!({"sub": "user-1"}),
        Some("user-1".to_owned()),
    );
    let request_parameters = serde_json::Map::from_iter([
        (
            "schema_version".to_owned(),
            serde_json::Value::String("web".to_owned()),
        ),
        (
            "selectedTeamId".to_owned(),
            serde_json::Value::String("team-1".to_owned()),
        ),
    ]);
    let context = ResolvedParameterContext::from_request(Some(&token), &request_parameters);
    let key_values = lookup
        .key_bindings
        .iter()
        .map(|(_, binding)| {
            context
                .binding_value(binding, &std::collections::BTreeMap::new())
                .expect("lookup key binding should resolve")
        })
        .collect::<Vec<_>>();
    let ingest_dir = TempDir::new().expect("ingest temp directory");
    let store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
    store
        .persist_initial_snapshot_lookup_rows(
            "Membership",
            membership_rows(),
            PostgresLsn(1),
            &plan,
        )
        .expect("persist lookup rows");
    let rows = store
        .read_parameter_lookup_rows(&lookup.lookup_id, &key_values, 10)
        .expect("read parameter lookup rows");
    let workspace_ids = rows
        .iter()
        .filter_map(|row| row.get("workspaceId").cloned())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        workspace_ids,
        std::collections::BTreeSet::from(["ws-1".to_owned(), "ws-2".to_owned()])
    );
}

#[tokio::test]
async fn sync_stream_subscription_resolves_fixture_parameter_query_buckets_over_http() {
    let state_dir = TempDir::new().expect("state temp directory should exist");
    let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
    let tail_dir = TempDir::new().expect("tail temp directory should exist");
    let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
    let sync_rules = parameter_sync_rules();
    unsafe {
        env::set_var("POWERSYNC_RUST_SYNC_RULES", sync_rules);
    }
    let service_context = ServiceContext::new_for_tests(
        state_dir.path().join("sync-rules-state.json"),
        vec![],
        Some(
            UserAuthConfig::from_hs256_secrets(
                vec![(None, b"stream-secret".to_vec())],
                vec!["powersync".to_owned()],
                vec!["https://issuer.example".to_owned()],
            )
            .expect("valid auth policy"),
        ),
        vec![],
    )
    .expect("service context");
    unsafe {
        env::remove_var("POWERSYNC_RUST_SYNC_RULES");
    }

    let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
    ingest_store
        .persist_initial_snapshot_lookup_rows(
            "Membership",
            membership_rows(),
            PostgresLsn(1),
            service_context.active_plan().as_ref(),
        )
        .expect("persist lookup rows");
    ingest_store
        .persist_initial_snapshot_marker_with_plan(
            PostgresLsn(0),
            service_context.active_plan().as_ref(),
            "live-parameter-query-source",
        )
        .expect("mark parameter-query storage ready");
    let storage: powersync_mdbx::SharedStorage = Arc::new(WireMdbxStorage::new_with_ingest(
        snapshot_dir.path(),
        tail_dir.path(),
        ingest_dir.path(),
    ));
    let token = signed_hs256_token(
        b"stream-secret",
        serde_json::json!({
            "sub": "user-1",
            "aud": "powersync",
            "iss": "https://issuer.example",
            "exp": 4_102_444_800_u64
        }),
    );
    let response = powersync_mdbx::build_app_with_storage_and_context(storage, service_context)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync/stream")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::ACCEPT, "application/x-ndjson")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{
                        "parameters": {
                            "schema_version": "web",
                            "selectedTeamId": "team-1"
                        },
                        "streams": {
                            "include_defaults": false,
                            "subscriptions": [{"stream": "web_workspaces"}]
                        }
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .expect("sync stream request should succeed");

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body should collect")
        .to_bytes();
    let body_text = std::str::from_utf8(&body).expect("response should be utf8 ndjson");
    assert!(body_text.contains(r#"1#web_workspaces|0[\"ws-1\"]"#));
    assert!(body_text.contains(r#"1#web_workspaces|0[\"ws-2\"]"#));
    assert!(!body_text.contains("ws-3"));
}

fn parameter_sync_rules() -> &'static str {
    r#"
config:
  edition: 3
streams:
  web_workspaces:
    with:
      web_workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE connection.parameters() ->> 'schema_version' = 'web' AND "userId" = auth.user_id() AND "workspaceId" IS NOT NULL AND "teamId" = connection.parameters() ->> 'selectedTeamId'
    queries:
      - 'SELECT "Workspace".* FROM "Workspace", web_workspace_scope AS bucket WHERE "Workspace"."id" = bucket."workspaceId"'
"#
}

fn membership_rows() -> Vec<RowData> {
    vec![
        RowData::from_pairs(vec![
            ("id", ColumnValue::text("membership-1")),
            ("userId", ColumnValue::text("user-1")),
            ("teamId", ColumnValue::text("team-1")),
            ("workspaceId", ColumnValue::text("ws-1")),
        ]),
        RowData::from_pairs(vec![
            ("id", ColumnValue::text("membership-2")),
            ("userId", ColumnValue::text("user-1")),
            ("teamId", ColumnValue::text("team-1")),
            ("workspaceId", ColumnValue::text("ws-2")),
        ]),
        RowData::from_pairs(vec![
            ("id", ColumnValue::text("membership-3")),
            ("userId", ColumnValue::text("other-user")),
            ("teamId", ColumnValue::text("team-1")),
            ("workspaceId", ColumnValue::text("ws-3")),
        ]),
    ]
}

fn signed_hs256_token(secret: &[u8], payload: serde_json::Value) -> String {
    let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});
    let header_segment = URL_SAFE_NO_PAD.encode(header.to_string());
    let payload_segment = URL_SAFE_NO_PAD.encode(payload.to_string());
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("secret should be valid");
    mac.update(format!("{header_segment}.{payload_segment}").as_bytes());
    let signature_segment = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{header_segment}.{payload_segment}.{signature_segment}")
}

struct LiveReplicationContext {
    client: Client,
    connection_task: JoinHandle<Result<(), tokio_postgres::Error>>,
    config: PostgresReplicationConfig,
    slot_name: String,
    publication_name: String,
    managed_table: Option<String>,
    suffix: String,
}

impl LiveReplicationContext {
    async fn new(prefix: &str, table_name: Option<&str>) -> Self {
        let uri = live_test_uri();
        let suffix = unique_suffix();
        let publication_name = format!("pub_{prefix}_{suffix}");
        let slot_name = format!("slot_{prefix}_{suffix}");
        let config = PostgresReplicationConfig {
            uri: uri.clone(),
            slot_name: slot_name.clone(),
            publication_name: publication_name.clone(),
            group_id: "default".to_owned(),
        };

        let (client, connection_task) = connect_control_plane(&uri).await;
        cleanup_replication_objects(&client, &slot_name, &publication_name, table_name).await;
        if table_name == Some("tasks") {
            recreate_tasks_table(&client).await;
        }

        Self {
            client,
            connection_task,
            config,
            slot_name,
            publication_name,
            managed_table: table_name.map(str::to_owned),
            suffix,
        }
    }

    async fn finish(self) {
        cleanup_replication_objects(
            &self.client,
            &self.slot_name,
            &self.publication_name,
            self.managed_table.as_deref(),
        )
        .await;
        self.connection_task.abort();
    }
}

struct LiveIngestHarness {
    context: LiveReplicationContext,
    ingest_store: Arc<ReplicationMdbxStore>,
    runner_task: JoinHandle<
        Result<
            powersync_mdbx::replication::runner::ReplicationRunSummary,
            powersync_mdbx::replication::runner::ReplicationRunnerError,
        >,
    >,
    ingest_dir: TempDir,
    snapshot_dir: TempDir,
    tail_dir: TempDir,
}

impl LiveIngestHarness {
    async fn new(prefix: &str) -> Self {
        let context = LiveReplicationContext::new(prefix, Some("tasks")).await;
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_store =
            ReplicationMdbxStore::shared(ingest_dir.path()).expect("shared ingest store");

        let runner_config = context.config.clone();
        let runner_store = Arc::clone(&ingest_store);
        let service_context = {
            const TASKS_ONLY_SYNC_RULES: &str = "edition: 3\ncompatibility_version: 1\nstorage_version: 1\nstreams:\n  tasks:\n    auto_subscribe: true\n    query: SELECT * FROM public.tasks\n";
            unsafe {
                env::set_var("POWERSYNC_RUST_SYNC_RULES", TASKS_ONLY_SYNC_RULES);
            }
            let service_context = ServiceContext::from_env().expect("tasks-only service context");
            unsafe {
                env::remove_var("POWERSYNC_RUST_SYNC_RULES");
            }
            service_context
        };
        let mut runner_task = tokio::spawn(async move {
            run_replication_ingest_with_store(
                &runner_config,
                ReplicationRunnerOptions { max_events: None },
                runner_store,
                service_context,
            )
            .await
        });

        tokio::select! {
            () = wait_until_slot_active(&context.client, &context.slot_name) => {}
            result = &mut runner_task => {
                panic!("replication runner exited before activating its slot: {result:?}");
            }
        }

        Self {
            context,
            ingest_store,
            runner_task,
            ingest_dir,
            snapshot_dir,
            tail_dir,
        }
    }

    fn storage(&self) -> powersync_mdbx::SharedStorage {
        Arc::new(WireMdbxStorage::new_with_ingest(
            self.snapshot_dir.path(),
            self.tail_dir.path(),
            self.ingest_dir.path(),
        ))
    }

    fn app(&self) -> axum::Router {
        let service_context = ServiceContext::new_for_tests(
            self.snapshot_dir.path().join("sync-rules-state.json"),
            Vec::new(),
            None,
            Vec::new(),
        )
        .expect("test service context")
        .with_allow_anonymous_sync(true);
        build_app_with_storage_and_context(self.storage(), service_context)
    }

    async fn finish(self) {
        self.runner_task.abort();
        let _ = self.runner_task.await;
        wait_until_slot_inactive(&self.context.client, &self.context.slot_name).await;
        self.context.finish().await;
    }
}

fn live_test_uri() -> String {
    env::var(LIVE_TEST_URI).unwrap_or_else(|_| DEFAULT_LIVE_TEST_URI.to_owned())
}

async fn live_test_guard() -> MutexGuard<'static, ()> {
    static LIVE_TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    LIVE_TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().await
}

fn unique_suffix() -> String {
    format!(
        "{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis()
    )
}

async fn connect_control_plane(
    uri: &str,
) -> (
    Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, connection) = tokio_postgres::connect(uri, NoTls)
        .await
        .expect("connect control plane");
    let connection_task = tokio::spawn(connection);
    (client, connection_task)
}

async fn recreate_tasks_table(client: &Client) {
    client
        .batch_execute(
            r#"
            DROP TABLE IF EXISTS tasks;
            CREATE TABLE tasks (
                id text PRIMARY KEY,
                org_id text NOT NULL,
                project_id text NOT NULL,
                title text NOT NULL,
                status text NOT NULL,
                priority integer NOT NULL,
                assignee_id text NOT NULL,
                story_points integer NOT NULL,
                updated_at text NOT NULL,
                summary text NOT NULL
            );
            "#,
        )
        .await
        .expect("recreate tasks table");
}

async fn insert_task(client: &Client, task_id: &str, title: &str, summary: &str) {
    client
        .execute(
            r#"
            INSERT INTO tasks (
                id, org_id, project_id, title, status, priority,
                assignee_id, story_points, updated_at, summary
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            &[
                &task_id,
                &"org-runtime",
                &"project-runtime",
                &title,
                &"todo",
                &3_i32,
                &"user-runtime",
                &5_i32,
                &"2026-04-11T00:00:00Z",
                &summary,
            ],
        )
        .await
        .expect("insert runtime task row");
}

async fn wait_until_slot_active(client: &Client, slot_name: &str) {
    wait_until_slot_state(client, slot_name, true).await;
}

async fn wait_until_slot_inactive(client: &Client, slot_name: &str) {
    wait_until_slot_state(client, slot_name, false).await;
}

async fn wait_until_slot_state(client: &Client, slot_name: &str, expected_active: bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let active = client
            .query_opt(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await
            .expect("query replication slot state")
            .and_then(|row| row.get::<usize, Option<bool>>(0))
            .unwrap_or(false);
        if active == expected_active {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let target = if expected_active {
        "active"
    } else {
        "inactive"
    };
    panic!("replication slot {slot_name} did not become {target} in time");
}

async fn wait_until_task_tail_exceeds(store: &ReplicationMdbxStore, previous: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let current = store
            .task_tail_last_op_id()
            .expect("read task tail metadata")
            .unwrap_or(0);
        if current > previous {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }

    panic!("task tail did not advance beyond {previous} in time");
}

async fn next_body_frame(body: &mut Body, timeout_after: Duration) -> Bytes {
    timeout(timeout_after, body.frame())
        .await
        .expect("response frame should arrive before timeout")
        .expect("response frame should exist")
        .expect("response frame should succeed")
        .into_data()
        .expect("response frame should contain data")
}

async fn cleanup_replication_objects(
    client: &Client,
    slot_name: &str,
    publication_name: &str,
    table_name: Option<&str>,
) {
    client
        .execute(
            "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active = FALSE)",
            &[&slot_name],
        )
        .await
        .ok();
    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS \"{publication_name}\";{}",
            table_name
                .map(|table| format!(" DROP TABLE IF EXISTS {table};"))
                .unwrap_or_default()
        ))
        .await
        .ok();
}
