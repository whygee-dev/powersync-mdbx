use std::collections::BTreeMap;

use pg_walstream::{ColumnValue, RowData};

use super::catalog::{
    SUPPORTED_COMPATIBILITY_VERSION, SUPPORTED_EDITION, SUPPORTED_STORAGE_VERSION,
};
use super::model::Predicate;
use super::*;

fn arbitrary_subset_plan() -> RustExecutionPlan {
    let canonical = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/projection_variants.sync-rules"
    ))
    .expect("arbitrary subset sync rules should compile");

    lower_canonical_semantic_plan(canonical).expect("arbitrary canonical plan should lower")
}

#[test]
fn parse_binding_unescapes_doubled_quotes_in_names() {
    // A binding name carrying an apostrophe must survive the SQL string-literal
    // round-trip: the emitter doubles it to '' and the parser folds it back to a
    // single '. Previously the parser stopped at the first quote, mis-binding it.
    assert_eq!(
        super::query::parse_binding("auth.parameter('o''brien')"),
        Some(CanonicalBinding::AuthParameter {
            name: "o'brien".to_owned()
        })
    );
    assert_eq!(
        super::query::parse_binding("request.parameters() ->> 'a''b'"),
        Some(CanonicalBinding::RequestParameter {
            name: "a'b".to_owned()
        })
    );
}

#[test]
fn split_top_level_csv_handles_single_and_double_quoted_identifiers() {
    // Locks the unified behavior shared with control_plane's parameter
    // query rewriting: a comma inside double quotes must not split.
    assert_eq!(
        split_top_level_csv(r#"users u, json_each(x) AS "a,b", 'lit,eral'"#),
        vec![r#"users u"#, r#"json_each(x) AS "a,b""#, "'lit,eral'"]
    );
    assert_eq!(split_top_level_csv("f(a, b), c"), vec!["f(a, b)", "c"]);
}

#[test]
fn builtin_sync_rules_source_compiles_to_fixture() {
    let fixture: CanonicalSemanticPlan = serde_json::from_str(include_str!(
        "../../tests/fixtures/sync_plan/benchmark_streams.json"
    ))
    .expect("fixture should deserialize");

    let compiled = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/benchmark_streams.sync-rules"
    ))
    .expect("benchmark source should compile");

    assert_eq!(compiled, fixture);
}

#[test]
fn builtin_canonical_plan_matches_fixture() {
    let fixture: CanonicalSemanticPlan = serde_json::from_str(include_str!(
        "../../tests/fixtures/sync_plan/benchmark_streams.json"
    ))
    .expect("fixture should deserialize");

    assert_eq!(execution_plan().canonical(), &fixture);
}

#[test]
fn request_binding_fixture_compiles_and_preserves_binding_sources() {
    let compiled = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/request_binding_variants.sync-rules"
    ))
    .expect("request binding fixture should compile");

    let bindings = compiled
        .streams
        .iter()
        .flat_map(|stream| {
            stream
                .bucket_parameters
                .iter()
                .map(|parameter| &parameter.binding)
        })
        .collect::<Vec<_>>();

    assert!(bindings
            .iter()
            .any(|binding| matches!(binding, CanonicalBinding::RequestParameter { name } if name == "project_id")));
    assert!(bindings
            .iter()
            .any(|binding| matches!(binding, CanonicalBinding::SubscriptionParameter { name } if name == "org_id")));
    assert!(bindings.iter().any(
        |binding| matches!(binding, CanonicalBinding::RequestJwt { claim } if claim == "org_id")
    ));
    assert!(bindings
        .iter()
        .any(|binding| matches!(binding, CanonicalBinding::RequestUserId)));
}

#[test]
fn compiles_official_config_wrapped_multiline_queries_and_lowercase_and() {
    let compiled = compile_sync_rules_source(
        r#"config:
  edition: 3
streams:
  tasks:
    auto_subscribe: true
    query: |
      SELECT id AS task_id, project_id, org_id
      FROM public.tasks
      WHERE project_id = auth.parameter('project_id') and org_id = request.jwt() ->> 'org_id'
"#,
    )
    .expect("config wrapped multiline sync rules should compile");

    assert_eq!(compiled.edition, SUPPORTED_EDITION);
    assert_eq!(
        compiled.compatibility_version,
        SUPPORTED_COMPATIBILITY_VERSION
    );
    assert_eq!(compiled.storage_version, SUPPORTED_STORAGE_VERSION);
    let stream = &compiled.streams[0];
    assert_eq!(stream.name, "tasks");
    assert_eq!(stream.source_table, "tasks");
    assert_eq!(stream.bucket_parameters.len(), 2);
    assert!(matches!(
        stream.bucket_parameters[0].binding,
        CanonicalBinding::AuthParameter { ref name } if name == "project_id"
    ));
    assert!(matches!(
        stream.bucket_parameters[1].binding,
        CanonicalBinding::RequestJwt { ref claim } if claim == "org_id"
    ));
}

#[test]
fn auth_perimeter_cte_compiles_to_parameter_query_bucket() {
    let compiled = compile_sync_rules_source(
            r#"config:
  edition: 3
streams:
  tasks_by_auth_project:
    with:
      accessible_projects: SELECT project_id AS project_id FROM user_project_access WHERE user_id = auth.user_id()
    queries:
      - SELECT tasks.id, tasks.org_id, tasks.project_id, tasks.title FROM tasks, accessible_projects AS bucket WHERE tasks.project_id = bucket.project_id
"#,
        )
        .expect("auth perimeter sync stream should compile");

    let stream = compiled
        .streams
        .iter()
        .find(|stream| stream.name == "tasks_by_auth_project")
        .expect("auth stream");
    assert_eq!(stream.source_table, "tasks");
    assert_eq!(stream.bucket_parameters.len(), 1);
    let parameter = &stream.bucket_parameters[0];
    assert_eq!(parameter.name, "project_id");
    assert_eq!(parameter.source_column, "project_id");
    assert!(matches!(
        &parameter.binding,
        CanonicalBinding::ParameterQueryColumn { name, lookup }
            if name == "project_id" && lookup.raw_query.contains("auth.user_id()")
    ));
}

#[test]
fn parameter_lookup_plan_parses_v1_forms() {
    let plan = parse_parameter_lookup_plan(
        r#"SELECT "workspaceId" AS "workspaceAlias", "other""x" AS "alias""x" FROM "Membership" WHERE "teamId" = connection.parameters() ->> 'teamId' AND auth.user_id() = "userId" AND "workspaceId" IS NOT NULL AND "kind" = 'shared'"#,
    )
    .expect("v1 lookup query should parse");
    assert_eq!(plan.source_table, "Membership");
    assert_eq!(
        plan.selected,
        vec![
            ParameterLookupSelectedColumn {
                alias: "workspaceAlias".to_owned(),
                column: "workspaceId".to_owned(),
            },
            ParameterLookupSelectedColumn {
                alias: "alias\"x".to_owned(),
                column: "other\"x".to_owned(),
            },
        ]
    );
    assert_eq!(
        plan.key_bindings
            .iter()
            .map(|(column, _)| column.as_str())
            .collect::<Vec<_>>(),
        vec!["teamId", "userId"]
    );
    assert!(matches!(plan.row_predicate, Some(Predicate::And { .. })));
}

#[test]
fn parameter_lookup_plan_accepts_reversed_key_and_literal_predicates() {
    let auth = parse_parameter_lookup_plan(
        r#"SELECT user_id FROM Membership WHERE auth.user_id() = "userId""#,
    )
    .expect("reversed binding should parse");
    assert_eq!(auth.key_bindings[0].0, "userId");

    let literal = parse_parameter_lookup_plan(
        "SELECT kind FROM Membership WHERE kind = 'shared' AND id = auth.parameter('id')",
    )
    .expect("string row predicate should parse");
    assert!(matches!(literal.row_predicate, Some(Predicate::Eq { .. })));
}

#[test]
fn parameter_lookup_plan_rejects_unsupported_forms() {
    for (query, expected) in [
        (
            "SELECT id FROM Membership WHERE id = auth.user_id() OR id = auth.parameter('id')",
            "OR",
        ),
        (
            "SELECT id FROM Membership WHERE id IN auth.user_id()",
            "IN",
        ),
        (
            "SELECT id FROM Membership, json_each(request.parameters() ->> 'ids') WHERE id = auth.user_id()",
            "json_each",
        ),
        (
            "SELECT id FROM Membership JOIN Users ON Users.id = Membership.user_id WHERE id = auth.user_id()",
            "joins",
        ),
        (
            "SELECT id FROM (SELECT id FROM Membership) WHERE id = auth.user_id()",
            "sub-selects",
        ),
        (
            "SELECT id FROM Membership WHERE id > auth.user_id()",
            "non-equality",
        ),
        (
            "SELECT id FROM Membership WHERE id = auth.user_id() AND id = auth.parameter('id')",
            "duplicate binding column",
        ),
        ("SELECT id WHERE id = auth.user_id()", "missing FROM"),
        ("SELECT FROM Membership WHERE id = auth.user_id()", "empty"),
        (
            "SELECT id FROM Membership WHERE auth.user_id() = auth.parameter('id')",
            "binding = binding",
        ),
        (
            "SELECT id FROM Membership WHERE id = connection.parameter('id')",
            "unsupported binding",
        ),
        (
            "SELECT id FROM analytics.Membership WHERE id = auth.user_id()",
            "public schema",
        ),
    ] {
        let error = parse_parameter_lookup_plan(query)
            .expect_err("unsupported lookup query should fail closed")
            .to_string();
        assert!(
            error.to_ascii_lowercase().contains(&expected.to_ascii_lowercase()),
            "error `{error}` should mention `{expected}`"
        );
    }
}

#[test]
fn unsupported_parameter_lookup_query_fails_the_full_compile() {
    let source = r#"config:
  edition: 3
with:
  scope: SELECT id FROM Membership WHERE user_id = auth.user_id() OR org_id = auth.parameter('org_id')
streams:
  first:
    query: SELECT * FROM Workspaces, scope AS bucket WHERE Workspaces.id = bucket.id
"#;
    let error = match compile_sync_rules_source(source) {
        Err(error) => error.to_string(),
        Ok(canonical) => lower_canonical_semantic_plan(canonical)
            .expect_err("unsupported lookup query must not lower")
            .to_string(),
    };
    assert!(
        error.contains("OR is not supported in parameter lookup queries"),
        "error `{error}` should carry the grammar rejection"
    );
}

#[test]
fn parameter_lookup_ids_are_stable_and_lowering_deduplicates() {
    let query = "SELECT id FROM Membership WHERE user_id = auth.user_id()";
    let first = parse_parameter_lookup_plan(query).expect("lookup query");
    let second = parse_parameter_lookup_plan(query).expect("lookup query");
    let different = parse_parameter_lookup_plan(
        "SELECT id FROM Membership WHERE user_id = auth.parameter('user_id')",
    )
    .expect("lookup query");
    assert_eq!(first.lookup_id, second.lookup_id);
    assert_ne!(first.lookup_id, different.lookup_id);

    let canonical = compile_sync_rules_source(
        r#"config:
  edition: 3
with:
  scope: SELECT id FROM Membership WHERE user_id = auth.user_id()
streams:
  first:
    query: SELECT * FROM Workspaces, scope AS bucket WHERE Workspaces.id = bucket.id
  second:
    query: SELECT * FROM Projects, scope AS bucket WHERE Projects.id = bucket.id
"#,
    )
    .expect("duplicate lookup source should compile");
    let plan = lower_canonical_semantic_plan(canonical).expect("execution plan");
    let tables = plan.lookup_source_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].lookups.len(), 1);
}

#[test]
fn bucket_catalog_exposes_default_tasks_bucket() {
    assert_eq!(
        bucket_catalog(),
        &[SyncBucketDescriptor {
            bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
            stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
            is_default: true,
        }]
    );
    assert!(is_supported_bucket(DEFAULT_TASKS_BUCKET_NAME));
    assert!(!is_supported_bucket("unknown"));
    assert_eq!(
        find_bucket_descriptor(DEFAULT_TASKS_BUCKET_NAME),
        Some(&SyncBucketDescriptor {
            bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
            stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
            is_default: true,
        })
    );
    assert_eq!(
        default_bucket_request().bucket_name(),
        DEFAULT_TASKS_BUCKET_NAME
    );
}

#[test]
fn resolves_project_task_bucket_requests() {
    let bucket_name = project_tasks_bucket_name("project-123");
    let resolved = resolve_bucket_request(&bucket_name).expect("project bucket");
    assert_eq!(resolved.bucket_name(), bucket_name);
    assert_eq!(resolved.stream_name(), TASKS_BY_PROJECT_STREAM_NAME);
    assert_eq!(resolved.object_type(), "tasks");
    assert_eq!(
        resolved
            .route_constraints()
            .get("project_id")
            .map(String::as_str),
        Some("project-123")
    );
}

#[test]
fn resolves_other_builtin_bucket_requests() {
    for (bucket_name, expected_stream, key, value) in [
        (
            org_tasks_bucket_name("org-123"),
            TASKS_BY_ORG_STREAM_NAME,
            "org_id",
            "org-123",
        ),
        (
            org_projects_bucket_name("org-123"),
            PROJECTS_BY_ORG_STREAM_NAME,
            "org_id",
            "org-123",
        ),
        (
            owner_projects_bucket_name("user-123"),
            PROJECTS_BY_OWNER_STREAM_NAME,
            "owner_id",
            "user-123",
        ),
        (
            task_comments_bucket_name("task-123"),
            COMMENTS_BY_TASK_STREAM_NAME,
            "task_id",
            "task-123",
        ),
        (
            org_comments_bucket_name("org-123"),
            COMMENTS_BY_ORG_STREAM_NAME,
            "org_id",
            "org-123",
        ),
        (
            org_memberships_bucket_name("org-123"),
            MEMBERSHIPS_BY_ORG_STREAM_NAME,
            "org_id",
            "org-123",
        ),
        (
            region_organizations_bucket_name("eu-west-1"),
            ORGANIZATIONS_BY_REGION_STREAM_NAME,
            "region",
            "eu-west-1",
        ),
    ] {
        let resolved = resolve_bucket_request(&bucket_name).expect("supported bucket");
        assert_eq!(resolved.stream_name(), expected_stream);
        assert_eq!(
            resolved.route_constraints().get(key).map(String::as_str),
            Some(value)
        );
    }
}

#[test]
fn compiles_arbitrary_in_subset_streams_without_table_allowlist() {
    let plan = arbitrary_subset_plan();
    let queue_bucket = plan
        .resolve_bucket_request("1#tickets_by_queue|0[\"queue-b\"]")
        .expect("queue bucket");
    assert_eq!(queue_bucket.stream_name(), "tickets_by_queue");
    assert_eq!(queue_bucket.object_type(), "tickets");
    assert_eq!(
        queue_bucket
            .route_constraints()
            .get("queue_id")
            .map(String::as_str),
        Some("queue-b")
    );
    assert_eq!(plan.default_bucket_requests().len(), 1);
    assert_eq!(plan.default_bucket_requests()[0].stream_name(), "tickets");
    assert!(plan.table_plan("tickets").is_some());
}

#[test]
fn lowers_generic_table_plan_with_union_route_columns() {
    let plan = arbitrary_subset_plan();
    let table = plan.table_plan("tickets").expect("tickets table plan");
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("ticket-1")),
        ("queue_id", ColumnValue::text("queue-a")),
        ("status", ColumnValue::text("open")),
        ("title", ColumnValue::text("Fix bug")),
    ]);
    assert_eq!(
        table
            .route_fields_for_row(&data, true)
            .expect("route fields"),
        BTreeMap::from([
            (String::from("queue_id"), String::from("queue-a")),
            (String::from("status"), String::from("open")),
        ])
    );
}

#[test]
fn preserves_per_stream_projection_for_same_table_streams() {
    let plan = arbitrary_subset_plan();
    let queue_bucket = plan
        .resolve_bucket_request("1#tickets_by_queue|0[\"queue-a\"]")
        .expect("queue bucket");
    let status_bucket = plan
        .resolve_bucket_request("1#tickets_by_status|0[\"open\"]")
        .expect("status bucket");
    let full_row_json = serde_json::json!({
        "id": "ticket-1",
        "queue_id": "queue-a",
        "status": "open",
        "title": "Fix bug"
    })
    .to_string();

    assert_eq!(
        queue_bucket
            .project_document_json("tickets", &full_row_json)
            .expect("queue projection"),
        serde_json::json!({
            "ticket_id": "ticket-1",
            "queue_id": "queue-a",
            "title": "Fix bug"
        })
        .to_string()
    );
    assert_eq!(
        status_bucket
            .project_document_json("tickets", &full_row_json)
            .expect("status projection"),
        serde_json::json!({
            "ticket_id": "ticket-1",
            "status": "open"
        })
        .to_string()
    );
}

#[test]
fn serializes_full_row_json_without_family_specific_code() {
    let plan = arbitrary_subset_plan();
    let table = plan.table_plan("tickets").expect("tickets table plan");
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("ticket-1")),
        ("queue_id", ColumnValue::text("queue-a")),
        ("status", ColumnValue::text("open")),
        ("title", ColumnValue::text("Fix bug")),
    ]);

    assert_eq!(
        table.serialize_full_row_json(&data).expect("full row json"),
        serde_json::json!({
            "id": "ticket-1",
            "queue_id": "queue-a",
            "status": "open",
            "title": "Fix bug"
        })
        .to_string()
    );
}

#[test]
fn presorted_full_row_json_matches_canonical_serialization() {
    let plan = arbitrary_subset_plan();
    let table = plan.table_plan("tickets").expect("tickets table plan");
    let unsorted = RowData::from_pairs(vec![
        ("title", ColumnValue::text("Fix bug")),
        ("status", ColumnValue::text("open")),
        ("queue_id", ColumnValue::text("queue-a")),
        ("id", ColumnValue::text("ticket-1")),
    ]);
    let presorted = RowData::from_pairs(vec![
        ("id", ColumnValue::text("ticket-1")),
        ("queue_id", ColumnValue::text("queue-a")),
        ("status", ColumnValue::text("open")),
        ("title", ColumnValue::text("Fix bug")),
    ]);

    assert_eq!(
        table.serialize_full_row_json(&unsorted).expect("canonical"),
        table
            .serialize_full_row_json_presorted(&presorted)
            .expect("presorted")
    );
}

#[test]
fn typed_projection_json_matches_powersync_postgres_scalars() {
    let plan = lower_canonical_semantic_plan(
            compile_sync_rules_source(
                r#"
config:
  edition: 3
streams:
  tasks:
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM tasks
"#,
            )
            .expect("tasks stream should compile"),
        )
        .expect("tasks stream should lower");
    let table = plan.table_plan("tasks").expect("tasks table plan");
    let bucket = plan
        .resolve_bucket_request(DEFAULT_TASKS_BUCKET_NAME)
        .expect("tasks default bucket");
    let row = RowData::from_pairs(vec![
        ("id", ColumnValue::text("task-1")),
        ("org_id", ColumnValue::text("org-1")),
        ("project_id", ColumnValue::text("project-1")),
        ("title", ColumnValue::text("Fix bug")),
        ("status", ColumnValue::text("todo")),
        ("priority", ColumnValue::text("1")),
        ("assignee_id", ColumnValue::text("user-1")),
        ("story_points", ColumnValue::text("2")),
        ("updated_at", ColumnValue::text("2026-01-01 00:00:00+00")),
        ("summary", ColumnValue::text("summary")),
    ]);
    let column_types = BTreeMap::from([
        ("priority".to_owned(), JsonColumnType::Number),
        ("story_points".to_owned(), JsonColumnType::Number),
        ("updated_at".to_owned(), JsonColumnType::Timestamp),
    ]);
    let full_row = table
        .serialize_full_row_json_with_column_types(&row, &column_types)
        .expect("typed full row");

    assert_eq!(
        bucket
            .project_document_json("tasks", &full_row)
            .expect("typed projection"),
        r#"{"id":"task-1","org_id":"org-1","project_id":"project-1","title":"Fix bug","status":"todo","priority":1,"assignee_id":"user-1","story_points":2,"updated_at":"2026-01-01T00:00:00.000000Z","summary":"summary"}"#
    );
}

#[test]
fn snapshot_row_projection_matches_document_projection_order() {
    let plan = arbitrary_subset_plan();
    let table = plan.table_plan("tickets").expect("tickets table plan");
    let bucket = plan
        .resolve_bucket_request("1#tickets_by_queue|0[\"queue-a\"]")
        .expect("queue bucket");
    let row = RowData::from_pairs(vec![
        ("id", ColumnValue::text("ticket-1")),
        ("queue_id", ColumnValue::text("queue-a")),
        ("status", ColumnValue::text("open")),
        ("title", ColumnValue::text("Fix bug")),
    ]);
    let full_row = table
        .serialize_full_row_json_presorted(&row)
        .expect("full row");

    assert_eq!(
        table
            .project_row_json_from_serialized(&row, bucket.projection(), &full_row, None)
            .expect("snapshot projection"),
        bucket
            .project_document_json("tickets", &full_row)
            .expect("document projection")
    );
}

#[test]
fn rejects_unsupported_where_predicates() {
    let error = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/reject/unsupported_predicate.sync-rules"
    ))
    .expect_err("non-equality predicate should be rejected");
    assert!(error.to_string().contains("unsupported WHERE predicate"));
}

#[test]
fn rejects_boolean_join_and_output_remap_fixture() {
    let error = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/reject/disallowed_boolean_join_remap.sync-rules"
    ))
    .expect_err("boolean join remap fixture should reject");
    assert!(error.to_string().contains("unsupported WHERE predicate"));
}

#[test]
fn rejects_unsupported_storage_version() {
    let error = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/reject/unsupported_storage_version.sync-rules"
    ))
    .expect("parser should succeed");
    let error = lower_canonical_semantic_plan(error)
        .expect_err("unsupported storage version should be rejected during lowering");
    assert!(error.to_string().contains("unsupported storage version"));
}

#[test]
fn rejects_unsupported_compatibility_version() {
    let error = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/reject/unsupported_compatibility_version.sync-rules"
    ))
    .expect("parser should succeed");
    let error = lower_canonical_semantic_plan(error)
        .expect_err("unsupported compatibility version should be rejected during lowering");
    assert!(error
        .to_string()
        .contains("unsupported compatibility version"));
}

#[test]
fn storage_contract_id_includes_compatibility_and_canonical_identity() {
    let builtin = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/benchmark_streams.sync-rules"
    ))
    .expect("benchmark source should compile");
    let variant = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/projection_variants.sync-rules"
    ))
    .expect("projection variants should compile");

    let builtin_id = canonical_storage_contract_id(&builtin);
    let variant_id = canonical_storage_contract_id(&variant);
    assert!(builtin_id.contains("compat=1"));
    assert!(builtin_id.contains("storage=1"));
    assert_ne!(builtin_id, variant_id);
}

#[test]
fn parses_stream_priority_key_with_warning_instead_of_error() {
    // `priority` is accepted (with a tracing warning) instead of failing the
    // deploy: bucket priority ordering is unimplemented, but official configs
    // that set it must still compile.
    let compiled = compile_sync_rules_source(
        "edition: 3\ncompatibility_version: 1\nstorage_version: 1\nstreams:\n  tickets:\n    priority: 1\n    auto_subscribe: true\n    query: SELECT * FROM public.tickets\n",
    )
    .expect("stream with priority key should parse successfully");

    assert_eq!(compiled.streams.len(), 1);
    assert_eq!(compiled.streams[0].name, "tickets");
}

#[test]
fn rejects_non_public_tables() {
    let error = compile_sync_rules_source(include_str!(
        "../../tests/fixtures/sync_plan/reject/non_public_table.sync-rules"
    ))
    .expect_err("non-public tables should be rejected");
    assert!(error
        .to_string()
        .contains("only public.<table> is supported"));
}
