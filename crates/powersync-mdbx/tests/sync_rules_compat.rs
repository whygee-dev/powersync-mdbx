#[test]
fn compiles_edition3_streams_with_with_queries_and_query_lists() {
    let source = r#"
config:
  edition: 3
  timestamps_iso8601: false
  versioned_bucket_ids: true
  fixed_json_extract: true
  custom_postgres_types: true
with:
  web_team_scope: SELECT DISTINCT "teamId" AS "teamId" FROM "Membership" WHERE connection.parameters() ->> 'schema_version' = 'web' AND "userId" = auth.user_id() AND "teamId" = connection.parameters() ->> 'selectedTeamId'
streams:
  web_workspaces:
    auto_subscribe: true
    with:
      web_workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE connection.parameters() ->> 'schema_version' = 'web' AND "userId" = auth.user_id() AND "workspaceId" IS NOT NULL AND "teamId" = connection.parameters() ->> 'selectedTeamId'
    queries:
      - 'SELECT "Workspace".* FROM "Workspace",web_workspace_scope AS bucket WHERE "Workspace"."id" = bucket."workspaceId" AND "Workspace"."archivedAt" IS NULL'
      - 'SELECT "WorkspaceSetting".* FROM "WorkspaceSetting",web_team_scope AS bucket WHERE "WorkspaceSetting"."teamId" = bucket."teamId" AND("WorkspaceSetting"."archivedAt" IS NULL OR "WorkspaceSetting"."flagValue" IS NOT NULL)'
  web_generic:
    queries:
      - 'SELECT "Document".* FROM "Document" WHERE (connection.parameters() ->> ''schema_version'' = ''web'' OR connection.parameters() ->> ''schema_version'' = ''mobile'')'
"#;

    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("edition 3 streams should compile");
    assert_eq!(canonical.edition, 3);
    assert_eq!(canonical.streams.len(), 2);
    assert_eq!(canonical.streams[0].source_table, "Workspace");
    assert_eq!(canonical.streams[0].name, "web_workspaces");
    assert_eq!(canonical.streams[0].data_queries.len(), 2);
    assert_eq!(
        canonical.streams[0].data_queries[0].source_table,
        "Workspace"
    );
    assert_eq!(
        canonical.streams[0].data_queries[1].source_table,
        "WorkspaceSetting"
    );
    assert_eq!(
        canonical.streams[0].data_queries[1].bucket_parameters[0].source_column,
        "teamId"
    );
    assert_eq!(canonical.streams[1].source_table, "Document");
    assert!(canonical.streams[1].bucket_parameters.is_empty());

    powersync_mdbx::sync_rules::lower_canonical_semantic_plan(canonical)
        .expect("edition 3 streams should lower");
}

#[test]
fn compiles_external_sync_rules_when_available() {
    let Some(path) = optional_external_path("POWERSYNC_COMPAT_SYNC_RULES") else {
        return;
    };
    let source = std::fs::read_to_string(path).expect("read external sync rules");
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(&source)
        .expect("external sync rules should compile");
    assert!(canonical.streams.len() > 20);
}

#[test]
fn generated_registry_streams_exist_in_external_sync_rules_when_available() {
    let Some(rules_path) = optional_external_path("POWERSYNC_COMPAT_SYNC_RULES") else {
        return;
    };
    let Some(registry_path) = optional_external_path("POWERSYNC_COMPAT_REGISTRY") else {
        return;
    };

    let source = std::fs::read_to_string(rules_path).expect("read external sync rules");
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(&source)
        .expect("external sync rules should compile");
    let available_streams = canonical
        .streams
        .iter()
        .map(|stream| stream.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    let registry = std::fs::read_to_string(registry_path).expect("read generated registry");
    let referenced_streams = quoted_registry_stream_names(&registry);
    let missing = referenced_streams
        .iter()
        .filter(|stream| !available_streams.contains(stream.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "generated registry references streams not present in sync rules: {missing:?}"
    );
    assert!(
        !registry.contains("parameters"),
        "current generated web registry should not require explicit stream parameters"
    );
}

fn optional_external_path(env_name: &str) -> Option<std::path::PathBuf> {
    let path = std::env::var_os(env_name).map(std::path::PathBuf::from)?;
    path.exists().then_some(path)
}

fn quoted_registry_stream_names(registry: &str) -> std::collections::BTreeSet<String> {
    registry
        .lines()
        .filter_map(|line| line.trim().strip_prefix("name: \""))
        .filter_map(|rest| rest.split_once('"').map(|(name, _)| name.to_owned()))
        .collect()
}

#[test]
fn multi_query_logical_stream_resolves_one_bucket_with_multiple_data_queries() {
    let source = r#"
config:
  edition: 3
streams:
  workspace:
    query: SELECT * FROM projects WHERE org_id = subscription.parameter('org_id')
  inbox:
    queries:
      - SELECT * FROM tasks WHERE org_id = subscription.parameter('org_id')
      - SELECT id AS comment_id, task_id, org_id, body FROM comments WHERE org_id = subscription.parameter('org_id')
"#;
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("multi-query stream should compile");
    let inbox = canonical
        .streams
        .iter()
        .find(|stream| stream.name == "inbox")
        .expect("inbox stream");
    assert_eq!(inbox.data_queries.len(), 2);
    assert!(!canonical
        .streams
        .iter()
        .any(|stream| stream.name.contains("__query_")));

    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(canonical)
        .expect("multi-query stream should lower");
    let bucket = plan
        .resolve_bucket_request("1#inbox|0[\"org-1\"]")
        .expect("logical stream bucket");
    assert_eq!(bucket.stream_name(), "inbox");
    assert_eq!(bucket.queries().len(), 2);
    assert!(bucket
        .queries()
        .iter()
        .any(|query| query.object_type() == "tasks"));
    assert!(bucket
        .queries()
        .iter()
        .any(|query| query.object_type() == "comments"));
}

#[test]
fn multi_query_stream_uses_distinct_bucket_groups_for_incompatible_parameters() {
    let source = r#"
config:
  edition: 3
streams:
  mixed:
    queries:
      - SELECT * FROM tasks WHERE org_id = subscription.parameter('org_id')
      - SELECT * FROM comments WHERE task_id = subscription.parameter('task_id')
"#;
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("multi-query stream should compile");
    let stream = &canonical.streams[0];
    assert_eq!(stream.name, "mixed");
    assert_eq!(stream.bucket_parameter_groups().len(), 2);

    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(canonical)
        .expect("multi-query stream should lower");
    let org_bucket = plan
        .resolve_bucket_request("1#mixed|0[\"org-1\"]")
        .expect("org bucket");
    let task_bucket = plan
        .resolve_bucket_request("1#mixed|1[\"task-1\"]")
        .expect("task bucket");

    assert_eq!(org_bucket.queries().len(), 1);
    assert_eq!(org_bucket.queries()[0].object_type(), "tasks");
    assert_eq!(task_bucket.queries().len(), 1);
    assert_eq!(task_bucket.queries()[0].object_type(), "comments");
    assert!(plan
        .resolve_bucket_request("1#mixed|0[\"org-1\",\"task-1\"]")
        .is_none());
}

#[test]
fn fixture_style_residual_filters_are_enforced_after_bucket_resolution() {
    let source = r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  tickets:
    queries:
      - 'SELECT "Ticket".* FROM "Ticket", workspace_scope AS bucket WHERE "Ticket"."workspaceId" = bucket."workspaceId" AND("Ticket"."archivedAt" IS NULL OR "Ticket"."flagChecked" IS NULL)AND "Ticket"."parentId" IS NOT NULL'
"#;
    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(
        powersync_mdbx::sync_rules::compile_sync_rules_source(source)
            .expect("Fixture-style residual filter should compile"),
    )
    .expect("Fixture-style residual filter should lower");
    let bucket = plan
        .resolve_bucket_request("1#tickets|0[\"ws-1\"]")
        .expect("ticket bucket");
    assert!(bucket.queries()[0].row_filter().is_some());

    let matching = serde_json::json!({
        "id": "eq-1",
        "workspaceId": "ws-1",
        "archivedAt": "2026-01-01T00:00:00Z",
        "flagChecked": null,
        "parentId": "component-1"
    })
    .to_string();
    let filtered = serde_json::json!({
        "id": "eq-2",
        "workspaceId": "ws-1",
        "archivedAt": "2026-01-01T00:00:00Z",
        "flagChecked": true,
        "parentId": "component-1"
    })
    .to_string();
    let missing_component = serde_json::json!({
        "id": "eq-3",
        "workspaceId": "ws-1",
        "archivedAt": null,
        "flagChecked": true,
        "parentId": null
    })
    .to_string();

    let route =
        std::collections::BTreeMap::from([(String::from("workspaceId"), String::from("ws-1"))]);
    assert!(bucket.matches_object_routes_and_data("Ticket", &route, &matching));
    assert!(!bucket.matches_object_routes_and_data("Ticket", &route, &filtered));
    assert!(!bucket.matches_object_routes_and_data("Ticket", &route, &missing_component));
}

#[test]
fn unsupported_row_filter_operators_are_rejected_at_compile_time() {
    let build = |predicate: &str| {
        let source = format!(
            r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  tickets:
    queries:
      - 'SELECT "Ticket".* FROM "Ticket", workspace_scope AS bucket WHERE "Ticket"."workspaceId" = bucket."workspaceId" AND {predicate}'
"#
        );
        powersync_mdbx::sync_rules::compile_sync_rules_source(&source)
            .and_then(powersync_mdbx::sync_rules::lower_canonical_semantic_plan)
    };

    // The row-filter evaluator only implements =, IN, and IS [NOT] NULL. Any
    // other operator (or a literal it cannot represent) must fail closed at
    // compile time rather than silently dropping every matching row at serve time.
    for predicate in [
        r#""Ticket"."rank" > 5"#,
        r#""Ticket"."rank" >= 5"#,
        r#""Ticket"."rank" != 5"#,
        r#""Ticket"."price" = 9.99"#,
    ] {
        assert!(
            build(predicate).is_err(),
            "expected a compile error for unsupported row filter `{predicate}`, got Ok"
        );
    }

    // Supported forms still compile.
    for predicate in [r#""Ticket"."rank" = 5"#, r#""Ticket"."archivedAt" IS NULL"#] {
        assert!(
            build(predicate).is_ok(),
            "expected supported row filter `{predicate}` to compile"
        );
    }
}

#[test]
fn fixture_style_bucket_value_in_array_column_routes_match_array_payloads() {
    let source = r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  issues:
    queries:
      - 'SELECT "Issue".* FROM "Issue", workspace_scope AS bucket WHERE bucket."workspaceId" IN "Issue"."workspaceIds" AND "Issue"."archivedAt" IS NULL'
"#;
    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(
        powersync_mdbx::sync_rules::compile_sync_rules_source(source)
            .expect("array route sync rule should compile"),
    )
    .expect("array route sync rule should lower");
    let bucket = plan
        .resolve_bucket_request("1#issues|0[\"ws-1\"]")
        .expect("issue bucket");
    let route = std::collections::BTreeMap::from([(
        String::from("workspaceIds"),
        serde_json::json!(["ws-1", "ws-2"]).to_string(),
    )]);
    let data = serde_json::json!({
        "id": "issue-1",
        "workspaceIds": ["ws-1", "ws-2"],
        "archivedAt": null
    })
    .to_string();
    assert!(bucket.matches_object_routes_and_data("Issue", &route, &data));
}

#[test]
fn fixture_style_computed_id_with_table_star_is_projected() {
    let source = r#"
config:
  edition: 3
streams:
  issue_label:
    query: SELECT "_IssueToLabel"."A" || '.' || "_IssueToLabel"."B" AS id, "_IssueToLabel".* FROM "_IssueToLabel" WHERE "teamId" = subscription.parameter('teamId')
"#;
    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(
        powersync_mdbx::sync_rules::compile_sync_rules_source(source)
            .expect("computed id star query should compile"),
    )
    .expect("computed id star query should lower");
    let bucket = plan
        .resolve_bucket_request("1#issue_label|0[\"team-1\"]")
        .expect("workspace label bucket");
    let projected = bucket
        .project_document_json(
            "_IssueToLabel",
            &serde_json::json!({
                "A": "ws-1",
                "B": "workspace-1",
                "teamId": "team-1",
                "other": "kept"
            })
            .to_string(),
        )
        .expect("computed projection");
    let value: serde_json::Value = serde_json::from_str(&projected).expect("json projection");
    assert_eq!(value["id"], "ws-1.workspace-1");
    assert_eq!(value["A"], "ws-1");
    assert_eq!(value["B"], "workspace-1");
    assert_eq!(value["other"], "kept");
}

#[test]
fn direct_request_parameter_guards_are_preserved_and_evaluated() {
    let source = r#"
config:
  edition: 3
streams:
  web_generic:
    queries:
      - 'SELECT "Document".* FROM "Document" WHERE (connection.parameters() ->> ''schema_version'' = ''web'' OR connection.parameters() ->> ''schema_version'' = ''mobile'')'
"#;
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("request-gated global query should compile");
    let stream = &canonical.streams[0];
    let groups = stream.bucket_groups();
    assert_eq!(groups.len(), 1);
    let request_filter = groups[0]
        .request_filter
        .as_ref()
        .expect("schema_version guard should be preserved");

    assert!(powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::RequestParameter { name }
                if name == "schema_version" =>
            {
                Some("web".to_owned())
            }
            _ => None,
        }
    ));
    assert!(powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::RequestParameter { name }
                if name == "schema_version" =>
            {
                Some("mobile".to_owned())
            }
            _ => None,
        }
    ));
    assert!(!powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::RequestParameter { name }
                if name == "schema_version" =>
            {
                Some("api".to_owned())
            }
            _ => None,
        }
    ));
}

#[test]
fn json_each_request_parameter_queries_expand_to_array_bucket_bindings() {
    let source = r#"
config:
  edition: 3
streams:
  program:
    with:
      project_param: SELECT json_each.value AS "workspaceId", project_each.value AS "projectId" FROM "Membership", json_each(connection.parameters() ->> 'workspaceIds'), json_each(connection.parameters() ->> 'projectIds') AS project_each WHERE "Membership"."workspaceId" = json_each.value
    queries:
      - 'SELECT "Activity".* FROM "Activity", project_param AS bucket WHERE "Activity"."workspaceId" = bucket."workspaceId" AND "Activity"."projectId" = bucket."projectId" AND "Activity"."archivedAt" IS NULL'
"#;
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("json_each parameter query should compile");
    let stream = &canonical.streams[0];
    let groups = stream.bucket_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].bucket_parameters.len(), 2);
    assert!(matches!(
        groups[0].bucket_parameters[0].binding,
        powersync_mdbx::sync_rules::CanonicalBinding::RequestParameterArray { ref name }
            if name == "workspaceIds"
    ));
    assert!(matches!(
        groups[0].bucket_parameters[1].binding,
        powersync_mdbx::sync_rules::CanonicalBinding::RequestParameterArray { ref name }
            if name == "projectIds"
    ));
    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(canonical)
        .expect("json_each parameter query should lower");
    let bucket = plan
        .resolve_bucket_request("1#program|0[\"ws-1\",\"program-1\"]")
        .expect("expanded json_each bucket should resolve");
    assert_eq!(
        bucket.queries()[0].route_constraints().get("workspaceId"),
        Some(&"ws-1".to_owned())
    );
    assert_eq!(
        bucket.queries()[0].route_constraints().get("projectId"),
        Some(&"program-1".to_owned())
    );
}

#[test]
fn parameter_query_request_guards_are_preserved_for_stream_bucket_groups() {
    let source = r#"
config:
  edition: 3
with:
  web_admin_scope: SELECT 1 AS ok WHERE auth.parameter('admin') = true
streams:
  admin_lists:
    queries:
      - 'SELECT "List".* FROM "List", web_admin_scope WHERE "List"."teamId" IS NULL'
"#;
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("admin scope query should compile");
    let groups = canonical.streams[0].bucket_groups();
    let request_filter = groups[0]
        .request_filter
        .as_ref()
        .expect("admin auth guard should be preserved");

    assert!(powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::AuthParameter { name }
                if name == "admin" =>
            {
                Some("true".to_owned())
            }
            _ => None,
        }
    ));
    assert!(!powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::AuthParameter { name }
                if name == "admin" =>
            {
                Some("false".to_owned())
            }
            _ => None,
        }
    ));
}

#[test]
fn parameter_query_request_non_null_guards_are_preserved() {
    let source = r#"
config:
  edition: 3
streams:
  workspaces:
    with:
      workspace_param: SELECT json_each.value AS "workspaceId" FROM "Membership", json_each(connection.parameters() ->> 'workspaceIds') WHERE connection.parameters() ->> 'workspaceIds' IS NOT NULL AND "Membership"."workspaceId" = json_each.value
    queries:
      - 'SELECT "Workspace".* FROM "Workspace", workspace_param AS bucket WHERE "Workspace"."id" = bucket."workspaceId"'
"#;
    let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
        .expect("non-null request guard should compile");
    let groups = canonical.streams[0].bucket_groups();
    let request_filter = groups[0]
        .request_filter
        .as_ref()
        .expect("non-null workspaceIds guard should be preserved");
    assert!(powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::RequestParameter { name }
                if name == "workspaceIds" =>
            {
                Some("[\"ws-1\"]".to_owned())
            }
            _ => None,
        }
    ));
    assert!(!powersync_mdbx::sync_rules::request_filter_matches(
        Some(request_filter),
        |_binding| None
    ));
}

#[test]
fn computed_projection_id_is_used_as_runtime_object_id() {
    use pg_walstream::{ColumnValue, RowData};

    let source = r#"
config:
  edition: 3
streams:
  issue_label:
    query: SELECT "_IssueToLabel"."A" || '.' || "_IssueToLabel"."B" AS id, "_IssueToLabel".* FROM "_IssueToLabel" WHERE "teamId" = subscription.parameter('teamId')
"#;
    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(
        powersync_mdbx::sync_rules::compile_sync_rules_source(source)
            .expect("computed id query should compile"),
    )
    .expect("computed id query should lower");
    let table_plan = plan
        .table_plan("_IssueToLabel")
        .expect("compiled table plan");
    let row = RowData::from_pairs(vec![
        ("A", ColumnValue::text("ws-1")),
        ("B", ColumnValue::text("workspace-1")),
        ("teamId", ColumnValue::text("team-1")),
    ]);
    assert_eq!(
        table_plan
            .object_id_for_row(&row)
            .expect("computed object id"),
        "ws-1.workspace-1"
    );
}

#[test]
fn request_gated_auto_subscribe_defaults_are_filtered_by_request_context() {
    let source = r#"
config:
  edition: 3
streams:
  web_entities:
    auto_subscribe: true
    query: SELECT "Entity".* FROM "Entity" WHERE connection.parameters() ->> 'schema_version' = 'web'
"#;
    let plan = powersync_mdbx::sync_rules::lower_canonical_semantic_plan(
        powersync_mdbx::sync_rules::compile_sync_rules_source(source)
            .expect("request-gated auto subscribe should compile"),
    )
    .expect("request-gated auto subscribe should lower");
    assert_eq!(plan.default_bucket_requests().len(), 1);
    assert_eq!(
        plan.default_bucket_requests_matching(|binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::RequestParameter { name }
                if name == "schema_version" =>
            {
                Some("web".to_owned())
            }
            _ => None,
        })
        .len(),
        1
    );
    assert_eq!(
        plan.default_bucket_requests_matching(|binding| match binding {
            powersync_mdbx::sync_rules::CanonicalBinding::RequestParameter { name }
                if name == "schema_version" =>
            {
                Some("mobile".to_owned())
            }
            _ => None,
        })
        .len(),
        0
    );
}
