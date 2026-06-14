//! Golden characterization corpus for the sync-rule parser.
//!
//! Each case is compiled through the public `compile_sync_rules_source` entry
//! and its canonical output is serialized to deterministic JSON (the canonical
//! model is Vec/String/enum only — no HashMap — so serialization is stable).
//! The combined snapshot is pinned in `tests/sync_rules_golden.snapshot`.
//!
//! This characterizes the compiler's canonical output and pins it against
//! drift: any change to the parser must reproduce every canonical plan below
//! byte-for-byte. Regenerate with:
//!
//!     REGEN_SYNC_RULES_GOLDEN=1 cargo test --test sync_rules_golden

use std::{fs, path::Path};

/// (name, sync-rule source). Names must be unique and stable; ordering is fixed.
/// The set covers the parser's full supported surface: single-query and
/// multi-query streams, top-level and per-stream `with` CTEs, bucket-parameter
/// joins, `json_each` request-parameter expansion, row (residual) filters,
/// request filters (connection/auth parameters), computed-id star projections,
/// array-column `IN` routes, `auto_subscribe`, non-null request guards, and
/// `DISTINCT` projections.
const CASES: &[(&str, &str)] = &[
    (
        "multi_query_subscription_params",
        r#"
config:
  edition: 3
streams:
  workspace:
    query: SELECT * FROM projects WHERE org_id = subscription.parameter('org_id')
  inbox:
    queries:
      - SELECT * FROM tasks WHERE org_id = subscription.parameter('org_id')
      - SELECT id AS comment_id, task_id, org_id, body FROM comments WHERE org_id = subscription.parameter('org_id')
"#,
    ),
    (
        "edition3_with_queries_and_request_filters",
        r#"
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
"#,
    ),
    (
        "residual_row_filter",
        r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  tickets:
    queries:
      - 'SELECT "Ticket".* FROM "Ticket", workspace_scope AS bucket WHERE "Ticket"."workspaceId" = bucket."workspaceId" AND("Ticket"."archivedAt" IS NULL OR "Ticket"."flagChecked" IS NULL)AND "Ticket"."parentId" IS NOT NULL'
"#,
    ),
    (
        "array_column_in_route",
        r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  issues:
    queries:
      - 'SELECT "Issue".* FROM "Issue", workspace_scope AS bucket WHERE bucket."workspaceId" IN "Issue"."workspaceIds" AND "Issue"."archivedAt" IS NULL'
"#,
    ),
    (
        "computed_id_star_projection",
        r#"
config:
  edition: 3
streams:
  issue_label:
    query: SELECT "_IssueToLabel"."A" || '.' || "_IssueToLabel"."B" AS id, "_IssueToLabel".* FROM "_IssueToLabel" WHERE "teamId" = subscription.parameter('teamId')
"#,
    ),
    (
        "json_each_request_parameter_expansion",
        r#"
config:
  edition: 3
streams:
  program:
    with:
      project_param: SELECT json_each.value AS "workspaceId", project_each.value AS "projectId" FROM "Membership", json_each(connection.parameters() ->> 'workspaceIds'), json_each(connection.parameters() ->> 'projectIds') AS project_each WHERE "Membership"."workspaceId" = json_each.value
    queries:
      - 'SELECT "Activity".* FROM "Activity", project_param AS bucket WHERE "Activity"."workspaceId" = bucket."workspaceId" AND "Activity"."projectId" = bucket."projectId" AND "Activity"."archivedAt" IS NULL'
"#,
    ),
    (
        "admin_scope_auth_parameter",
        r#"
config:
  edition: 3
with:
  web_admin_scope: SELECT 1 AS ok WHERE auth.parameter('admin') = true
streams:
  admin_lists:
    queries:
      - 'SELECT "List".* FROM "List", web_admin_scope WHERE "List"."teamId" IS NULL'
"#,
    ),
    (
        "non_null_request_guard",
        r#"
config:
  edition: 3
streams:
  workspaces:
    with:
      workspace_param: SELECT json_each.value AS "workspaceId" FROM "Membership", json_each(connection.parameters() ->> 'workspaceIds') WHERE connection.parameters() ->> 'workspaceIds' IS NOT NULL AND "Membership"."workspaceId" = json_each.value
    queries:
      - 'SELECT "Workspace".* FROM "Workspace", workspace_param AS bucket WHERE "Workspace"."id" = bucket."workspaceId"'
"#,
    ),
    (
        "request_gated_auto_subscribe",
        r#"
config:
  edition: 3
streams:
  web_entities:
    auto_subscribe: true
    query: SELECT "Entity".* FROM "Entity" WHERE connection.parameters() ->> 'schema_version' = 'web'
"#,
    ),
];

#[test]
fn sync_rules_canonical_output_matches_golden() {
    let mut actual = String::new();
    for (name, source) in CASES {
        let canonical = powersync_mdbx::sync_rules::compile_sync_rules_source(source)
            .unwrap_or_else(|error| panic!("corpus case `{name}` should compile: {error}"));
        let json = serde_json::to_string_pretty(&canonical).expect("canonical plan serializes");
        actual.push_str(&format!("===== {name} =====\n{json}\n\n"));
    }

    let golden_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sync_rules_golden.snapshot");

    if std::env::var_os("REGEN_SYNC_RULES_GOLDEN").is_some() {
        fs::write(&golden_path, &actual).expect("write golden snapshot");
        return;
    }

    let expected = fs::read_to_string(&golden_path).expect(
        "read tests/sync_rules_golden.snapshot \
         (run with REGEN_SYNC_RULES_GOLDEN=1 to create it)",
    );
    assert_eq!(
        actual, expected,
        "canonical sync-rule output drifted from the golden corpus; a parser change \
         must reproduce it byte-for-byte (or regenerate with REGEN_SYNC_RULES_GOLDEN=1 \
         after an intentional change)"
    );
}
