//! Golden tests for sync-rule predicate evaluation.
//!
//! The golden corpus (`sync_rules_golden.rs`) pins the parse output. This suite
//! pins evaluation behavior. For each filter-bearing rule it runs synthetic rows
//! and request contexts through the evaluator and snapshots boolean and projection results in
//! `tests/sync_rules_eval_golden.snapshot`.
//!
//! Regenerate the snapshot intentionally with:
//!
//!     REGEN_SYNC_RULES_EVAL_GOLDEN=1 cargo test --test sync_rules_eval_golden

use std::{collections::BTreeMap, fs, path::Path};

use powersync_mdbx::sync_rules::{
    compile_sync_rules_source, lower_canonical_semantic_plan, request_filter_matches,
    CanonicalBinding,
};

fn route(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

/// A single-query stream `rule` whose data query joins a bucket scope and
/// applies `{FILTER}` as an extra row-filter conjunct (resolve `1#rule|0["ws-1"]`).
const ROW_FILTER_RULE: &str = r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  rule:
    queries:
      - 'SELECT "Ticket".* FROM "Ticket", workspace_scope AS bucket WHERE "Ticket"."workspaceId" = bucket."workspaceId" AND {FILTER}'
"#;

#[test]
fn sync_rules_eval_behavior_matches_golden() {
    let mut out = String::new();

    // --- Row filters: residual (archivedAt IS NULL OR flagChecked IS NULL) AND parentId IS NOT NULL
    {
        let plan = lower(
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
        );
        let bucket = plan
            .resolve_bucket_request("1#tickets|0[\"ws-1\"]")
            .expect("ticket bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "residual_row_filter");
        for (label, data) in [
            (
                "match: deleted null, flag null, component set",
                r#"{"archivedAt":null,"flagChecked":null,"parentId":"c1"}"#,
            ),
            (
                "filtered: deleted set, flag true, component set",
                r#"{"archivedAt":"2026-01-01","flagChecked":true,"parentId":"c1"}"#,
            ),
            (
                "match: deleted set, flag null, component set",
                r#"{"archivedAt":"2026-01-01","flagChecked":null,"parentId":"c1"}"#,
            ),
            (
                "filtered: component null",
                r#"{"archivedAt":null,"flagChecked":null,"parentId":null}"#,
            ),
            (
                "filtered: component missing",
                r#"{"archivedAt":null,"flagChecked":null}"#,
            ),
            (
                "wrong object type",
                r#"{"archivedAt":null,"flagChecked":null,"parentId":"c1"}"#,
            ),
        ] {
            let object_type = if label == "wrong object type" {
                "Other"
            } else {
                "Ticket"
            };
            let matched = bucket.matches_object_routes_and_data(object_type, &route, data);
            line(&mut out, label, matched);
        }
    }

    // --- Array-column IN route: bucket."workspaceId" IN "Issue"."workspaceIds" AND archivedAt IS NULL
    {
        let plan = lower(
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
        );
        let bucket = plan
            .resolve_bucket_request("1#issues|0[\"ws-1\"]")
            .expect("issue bucket");
        let route = route(&[("workspaceIds", r#"["ws-1","ws-2"]"#)]);
        section(&mut out, "array_column_in_route");
        for (label, data) in [
            (
                "match: ws-1 in array, not deleted",
                r#"{"workspaceIds":["ws-1","ws-2"],"archivedAt":null}"#,
            ),
            (
                "no match: array lacks ws-1",
                r#"{"workspaceIds":["ws-9"],"archivedAt":null}"#,
            ),
            (
                "filtered: deleted",
                r#"{"workspaceIds":["ws-1"],"archivedAt":"2026-01-01"}"#,
            ),
        ] {
            let matched = bucket.matches_object_routes_and_data("Issue", &route, data);
            line(&mut out, label, matched);
        }
    }

    // --- Request filter: (schema_version = web OR schema_version = mobile)
    {
        let canonical = compile_sync_rules_source(
            r#"
config:
  edition: 3
streams:
  web_generic:
    queries:
      - 'SELECT "Document".* FROM "Document" WHERE (connection.parameters() ->> ''schema_version'' = ''web'' OR connection.parameters() ->> ''schema_version'' = ''mobile'')'
"#,
        )
        .expect("compile request-filter rule");
        let groups = canonical.streams[0].bucket_groups();
        let request_filter = groups[0].request_filter.clone();
        section(&mut out, "request_filter_schema_version_or");
        for value in ["web", "mobile", "api", "MISSING"] {
            let matched =
                request_filter_matches(request_filter.as_ref(), |binding| match binding {
                    CanonicalBinding::RequestParameter { name } if name == "schema_version" => {
                        if value == "MISSING" {
                            None
                        } else {
                            Some(value.to_owned())
                        }
                    }
                    _ => None,
                });
            line(&mut out, value, matched);
        }
    }

    // --- Request filter: auth.parameter('admin') = true
    {
        let canonical = compile_sync_rules_source(
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
        )
        .expect("compile admin rule");
        let groups = canonical.streams[0].bucket_groups();
        let request_filter = groups[0].request_filter.clone();
        section(&mut out, "request_filter_auth_admin");
        for value in ["true", "false", "MISSING"] {
            let matched =
                request_filter_matches(request_filter.as_ref(), |binding| match binding {
                    CanonicalBinding::AuthParameter { name } if name == "admin" => {
                        if value == "MISSING" {
                            None
                        } else {
                            Some(value.to_owned())
                        }
                    }
                    _ => None,
                });
            line(&mut out, value, matched);
        }
    }

    // Row-filter equality exercises directional JSON scalar coercion.
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", r#""Ticket"."rank" = 5"#)
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "row_eq_number_coercion (col = 5)");
        for (label, data) in [
            ("rank number 5", r#"{"rank":5}"#),
            ("rank string \"5\" coerces", r#"{"rank":"5"}"#),
            ("rank number 6", r#"{"rank":6}"#),
            ("rank missing", r#"{}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }
    {
        // Directional: a literal on the LEFT does not coerce (json_values_equal is string-anchored).
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", r#"5 = "Ticket"."rank""#)
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "row_eq_number_coercion (5 = col, directional)");
        for (label, data) in [
            ("rank number 5", r#"{"rank":5}"#),
            (
                "rank string \"5\" does NOT coerce (literal-left)",
                r#"{"rank":"5"}"#,
            ),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", r#""Ticket"."flag" = true"#)
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "row_eq_bool_coercion (flag = true)");
        for (label, data) in [
            ("flag bool true", r#"{"flag":true}"#),
            ("flag string \"true\" coerces", r#"{"flag":"true"}"#),
            ("flag bool false", r#"{"flag":false}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }

    // Row-filter `IN` covers array membership and the scalar fallback.
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", r#""Ticket"."tag" IN "Ticket"."tags""#)
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "row_in_column (tag IN tags)");
        for (label, data) in [
            ("tag in array", r#"{"tag":"a","tags":["a","b"]}"#),
            ("tag not in array", r#"{"tag":"a","tags":["x"]}"#),
            ("tags scalar equal", r#"{"tag":"a","tags":"a"}"#),
            ("tags scalar different", r#"{"tag":"a","tags":"b"}"#),
            ("tags missing", r#"{"tag":"a"}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }

    // Row-filter IS NULL treats a missing key and JSON null as null. An empty
    // string is not null in the row context, unlike the request context.
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", r#""Ticket"."archivedAt" IS NULL"#)
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "row_is_null (archivedAt IS NULL)");
        for (label, data) in [
            ("missing key is null -> true", r#"{}"#),
            ("json null -> true", r#"{"archivedAt":null}"#),
            ("value -> false", r#"{"archivedAt":"2026-01-01"}"#),
            (
                "empty string is NOT null in row context -> false",
                r#"{"archivedAt":""}"#,
            ),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }

    // `bucket."workspaceId" IN "Issue"."workspaceIds"` lowers to a route constraint.
    // Membership is evaluated against the request route field, not the row's own
    // `workspaceIds` column. Vary the route here to cover array and scalar values.
    {
        let plan = lower(
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
        );
        let bucket = plan
            .resolve_bucket_request("1#issues|0[\"ws-1\"]")
            .expect("issue bucket");
        let data = r#"{"archivedAt":null}"#;
        section(
            &mut out,
            "array_route_membership (bucket value ws-1, vary route)",
        );
        for (label, route_value) in [
            ("route array contains ws-1 -> true", r#"["ws-1","ws-2"]"#),
            ("route array lacks ws-1 -> false", r#"["ws-9"]"#),
            ("route scalar equals ws-1 -> true", "ws-1"),
            ("route scalar differs -> false", "ws-2"),
        ] {
            let route = route(&[("workspaceIds", route_value)]);
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Issue", &route, data),
            );
        }
    }

    // Three-way OR covers mixed IS NULL and equality predicates.
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace(
                    "{FILTER}",
                    r#"("Ticket"."a" IS NULL OR "Ticket"."b" IS NULL OR "Ticket"."rank" = 5)"#,
                )
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(
            &mut out,
            "row_three_way_or (a IS NULL OR b IS NULL OR rank = 5)",
        );
        for (label, data) in [
            ("first branch: a null", r#"{"a":null,"b":"x","rank":1}"#),
            ("second branch: b null", r#"{"a":"x","b":null,"rank":1}"#),
            ("third branch: rank = 5", r#"{"a":"x","b":"x","rank":5}"#),
            ("none: all set, rank != 5", r#"{"a":"x","b":"x","rank":1}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }

    // Nested parentheses exercise repeated wrapper removal.
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", r#"(("Ticket"."archivedAt" IS NULL))"#)
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "row_double_parens ((archivedAt IS NULL))");
        for (label, data) in [
            ("null -> true", r#"{"archivedAt":null}"#),
            ("set -> false", r#"{"archivedAt":"2026-01-01"}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }

    // Request-context IS NULL treats both an absent binding and an empty string as null.
    {
        let canonical = compile_sync_rules_source(
            r#"
config:
  edition: 3
streams:
  team_unset:
    queries:
      - 'SELECT "List".* FROM "List" WHERE connection.parameters() ->> ''team'' IS NULL'
"#,
        )
        .expect("compile request IS NULL rule");
        let groups = canonical.streams[0].bucket_groups();
        let request_filter = groups[0].request_filter.clone();
        section(&mut out, "request_is_null (team IS NULL)");
        for (label, value) in [
            ("present non-empty -> false", Some("eu")),
            ("empty string -> true", Some("")),
            ("missing -> true", None),
        ] {
            let matched =
                request_filter_matches(request_filter.as_ref(), |binding| match binding {
                    CanonicalBinding::RequestParameter { name } if name == "team" => {
                        value.map(str::to_owned)
                    }
                    _ => None,
                });
            line(&mut out, label, matched);
        }
    }

    // Reversed request equality preserves literal-left operand order.
    {
        let canonical = compile_sync_rules_source(
            r#"
config:
  edition: 3
streams:
  web_reversed:
    queries:
      - 'SELECT "List".* FROM "List" WHERE ''web'' = connection.parameters() ->> ''schema_version'''
"#,
        )
        .expect("compile reversed request rule");
        let groups = canonical.streams[0].bucket_groups();
        let request_filter = groups[0].request_filter.clone();
        section(
            &mut out,
            "request_reversed_literal ('web' = schema_version)",
        );
        for value in ["web", "mobile", "MISSING"] {
            let matched =
                request_filter_matches(request_filter.as_ref(), |binding| match binding {
                    CanonicalBinding::RequestParameter { name } if name == "schema_version" => {
                        if value == "MISSING" {
                            None
                        } else {
                            Some(value.to_owned())
                        }
                    }
                    _ => None,
                });
            line(&mut out, value, matched);
        }
    }

    // Request filters with two binding predicates require both to match.
    {
        let canonical = compile_sync_rules_source(
            r#"
config:
  edition: 3
streams:
  web_eu:
    queries:
      - 'SELECT "List".* FROM "List" WHERE connection.parameters() ->> ''schema_version'' = ''web'' AND connection.parameters() ->> ''team'' = ''eu'''
"#,
        )
        .expect("compile request multi-AND rule");
        let groups = canonical.streams[0].bucket_groups();
        let request_filter = groups[0].request_filter.clone();
        section(
            &mut out,
            "request_multi_and (schema_version = web AND team = eu)",
        );
        for (label, schema_version, team) in [
            ("both match -> true", "web", "eu"),
            ("team wrong -> false", "web", "us"),
            ("schema wrong -> false", "mobile", "eu"),
        ] {
            let matched =
                request_filter_matches(request_filter.as_ref(), |binding| match binding {
                    CanonicalBinding::RequestParameter { name } if name == "schema_version" => {
                        Some(schema_version.to_owned())
                    }
                    CanonicalBinding::RequestParameter { name } if name == "team" => {
                        Some(team.to_owned())
                    }
                    _ => None,
                });
            line(&mut out, label, matched);
        }
    }

    // Queries without row or request filters take the unconditional fast paths.
    {
        let plan = lower(
            r#"
config:
  edition: 3
with:
  workspace_scope: SELECT "workspaceId" AS "workspaceId" FROM "Membership" WHERE "userId" = auth.user_id()
streams:
  plain:
    queries:
      - 'SELECT "Plain".* FROM "Plain", workspace_scope AS bucket WHERE "Plain"."workspaceId" = bucket."workspaceId"'
"#,
        );
        let bucket = plan
            .resolve_bucket_request("1#plain|0[\"ws-1\"]")
            .expect("plain bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(&mut out, "no_filter_fast_path");
        for (label, data) in [
            (
                "arbitrary data matches (no row filter)",
                r#"{"anything":"goes"}"#,
            ),
            ("empty object matches", r#"{}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Plain", &route, data),
            );
        }
        line(
            &mut out,
            "request_filter_matches(None) -> true",
            request_filter_matches(None, |_| Option::<String>::None),
        );
    }

    // An auto-subscribe stream without bucket parameters still applies its request filter.
    {
        let plan = lower(
            r#"
config:
  edition: 3
streams:
  web_default:
    auto_subscribe: true
    queries:
      - 'SELECT "List".* FROM "List" WHERE connection.parameters() ->> ''platform'' = ''web'''
"#,
        );
        section(
            &mut out,
            "default_bucket_request_filter_threading (count of matched buckets)",
        );
        for value in ["web", "mobile", "MISSING"] {
            let buckets = plan.default_bucket_requests_matching(|binding| match binding {
                CanonicalBinding::RequestParameter { name } if name == "platform" => {
                    if value == "MISSING" {
                        None
                    } else {
                        Some(value.to_owned())
                    }
                }
                _ => None,
            });
            line_count(&mut out, value, buckets.len());
        }
    }

    // A row-context IS NULL operand is resolved by column lookup: `5 IS NULL`
    // tests the column "5", not the integer literal 5. An absent column is null.
    {
        let plan = lower(ROW_FILTER_RULE.replace("{FILTER}", "5 IS NULL").as_str());
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(
            &mut out,
            "row_literal_token_is_null (5 IS NULL => column \"5\")",
        );
        for (label, data) in [
            ("column \"5\" absent -> true", r#"{}"#),
            ("column \"5\" present non-null -> false", r#"{"5":5}"#),
            ("column \"5\" present json-null -> true", r#"{"5":null}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }
    {
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", "5 IS NOT NULL")
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(
            &mut out,
            "row_literal_token_is_not_null (5 IS NOT NULL => column \"5\")",
        );
        for (label, data) in [
            ("column \"5\" absent -> false", r#"{}"#),
            ("column \"5\" present -> true", r#"{"5":5}"#),
        ] {
            line(
                &mut out,
                label,
                bucket.matches_object_routes_and_data("Ticket", &route, data),
            );
        }
    }
    {
        // `''x''` is the YAML escaping for the SQL literal `'x'` inside the single-quoted scalar.
        let plan = lower(
            ROW_FILTER_RULE
                .replace("{FILTER}", "''x'' IS NULL")
                .as_str(),
        );
        let bucket = plan
            .resolve_bucket_request("1#rule|0[\"ws-1\"]")
            .expect("rule bucket");
        let route = route(&[("workspaceId", "ws-1")]);
        section(
            &mut out,
            "row_quoted_literal_is_null ('x' IS NULL => column \"x\")",
        );
        line(
            &mut out,
            "column \"x\" absent -> true",
            bucket.matches_object_routes_and_data("Ticket", &route, r#"{}"#),
        );
    }

    let golden_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sync_rules_eval_golden.snapshot");
    if std::env::var_os("REGEN_SYNC_RULES_EVAL_GOLDEN").is_some() {
        fs::write(&golden_path, &out).expect("write eval golden");
        return;
    }
    let expected = fs::read_to_string(&golden_path).expect(
        "read tests/sync_rules_eval_golden.snapshot \
         (run with REGEN_SYNC_RULES_EVAL_GOLDEN=1 to create it)",
    );
    assert_eq!(
        out, expected,
        "sync-rule evaluator behavior drifted from the golden snapshot"
    );
}

fn lower(source: &str) -> powersync_mdbx::sync_rules::RustExecutionPlan {
    lower_canonical_semantic_plan(compile_sync_rules_source(source).expect("compile"))
        .expect("lower")
}

fn section(out: &mut String, name: &str) {
    out.push_str(&format!("===== {name} =====\n"));
}

fn line(out: &mut String, label: &str, value: bool) {
    out.push_str(&format!("{label}: {value}\n"));
}

fn line_count(out: &mut String, label: &str, value: usize) {
    out.push_str(&format!("{label}: {value}\n"));
}
