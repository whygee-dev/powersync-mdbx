//! Sync-rules debug / introspection: builds the schema, bucket, and query
//! views surfaced by the diagnostics endpoints.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::sync_rules::{
    compile_sync_rules_source, CanonicalBinding, CanonicalProjection, CanonicalSemanticPlan,
    CanonicalStream, SyncRuleError,
};

use super::binding_sql;

#[derive(Debug, Clone)]
pub struct SyncRulesDebugInfo {
    pub canonical: CanonicalSemanticPlan,
    pub bucket_definitions: Vec<Value>,
    pub source_table_patterns: Vec<Value>,
    pub source_tables: Vec<Value>,
    pub data_tables: BTreeMap<String, Vec<Value>>,
}

impl SyncRulesDebugInfo {
    pub(super) fn as_validate_payload(&self) -> Value {
        json!({
            "valid": true,
            "bucket_definitions": self.bucket_definitions,
            "source_tables": self.source_table_patterns,
            "data_tables": self.data_tables,
            "errors": [],
        })
    }

    pub(super) fn schema_tables(&self) -> Vec<Value> {
        self.canonical
            .streams
            .iter()
            .flat_map(|stream| {
                stream
                    .data_queries
                    .iter()
                    .map(|query| known_query_table_schema(&query.source_table, query))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .chain(std::iter::once(known_table_schema(
                        &stream.source_table,
                        stream,
                    )))
            })
            .fold(BTreeMap::<String, Value>::new(), |mut acc, table| {
                let name = table
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                acc.entry(name).or_insert(table);
                acc
            })
            .into_values()
            .collect()
    }
}

pub fn debug_sync_rules(content: &str) -> Result<SyncRulesDebugInfo, SyncRuleError> {
    let canonical = compile_sync_rules_source(content)?;
    let bucket_definitions = canonical
        .streams
        .iter()
        .map(stream_bucket_definition)
        .collect::<Vec<_>>();
    let source_table_patterns = canonical
        .streams
        .iter()
        .flat_map(stream_source_table_patterns)
        .fold(BTreeMap::<String, Value>::new(), |mut acc, value| {
            let pattern = value
                .get("pattern")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            acc.entry(pattern).or_insert(value);
            acc
        })
        .into_values()
        .collect::<Vec<_>>();
    let source_tables = source_table_patterns
        .iter()
        .map(|entry| entry.get("table").cloned().unwrap_or_else(|| json!({})))
        .collect::<Vec<_>>();
    let data_tables = canonical.streams.iter().flat_map(stream_data_queries).fold(
        BTreeMap::<String, Vec<Value>>::new(),
        |mut acc, query| {
            acc.entry(query.source_table.clone())
                .or_default()
                .push(json!({"query": query_projection_query(&query)}));
            acc
        },
    );

    Ok(SyncRulesDebugInfo {
        canonical,
        bucket_definitions,
        source_table_patterns,
        source_tables,
        data_tables,
    })
}

fn stream_bucket_definition(stream: &CanonicalStream) -> Value {
    let data_queries = stream_data_queries(stream);
    json!({
        "name": stream.name,
        "type": "Stream",
        "bucket_parameters": stream.bucket_parameters.iter().map(|parameter| json!({
            "name": parameter.name,
            "source_column": parameter.source_column,
            "binding": binding_json(&parameter.binding),
        })).collect::<Vec<_>>(),
        "auto_subscribe": stream.auto_subscribe,
        "data_queries": data_queries.iter().map(|query| json!({
            "sql": query_sql(query),
            "table": query.source_table,
            "columns": query_projection_columns(query),
        })).collect::<Vec<_>>(),
    })
}

fn stream_source_table_patterns(stream: &CanonicalStream) -> Vec<Value> {
    stream_data_queries(stream)
        .into_iter()
        .map(|query| {
            let table = known_query_table_schema(&query.source_table, &query);
            json!({
                "schema": "public",
                "pattern": query.source_table,
                "wildcard": false,
                "table": table,
            })
        })
        .collect()
}

fn known_table_schema(table: &str, stream: &CanonicalStream) -> Value {
    known_table_schema_with_columns(
        table,
        projection_columns(stream),
        !stream.bucket_parameters.is_empty(),
    )
}

fn known_query_table_schema(table: &str, query: &crate::sync_rules::CanonicalDataQuery) -> Value {
    known_table_schema_with_columns(
        table,
        query_projection_columns(query),
        !query.bucket_parameters.is_empty(),
    )
}

fn known_table_schema_with_columns(
    table: &str,
    column_names: Vec<String>,
    has_parameters: bool,
) -> Value {
    json!({
        "schema": "public",
        "name": table,
        "replication_id": ["id"],
        "data_queries": true,
        "parameter_queries": has_parameters,
        "errors": [],
        "columns": column_names.into_iter().map(|name| json!({
            "name": name,
            "sqlite_type": "text",
            "internal_type": "text",
            "type": "text",
            "pg_type": "text",
        })).collect::<Vec<_>>()
    })
}

fn query_sql(query: &crate::sync_rules::CanonicalDataQuery) -> String {
    let mut sql = format!(
        "SELECT {} FROM public.{}",
        query_projection_query(query),
        query.source_table
    );
    if !query.bucket_parameters.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(
            &query
                .bucket_parameters
                .iter()
                .map(|parameter| {
                    format!(
                        "{} = {}",
                        parameter.source_column,
                        binding_sql(&parameter.binding)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
        );
    }
    sql
}

fn projection_columns(stream: &CanonicalStream) -> Vec<String> {
    query_projection_columns(&crate::sync_rules::CanonicalDataQuery {
        source_table: stream.source_table.clone(),
        output_table: stream.output_table.clone(),
        bucket_parameters: stream.bucket_parameters.clone(),
        row_filter: stream.row_filter.clone(),
        request_filter: stream.request_filter.clone(),
        projection: stream.projection.clone(),
    })
}

fn query_projection_query(query: &crate::sync_rules::CanonicalDataQuery) -> String {
    match &query.projection {
        CanonicalProjection::Star => "*".to_owned(),
        CanonicalProjection::StarWithComputed { computed } => {
            let mut items = computed
                .iter()
                .map(|column| column.alias.clone())
                .collect::<Vec<_>>();
            items.push("*".to_owned());
            items.join(", ")
        }
        CanonicalProjection::Columns { columns } => columns
            .iter()
            .map(|column| {
                if column.alias == column.source_column {
                    column.source_column.clone()
                } else {
                    format!("{} AS {}", column.source_column, column.alias)
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

fn query_projection_columns(query: &crate::sync_rules::CanonicalDataQuery) -> Vec<String> {
    match &query.projection {
        CanonicalProjection::Star => vec!["*".to_owned()],
        CanonicalProjection::StarWithComputed { computed } => {
            let mut columns = vec!["*".to_owned()];
            columns.extend(computed.iter().map(|column| column.alias.clone()));
            columns
        }
        CanonicalProjection::Columns { columns } => {
            columns.iter().map(|column| column.alias.clone()).collect()
        }
    }
}

fn stream_data_queries(stream: &CanonicalStream) -> Vec<crate::sync_rules::CanonicalDataQuery> {
    if stream.data_queries.is_empty() {
        vec![crate::sync_rules::CanonicalDataQuery {
            source_table: stream.source_table.clone(),
            output_table: stream.output_table.clone(),
            bucket_parameters: stream.bucket_parameters.clone(),
            row_filter: stream.row_filter.clone(),
            request_filter: stream.request_filter.clone(),
            projection: stream.projection.clone(),
        }]
    } else {
        stream.data_queries.clone()
    }
}

fn binding_json(binding: &CanonicalBinding) -> Value {
    match binding {
        CanonicalBinding::AuthParameter { name } => json!({"type": "auth_parameter", "name": name}),
        CanonicalBinding::SubscriptionParameter { name } => {
            json!({"type": "subscription_parameter", "name": name})
        }
        CanonicalBinding::RequestUserId => json!({"type": "request_user_id"}),
        CanonicalBinding::RequestJwt { claim } => json!({"type": "request_jwt", "claim": claim}),
        CanonicalBinding::RequestParameter { name } => {
            json!({"type": "request_parameter", "name": name})
        }
        CanonicalBinding::RequestParameterArray { name } => {
            json!({"type": "request_parameter_array", "name": name})
        }
        CanonicalBinding::ParameterQueryColumn { name, lookup } => {
            json!({"type": "parameter_query_column", "name": name, "query": lookup.raw_query})
        }
        CanonicalBinding::BucketParameter { name } => {
            json!({"type": "bucket_parameter", "name": name})
        }
    }
}
