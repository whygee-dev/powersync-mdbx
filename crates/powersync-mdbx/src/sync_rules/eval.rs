use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use pg_walstream::{ColumnValue, RowData};
use serde::ser::{SerializeMap, Serializer as _};
use serde::Serialize;

use super::catalog::{bucket_name_for_stream_group, parse_bucket_values, STREAM_BUCKET_PREFIX};
use super::model::{
    AccumulatorQueryTemplate, CanonicalBinding, CanonicalBucketParameter,
    CanonicalComputedExpression, CanonicalComputedTerm, CanonicalProjectedColumn,
    CanonicalProjection, CanonicalSemanticPlan, CanonicalStream, CompiledTablePlan, JsonColumnType,
    JsonColumnTypes, Operand, Predicate, ResolvedSyncBucket, ResolvedSyncQuery, RustExecutionPlan,
    StreamBucketGroup, SyncRuleError,
};

impl ResolvedSyncBucket {
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn object_type(&self) -> &str {
        &self.object_type
    }

    pub fn is_default(&self) -> bool {
        self.is_default
    }

    pub fn route_constraints(&self) -> &BTreeMap<String, String> {
        &self.route_constraints
    }

    pub fn projection(&self) -> &CanonicalProjection {
        &self.projection
    }

    pub fn projection_key(&self) -> &str {
        &self.projection_key
    }

    pub fn queries(&self) -> &[ResolvedSyncQuery] {
        &self.queries
    }

    pub fn matches_object_and_routes(
        &self,
        object_type: &str,
        route_fields: &BTreeMap<String, String>,
    ) -> bool {
        self.query_for_object_and_routes(object_type, route_fields)
            .is_some()
    }

    fn query_for_object_and_routes(
        &self,
        object_type: &str,
        route_fields: &BTreeMap<String, String>,
    ) -> Option<&ResolvedSyncQuery> {
        self.queries.iter().find(|query| {
            query.object_type == object_type
                && route_constraints_match(&query.route_constraints, route_fields)
        })
    }

    pub fn matches_object_routes_and_data(
        &self,
        object_type: &str,
        route_fields: &BTreeMap<String, String>,
        data_json: &str,
    ) -> bool {
        self.query_for_object_and_routes(object_type, route_fields)
            .is_some_and(|query| row_filter_matches(query.row_filter.as_ref(), data_json))
    }

    pub fn project_document_json(
        &self,
        object_type: &str,
        data_json: &str,
    ) -> Result<String, SyncRuleError> {
        let query = self
            .queries
            .iter()
            .find(|query| query.object_type == object_type)
            .ok_or_else(|| {
                SyncRuleError(format!(
                    "stream {} has no data query for object type {}",
                    self.stream_name, object_type
                ))
            })?;
        match &query.projection {
            CanonicalProjection::Star => Ok(data_json.to_owned()),
            CanonicalProjection::StarWithComputed { computed } => {
                let value: serde_json::Value =
                    serde_json::from_str(data_json).map_err(|error| {
                        SyncRuleError(format!(
                            "failed to decode persisted JSON for stream {}: {error}",
                            self.stream_name
                        ))
                    })?;
                let mut object = value.as_object().cloned().ok_or_else(|| {
                    SyncRuleError(format!(
                        "persisted JSON for stream {} is not an object",
                        self.stream_name
                    ))
                })?;
                for column in computed {
                    object.insert(
                        column.alias.clone(),
                        serde_json::Value::String(evaluate_computed_expression(
                            &column.expression,
                            &object,
                        )?),
                    );
                }
                Ok(serde_json::Value::Object(object).to_string())
            }
            CanonicalProjection::Columns { columns } => {
                let value: serde_json::Value =
                    serde_json::from_str(data_json).map_err(|error| {
                        SyncRuleError(format!(
                            "failed to decode persisted JSON for stream {}: {error}",
                            self.stream_name
                        ))
                    })?;
                let object = value.as_object().ok_or_else(|| {
                    SyncRuleError(format!(
                        "persisted JSON for stream {} is not an object",
                        self.stream_name
                    ))
                })?;
                let mut projected = serde_json::Map::with_capacity(columns.len());
                for column in columns {
                    let value = object.get(&column.source_column).ok_or_else(|| {
                        SyncRuleError(format!(
                            "persisted JSON for stream {} is missing column {}",
                            self.stream_name, column.source_column
                        ))
                    })?;
                    projected.insert(column.alias.clone(), value.clone());
                }
                Ok(serde_json::Value::Object(projected).to_string())
            }
        }
    }
}

impl CompiledTablePlan {
    pub fn source_table(&self) -> &str {
        &self.source_table
    }

    pub fn object_type(&self) -> &str {
        &self.object_type
    }

    pub fn route_fields_for_row(
        &self,
        data: &RowData,
        required: bool,
    ) -> Result<BTreeMap<String, String>, SyncRuleError> {
        self.route_columns
            .iter()
            .map(|column| {
                let value = data.get(column).and_then(column_value_to_route_string);
                match (required, value) {
                    (true, Some(value)) => Ok(Some((column.clone(), value))),
                    (true, None) => Err(SyncRuleError(format!(
                        "{} change is missing required route column {}",
                        self.source_table, column
                    ))),
                    (false, Some(value)) => Ok(Some((column.clone(), value))),
                    (false, None) => Ok(None),
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|pairs| pairs.into_iter().flatten().collect())
    }

    pub fn serialize_full_row_json(&self, data: &RowData) -> Result<String, SyncRuleError> {
        let mut fields = data.iter().collect::<Vec<_>>();
        fields.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
        self.serialize_row_json_fields(fields, data.len(), None)
    }

    pub fn serialize_full_row_json_with_column_types(
        &self,
        data: &RowData,
        column_types: &JsonColumnTypes,
    ) -> Result<String, SyncRuleError> {
        let mut fields = data.iter().collect::<Vec<_>>();
        fields.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
        self.serialize_row_json_fields(fields, data.len(), Some(column_types))
    }

    pub fn serialize_full_row_json_presorted(
        &self,
        data: &RowData,
    ) -> Result<String, SyncRuleError> {
        self.serialize_row_json_fields(data.iter(), data.len(), None)
    }

    pub fn serialize_full_row_json_presorted_with_column_types(
        &self,
        data: &RowData,
        column_types: &JsonColumnTypes,
    ) -> Result<String, SyncRuleError> {
        self.serialize_row_json_fields(data.iter(), data.len(), Some(column_types))
    }

    pub fn project_row_json_from_serialized(
        &self,
        data: &RowData,
        projection: &CanonicalProjection,
        data_json: &str,
        column_types: Option<&JsonColumnTypes>,
    ) -> Result<String, SyncRuleError> {
        match projection {
            CanonicalProjection::Star => Ok(data_json.to_owned()),
            CanonicalProjection::StarWithComputed { computed } => {
                let value: serde_json::Value =
                    serde_json::from_str(data_json).map_err(|error| {
                        SyncRuleError(format!(
                            "failed to decode persisted JSON for {}: {error}",
                            self.source_table
                        ))
                    })?;
                let mut object = value.as_object().cloned().ok_or_else(|| {
                    SyncRuleError(format!(
                        "persisted JSON for {} is not an object",
                        self.source_table
                    ))
                })?;
                for column in computed {
                    object.insert(
                        column.alias.clone(),
                        serde_json::Value::String(evaluate_computed_expression(
                            &column.expression,
                            &object,
                        )?),
                    );
                }
                Ok(serde_json::Value::Object(object).to_string())
            }
            CanonicalProjection::Columns { columns } => {
                self.serialize_projected_row_json_fields(data, columns, column_types)
            }
        }
    }

    fn serialize_row_json_fields<'a, I>(
        &self,
        fields: I,
        field_count: usize,
        column_types: Option<&JsonColumnTypes>,
    ) -> Result<String, SyncRuleError>
    where
        I: IntoIterator<Item = (&'a Arc<str>, &'a ColumnValue)>,
    {
        let mut buffer = Vec::with_capacity(field_count.saturating_mul(32));
        {
            let mut serializer = serde_json::Serializer::new(&mut buffer);
            let mut map = serializer
                .serialize_map(Some(field_count))
                .map_err(|error| {
                    SyncRuleError(format!(
                        "failed to serialize {} row: {error}",
                        self.source_table
                    ))
                })?;
            for (name, value) in fields {
                let value = JsonColumnValue {
                    value,
                    column_type: column_types
                        .and_then(|types| types.get(name.as_ref()))
                        .copied()
                        .unwrap_or(JsonColumnType::String),
                };
                map.serialize_entry(name.as_ref(), &value)
                    .map_err(|error| {
                        SyncRuleError(format!(
                            "failed to serialize {}.{} value: {error}",
                            self.source_table, name
                        ))
                    })?;
            }
            map.end().map_err(|error| {
                SyncRuleError(format!(
                    "failed to serialize {} row: {error}",
                    self.source_table
                ))
            })?;
        }
        String::from_utf8(buffer).map_err(|error| {
            SyncRuleError(format!(
                "failed to encode serialized {} row as UTF-8: {error}",
                self.source_table
            ))
        })
    }

    fn serialize_projected_row_json_fields(
        &self,
        data: &RowData,
        columns: &[CanonicalProjectedColumn],
        column_types: Option<&JsonColumnTypes>,
    ) -> Result<String, SyncRuleError> {
        let mut buffer = Vec::with_capacity(columns.len().saturating_mul(32));
        {
            let mut serializer = serde_json::Serializer::new(&mut buffer);
            let mut map = serializer
                .serialize_map(Some(columns.len()))
                .map_err(|error| {
                    SyncRuleError(format!(
                        "failed to serialize {} projected row: {error}",
                        self.source_table
                    ))
                })?;
            for column in columns {
                let column_value = data.get(&column.source_column).ok_or_else(|| {
                    SyncRuleError(format!(
                        "{} row is missing projected column {}",
                        self.source_table, column.source_column
                    ))
                })?;
                let value = JsonColumnValue {
                    value: column_value,
                    column_type: column_types
                        .and_then(|types| types.get(&column.source_column))
                        .copied()
                        .unwrap_or(JsonColumnType::String),
                };
                map.serialize_entry(column.alias.as_str(), &value)
                    .map_err(|error| {
                        SyncRuleError(format!(
                            "failed to serialize {}.{} projected value: {error}",
                            self.source_table, column.source_column
                        ))
                    })?;
            }
            map.end().map_err(|error| {
                SyncRuleError(format!(
                    "failed to serialize {} projected row: {error}",
                    self.source_table
                ))
            })?;
        }
        String::from_utf8(buffer).map_err(|error| {
            SyncRuleError(format!(
                "failed to encode serialized {} projected row as UTF-8: {error}",
                self.source_table
            ))
        })
    }

    pub fn object_id_for_row(&self, data: &RowData) -> Result<String, SyncRuleError> {
        let Some(expression) = &self.object_id_expression else {
            return data
                .get("id")
                .and_then(column_value_to_route_string)
                .ok_or_else(|| {
                    SyncRuleError(format!(
                        "{} change is missing required object id column id",
                        self.source_table
                    ))
                });
        };
        evaluate_computed_expression_for_row(expression, data, &self.source_table)
    }
}

struct JsonColumnValue<'a> {
    value: &'a ColumnValue,
    column_type: JsonColumnType,
}

impl Serialize for JsonColumnValue<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let Some(text) = self.value.as_str() else {
            return self.value.serialize(serializer);
        };

        match self.column_type {
            JsonColumnType::String => serializer.serialize_str(text),
            JsonColumnType::Number => serialize_json_number(text, serializer),
            JsonColumnType::Boolean => serialize_json_bool(text, serializer),
            JsonColumnType::Timestamp => match canonical_timestamp_string(text) {
                Some(canonical) => serializer.serialize_str(&canonical),
                // Not a recognizable timestamp (e.g. a mistyped column): emit the
                // raw text rather than fabricating an ISO value from it.
                None => serializer.serialize_str(text),
            },
            JsonColumnType::Json => serialize_json_text(text, serializer),
        }
    }
}

fn serialize_json_number<S: serde::Serializer>(
    text: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    if let Ok(value) = text.parse::<i64>() {
        return serializer.serialize_i64(value);
    }
    if let Ok(value) = text.parse::<u64>() {
        return serializer.serialize_u64(value);
    }
    if let Ok(value) = text.parse::<f64>() {
        if value.is_finite() {
            return serializer.serialize_f64(value);
        }
    }
    serializer.serialize_str(text)
}

fn serialize_json_bool<S: serde::Serializer>(
    text: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match text {
        "t" | "true" | "TRUE" => serializer.serialize_bool(true),
        "f" | "false" | "FALSE" => serializer.serialize_bool(false),
        _ => serializer.serialize_str(text),
    }
}

fn serialize_json_text<S: serde::Serializer>(
    text: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(value) => value.serialize(serializer),
        Err(_) => serializer.serialize_str(text),
    }
}

fn canonical_timestamp_string(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if !looks_like_timestamp(trimmed) {
        return None;
    }
    let (date_time, timezone) = split_timestamp_timezone(trimmed);
    let mut normalized = date_time.replacen(' ', "T", 1);
    if !normalized.contains('.') {
        normalized.push_str(".000000");
    } else if let Some((prefix, fraction)) = normalized.rsplit_once('.') {
        let mut fraction = fraction.to_owned();
        if fraction.len() < 6 {
            fraction.extend(std::iter::repeat_n('0', 6 - fraction.len()));
        } else if fraction.len() > 6 {
            fraction.truncate(6);
        }
        normalized = format!("{prefix}.{fraction}");
    }

    Some(match timezone {
        "" | "Z" | "+00" | "+00:00" | "+0000" => {
            normalized.push('Z');
            normalized
        }
        other => format!("{normalized}{other}"),
    })
}

/// Cheap structural guard that `text` begins with an ISO date (`YYYY-MM-DD`)
/// before it is reshaped into a canonical timestamp — without it, arbitrary text
/// in a timestamp-typed column would be silently fabricated into an ISO value.
fn looks_like_timestamp(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() >= 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5].is_ascii_digit()
        && bytes[6].is_ascii_digit()
        && bytes[7] == b'-'
        && bytes[8].is_ascii_digit()
        && bytes[9].is_ascii_digit()
}

fn split_timestamp_timezone(text: &str) -> (&str, &str) {
    if let Some(value) = text.strip_suffix('Z') {
        return (value, "Z");
    }
    for index in (10..text.len()).rev() {
        let byte = text.as_bytes()[index];
        if byte == b'+' || byte == b'-' {
            return (&text[..index], &text[index..]);
        }
    }
    (text, "")
}

impl RustExecutionPlan {
    pub fn canonical(&self) -> &CanonicalSemanticPlan {
        &self.canonical
    }

    pub fn storage_contract_id(&self) -> &str {
        &self.storage_contract_id
    }

    /// Short stable digest for logging: the full contract id embeds the
    /// entire canonical plan JSON.
    pub fn storage_contract_fingerprint(&self) -> &str {
        &self.storage_contract_fingerprint
    }

    pub fn resolve_bucket_request(&self, name: &str) -> Option<ResolvedSyncBucket> {
        let suffix = name.strip_prefix(STREAM_BUCKET_PREFIX)?;
        let (stream_name, group_and_values) = suffix.split_once('|')?;
        let values_start = group_and_values.find('[')?;
        let group_index = group_and_values[..values_start].parse::<usize>().ok()?;
        let encoded_values = &group_and_values[values_start..];
        let values = parse_bucket_values(encoded_values).ok()?;
        let stream = self.streams_by_name.get(stream_name)?;
        let group = self
            .stream_bucket_groups_by_name
            .get(stream_name)?
            .iter()
            .find(|group| group.index == group_index)?;
        let bucket_parameters = &group.bucket_parameters;
        if values.len() != bucket_parameters.len() {
            return None;
        }

        let route_values = bucket_parameters
            .iter()
            .zip(values)
            .map(|(parameter, value)| (parameter.name.clone(), value))
            .collect::<BTreeMap<_, _>>();
        let queries = group
            .queries
            .iter()
            .map(|query| {
                let route_constraints = query
                    .bucket_parameters
                    .iter()
                    .filter_map(|parameter| {
                        route_values
                            .get(&parameter.name)
                            .map(|value| (parameter.source_column.clone(), value.clone()))
                    })
                    .collect::<BTreeMap<_, _>>();
                ResolvedSyncQuery {
                    object_type: query.output_table.clone(),
                    checkpoint_accumulator_key: checkpoint_accumulator_name(
                        &query.output_table,
                        &route_constraints,
                        query.row_filter.as_ref(),
                        query.request_filter.as_ref(),
                        &query.projection,
                    ),
                    route_constraints,
                    row_filter: query.row_filter.clone(),
                    request_filter: query.request_filter.clone(),
                    projection: query.projection.clone(),
                }
            })
            .collect::<Vec<_>>();
        let first = queries.first()?;

        Some(ResolvedSyncBucket {
            bucket_name: name.to_owned(),
            stream_name: stream.name.clone(),
            object_type: first.object_type.clone(),
            route_constraints: first.route_constraints.clone(),
            projection: first.projection.clone(),
            projection_key: projection_key(&first.projection),
            queries,
            is_default: stream.auto_subscribe && bucket_parameters.is_empty(),
        })
    }

    pub fn default_bucket_requests(&self) -> Vec<ResolvedSyncBucket> {
        self.default_stream_names
            .iter()
            .filter_map(|stream_name| {
                let groups = self.stream_bucket_groups_by_name.get(stream_name)?;
                Some(
                    groups
                        .iter()
                        .filter(|group| group.bucket_parameters.is_empty())
                        .filter_map(|group| {
                            let bucket_name =
                                bucket_name_for_stream_group(stream_name, group.index, &[]);
                            self.resolve_bucket_request(&bucket_name)
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect()
    }

    pub fn default_bucket_requests_matching<F>(&self, binding_value: F) -> Vec<ResolvedSyncBucket>
    where
        F: Fn(&CanonicalBinding) -> Option<String> + Copy,
    {
        self.default_stream_names
            .iter()
            .filter_map(|stream_name| {
                let groups = self.stream_bucket_groups_by_name.get(stream_name)?;
                Some(
                    groups
                        .iter()
                        .filter(|group| {
                            group.bucket_parameters.is_empty()
                                && request_filter_matches(
                                    group.request_filter.as_ref(),
                                    binding_value,
                                )
                        })
                        .filter_map(|group| {
                            let bucket_name =
                                bucket_name_for_stream_group(stream_name, group.index, &[]);
                            self.resolve_bucket_request(&bucket_name)
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect()
    }

    pub fn table_plan(&self, source_table: &str) -> Option<&CompiledTablePlan> {
        self.tables_by_source.get(source_table)
    }

    pub fn stream(&self, name: &str) -> Option<&CanonicalStream> {
        self.streams_by_name.get(name)
    }

    pub fn source_tables(&self) -> Vec<&CompiledTablePlan> {
        self.tables_by_source.values().collect()
    }

    pub fn required_route_indexes_for_row(
        &self,
        object_type: &str,
        route_fields: &BTreeMap<String, String>,
    ) -> Vec<BTreeMap<String, String>> {
        self.route_index_columns_by_object
            .get(object_type)
            .into_iter()
            .flat_map(|indexes| indexes.iter())
            .filter_map(|columns| {
                let mut constraints = BTreeMap::new();
                for column in columns {
                    let value = route_fields.get(column)?;
                    constraints.insert(column.clone(), value.clone());
                }
                Some(constraints)
            })
            .collect()
    }

    pub fn accumulator_buckets_for_row(
        &self,
        object_type: &str,
        route_fields: &BTreeMap<String, String>,
    ) -> Vec<ResolvedSyncBucket> {
        let mut buckets = Vec::new();
        let mut seen = BTreeSet::new();

        for query in self
            .accumulator_queries_by_object
            .get(object_type)
            .into_iter()
            .flat_map(|queries| queries.iter())
        {
            let Some(route_constraints) = route_constraints_from_template(query, route_fields)
            else {
                continue;
            };
            let fingerprint = serde_json::to_string(&(
                &query.object_type,
                &route_constraints,
                &query.row_filter,
                &query.request_filter,
                &query.projection,
            ))
            .expect("accumulator fingerprint should serialize");
            if !seen.insert(fingerprint.clone()) {
                continue;
            }

            buckets.push(ResolvedSyncBucket {
                bucket_name: format!("accumulator:{fingerprint}"),
                stream_name: query.stream_name.clone(),
                object_type: query.object_type.clone(),
                route_constraints: route_constraints.clone(),
                projection: query.projection.clone(),
                projection_key: query.projection_key.clone(),
                queries: vec![ResolvedSyncQuery {
                    object_type: query.object_type.clone(),
                    checkpoint_accumulator_key: checkpoint_accumulator_name(
                        &query.object_type,
                        &route_constraints,
                        query.row_filter.as_ref(),
                        query.request_filter.as_ref(),
                        &query.projection,
                    ),
                    route_constraints,
                    row_filter: query.row_filter.clone(),
                    request_filter: query.request_filter.clone(),
                    projection: query.projection.clone(),
                }],
                is_default: query.is_default,
            });
        }

        buckets
    }
}

fn route_constraints_from_template(
    query: &AccumulatorQueryTemplate,
    route_fields: &BTreeMap<String, String>,
) -> Option<BTreeMap<String, String>> {
    query
        .bucket_parameters
        .iter()
        .map(|parameter| {
            route_fields
                .get(&parameter.source_column)
                .map(|value| (parameter.source_column.clone(), value.clone()))
        })
        .collect()
}

fn checkpoint_accumulator_name(
    object_type: &str,
    route_constraints: &BTreeMap<String, String>,
    row_filter: Option<&Predicate>,
    request_filter: Option<&Predicate>,
    projection: &CanonicalProjection,
) -> String {
    serde_json::json!({
        "object_type": object_type,
        "route_constraints": route_constraints,
        "row_filter": row_filter,
        "request_filter": request_filter,
        "projection": projection,
    })
    .to_string()
}

fn projection_key(projection: &CanonicalProjection) -> String {
    serde_json::to_string(projection).expect("current accumulator projection should serialize")
}

pub(super) fn stream_bucket_groups(stream: &CanonicalStream) -> Vec<StreamBucketGroup> {
    let mut groups: Vec<StreamBucketGroup> = Vec::new();
    for query in stream.data_queries() {
        let bucket_parameters = merged_bucket_parameters_from_queries([&query.bucket_parameters]);
        let request_filter = query.request_filter.clone();
        if let Some(group) = groups.iter_mut().find(|group| {
            group.bucket_parameters == bucket_parameters && group.request_filter == request_filter
        }) {
            group.queries.push(query);
            continue;
        }
        groups.push(StreamBucketGroup {
            index: groups.len(),
            bucket_parameters,
            request_filter,
            queries: vec![query],
        });
    }
    groups
}

pub(super) fn merged_bucket_parameters_from_queries<'a>(
    queries: impl IntoIterator<Item = &'a Vec<CanonicalBucketParameter>>,
) -> Vec<CanonicalBucketParameter> {
    let mut merged = Vec::new();
    for query in queries {
        for parameter in query {
            if merged.iter().any(|existing: &CanonicalBucketParameter| {
                existing.name == parameter.name && existing.binding == parameter.binding
            }) {
                continue;
            }
            merged.push(CanonicalBucketParameter {
                name: parameter.name.clone(),
                source_column: parameter.name.clone(),
                binding: parameter.binding.clone(),
            });
        }
    }
    merged
}

fn route_constraints_match(
    route_constraints: &BTreeMap<String, String>,
    route_fields: &BTreeMap<String, String>,
) -> bool {
    route_constraints.iter().all(|(key, expected)| {
        route_fields
            .get(key)
            .is_some_and(|actual| actual == expected || route_json_array_contains(actual, expected))
    })
}

fn evaluate_computed_expression(
    expression: &CanonicalComputedExpression,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, SyncRuleError> {
    let mut output = String::new();
    for term in &expression.terms {
        match term {
            CanonicalComputedTerm::Literal { value } => output.push_str(value),
            CanonicalComputedTerm::Column { source_column } => {
                let value = object.get(source_column).ok_or_else(|| {
                    SyncRuleError(format!(
                        "persisted JSON is missing computed projection column {source_column}"
                    ))
                })?;
                if let Some(value) = value.as_str() {
                    output.push_str(value);
                } else if let Some(value) = value.as_i64() {
                    output.push_str(&value.to_string());
                } else if let Some(value) = value.as_u64() {
                    output.push_str(&value.to_string());
                } else if let Some(value) = value.as_bool() {
                    output.push_str(if value { "true" } else { "false" });
                } else {
                    return Err(SyncRuleError(format!(
                        "computed projection column {source_column} is not scalar"
                    )));
                }
            }
        }
    }
    Ok(output)
}

fn evaluate_computed_expression_for_row(
    expression: &CanonicalComputedExpression,
    row: &RowData,
    source_table: &str,
) -> Result<String, SyncRuleError> {
    let mut output = String::new();
    for term in &expression.terms {
        match term {
            CanonicalComputedTerm::Literal { value } => output.push_str(value),
            CanonicalComputedTerm::Column { source_column } => {
                let value = row
                    .get(source_column)
                    .and_then(column_value_to_route_string)
                    .ok_or_else(|| {
                        SyncRuleError(format!(
                            "{} change is missing required object id column {}",
                            source_table, source_column
                        ))
                    })?;
                output.push_str(&value);
            }
        }
    }
    Ok(output)
}

fn route_json_array_contains(actual: &str, expected: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(actual)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .is_some_and(|values| {
            values.iter().any(|value| {
                value.as_str() == Some(expected)
                    || value
                        .as_i64()
                        .map(|number| number.to_string() == expected)
                        .unwrap_or(false)
                    || value
                        .as_u64()
                        .map(|number| number.to_string() == expected)
                        .unwrap_or(false)
            })
        })
}

fn row_filter_matches(row_filter: Option<&Predicate>, data_json: &str) -> bool {
    let Some(row_filter) = row_filter else {
        return true;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(data_json) else {
        return false;
    };
    row_predicate_eval(row_filter, &value)
}

pub fn request_filter_matches<F>(request_filter: Option<&Predicate>, binding_value: F) -> bool
where
    F: Fn(&CanonicalBinding) -> Option<String> + Copy,
{
    match request_filter {
        Some(request_filter) => request_predicate_eval(request_filter, binding_value),
        None => true,
    }
}

/// Evaluate a predicate against a persisted row's JSON. In the row context a
/// `Column` operand resolves against the row and a `Binding` cannot occur (row
/// filters never carry bindings). `Eq`/`In` compare positionally, preserving the
/// directional, string-anchored coercion of `json_values_equal`.
fn row_predicate_eval(predicate: &Predicate, row: &serde_json::Value) -> bool {
    match predicate {
        Predicate::And { terms } => terms.iter().all(|term| row_predicate_eval(term, row)),
        Predicate::Or { terms } => terms.iter().any(|term| row_predicate_eval(term, row)),
        Predicate::IsNull { operand, negated } => {
            let is_null = row_operand_value(operand, row).is_none_or(|value| value.is_null());
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        Predicate::In { left, right } => {
            let left_value = row_operand_value(left, row);
            let right_value = row_operand_value(right, row);
            match (left_value.as_ref(), right_value.as_ref()) {
                (Some(left), Some(serde_json::Value::Array(values))) => {
                    values.iter().any(|value| json_values_equal(left, value))
                }
                (Some(left), Some(right)) => json_values_equal(left, right),
                _ => false,
            }
        }
        Predicate::Eq { left, right } => {
            let left_value = row_operand_value(left, row);
            let right_value = row_operand_value(right, row);
            match (left_value.as_ref(), right_value.as_ref()) {
                (Some(left), Some(right)) => json_values_equal(left, right),
                _ => false,
            }
        }
    }
}

/// Resolve a row-context operand to the JSON value it compares as: a literal's
/// own value, or the named column looked up in the row (absent → `None`). A
/// `Binding` cannot appear in a row filter, so it resolves to `None`.
fn row_operand_value(operand: &Operand, row: &serde_json::Value) -> Option<serde_json::Value> {
    match operand {
        Operand::Literal { value } => Some(value.to_json()),
        Operand::Column { name } => row.as_object().and_then(|object| object.get(name)).cloned(),
        Operand::Binding { .. } => None,
    }
}

/// Evaluate a predicate against the request context. Only `Binding` operands
/// resolve here (via `binding_value`); a `Column` cannot occur in a request
/// filter, and neither can `In`, so it never matches.
fn request_predicate_eval<F>(predicate: &Predicate, binding_value: F) -> bool
where
    F: Fn(&CanonicalBinding) -> Option<String> + Copy,
{
    match predicate {
        Predicate::And { terms } => terms
            .iter()
            .all(|term| request_predicate_eval(term, binding_value)),
        Predicate::Or { terms } => terms
            .iter()
            .any(|term| request_predicate_eval(term, binding_value)),
        Predicate::IsNull { operand, negated } => {
            let is_null =
                request_operand_value(operand, binding_value).is_none_or(|value| value.is_empty());
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        Predicate::Eq { left, right } => request_eq(left, right, binding_value),
        Predicate::In { .. } => false,
    }
}

/// Resolve a request-context operand's bound string value, or `None` for a
/// non-binding operand (which cannot legitimately appear in a request filter).
fn request_operand_value<F>(operand: &Operand, binding_value: F) -> Option<String>
where
    F: Fn(&CanonicalBinding) -> Option<String>,
{
    match operand {
        Operand::Binding { binding } => binding_value(binding),
        Operand::Literal { .. } | Operand::Column { .. } => None,
    }
}

/// Request-context equality. Whichever side is the binding is resolved and
/// wrapped as a JSON string, then compared against the literal side via
/// `json_values_equal` — anchoring the bound value on the left so its string
/// coercion applies, exactly as the old string evaluator did. The binding side
/// is checked left-first to match the former operand precedence.
fn request_eq<F>(left: &Operand, right: &Operand, binding_value: F) -> bool
where
    F: Fn(&CanonicalBinding) -> Option<String> + Copy,
{
    let (binding, literal) = match (left, right) {
        (Operand::Binding { binding }, other) => (binding, other),
        (other, Operand::Binding { binding }) => (binding, other),
        _ => return false,
    };
    let Some(actual) = binding_value(binding) else {
        return false;
    };
    let Operand::Literal { value } = literal else {
        return false;
    };
    json_values_equal(&serde_json::Value::String(actual), &value.to_json())
}

fn json_values_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    left == right
        || left.as_str().is_some_and(|left| {
            right.as_str() == Some(left)
                || right
                    .as_i64()
                    .map(|number| left == number.to_string())
                    .unwrap_or(false)
                || right
                    .as_u64()
                    .map(|number| left == number.to_string())
                    .unwrap_or(false)
                || right
                    .as_bool()
                    .map(|boolean| {
                        left.eq_ignore_ascii_case(if boolean { "true" } else { "false" })
                    })
                    .unwrap_or(false)
        })
}

fn column_value_to_route_string(value: &pg_walstream::ColumnValue) -> Option<String> {
    if value.is_null() {
        None
    } else {
        Some(value.to_string())
    }
}
