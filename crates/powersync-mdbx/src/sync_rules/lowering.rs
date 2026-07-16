use std::collections::{BTreeMap, BTreeSet, HashMap};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest as _, Sha256};

use super::catalog::{
    canonical_storage_contract_id, contract_fingerprint, CANONICAL_PLAN_VERSION,
    SUPPORTED_COMPATIBILITY_VERSION, SUPPORTED_EDITION, SUPPORTED_STORAGE_VERSION,
};
use super::eval::{merged_bucket_parameters_from_queries, stream_bucket_groups};
use super::model::{
    AccumulatorQueryTemplate, CanonicalBinding, CanonicalBucketParameter,
    CanonicalComputedExpression, CanonicalComputedTerm, CanonicalDataQuery, CanonicalProjection,
    CanonicalSemanticPlan, CanonicalStream, CompiledLookupTablePlan, CompiledTablePlan,
    LiteralValue, Operand, ParameterLookupPlan, ParameterLookupSelectedColumn, Predicate,
    RustExecutionPlan, StreamDefinition, SyncRuleError,
};
use super::query::{
    contains_binding_reference, is_request_filter_predicate, is_row_filter_predicate,
    json_each_alias, last_dotted_identifier, normalize_identifier, parse_binding,
    parse_stream_query, split_and_predicates, split_ascii_case_insensitive_once, split_is_null,
    split_or_predicates, split_top_level_csv, sql_literal_to_json, strip_arrow_string_argument,
    trim_wrapping_parens, ParsedStreamQuery,
};
use super::yaml::parse_sync_rules_source;
use serde::Serialize;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(super) struct ParsedParameterQuery {
    columns: BTreeMap<String, Option<CanonicalBinding>>,
    request_filter: Option<String>,
    query: String,
}

pub fn compile_streams(
    definitions: &[StreamDefinition<'_>],
) -> Result<CanonicalSemanticPlan, SyncRuleError> {
    let streams = definitions
        .iter()
        .map(|definition| {
            compile_stream_definition(
                definition.name.to_owned(),
                &[definition.query.to_owned()],
                definition.auto_subscribe,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CanonicalSemanticPlan {
        version: CANONICAL_PLAN_VERSION,
        edition: SUPPORTED_EDITION,
        compatibility_version: SUPPORTED_COMPATIBILITY_VERSION,
        storage_version: SUPPORTED_STORAGE_VERSION,
        streams,
    })
}

pub fn compile_sync_rules_source(source: &str) -> Result<CanonicalSemanticPlan, SyncRuleError> {
    let parsed = parse_sync_rules_source(source)?;
    let streams = parsed
        .streams
        .into_iter()
        .map(|definition| {
            compile_stream_definition(
                definition.name,
                &definition.queries,
                definition.auto_subscribe,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CanonicalSemanticPlan {
        version: CANONICAL_PLAN_VERSION,
        edition: parsed.edition,
        compatibility_version: parsed.compatibility_version,
        storage_version: parsed.storage_version,
        streams,
    })
}

pub fn lower_canonical_semantic_plan(
    canonical: CanonicalSemanticPlan,
) -> Result<RustExecutionPlan, SyncRuleError> {
    if canonical.version != CANONICAL_PLAN_VERSION {
        return Err(SyncRuleError(format!(
            "unsupported canonical semantic plan version {}",
            canonical.version
        )));
    }
    if canonical.edition != SUPPORTED_EDITION {
        return Err(SyncRuleError(format!(
            "unsupported sync-rules edition {}; expected {}",
            canonical.edition, SUPPORTED_EDITION
        )));
    }
    if canonical.compatibility_version != SUPPORTED_COMPATIBILITY_VERSION {
        return Err(SyncRuleError(format!(
            "unsupported compatibility version {}; expected {}",
            canonical.compatibility_version, SUPPORTED_COMPATIBILITY_VERSION
        )));
    }
    if canonical.storage_version != SUPPORTED_STORAGE_VERSION {
        return Err(SyncRuleError(format!(
            "unsupported storage version {}; expected {}",
            canonical.storage_version, SUPPORTED_STORAGE_VERSION
        )));
    }

    let mut streams_by_name = HashMap::new();
    let mut tables_by_source: HashMap<String, CompiledTablePlan> = HashMap::new();
    let mut lookup_tables_by_source: BTreeMap<String, CompiledLookupTablePlan> = BTreeMap::new();
    let mut route_index_columns_by_object: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    let mut stream_bucket_groups_by_name = HashMap::new();
    let mut accumulator_queries_by_object: HashMap<String, Vec<AccumulatorQueryTemplate>> =
        HashMap::new();
    let mut route_index_columns_seen = BTreeSet::<(String, Vec<String>)>::new();
    let mut default_stream_names = Vec::new();

    for stream in &canonical.streams {
        let data_queries = stream.data_queries();

        validate_parameter_lookup_request_filters(stream)?;

        if streams_by_name
            .insert(stream.name.clone(), stream.clone())
            .is_some()
        {
            return Err(SyncRuleError(format!(
                "duplicate stream name {} in canonical semantic plan",
                stream.name
            )));
        }

        if stream.auto_subscribe {
            default_stream_names.push(stream.name.clone());
        }

        if stream_bucket_groups_by_name
            .insert(stream.name.clone(), stream_bucket_groups(stream))
            .is_some()
        {
            return Err(SyncRuleError(format!(
                "duplicate stream name {} in canonical semantic plan",
                stream.name
            )));
        }

        for query in data_queries {
            for parameter in &query.bucket_parameters {
                let CanonicalBinding::ParameterQueryColumn { lookup, .. } = &parameter.binding
                else {
                    continue;
                };
                let table = lookup_tables_by_source
                    .entry(lookup.source_table.clone())
                    .or_insert_with(|| CompiledLookupTablePlan {
                        source_table: lookup.source_table.clone(),
                        lookups: Vec::new(),
                    });
                if !table
                    .lookups
                    .iter()
                    .any(|existing| existing.lookup_id == lookup.lookup_id)
                {
                    table.lookups.push((**lookup).clone());
                }
            }

            accumulator_queries_by_object
                .entry(query.output_table.clone())
                .or_default()
                .push(AccumulatorQueryTemplate {
                    stream_name: stream.name.clone(),
                    object_type: query.output_table.clone(),
                    bucket_parameters: query.bucket_parameters.clone(),
                    row_filter: query.row_filter.clone(),
                    request_filter: query.request_filter.clone(),
                    projection: query.projection.clone(),
                    projection_key: serde_json::to_string(&query.projection)
                        .expect("current accumulator projection should serialize"),
                    is_default: stream.auto_subscribe && query.bucket_parameters.is_empty(),
                });

            let mut route_columns = query
                .bucket_parameters
                .iter()
                .map(|parameter| parameter.source_column.clone())
                .collect::<Vec<_>>();
            route_columns.sort();

            if !route_columns.is_empty()
                && route_index_columns_seen
                    .insert((query.output_table.clone(), route_columns.clone()))
            {
                route_index_columns_by_object
                    .entry(query.output_table.clone())
                    .or_default()
                    .push(route_columns.clone());
            }
            let object_id_expression = object_id_expression_from_projection(&query.projection);

            match tables_by_source.get_mut(&query.source_table) {
                Some(existing) => {
                    if existing.object_type != query.output_table {
                        return Err(SyncRuleError(format!(
                            "source table {} maps to conflicting output tables {} and {}",
                            query.source_table, existing.object_type, query.output_table
                        )));
                    }
                    for column in route_columns {
                        if !existing.route_columns.contains(&column) {
                            existing.route_columns.push(column);
                        }
                    }
                    if existing.object_id_expression != object_id_expression {
                        return Err(SyncRuleError(format!(
                            "source table {} maps to conflicting object id expressions",
                            query.source_table
                        )));
                    }
                    existing.route_columns.sort();
                }
                None => {
                    tables_by_source.insert(
                        query.source_table.clone(),
                        CompiledTablePlan {
                            source_table: query.source_table,
                            object_type: query.output_table,
                            route_columns,
                            object_id_expression,
                        },
                    );
                }
            }
        }
    }

    let storage_contract_id = canonical_storage_contract_id(&canonical);
    let storage_contract_fingerprint = contract_fingerprint(&storage_contract_id);
    Ok(RustExecutionPlan {
        canonical,
        streams_by_name,
        tables_by_source,
        lookup_tables_by_source,
        route_index_columns_by_object,
        stream_bucket_groups_by_name,
        accumulator_queries_by_object,
        default_stream_names,
        storage_contract_id,
        storage_contract_fingerprint,
    })
}

fn compile_stream_definition(
    name: String,
    queries: &[String],
    auto_subscribe: bool,
) -> Result<CanonicalStream, SyncRuleError> {
    let parsed = queries
        .iter()
        .map(|query| parse_stream_query(query))
        .collect::<Result<Vec<_>, _>>()?;
    let first = parsed
        .first()
        .ok_or_else(|| SyncRuleError(format!("stream {name} is missing a query definition")))?;
    let first_source_table = first.source_table.clone();
    let first_projection = first.projection.clone();
    let bucket_parameters = merged_bucket_parameters(&parsed)?;
    let data_queries = parsed
        .into_iter()
        .map(|query| CanonicalDataQuery {
            source_table: query.source_table.clone(),
            output_table: query.source_table,
            bucket_parameters: query.bucket_parameters,
            row_filter: query.row_filter,
            request_filter: query.request_filter,
            projection: query.projection,
        })
        .collect::<Vec<_>>();
    Ok(CanonicalStream {
        name,
        source_table: first_source_table.clone(),
        output_table: first_source_table,
        auto_subscribe,
        bucket_parameters,
        row_filter: data_queries
            .first()
            .and_then(|query| query.row_filter.clone()),
        request_filter: data_queries
            .first()
            .and_then(|query| query.request_filter.clone()),
        projection: first_projection,
        data_queries: if data_queries.len() <= 1
            && data_queries.iter().all(|query| {
                query.row_filter.is_none()
                    && query.request_filter.is_none()
                    && query
                        .bucket_parameters
                        .iter()
                        .all(|parameter| parameter.source_column == parameter.name)
            }) {
            Vec::new()
        } else {
            data_queries
        },
    })
}

pub(super) fn parse_parameter_query(query: &str) -> ParsedParameterQuery {
    let mut parsed = ParsedParameterQuery {
        query: query.trim().to_owned(),
        ..ParsedParameterQuery::default()
    };
    let upper = query.to_ascii_uppercase();
    let from_index = upper.find(" FROM ");
    let where_index = upper.find(" WHERE ");
    let select_end = from_index.or(where_index).unwrap_or(query.len());
    let select_clause = query
        .get("SELECT ".len()..select_end)
        .unwrap_or_default()
        .trim();
    let from_part = match (from_index, where_index) {
        (Some(from_index), Some(where_index)) if where_index > from_index => {
            &query[from_index + " FROM ".len()..where_index]
        }
        (Some(from_index), _) => &query[from_index + " FROM ".len()..],
        (None, _) => "",
    };
    let json_each_bindings = json_each_request_bindings(from_part);
    for item in split_top_level_csv(select_clause) {
        let Some(alias) = projection_alias(item) else {
            continue;
        };
        let binding = projection_json_each_binding(item, &json_each_bindings);
        parsed.columns.insert(alias, binding);
    }
    let Some(where_index) = where_index else {
        return parsed;
    };
    let where_part = &query[where_index + " WHERE ".len()..];
    let mut request_predicates = Vec::new();
    for predicate in split_and_predicates(where_part) {
        let predicate = predicate.trim();
        if is_request_filter_predicate(predicate) {
            request_predicates.push(predicate.to_owned());
        }
        let Some((left, right)) = predicate.split_once('=') else {
            continue;
        };
        let left_column = normalize_identifier(last_dotted_identifier(left));
        if let Some(binding) = parse_binding(right.trim()) {
            if let Some(slot) = parsed.columns.get_mut(&left_column) {
                *slot = Some(binding);
            }
            continue;
        }
        let right_column = normalize_identifier(last_dotted_identifier(right));
        if let Some(binding) = parse_binding(left.trim()) {
            if let Some(slot) = parsed.columns.get_mut(&right_column) {
                *slot = Some(binding);
            }
        }
    }
    parsed.request_filter =
        (!request_predicates.is_empty()).then(|| request_predicates.join(" AND "));
    parsed
}

pub fn parse_parameter_lookup_plan(raw: &str) -> Result<ParameterLookupPlan, SyncRuleError> {
    let raw_query = raw.trim().to_owned();
    let upper = raw_query.to_ascii_uppercase();
    if !upper.starts_with("SELECT ") {
        return Err(SyncRuleError(format!(
            "parameter lookup query must start with SELECT: {raw_query}"
        )));
    }

    let from_index = upper.find(" FROM ").ok_or_else(|| {
        SyncRuleError(format!(
            "parameter lookup query is missing FROM: {raw_query}"
        ))
    })?;
    let where_start = from_index + " FROM ".len();
    let where_index = upper[where_start..]
        .find(" WHERE ")
        .map(|index| where_start + index)
        .ok_or_else(|| {
            SyncRuleError(format!(
                "parameter lookup query is missing WHERE: {raw_query}"
            ))
        })?;

    let select_clause = if from_index <= "SELECT ".len() {
        ""
    } else {
        raw_query["SELECT ".len()..from_index].trim()
    };
    if select_clause.is_empty() {
        return Err(SyncRuleError(
            "parameter lookup query SELECT list must not be empty".to_owned(),
        ));
    }
    let selected = split_top_level_csv(select_clause)
        .into_iter()
        .map(parse_lookup_projection)
        .collect::<Result<Vec<_>, _>>()?;

    let from_clause = raw_query[where_start..where_index].trim();
    let from_items = split_top_level_csv(from_clause);
    if from_items.len() != 1
        || from_clause.to_ascii_lowercase().contains("json_each")
        || from_clause.to_ascii_lowercase().contains(" join ")
        || from_clause.contains('(')
    {
        if from_clause.to_ascii_lowercase().contains("json_each") {
            return Err(SyncRuleError(
                "json_each is not supported inside table-backed parameter lookup queries"
                    .to_owned(),
            ));
        }
        if from_clause.contains('(') {
            return Err(SyncRuleError(
                "sub-selects are not supported in parameter lookup queries".to_owned(),
            ));
        }
        return Err(SyncRuleError(
            "parameter lookup queries must have exactly one table and no joins".to_owned(),
        ));
    }
    let table_tokens = from_clause.split_whitespace().collect::<Vec<_>>();
    if table_tokens.len() != 1 {
        return Err(SyncRuleError(
            "parameter lookup queries must have exactly one table and no joins".to_owned(),
        ));
    }
    let source_table = normalize_identifier(table_tokens[0])
        .strip_prefix("public.")
        .map(str::to_owned)
        .unwrap_or_else(|| normalize_identifier(table_tokens[0]));
    if source_table.is_empty() {
        return Err(SyncRuleError(
            "parameter lookup query FROM table must not be empty".to_owned(),
        ));
    }
    // The WAL path only observes public-schema change events, so a lookup
    // table outside public would silently miss churn; reject it up front.
    if source_table.contains('.') {
        return Err(SyncRuleError(format!(
            "parameter lookup tables must be unqualified or in the public schema: {source_table}"
        )));
    }

    let where_clause = raw_query[where_index + " WHERE ".len()..].trim();
    if where_clause.is_empty() {
        return Err(SyncRuleError(
            "parameter lookup query WHERE clause must not be empty".to_owned(),
        ));
    }

    let mut key_bindings = BTreeMap::new();
    let mut row_predicates = Vec::new();
    for predicate in split_and_predicates(where_clause) {
        let predicate = trim_wrapping_parens(predicate.trim());
        if predicate.to_ascii_lowercase().contains("(select") {
            return Err(SyncRuleError(
                "sub-selects are not supported in parameter lookup queries".to_owned(),
            ));
        }
        if split_or_predicates(predicate).len() > 1 {
            return Err(SyncRuleError(format!(
                "OR is not supported in parameter lookup queries: {predicate}"
            )));
        }
        if split_ascii_case_insensitive_once(predicate, " IN ").is_some() {
            return Err(SyncRuleError(format!(
                "IN is not supported in parameter lookup queries: {predicate}"
            )));
        }

        // Mask literal contents first so a value like 'a>b' cannot trip the
        // operator scan.
        let comparison = mask_string_literals(predicate).replace("->>", "");
        if comparison.contains('>') || comparison.contains('<') || comparison.contains("!=") {
            return Err(SyncRuleError(format!(
                "non-equality comparison is not supported in parameter lookup queries: {predicate}"
            )));
        }

        if let Some((operand, negated)) = split_is_null(predicate) {
            if parse_binding(operand.trim()).is_some() {
                return Err(SyncRuleError(format!(
                    "request IS [NOT] NULL predicates are not supported in parameter lookup queries: {predicate}"
                )));
            }
            let column = lookup_column_name(operand, predicate)?;
            row_predicates.push(Predicate::IsNull {
                operand: Operand::Column { name: column },
                negated,
            });
            continue;
        }

        let Some((left, right)) = predicate.split_once('=') else {
            return Err(SyncRuleError(format!(
                "unsupported predicate in parameter lookup query: {predicate}"
            )));
        };
        let left = left.trim();
        let right = right.trim();
        let left_binding = parse_binding(left);
        let right_binding = parse_binding(right);

        match (left_binding, right_binding) {
            (Some(_), Some(_)) => {
                return Err(SyncRuleError(format!(
                    "binding = binding is not supported in parameter lookup queries: {predicate}"
                )))
            }
            (Some(binding), None) | (None, Some(binding)) => {
                let other = if parse_binding(left).is_some() {
                    right
                } else {
                    left
                };
                if sql_literal_to_json(other).is_some() {
                    if !is_lookup_request_binding(&binding) {
                        return Err(SyncRuleError(format!(
                            "unsupported binding in parameter lookup query: {predicate}"
                        )));
                    }
                    continue;
                }
                let column = lookup_column_name(other, predicate)?;
                if !is_lookup_key_binding(&binding) {
                    return Err(SyncRuleError(format!(
                        "unsupported binding in parameter lookup query: {predicate}"
                    )));
                }
                if key_bindings.insert(column.clone(), binding).is_some() {
                    return Err(SyncRuleError(format!(
                        "duplicate binding column {column} in parameter lookup query"
                    )));
                }
            }
            (None, None) => {
                let left_value = sql_literal_to_json(left);
                let right_value = sql_literal_to_json(right);
                let (column, value, column_on_left) = match (left_value, right_value) {
                    (None, Some(value)) => (lookup_column_name(left, predicate)?, value, true),
                    (Some(value), None) => (lookup_column_name(right, predicate)?, value, false),
                    _ => {
                        return Err(SyncRuleError(format!(
                            "row predicates in parameter lookup queries must compare a column to a string literal: {predicate}"
                        )))
                    }
                };
                let value = literal_value(value)?;
                if !matches!(value, LiteralValue::String(_)) {
                    return Err(SyncRuleError(format!(
                        "row predicates in parameter lookup queries must compare a column to a string literal: {predicate}"
                    )));
                }
                let literal = Operand::Literal { value };
                let column = Operand::Column { name: column };
                row_predicates.push(Predicate::Eq {
                    left: if column_on_left {
                        column.clone()
                    } else {
                        literal.clone()
                    },
                    right: if column_on_left { literal } else { column },
                });
            }
        }
    }

    if key_bindings.is_empty() {
        return Err(SyncRuleError(
            "parameter lookup query must contain at least one key binding".to_owned(),
        ));
    }
    let key_bindings = key_bindings.into_iter().collect::<Vec<_>>();
    let row_predicate = combine_lookup_predicates(row_predicates);
    let lookup_id = lookup_id(
        &raw_query,
        &source_table,
        &selected,
        &key_bindings,
        &row_predicate,
    );
    Ok(ParameterLookupPlan {
        raw_query,
        source_table,
        selected,
        key_bindings,
        row_predicate,
        lookup_id,
    })
}

fn parse_lookup_projection(item: &str) -> Result<ParameterLookupSelectedColumn, SyncRuleError> {
    let alias = projection_alias(item).ok_or_else(|| {
        SyncRuleError(format!(
            "unsupported SELECT item in parameter lookup query: {}",
            item.trim()
        ))
    })?;
    let source = split_ascii_case_insensitive_once(item, " AS ")
        .map(|(source, _)| source)
        .unwrap_or(item);
    let column = normalize_identifier(last_dotted_identifier(source));
    if !is_lookup_identifier(&column) {
        return Err(SyncRuleError(format!(
            "unsupported SELECT item in parameter lookup query: {}",
            item.trim()
        )));
    }
    Ok(ParameterLookupSelectedColumn { alias, column })
}

fn lookup_column_name(value: &str, predicate: &str) -> Result<String, SyncRuleError> {
    let column = normalize_identifier(last_dotted_identifier(value));
    if !is_lookup_identifier(&column) {
        return Err(SyncRuleError(format!(
            "unsupported column in parameter lookup predicate: {predicate}"
        )));
    }
    Ok(column)
}

/// Drop the contents of single-quoted SQL string literals (honoring `''`
/// escapes) so character scans cannot false-positive on literal contents.
fn mask_string_literals(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        output.push(character);
        if character != '\'' {
            continue;
        }
        while let Some(inner) = characters.next() {
            if inner == '\'' {
                if characters.peek() == Some(&'\'') {
                    characters.next();
                    continue;
                }
                output.push('\'');
                break;
            }
        }
    }
    output
}

fn is_lookup_identifier(value: &str) -> bool {
    // Values arrive normalized (outer quotes stripped), so embedded quote and
    // whitespace characters are what remains of a quoted PostgreSQL
    // identifier; both stay legal here.
    !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty()
                && ((part.starts_with('"') && part.ends_with('"'))
                    || part.chars().all(|character| {
                        character.is_ascii_alphanumeric()
                            || matches!(character, '_' | '"')
                            || character.is_ascii_whitespace()
                    }))
        })
}

fn is_lookup_key_binding(binding: &CanonicalBinding) -> bool {
    matches!(
        binding,
        CanonicalBinding::AuthParameter { .. }
            | CanonicalBinding::SubscriptionParameter { .. }
            | CanonicalBinding::RequestUserId
            | CanonicalBinding::RequestParameter { .. }
    )
}

fn is_lookup_request_binding(binding: &CanonicalBinding) -> bool {
    matches!(
        binding,
        CanonicalBinding::AuthParameter { .. }
            | CanonicalBinding::SubscriptionParameter { .. }
            | CanonicalBinding::RequestUserId
            | CanonicalBinding::RequestJwt { .. }
            | CanonicalBinding::RequestParameter { .. }
    )
}

fn literal_value(value: serde_json::Value) -> Result<LiteralValue, SyncRuleError> {
    match value {
        serde_json::Value::String(value) => Ok(LiteralValue::String(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(LiteralValue::Integer)
            .ok_or_else(|| SyncRuleError("unsupported numeric SQL literal".to_owned())),
        serde_json::Value::Bool(value) => Ok(LiteralValue::Boolean(value)),
        serde_json::Value::Null => Ok(LiteralValue::Null),
        _ => Err(SyncRuleError("unsupported SQL literal".to_owned())),
    }
}

fn combine_lookup_predicates(predicates: Vec<Predicate>) -> Option<Predicate> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(Predicate::And { terms: predicates }),
    }
}

#[derive(Serialize)]
struct LookupIdInput<'a> {
    raw_query: &'a str,
    source_table: &'a str,
    selected: &'a [ParameterLookupSelectedColumn],
    key_bindings: &'a [(String, CanonicalBinding)],
    row_predicate: &'a Option<Predicate>,
}

/// Identity is deliberately conservative: the raw query text is hashed
/// alongside the structured fields, so two spellings of the same plan get
/// distinct ids and materialize separately. The id feeds MDBX key prefixes
/// and therefore the storage contract; changing how it is derived is a
/// layout-version bump.
fn lookup_id(
    raw_query: &str,
    source_table: &str,
    selected: &[ParameterLookupSelectedColumn],
    key_bindings: &[(String, CanonicalBinding)],
    row_predicate: &Option<Predicate>,
) -> String {
    let input = LookupIdInput {
        raw_query,
        source_table,
        selected,
        key_bindings,
        row_predicate,
    };
    Sha256::digest(serde_json::to_vec(&input).expect("parameter lookup plan should serialize"))
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn lookup_request_predicates(raw: &str) -> Result<Vec<Predicate>, SyncRuleError> {
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();
    let from_index = upper
        .find(" FROM ")
        .ok_or_else(|| SyncRuleError("parameter lookup query is missing FROM".to_owned()))?;
    let where_start = from_index + " FROM ".len();
    let where_index = upper[where_start..]
        .find(" WHERE ")
        .map(|index| where_start + index)
        .ok_or_else(|| SyncRuleError("parameter lookup query is missing WHERE".to_owned()))?;
    let where_clause = trimmed[where_index + " WHERE ".len()..].trim();
    let mut predicates = Vec::new();
    for term in split_and_predicates(where_clause) {
        let term = trim_wrapping_parens(term.trim());
        let Some((left, right)) = term.split_once('=') else {
            continue;
        };
        let left = left.trim();
        let right = right.trim();
        let (binding, literal, binding_on_left) =
            match (parse_binding(left), sql_literal_to_json(right)) {
                (Some(binding), Some(literal)) => (binding, literal, true),
                _ => match (sql_literal_to_json(left), parse_binding(right)) {
                    (Some(literal), Some(binding)) => (binding, literal, false),
                    _ => continue,
                },
            };
        if !is_lookup_request_binding(&binding) {
            continue;
        }
        let binding = Operand::Binding { binding };
        let literal = Operand::Literal {
            value: literal_value(literal)?,
        };
        predicates.push(Predicate::Eq {
            left: if binding_on_left {
                binding.clone()
            } else {
                literal.clone()
            },
            right: if binding_on_left { literal } else { binding },
        });
    }
    Ok(predicates)
}

fn flattened_predicates(predicate: Option<&Predicate>) -> Vec<&Predicate> {
    fn flatten<'a>(predicate: &'a Predicate, output: &mut Vec<&'a Predicate>) {
        if let Predicate::And { terms } = predicate {
            for term in terms {
                flatten(term, output);
            }
        } else {
            output.push(predicate);
        }
    }
    let mut output = Vec::new();
    if let Some(predicate) = predicate {
        flatten(predicate, &mut output);
    }
    output
}

fn validate_parameter_lookup_request_filters(
    stream: &CanonicalStream,
) -> Result<(), SyncRuleError> {
    for group in stream_bucket_groups(stream) {
        let group_terms = flattened_predicates(group.request_filter.as_ref());
        for parameter in &group.bucket_parameters {
            let CanonicalBinding::ParameterQueryColumn { lookup, .. } = &parameter.binding else {
                continue;
            };
            for predicate in lookup_request_predicates(&lookup.raw_query)? {
                if !group_terms.iter().any(|term| **term == predicate) {
                    return Err(SyncRuleError(format!(
                        "parameter lookup request predicate is not mirrored in stream {} bucket group {}",
                        stream.name, group.index
                    )));
                }
            }
        }
    }
    Ok(())
}

fn json_each_request_bindings(from_part: &str) -> BTreeMap<String, CanonicalBinding> {
    let mut bindings = BTreeMap::new();
    let mut unaliased_count = 0_usize;
    for part in split_top_level_csv(from_part) {
        let Some(name) = json_each_request_parameter_name(part) else {
            continue;
        };
        let alias = json_each_alias(part).unwrap_or_else(|| {
            unaliased_count += 1;
            if unaliased_count == 1 {
                "json_each".to_owned()
            } else {
                format!("json_each_{unaliased_count}")
            }
        });
        bindings.insert(alias, CanonicalBinding::RequestParameterArray { name });
    }
    bindings
}

fn json_each_request_parameter_name(part: &str) -> Option<String> {
    let (_, after) = split_ascii_case_insensitive_once(part, "json_each(")?;
    let argument = &after[..matching_call_argument_len(after)?];
    strip_arrow_string_argument(argument.trim(), "connection.parameters()", "->>")
        .or_else(|| strip_arrow_string_argument(argument.trim(), "request.parameters()", "->>"))
}

fn matching_call_argument_len(value: &str) -> Option<usize> {
    let mut depth = 0_i32;
    let mut quote = None;
    for (index, byte) in value.bytes().enumerate() {
        if let Some(active) = quote {
            if byte == active {
                quote = None;
            }
            continue;
        }
        if byte == b'\'' {
            quote = Some(byte);
            continue;
        }
        match byte {
            b'(' => depth += 1,
            b')' if depth == 0 => return Some(index),
            b')' => depth -= 1,
            _ => {}
        }
    }
    None
}

fn projection_json_each_binding(
    item: &str,
    bindings: &BTreeMap<String, CanonicalBinding>,
) -> Option<CanonicalBinding> {
    let source = split_ascii_case_insensitive_once(item, " AS ")
        .map(|(source, _)| source)
        .unwrap_or(item)
        .trim();
    let (left, right) = source.split_once('.')?;
    if normalize_identifier(right) != "value" {
        return None;
    }
    bindings.get(&normalize_identifier(left)).cloned()
}

pub(super) fn normalize_data_query_with_parameters(
    query: &str,
    parameter_queries: &BTreeMap<String, ParsedParameterQuery>,
) -> Result<String, SyncRuleError> {
    let trimmed = query.trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(from_index) = upper.find(" FROM ") else {
        return Ok(trimmed.to_owned());
    };
    let Some(where_index) = upper[from_index + " FROM ".len()..].find(" WHERE ") else {
        return Ok(trimmed.to_owned());
    };
    let where_index = from_index + " FROM ".len() + where_index;
    let from_part = &trimmed[from_index + " FROM ".len()..where_index];
    let where_part = &trimmed[where_index + " WHERE ".len()..];
    let Some((cte_name, cte_alias)) = find_parameter_source_alias(from_part, parameter_queries)
    else {
        return Ok(normalize_query_without_parameter_source(
            trimmed, from_index, from_part, where_part,
        ));
    };
    let Some(parameter_query) = parameter_queries.get(&cte_name) else {
        return Ok(trimmed.to_owned());
    };

    let mut predicates = Vec::new();
    if let Some(request_filter) = &parameter_query.request_filter {
        predicates.push(request_filter.clone());
    }
    for predicate in split_and_predicates(where_part) {
        if let Some(rewritten) =
            rewrite_bucket_join_predicate(predicate, &cte_alias, parameter_query)?
        {
            predicates.push(rewritten);
        } else if parse_direct_parameter_predicate(predicate).is_some()
            || is_row_filter_predicate(predicate)
            || is_request_filter_predicate(predicate)
            || binding_or_has_direct_parameter(predicate)
        {
            predicates.push(predicate.trim().to_owned());
        }
    }

    if predicates.is_empty() {
        return Ok(format!(
            "{} FROM {}",
            &trimmed[..from_index],
            first_from_table(from_part),
        ));
    }
    Ok(format!(
        "{} FROM {} WHERE {}",
        &trimmed[..from_index],
        first_from_table(from_part),
        predicates.join(" AND ")
    ))
}

fn normalize_query_without_parameter_source(
    query: &str,
    from_index: usize,
    from_part: &str,
    where_part: &str,
) -> String {
    let predicates = split_and_predicates(where_part)
        .into_iter()
        .filter_map(|predicate| {
            if parse_direct_parameter_predicate(predicate).is_some()
                || is_row_filter_predicate(predicate)
                || is_request_filter_predicate(predicate)
                || binding_or_has_direct_parameter(predicate)
            {
                Some(predicate.trim().to_owned())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if predicates.is_empty() {
        return format!(
            "{} FROM {}",
            &query[..from_index],
            first_from_table(from_part)
        );
    }
    format!(
        "{} FROM {} WHERE {}",
        &query[..from_index],
        first_from_table(from_part),
        predicates.join(" AND ")
    )
}

fn find_parameter_source_alias(
    from_part: &str,
    parameter_queries: &BTreeMap<String, ParsedParameterQuery>,
) -> Option<(String, String)> {
    for part in split_top_level_csv(from_part).into_iter().skip(1) {
        let tokens = part.split_whitespace().collect::<Vec<_>>();
        let name = normalize_identifier(tokens.first().copied().unwrap_or_default());
        if !parameter_queries.contains_key(&name) {
            continue;
        }
        let alias = tokens
            .windows(2)
            .find_map(|pair| {
                if pair[0].eq_ignore_ascii_case("AS") {
                    Some(normalize_identifier(pair[1]))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| name.clone());
        return Some((name, alias));
    }
    None
}

fn rewrite_bucket_join_predicate(
    predicate: &str,
    cte_alias: &str,
    parameter_query: &ParsedParameterQuery,
) -> Result<Option<String>, SyncRuleError> {
    let (left, right, operator) =
        if let Some((left, right)) = split_ascii_case_insensitive_once(predicate, " IN ") {
            (left, right, "IN")
        } else {
            let Some((left, right)) = predicate.split_once('=') else {
                return Ok(None);
            };
            (left, right, "=")
        };
    let left_bucket = bucket_alias_column(left, cte_alias);
    let right_bucket = bucket_alias_column(right, cte_alias);
    match (left_bucket, right_bucket) {
        (Some(bucket_column), None) => {
            let source_column = normalize_identifier(last_dotted_identifier(right));
            let binding = parameter_query_binding(parameter_query, &bucket_column)?;
            Ok(Some(format_bucket_predicate(
                &source_column,
                &binding,
                operator,
                false,
            )))
        }
        (None, Some(bucket_column)) => {
            let source_column = normalize_identifier(last_dotted_identifier(left));
            let binding = parameter_query_binding(parameter_query, &bucket_column)?;
            Ok(Some(format_bucket_predicate(
                &source_column,
                &binding,
                operator,
                true,
            )))
        }
        _ => Ok(None),
    }
}

fn parameter_query_binding(
    parameter_query: &ParsedParameterQuery,
    column: &str,
) -> Result<CanonicalBinding, SyncRuleError> {
    if let Some(binding) = parameter_query.columns.get(column).and_then(Clone::clone) {
        return Ok(binding);
    }
    Ok(CanonicalBinding::ParameterQueryColumn {
        name: column.to_owned(),
        lookup: Box::new(parse_parameter_lookup_plan(&parameter_query.query)?),
    })
}

fn format_bucket_predicate(
    source_column: &str,
    binding: &CanonicalBinding,
    operator: &str,
    source_on_left: bool,
) -> String {
    let binding = binding_sql_fragment(binding);
    if operator.eq_ignore_ascii_case("IN") && !source_on_left {
        format!("{binding} IN {source_column}")
    } else {
        format!("{source_column} = {binding}")
    }
}

fn parse_direct_parameter_predicate(predicate: &str) -> Option<()> {
    let (left, right) = predicate.split_once('=')?;
    let source_column = normalize_identifier(last_dotted_identifier(left));
    if source_column.is_empty() {
        return None;
    }
    parse_binding(right.trim()).map(|_| ())
}

fn binding_sql_fragment(binding: &CanonicalBinding) -> String {
    match binding {
        CanonicalBinding::AuthParameter { name } => {
            format!("auth.parameter('{}')", escape_sql_string_literal(name))
        }
        CanonicalBinding::SubscriptionParameter { name } => {
            format!(
                "subscription.parameter('{}')",
                escape_sql_string_literal(name)
            )
        }
        CanonicalBinding::RequestUserId => "request.user_id()".to_owned(),
        CanonicalBinding::RequestJwt { claim } => {
            format!("request.jwt() ->> '{}'", escape_sql_string_literal(claim))
        }
        CanonicalBinding::RequestParameter { name } => {
            format!(
                "request.parameters() ->> '{}'",
                escape_sql_string_literal(name)
            )
        }
        CanonicalBinding::RequestParameterArray { name } => {
            format!(
                "request.parameter_array('{}')",
                escape_sql_string_literal(name)
            )
        }
        CanonicalBinding::ParameterQueryColumn { name, lookup } => {
            // The query is URL-safe base64 (no quotes); the column name is escaped.
            format!(
                "parameter_query_column('{}','{}')",
                URL_SAFE_NO_PAD.encode(&lookup.raw_query),
                escape_sql_string_literal(name)
            )
        }
        CanonicalBinding::BucketParameter { name } => format!("bucket.{name}"),
    }
}

/// Escape a value for embedding inside a single-quoted SQL string literal, so it
/// round-trips through the parser's matching un-escape. Names carrying an
/// apostrophe (e.g. namespaced JWT claims are quote-free, but defensive) must not
/// terminate the literal early.
fn escape_sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn bucket_alias_column(value: &str, alias: &str) -> Option<String> {
    let trimmed = value.trim();
    let (left, right) = trimmed.split_once('.')?;
    if normalize_identifier(left) == alias {
        return Some(normalize_identifier(right));
    }
    None
}

pub(super) fn first_from_table(from_part: &str) -> String {
    split_top_level_csv(from_part)
        .into_iter()
        .next()
        .unwrap_or(from_part)
        .trim()
        .to_owned()
}

fn projection_alias(item: &str) -> Option<String> {
    split_ascii_case_insensitive_once(item, " AS ")
        .map(|(_, alias)| normalize_identifier(alias))
        .or_else(|| Some(normalize_identifier(last_dotted_identifier(item))))
        .filter(|alias| !alias.is_empty() && alias != "*")
}

fn merged_bucket_parameters(
    queries: &[ParsedStreamQuery],
) -> Result<Vec<CanonicalBucketParameter>, SyncRuleError> {
    Ok(merged_bucket_parameters_from_queries(
        queries.iter().map(|query| &query.bucket_parameters),
    ))
}

fn object_id_expression_from_projection(
    projection: &CanonicalProjection,
) -> Option<CanonicalComputedExpression> {
    match projection {
        CanonicalProjection::Star => None,
        CanonicalProjection::StarWithComputed { computed } => computed
            .iter()
            .find(|column| column.alias == "id")
            .map(|column| column.expression.clone()),
        CanonicalProjection::Columns { columns } => columns
            .iter()
            .find(|column| column.alias == "id" && column.source_column != "id")
            .map(|column| CanonicalComputedExpression {
                terms: vec![CanonicalComputedTerm::Column {
                    source_column: column.source_column.clone(),
                }],
            }),
    }
}

fn binding_or_has_direct_parameter(predicate: &str) -> bool {
    contains_binding_reference(predicate)
        && split_or_predicates(trim_wrapping_parens(predicate))
            .into_iter()
            .any(|term| parse_direct_parameter_predicate(term).is_some())
}
