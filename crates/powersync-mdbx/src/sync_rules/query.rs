use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

use super::lowering::first_from_table;
use super::model::{
    CanonicalBinding, CanonicalBucketParameter, CanonicalComputedColumn,
    CanonicalComputedExpression, CanonicalComputedTerm, CanonicalProjectedColumn,
    CanonicalProjection, LiteralValue, Operand, Predicate, SyncRuleError,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct ParsedStreamQuery {
    pub(super) source_table: String,
    pub(super) bucket_parameters: Vec<CanonicalBucketParameter>,
    pub(super) row_filter: Option<Predicate>,
    pub(super) request_filter: Option<Predicate>,
    pub(super) projection: CanonicalProjection,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ParsedWhereClause {
    bucket_parameters: Vec<CanonicalBucketParameter>,
    row_filter: Option<Predicate>,
    request_filter: Option<Predicate>,
}

pub(crate) fn json_each_alias(part: &str) -> Option<String> {
    let tokens = part.split_whitespace().collect::<Vec<_>>();
    tokens
        .windows(2)
        .find_map(|pair| {
            if pair[0].eq_ignore_ascii_case("AS") {
                Some(normalize_identifier(pair[1]))
            } else {
                None
            }
        })
        .filter(|alias| !alias.is_empty())
}

// Shared with control_plane's parameter-query rewriting: both sides must
// parse the same `with:` SQL fragments identically (notably double-quoted
// identifiers inside CSV lists).
pub(crate) fn split_top_level_csv(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut quote: Option<char> = None;
    for (index, ch) in value.char_indices() {
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(value[start..].trim());
    parts
}

fn split_top_level_operator<'a>(value: &'a str, operator: &str) -> Vec<&'a str> {
    let bytes = value.as_bytes();
    let operator_bytes = operator.as_bytes();
    let operator_len = operator_bytes.len();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut index = 0;
    let mut depth = 0_i32;
    let mut quote: Option<u8> = None;
    while index + operator_len <= bytes.len() {
        let byte = bytes[index];
        if let Some(active) = quote {
            if byte == active {
                quote = None;
            }
            index += 1;
            continue;
        }
        if byte == b'\'' {
            quote = Some(byte);
            index += 1;
            continue;
        }
        match byte {
            b'(' => {
                depth += 1;
                index += 1;
                continue;
            }
            b')' => {
                depth -= 1;
                index += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 && bytes[index..index + operator_len] == *operator_bytes {
            let part = value[start..index].trim();
            if !part.is_empty() {
                parts.push(part);
            }
            start = index + operator_len;
            index += operator_len;
            continue;
        }
        index += 1;
    }
    let part = value[start..].trim();
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

pub(super) fn last_dotted_identifier(value: &str) -> &str {
    value.trim().rsplit('.').next().unwrap_or(value).trim()
}

pub(super) fn parse_stream_query(query: &str) -> Result<ParsedStreamQuery, SyncRuleError> {
    let trimmed = query.trim();
    let select_prefix = "SELECT ";
    let from_delimiter = " FROM ";
    let where_delimiter = " WHERE ";

    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with(select_prefix) {
        return Err(SyncRuleError(format!(
            "unsupported sync-rule SQL (missing SELECT): {trimmed}"
        )));
    }

    let from_index = upper.find(from_delimiter).ok_or_else(|| {
        SyncRuleError(format!(
            "unsupported sync-rule SQL (missing FROM): {trimmed}"
        ))
    })?;

    let select_clause = trimmed[select_prefix.len()..from_index].trim();
    let after_from = &trimmed[from_index + from_delimiter.len()..];
    let after_from_upper = &upper[from_index + from_delimiter.len()..];
    let (table_part, where_part) = match after_from_upper.find(where_delimiter) {
        Some(where_index) => (
            after_from[..where_index].trim(),
            Some(after_from[where_index + where_delimiter.len()..].trim()),
        ),
        None => (after_from.trim(), None),
    };

    let source_table = parse_source_table(table_part)?;
    let projection = parse_projection(select_clause)?;
    let where_clause = parse_where_clause(where_part)?;

    Ok(ParsedStreamQuery {
        source_table,
        bucket_parameters: where_clause.bucket_parameters,
        row_filter: where_clause.row_filter,
        request_filter: where_clause.request_filter,
        projection,
    })
}

fn parse_source_table(raw: &str) -> Result<String, SyncRuleError> {
    let first_table = first_from_table(raw);
    let table_token = first_table
        .split_whitespace()
        .next()
        .unwrap_or(first_table.as_str());
    let normalized = normalize_identifier(table_token);
    let normalized = if let Some(table) = normalized.strip_prefix("public.") {
        table.to_owned()
    } else if normalized.contains('.') {
        return Err(SyncRuleError(format!(
            "unsupported source table {}; only public.<table> is supported",
            raw.trim()
        )));
    } else {
        normalized
    };

    if normalized.is_empty() {
        return Err(SyncRuleError("source table must not be empty".to_owned()));
    }

    Ok(normalized)
}

fn parse_projection(select_clause: &str) -> Result<CanonicalProjection, SyncRuleError> {
    if select_clause.trim() == "*" {
        return Ok(CanonicalProjection::Star);
    }

    let items = split_top_level_csv(select_clause);
    if items
        .iter()
        .any(|item| item.trim() == "*" || item.contains(".*"))
    {
        let computed = items
            .into_iter()
            .filter(|item| {
                let trimmed = item.trim();
                !(trimmed == "*" || trimmed.contains(".*"))
            })
            .map(parse_computed_projection_item)
            .collect::<Result<Vec<_>, _>>()?;
        return if computed.is_empty() {
            Ok(CanonicalProjection::Star)
        } else {
            Ok(CanonicalProjection::StarWithComputed { computed })
        };
    }

    let columns = items
        .into_iter()
        .map(parse_projection_item)
        .collect::<Result<Vec<_>, _>>()?;

    if columns.is_empty() {
        return Err(SyncRuleError("projection must not be empty".to_owned()));
    }

    Ok(CanonicalProjection::Columns { columns })
}

fn parse_projection_item(item: &str) -> Result<CanonicalProjectedColumn, SyncRuleError> {
    let trimmed = item.trim();
    if trimmed.is_empty() {
        return Err(SyncRuleError(
            "projection item must not be empty".to_owned(),
        ));
    }

    if let Some((source, alias)) = split_ascii_case_insensitive_once(trimmed, " AS ") {
        let source_column = normalize_identifier(last_dotted_identifier(source));
        let alias = normalize_identifier(alias);
        if source_column.is_empty() || alias.is_empty() {
            return Err(SyncRuleError(format!(
                "unsupported projection item {}",
                item.trim()
            )));
        }
        return Ok(CanonicalProjectedColumn {
            source_column,
            alias,
        });
    }

    let source_column = normalize_identifier(last_dotted_identifier(trimmed));
    if source_column.is_empty() {
        return Err(SyncRuleError(format!(
            "unsupported projection item {}",
            item.trim()
        )));
    }

    Ok(CanonicalProjectedColumn {
        alias: source_column.clone(),
        source_column,
    })
}

fn parse_computed_projection_item(item: &str) -> Result<CanonicalComputedColumn, SyncRuleError> {
    let (expression, alias) = split_ascii_case_insensitive_once(item, " AS ").ok_or_else(|| {
        SyncRuleError(format!(
            "unsupported computed projection item {}",
            item.trim()
        ))
    })?;
    let alias = normalize_identifier(alias);
    if alias.is_empty() {
        return Err(SyncRuleError(format!(
            "unsupported computed projection item {}",
            item.trim()
        )));
    }
    Ok(CanonicalComputedColumn {
        alias,
        expression: parse_computed_expression(expression)?,
    })
}

fn parse_computed_expression(
    expression: &str,
) -> Result<CanonicalComputedExpression, SyncRuleError> {
    let terms = split_top_level_operator(expression, "||")
        .into_iter()
        .map(|term| {
            let term = term.trim();
            if term.starts_with('\'') {
                let value = parse_string_literal(term).ok_or_else(|| {
                    SyncRuleError(format!(
                        "unsupported computed projection expression {}",
                        expression.trim()
                    ))
                })?;
                Ok(CanonicalComputedTerm::Literal { value })
            } else {
                let source_column = normalize_identifier(last_dotted_identifier(term));
                if source_column.is_empty() {
                    Err(SyncRuleError(format!(
                        "unsupported computed projection expression {}",
                        expression.trim()
                    )))
                } else {
                    Ok(CanonicalComputedTerm::Column { source_column })
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if terms.is_empty() {
        return Err(SyncRuleError(format!(
            "unsupported computed projection expression {}",
            expression.trim()
        )));
    }
    Ok(CanonicalComputedExpression { terms })
}

fn parse_where_clause(where_clause: Option<&str>) -> Result<ParsedWhereClause, SyncRuleError> {
    let Some(where_clause) = where_clause else {
        return Ok(ParsedWhereClause {
            bucket_parameters: Vec::new(),
            row_filter: None,
            request_filter: None,
        });
    };

    if where_clause.is_empty() {
        return Ok(ParsedWhereClause {
            bucket_parameters: Vec::new(),
            row_filter: None,
            request_filter: None,
        });
    }

    let mut bucket_parameters = Vec::new();
    let mut row_predicates = Vec::new();
    let mut request_predicates = Vec::new();
    for predicate in split_and_predicates(where_clause) {
        let predicate = predicate.trim();
        if is_request_filter_predicate(predicate) {
            request_predicates.push(parse_disjunct(predicate)?);
            continue;
        }
        match parse_bucket_parameter(predicate) {
            Ok(parameter) => bucket_parameters.push(parameter),
            Err(_) if is_row_filter_predicate(predicate) => {
                validate_row_filter_predicate(predicate)?;
                row_predicates.push(parse_disjunct(predicate)?);
            }
            Err(error) => return Err(error),
        }
    }

    Ok(ParsedWhereClause {
        bucket_parameters,
        row_filter: combine_conjuncts(row_predicates),
        request_filter: combine_conjuncts(request_predicates),
    })
}

/// Fold the per-conjunct predicates of one filter into a single `Predicate`.
/// A WHERE filter is a top-level conjunction; a singleton is left unwrapped so
/// the AST stays flat (`x IS NULL`, not `And([x IS NULL])`).
fn combine_conjuncts(mut conjuncts: Vec<Predicate>) -> Option<Predicate> {
    match conjuncts.len() {
        0 => None,
        1 => Some(conjuncts.pop().expect("length checked")),
        _ => Some(Predicate::And { terms: conjuncts }),
    }
}

/// Parse one conjunct into a `Predicate`, mirroring the exact decision sequence
/// the old string evaluator applied per predicate: strip wrapping parens, split
/// on top-level OR (a multi-term split becomes a flat `Or`), otherwise parse the
/// leaf comparison. The split helpers are shared with the evaluator's former
/// path, so the structure produced is byte-for-byte what it used to walk.
fn parse_disjunct(predicate: &str) -> Result<Predicate, SyncRuleError> {
    let predicate = trim_wrapping_parens(predicate.trim());
    let or_terms = split_or_predicates(predicate);
    if or_terms.len() > 1 {
        let terms = or_terms
            .into_iter()
            .map(parse_disjunct)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Predicate::Or { terms });
    }
    parse_leaf(predicate)
}

/// Parse a leaf comparison: IS [NOT] NULL, IN, or `=` — the only forms the
/// evaluator supports. Validation runs before this for row filters, and
/// `is_request_filter_predicate` gates request filters, so the trailing error is
/// unreachable for accepted input; it stays as a fail-closed guard.
fn parse_leaf(predicate: &str) -> Result<Predicate, SyncRuleError> {
    if let Some((left, negated)) = split_is_null(predicate) {
        return Ok(Predicate::IsNull {
            operand: classify_is_null_operand(left),
            negated,
        });
    }
    if let Some((left, right)) = split_ascii_case_insensitive_once(predicate, " IN ") {
        return Ok(Predicate::In {
            left: classify_operand(left),
            right: classify_operand(right),
        });
    }
    if let Some((left, right)) = predicate.split_once('=') {
        return Ok(Predicate::Eq {
            left: classify_operand(left),
            right: classify_operand(right),
        });
    }
    Err(SyncRuleError(format!(
        "internal: predicate `{predicate}` is neither IS [NOT] NULL, IN, nor `=`"
    )))
}

/// Classify one comparison operand at compile time with the precedence
/// binding → literal → column. Row filters never carry bindings (they are
/// separated out upstream) so they yield only `Column`/`Literal`; request
/// filters never carry bare columns so they yield only `Binding`/`Literal`.
fn classify_operand(operand: &str) -> Operand {
    let operand = operand.trim();
    if let Some(binding) = parse_binding(operand) {
        return Operand::Binding { binding };
    }
    if let Some(value) = sql_literal_to_json(operand).and_then(literal_value_from_json) {
        return Operand::Literal { value };
    }
    Operand::Column {
        name: normalize_identifier(last_dotted_identifier(operand)),
    }
}

/// Classify an IS [NOT] NULL operand. Unlike a comparison operand this never
/// yields a `Literal`: the old evaluator resolved a row-context IS NULL operand
/// purely by column lookup and a request-context one purely via `parse_binding`,
/// so a literal-looking token (`5`, `'x'`, `true`) must stay a `Column` to
/// preserve "absent column ⇒ null". Precedence is binding → column.
fn classify_is_null_operand(operand: &str) -> Operand {
    let operand = operand.trim();
    if let Some(binding) = parse_binding(operand) {
        return Operand::Binding { binding };
    }
    Operand::Column {
        name: normalize_identifier(last_dotted_identifier(operand)),
    }
}

/// Narrow a `sql_literal_to_json` result to the literal kinds the evaluator
/// honors. `sql_literal_to_json` only ever produces String/Bool/Null/i64, so the
/// array/object arms are unreachable; floats were already rejected upstream.
fn literal_value_from_json(value: serde_json::Value) -> Option<LiteralValue> {
    match value {
        serde_json::Value::String(text) => Some(LiteralValue::String(text)),
        serde_json::Value::Bool(boolean) => Some(LiteralValue::Boolean(boolean)),
        serde_json::Value::Null => Some(LiteralValue::Null),
        serde_json::Value::Number(number) => number.as_i64().map(LiteralValue::Integer),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

fn parse_bucket_parameter(predicate: &str) -> Result<CanonicalBucketParameter, SyncRuleError> {
    let predicate_without_json_arrows = predicate.replace("->>", "");
    let predicate_upper = predicate_without_json_arrows.to_ascii_uppercase();

    if predicate_upper.contains(" OR ")
        || predicate_without_json_arrows.contains("!=")
        || predicate.contains(">=")
        || predicate.contains("<=")
        || predicate_without_json_arrows.contains('>')
        || predicate_without_json_arrows.contains('<')
    {
        return Err(SyncRuleError(format!(
            "unsupported WHERE predicate: {predicate}"
        )));
    }

    let (source_column, binding) = if let Some((left, right)) = predicate.split_once('=') {
        if let Some(binding) = parse_binding(right.trim()) {
            (normalize_identifier(last_dotted_identifier(left)), binding)
        } else if let Some(binding) = parse_binding(left.trim()) {
            (normalize_identifier(last_dotted_identifier(right)), binding)
        } else {
            return Err(SyncRuleError(format!(
                "unsupported WHERE predicate: {predicate}"
            )));
        }
    } else if let Some((left, right)) = split_ascii_case_insensitive_once(predicate, " IN ") {
        if let Some(binding) = parse_binding(left.trim()) {
            (normalize_identifier(last_dotted_identifier(right)), binding)
        } else if let Some(binding) = parse_binding(right.trim()) {
            (normalize_identifier(last_dotted_identifier(left)), binding)
        } else {
            return Err(SyncRuleError(format!(
                "unsupported WHERE predicate: {predicate}"
            )));
        }
    } else {
        return Err(SyncRuleError(format!(
            "unsupported WHERE predicate: {predicate}"
        )));
    };
    let name = binding_name(&binding);

    Ok(CanonicalBucketParameter {
        name,
        source_column,
        binding,
    })
}

pub(super) fn parse_binding(binding: &str) -> Option<CanonicalBinding> {
    let binding = binding.trim();

    if let Some(name) = strip_call_argument(binding, "auth.parameter(", ")") {
        return Some(CanonicalBinding::AuthParameter { name });
    }
    if binding.eq_ignore_ascii_case("auth.user_id()") {
        return Some(CanonicalBinding::RequestUserId);
    }
    if let Some(name) = strip_arrow_string_argument(binding, "connection.parameters()", "->>") {
        return Some(CanonicalBinding::RequestParameter { name });
    }
    if let Some(name) = strip_call_argument(binding, "connection.parameter(", ")") {
        return Some(CanonicalBinding::RequestParameter { name });
    }
    if let Some(name) = strip_call_argument(binding, "subscription.parameter(", ")") {
        return Some(CanonicalBinding::SubscriptionParameter { name });
    }
    if binding.eq_ignore_ascii_case("request.user_id()") {
        return Some(CanonicalBinding::RequestUserId);
    }
    if let Some(name) = strip_arrow_string_argument(binding, "request.jwt()", "->>") {
        return Some(CanonicalBinding::RequestJwt { claim: name });
    }
    if let Some(name) = strip_arrow_string_argument(binding, "request.parameters()", "->>") {
        return Some(CanonicalBinding::RequestParameter { name });
    }
    if let Some(name) = strip_call_argument(binding, "request.parameter_array(", ")") {
        return Some(CanonicalBinding::RequestParameterArray { name });
    }
    if let Some(binding) = parse_parameter_query_column_binding(binding) {
        return Some(binding);
    }
    if let Some(name) = binding.strip_prefix("bucket.") {
        return Some(CanonicalBinding::BucketParameter {
            name: normalize_identifier(name),
        });
    }

    None
}

fn parse_parameter_query_column_binding(binding: &str) -> Option<CanonicalBinding> {
    let inner = strip_call_inner(binding, "parameter_query_column(", ")")?;
    let args = split_top_level_csv(inner);
    if args.len() != 2 {
        return None;
    }
    let encoded_query = parse_string_literal(args[0])?;
    let query = String::from_utf8(URL_SAFE_NO_PAD.decode(encoded_query).ok()?).ok()?;
    let name = normalize_identifier(&parse_string_literal(args[1])?);
    if name.is_empty() {
        return None;
    }
    // The b64 payload is only ever produced by `binding_sql_fragment` from a
    // plan that already passed `parse_parameter_lookup_plan` in lowering, so
    // this re-parse cannot fail for authored rules; a `None` here would fall
    // through to `Column` classification, which is why unsupported forms must
    // be rejected at lowering rather than at this decode.
    let lookup = super::lowering::parse_parameter_lookup_plan(&query).ok()?;
    Some(CanonicalBinding::ParameterQueryColumn {
        name,
        lookup: Box::new(lookup),
    })
}

fn binding_name(binding: &CanonicalBinding) -> String {
    match binding {
        CanonicalBinding::AuthParameter { name }
        | CanonicalBinding::SubscriptionParameter { name }
        | CanonicalBinding::RequestParameter { name }
        | CanonicalBinding::RequestParameterArray { name }
        | CanonicalBinding::ParameterQueryColumn { name, .. }
        | CanonicalBinding::BucketParameter { name } => name.clone(),
        CanonicalBinding::RequestUserId => "user_id".to_owned(),
        CanonicalBinding::RequestJwt { claim } => claim.clone(),
    }
}

pub(super) fn is_row_filter_predicate(predicate: &str) -> bool {
    let lower = predicate.to_ascii_lowercase();
    !contains_binding_reference_lower(&lower) && !lower.contains("bucket.")
}

/// Reject row-filter predicates the evaluator cannot honor. `row_predicate_matches`
/// only implements `=`, `IN`, and `IS [NOT] NULL` over columns and representable
/// literals; any other operator (`>`, `<`, `>=`, `<=`, `!=`, `LIKE`, …) or a literal
/// it cannot parse (e.g. a float) would otherwise compile cleanly and then silently
/// drop every matching row at serve time. Fail closed at compile instead.
fn validate_row_filter_predicate(predicate: &str) -> Result<(), SyncRuleError> {
    for term in split_or_predicates(trim_wrapping_parens(predicate.trim())) {
        validate_row_filter_term(term)?;
    }
    Ok(())
}

fn validate_row_filter_term(term: &str) -> Result<(), SyncRuleError> {
    let term = trim_wrapping_parens(term.trim());
    let or_terms = split_or_predicates(term);
    if or_terms.len() > 1 {
        for inner in or_terms {
            validate_row_filter_term(inner)?;
        }
        return Ok(());
    }
    if split_is_null(term).is_some() {
        return Ok(());
    }
    let (left, right) = if let Some(parts) = split_ascii_case_insensitive_once(term, " IN ") {
        parts
    } else if let Some(parts) = term.split_once('=') {
        parts
    } else {
        return Err(SyncRuleError(format!(
            "unsupported row-filter predicate `{term}`: only =, IN, and IS [NOT] NULL are supported"
        )));
    };
    validate_row_filter_operand(left, term)?;
    validate_row_filter_operand(right, term)
}

fn validate_row_filter_operand(operand: &str, term: &str) -> Result<(), SyncRuleError> {
    let operand = trim_wrapping_parens(operand.trim());
    if sql_literal_to_json(operand).is_some() || is_plain_column_reference(operand) {
        return Ok(());
    }
    Err(SyncRuleError(format!(
        "unsupported row-filter operand `{operand}` in `{term}`: a literal must be a quoted \
         string, integer, boolean, or null, and a column reference must be a bare identifier"
    )))
}

/// A bare (optionally dotted/quoted) column identifier — not a literal or an
/// expression carrying a leftover operator character.
fn is_plain_column_reference(operand: &str) -> bool {
    let Some(first) = operand.chars().next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_' || first == '"') {
        return false;
    }
    operand
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '"'))
}

pub(super) fn is_request_filter_predicate(predicate: &str) -> bool {
    contains_binding_reference(predicate)
        && split_or_predicates(trim_wrapping_parens(predicate))
            .into_iter()
            .all(request_predicate_term_supported)
}

fn request_predicate_term_supported(predicate: &str) -> bool {
    let predicate = trim_wrapping_parens(predicate.trim());
    if let Some((left, _negate)) = split_is_null(predicate) {
        return parse_binding(left.trim()).is_some();
    }
    let Some((left, right)) = predicate.split_once('=') else {
        return false;
    };
    (parse_binding(left.trim()).is_some() && sql_literal_to_json(right.trim()).is_some())
        || (parse_binding(right.trim()).is_some() && sql_literal_to_json(left.trim()).is_some())
}

pub(super) fn contains_binding_reference(predicate: &str) -> bool {
    contains_binding_reference_lower(&predicate.to_ascii_lowercase())
}

fn contains_binding_reference_lower(lower: &str) -> bool {
    lower.contains("connection.parameter")
        || lower.contains("connection.parameters")
        || lower.contains("auth.")
        || lower.contains("request.")
        || lower.contains("subscription.")
}

pub(super) fn split_is_null(predicate: &str) -> Option<(&str, bool)> {
    if let Some((left, _)) = split_ascii_case_insensitive_once(predicate, " IS NOT NULL") {
        return Some((left, true));
    }
    split_ascii_case_insensitive_once(predicate, " IS NULL").map(|(left, _)| (left, false))
}

pub(super) fn sql_literal_to_json(value: &str) -> Option<serde_json::Value> {
    let trimmed = trim_wrapping_parens(value.trim());
    if let Some(text) = parse_sql_string_literal(trimmed) {
        return Some(serde_json::Value::String(text.replace("''", "'")));
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(serde_json::Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(serde_json::Value::Bool(false));
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return Some(serde_json::Value::Null);
    }
    trimmed
        .parse::<i64>()
        .ok()
        .map(|number| serde_json::Value::Number(number.into()))
}

fn parse_sql_string_literal(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() < 2 || !value.starts_with('\'') || !value.ends_with('\'') {
        return None;
    }
    Some(value[1..value.len() - 1].to_owned())
}

pub(super) fn trim_wrapping_parens(value: &str) -> &str {
    let mut trimmed = value.trim();
    loop {
        if trimmed.starts_with(')') || !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            return trimmed;
        }
        let inner = &trimmed[1..trimmed.len() - 1];
        if has_balanced_parens(inner) {
            trimmed = inner.trim();
        } else {
            return trimmed;
        }
    }
}

fn has_balanced_parens(value: &str) -> bool {
    let mut depth = 0_i32;
    let mut quote = None;
    for byte in value.bytes() {
        if let Some(active) = quote {
            if byte == active {
                quote = None;
            }
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
            continue;
        }
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

pub(super) fn split_or_predicates(value: &str) -> Vec<&str> {
    split_top_level_keyword(value, "OR")
}

fn strip_call_argument(value: &str, prefix: &str, suffix: &str) -> Option<String> {
    let inner = strip_call_inner(value, prefix, suffix)?;
    parse_string_literal(inner.trim())
}

fn strip_call_inner<'a>(value: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let lower = value.to_ascii_lowercase();
    if !lower.starts_with(prefix) || !value.ends_with(suffix) {
        return None;
    }

    Some(&value[prefix.len()..value.len() - suffix.len()])
}

pub(super) fn strip_arrow_string_argument(value: &str, left: &str, arrow: &str) -> Option<String> {
    let (left_expr, right_expr) = split_ascii_case_insensitive_once(value, arrow)?;
    if !left_expr.trim().eq_ignore_ascii_case(left) {
        return None;
    }
    parse_string_literal(right_expr.trim())
}

pub(super) fn normalize_identifier(identifier: &str) -> String {
    identifier
        .trim()
        .split('.')
        .map(|part| part.trim().trim_matches('"').replace("\"\"", "\""))
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_string_literal(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() < 2 {
        return None;
    }

    let quote = value.chars().next()?;
    if !matches!(quote, '\'' | '"') || !value.ends_with(quote) {
        return None;
    }

    // Un-escape doubled quotes (SQL string-literal escaping): '' -> ', "" -> ".
    let inner = &value[1..value.len() - 1];
    Some(match quote {
        '\'' => inner.replace("''", "'"),
        _ => inner.replace("\"\"", "\""),
    })
}

pub(super) fn split_and_predicates(value: &str) -> Vec<&str> {
    split_top_level_keyword(value, "AND")
}

fn split_top_level_keyword<'a>(value: &'a str, keyword: &str) -> Vec<&'a str> {
    let bytes = value.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut index = 0;
    let mut quote: Option<u8> = None;
    let keyword_bytes = keyword.as_bytes();
    let keyword_len = keyword_bytes.len();
    let mut depth = 0_i32;
    while index + keyword_len <= bytes.len() {
        let byte = bytes[index];
        if let Some(active) = quote {
            if byte == active {
                quote = None;
            }
            index += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
            index += 1;
            continue;
        }
        match byte {
            b'(' => {
                depth += 1;
                index += 1;
                continue;
            }
            b')' => {
                depth -= 1;
                index += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0
            && bytes[index..].len() >= keyword_len
            && bytes[index..index + keyword_len].eq_ignore_ascii_case(keyword_bytes)
            && is_and_boundary(bytes.get(index.wrapping_sub(1)).copied())
            && is_and_boundary(bytes.get(index + keyword_len).copied())
        {
            let part = value[start..index].trim();
            if !part.is_empty() {
                parts.push(part);
            }
            start = index + keyword_len;
            index += keyword_len;
            continue;
        }
        index += 1;
    }
    let part = value[start..].trim();
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

fn is_and_boundary(byte: Option<u8>) -> bool {
    match byte {
        None => true,
        Some(value) => !value.is_ascii_alphanumeric() && value != b'_',
    }
}

pub(super) fn split_ascii_case_insensitive_once<'a>(
    value: &'a str,
    needle: &str,
) -> Option<(&'a str, &'a str)> {
    let upper = value.to_ascii_uppercase();
    let needle_upper = needle.to_ascii_uppercase();
    let index = upper.find(&needle_upper)?;
    Some((&value[..index], &value[index + needle.len()..]))
}
