//! Request/auth parameter resolution and the parameter-query SQL rewriter.
//!
//! This is the security-critical seam that turns a sync-rule parameter query
//! plus a request's auth/connection parameters into a parameterized SQL
//! statement: every binding becomes a `$n` placeholder with its value bound
//! out-of-band, so request-controlled values never reach the SQL text.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{auth::TokenPayload, sync_rules::CanonicalBinding};

use super::{binding_sql, flatten_json_map};

#[derive(Debug, Clone)]
pub struct ResolvedParameterContext {
    auth_parameters: BTreeMap<String, String>,
    request_parameters: BTreeMap<String, String>,
    jwt_claims: BTreeMap<String, String>,
    user_id: Option<String>,
}

impl ResolvedParameterContext {
    pub fn from_request(
        token: Option<&TokenPayload>,
        request_parameters: &serde_json::Map<String, Value>,
    ) -> Self {
        let mut request_map = BTreeMap::new();
        flatten_json_map("", request_parameters, &mut request_map);

        let mut jwt_claims = token
            .map(TokenPayload::flattened_claim_strings)
            .unwrap_or_default();
        let user_id = token.and_then(|payload| payload.user_id().map(str::to_owned));
        let mut auth_parameters = jwt_claims.clone();
        if let Some(user_id) = &user_id {
            auth_parameters
                .entry("user_id".to_owned())
                .or_insert_with(|| user_id.clone());
        }

        Self {
            auth_parameters,
            request_parameters: request_map,
            jwt_claims: std::mem::take(&mut jwt_claims),
            user_id,
        }
    }

    pub fn binding_value(
        &self,
        binding: &CanonicalBinding,
        subscription_parameters: &BTreeMap<String, String>,
    ) -> Option<String> {
        match binding {
            CanonicalBinding::AuthParameter { name } => self.auth_parameters.get(name).cloned(),
            CanonicalBinding::SubscriptionParameter { name } => {
                subscription_parameters.get(name).cloned()
            }
            CanonicalBinding::RequestUserId => self.user_id.clone(),
            CanonicalBinding::RequestJwt { claim } => self.jwt_claims.get(claim).cloned(),
            CanonicalBinding::RequestParameter { name } => {
                self.request_parameters.get(name).cloned()
            }
            CanonicalBinding::RequestParameterArray { name } => {
                self.request_parameters.get(name).cloned()
            }
            CanonicalBinding::ParameterQueryColumn { name, .. } => {
                subscription_parameters.get(name).cloned()
            }
            CanonicalBinding::BucketParameter { name } => {
                subscription_parameters.get(name).cloned()
            }
        }
    }

    pub fn binding_values(
        &self,
        binding: &CanonicalBinding,
        subscription_parameters: &BTreeMap<String, String>,
    ) -> Vec<String> {
        match binding {
            CanonicalBinding::RequestParameterArray { name } => self
                .request_parameters
                .get(name)
                .map(|value| parse_request_array_values(value))
                .unwrap_or_default(),
            _ => self
                .binding_value(binding, subscription_parameters)
                .into_iter()
                .collect(),
        }
    }
}

fn parse_request_array_values(value: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .map(|values| {
            values
                .into_iter()
                .filter_map(|value| match value {
                    serde_json::Value::String(value) => Some(value),
                    serde_json::Value::Number(value) => Some(value.to_string()),
                    serde_json::Value::Bool(value) => Some(value.to_string()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_else(|| vec![value.to_owned()])
}

pub(super) fn prepare_parameter_query_sql(
    query: &str,
    context: &ResolvedParameterContext,
    subscription_parameters: &BTreeMap<String, String>,
) -> Result<(String, Vec<String>), String> {
    let mut values = Vec::new();
    let query = rewrite_json_each_from_items(query, context, subscription_parameters, &mut values)?;
    let sql = replace_parameter_bindings(&query, context, subscription_parameters, &mut values)?;
    Ok((sql, values))
}

fn rewrite_json_each_from_items(
    query: &str,
    context: &ResolvedParameterContext,
    subscription_parameters: &BTreeMap<String, String>,
    values: &mut Vec<String>,
) -> Result<String, String> {
    let upper = query.to_ascii_uppercase();
    let Some(from_index) = upper.find(" FROM ") else {
        return Ok(query.to_owned());
    };
    let where_index = upper[from_index + " FROM ".len()..]
        .find(" WHERE ")
        .map(|index| from_index + " FROM ".len() + index);
    let from_start = from_index + " FROM ".len();
    let from_end = where_index.unwrap_or(query.len());
    let from_part = &query[from_start..from_end];
    let mut unaliased_json_each = 0_usize;
    let rewritten_from = crate::sync_rules::split_top_level_csv(from_part)
        .into_iter()
        .map(|part| {
            let trimmed = part.trim();
            let Some(argument) = json_each_argument(trimmed) else {
                return Ok(trimmed.to_owned());
            };
            let name = request_parameter_name_from_arrow(argument.trim()).ok_or_else(|| {
                format!("unsupported json_each parameter query argument: {argument}")
            })?;
            let value = context
                .binding_value(
                    &CanonicalBinding::RequestParameter { name: name.clone() },
                    subscription_parameters,
                )
                .ok_or_else(|| format!("missing request parameter {name}"))?;
            values.push(value);
            let placeholder = format!("${}", values.len());
            let alias = crate::sync_rules::json_each_alias(trimmed).unwrap_or_else(|| {
                unaliased_json_each += 1;
                if unaliased_json_each == 1 {
                    "json_each".to_owned()
                } else {
                    format!("json_each_{unaliased_json_each}")
                }
            });
            Ok(format!(
                "jsonb_array_elements_text(({placeholder})::jsonb) AS {alias}(value)"
            ))
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    Ok(format!(
        "{} FROM {}{}",
        &query[..from_index],
        rewritten_from,
        where_index.map(|index| &query[index..]).unwrap_or("")
    ))
}

fn replace_parameter_bindings(
    query: &str,
    context: &ResolvedParameterContext,
    subscription_parameters: &BTreeMap<String, String>,
    values: &mut Vec<String>,
) -> Result<String, String> {
    let mut sql = query.to_owned();
    while let Some((start, end, binding)) = find_next_parameter_binding(&sql) {
        let value = context
            .binding_value(&binding, subscription_parameters)
            .ok_or_else(|| format!("missing value for {}", binding_sql(&binding)))?;
        values.push(value);
        let placeholder = format!("${}", values.len());
        sql.replace_range(start..end, &placeholder);
    }
    Ok(sql)
}

fn find_next_parameter_binding(sql: &str) -> Option<(usize, usize, CanonicalBinding)> {
    let lower = sql.to_ascii_lowercase();
    let candidates = [
        "connection.parameters()",
        "request.parameters()",
        "auth.parameter(",
        "subscription.parameter(",
        "auth.user_id()",
        "request.user_id()",
    ];
    let (start, candidate) = candidates
        .iter()
        .filter_map(|candidate| lower.find(candidate).map(|index| (index, *candidate)))
        .min_by_key(|(index, _)| *index)?;
    match candidate {
        "connection.parameters()" | "request.parameters()" => {
            let rest = &sql[start..];
            let name = request_parameter_name_from_arrow(rest)?;
            let arrow_index = rest.find("->>")?;
            let literal_start = rest[arrow_index + 3..].find('\'')? + arrow_index + 3;
            let literal_end = rest[literal_start + 1..].find('\'')? + literal_start + 2;
            Some((
                start,
                start + literal_end,
                CanonicalBinding::RequestParameter { name },
            ))
        }
        "auth.parameter(" => {
            let rest = &sql[start..];
            let end = rest.find(')')? + 1;
            let name = call_string_argument_local(&rest[..end])?;
            Some((start, start + end, CanonicalBinding::AuthParameter { name }))
        }
        "subscription.parameter(" => {
            let rest = &sql[start..];
            let end = rest.find(')')? + 1;
            let name = call_string_argument_local(&rest[..end])?;
            Some((
                start,
                start + end,
                CanonicalBinding::SubscriptionParameter { name },
            ))
        }
        "auth.user_id()" | "request.user_id()" => Some((
            start,
            start + candidate.len(),
            CanonicalBinding::RequestUserId,
        )),
        _ => None,
    }
}

fn request_parameter_name_from_arrow(value: &str) -> Option<String> {
    let arrow_index = value.find("->>")?;
    let after_arrow = value[arrow_index + 3..].trim_start();
    parse_sql_string_argument_local(after_arrow)
}

fn json_each_argument(value: &str) -> Option<&str> {
    let lower = value.to_ascii_lowercase();
    let start = lower.find("json_each(")? + "json_each(".len();
    let mut depth = 0_i32;
    let mut quote = None;
    for (offset, byte) in value[start..].bytes().enumerate() {
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
            b')' if depth == 0 => return Some(&value[start..start + offset]),
            b')' => depth -= 1,
            _ => {}
        }
    }
    None
}

fn call_string_argument_local(value: &str) -> Option<String> {
    let start = value.find('(')? + 1;
    let end = value.rfind(')')?;
    parse_sql_string_argument_local(&value[start..end])
}

fn parse_sql_string_argument_local(value: &str) -> Option<String> {
    let mut chars = value.trim().chars();
    if chars.next()? != '\'' {
        return None;
    }
    // Scan to the closing quote, treating a doubled '' as an escaped apostrophe.
    let mut result = String::new();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.clone().next() == Some('\'') {
                chars.next();
                result.push('\'');
            } else {
                return Some(result);
            }
        } else {
            result.push(ch);
        }
    }
    None
}

pub(super) fn row_value_to_string(
    row: &tokio_postgres::Row,
    column: &str,
) -> Result<String, String> {
    if let Ok(value) = row.try_get::<_, String>(column) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<_, Option<String>>(column) {
        return value.ok_or_else(|| "null value".to_owned());
    }
    if let Ok(value) = row.try_get::<_, i64>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<_, i32>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<_, bool>(column) {
        return Ok(value.to_string());
    }
    Err("unsupported column type".to_owned())
}
