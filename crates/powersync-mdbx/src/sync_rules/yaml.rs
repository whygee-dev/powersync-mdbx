use std::collections::BTreeMap;

use super::catalog::{
    SUPPORTED_COMPATIBILITY_VERSION, SUPPORTED_EDITION, SUPPORTED_STORAGE_VERSION,
};
use super::lowering::{
    normalize_data_query_with_parameters, parse_parameter_query, ParsedParameterQuery,
};
use super::model::SyncRuleError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct ParsedSyncRulesSource {
    pub(super) edition: u32,
    pub(super) compatibility_version: u32,
    pub(super) storage_version: u32,
    pub(super) streams: Vec<ParsedStreamDefinition>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct ParsedStreamDefinition {
    pub(super) name: String,
    pub(super) queries: Vec<String>,
    pub(super) auto_subscribe: bool,
}

pub(super) fn parse_sync_rules_source(
    source: &str,
) -> Result<ParsedSyncRulesSource, SyncRuleError> {
    let document: serde_yaml::Value = serde_yaml::from_str(source)
        .map_err(|error| SyncRuleError(format!("invalid sync-rules YAML: {error}")))?;

    let edition = read_config_version(&document, "edition", SUPPORTED_EDITION)?;
    let compatibility_version = read_config_version(
        &document,
        "compatibility_version",
        SUPPORTED_COMPATIBILITY_VERSION,
    )?;
    let storage_version =
        read_config_version(&document, "storage_version", SUPPORTED_STORAGE_VERSION)?;

    let global_parameter_queries = read_parameter_queries(document.get("with"), "with")?;

    let stream_entries = match document.get("streams") {
        Some(serde_yaml::Value::Mapping(streams)) => streams,
        Some(_) => {
            return Err(SyncRuleError(
                "sync-rules `streams` must be a mapping of stream name to definition".to_owned(),
            ))
        }
        None => {
            return Err(SyncRuleError(
                "sync-rules source must define at least one stream".to_owned(),
            ))
        }
    };

    // `serde_yaml::Mapping` preserves insertion order, so streams keep their
    // document order (the canonical plan is order-sensitive).
    let mut streams = Vec::with_capacity(stream_entries.len());
    for (name, definition) in stream_entries {
        let name = name
            .as_str()
            .ok_or_else(|| SyncRuleError("stream names must be strings".to_owned()))?
            .to_owned();
        let definition = definition
            .as_mapping()
            .ok_or_else(|| SyncRuleError(format!("stream {name} must be a mapping")))?;

        let auto_subscribe = match definition.get("auto_subscribe") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                SyncRuleError(format!(
                    "stream {name} auto_subscribe must be true or false"
                ))
            })?,
        };

        if definition.get("priority").is_some() {
            tracing::warn!(
                stream = %name,
                "sync-rules stream `priority` is parsed but ignored: bucket priority ordering is not implemented"
            );
        }

        // Per-stream `with` parameter queries override global ones of the same name.
        let mut parameter_queries = global_parameter_queries.clone();
        parameter_queries.extend(read_parameter_queries(definition.get("with"), "with")?);

        let mut raw_queries = Vec::new();
        if let Some(query) = definition.get("query") {
            raw_queries.push(query_string(query, &name)?);
        }
        if let Some(queries) = definition.get("queries") {
            let sequence = queries.as_sequence().ok_or_else(|| {
                SyncRuleError(format!("stream {name} `queries` must be a sequence"))
            })?;
            for query in sequence {
                raw_queries.push(query_string(query, &name)?);
            }
        }
        if raw_queries.is_empty() {
            return Err(SyncRuleError(format!(
                "stream {name} is missing a query definition"
            )));
        }

        let queries = raw_queries
            .iter()
            .map(|query| normalize_data_query_with_parameters(query, &parameter_queries))
            .collect::<Result<Vec<_>, _>>()?;

        streams.push(ParsedStreamDefinition {
            name,
            queries,
            auto_subscribe,
        });
    }

    if streams.is_empty() {
        return Err(SyncRuleError(
            "sync-rules source must define at least one stream".to_owned(),
        ));
    }

    Ok(ParsedSyncRulesSource {
        edition,
        compatibility_version,
        storage_version,
        streams,
    })
}

/// Read a `config.<key>` (or top-level `<key>`) unsigned-integer version,
/// defaulting when absent. The previous parser accepted the version keys at
/// either the document root or under `config:`, with `config` taking precedence.
fn read_config_version(
    document: &serde_yaml::Value,
    key: &str,
    default: u32,
) -> Result<u32, SyncRuleError> {
    let value = document
        .get("config")
        .and_then(|config| config.get(key))
        .or_else(|| document.get(key));
    match value {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .and_then(|number| u32::try_from(number).ok())
            .ok_or_else(|| SyncRuleError(format!("{key} must be a non-negative integer"))),
    }
}

/// Read a `with:` block (a map of parameter-query name to SQL) into parsed
/// parameter queries. A missing block yields an empty map.
fn read_parameter_queries(
    value: Option<&serde_yaml::Value>,
    field: &str,
) -> Result<BTreeMap<String, ParsedParameterQuery>, SyncRuleError> {
    let mut parameter_queries = BTreeMap::new();
    let Some(value) = value else {
        return Ok(parameter_queries);
    };
    let mapping = value
        .as_mapping()
        .ok_or_else(|| SyncRuleError(format!("`{field}` must be a mapping of name to query")))?;
    for (name, query) in mapping {
        let name = name
            .as_str()
            .ok_or_else(|| SyncRuleError(format!("`{field}` keys must be strings")))?;
        let query = query
            .as_str()
            .ok_or_else(|| SyncRuleError(format!("`{field}` query for {name} must be a string")))?;
        parameter_queries.insert(name.to_owned(), parse_parameter_query(&fold_query(query)));
    }
    Ok(parameter_queries)
}

/// Extract a query string from a YAML scalar.
fn query_string(value: &serde_yaml::Value, stream: &str) -> Result<String, SyncRuleError> {
    let query = value
        .as_str()
        .ok_or_else(|| SyncRuleError(format!("stream {stream} query must be a string")))?;
    Ok(fold_query(query))
}

/// Fold any block-scalar newlines into single spaces. The downstream SQL
/// handling is whitespace-delimited, so a multi-line `query: |`/`>` block is
/// collapsed to one line, matching the previous parser.
fn fold_query(query: &str) -> String {
    query
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
