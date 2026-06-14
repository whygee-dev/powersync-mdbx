use std::collections::BTreeMap;

use crate::replication::postgres::PostgresLsn;

pub(super) const META_LAYOUT_VERSION_KEY: &[u8] = b"meta:layout_version";
pub(super) const META_LAST_COMMIT_END_LSN_KEY: &[u8] = b"meta:last_commit_end_lsn";
pub(super) const META_INITIAL_SNAPSHOT_COMPLETE_KEY: &[u8] = b"meta:initial_snapshot_complete";
pub(super) const META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY: &[u8] =
    b"meta:initial_snapshot_cursor_floor";
pub(super) const META_INITIAL_SNAPSHOT_BOOTSTRAP_INTENT_KEY: &[u8] =
    b"meta:initial_snapshot_bootstrap_intent";
pub(super) const META_INITIAL_SNAPSHOT_SOURCE_IDENTITY_KEY: &[u8] =
    b"meta:initial_snapshot_source_identity";
pub(super) const META_SYNC_TAIL_LAST_OP_ID_KEY: &[u8] = b"meta:sync_tail_last_op_id";
pub(super) const META_SYNC_TAIL_RETAINED_FLOOR_KEY: &[u8] = b"meta:sync_tail_retained_floor";
pub(super) const META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY: &[u8] =
    b"meta:sync_tail_indexed_through_op_id";
#[cfg(test)]
pub(super) const META_SYNC_STATE_JSON_KEY: &[u8] = b"meta:sync_state_json";
const BATCH_KEY_PREFIX: &str = "batch:";
pub(super) const CURRENT_DOC_KEY_PREFIX: &str = "sync:current:doc:";
const CURRENT_ROUTE_KEY_PREFIX: &str = "sync:current:route:";
pub(super) const CURRENT_CHECKPOINT_ACCUMULATOR_KEY_PREFIX: &str = "sync:current:checkpoint:";
pub(super) const CURRENT_DOC_BINARY_MAGIC: &[u8] = b"PSCD1\0";
const SYNC_TAIL_OP_KEY_PREFIX: &str = "sync:tail:op:";
const SYNC_TAIL_REFS_KEY_PREFIX: &str = "sync:tail:refs:";
const SYNC_TAIL_INDEX_ENTRY_KEY_PREFIX: &str = "sync:tail:index:entry:";
pub(super) const SYNC_TAIL_CHECKPOINT_ACCUMULATOR_KEY_PREFIX: &str = "sync:tail:checkpoint:";

pub(super) fn current_doc_prefix(object_type: &str) -> Vec<u8> {
    format!("{CURRENT_DOC_KEY_PREFIX}{object_type}:").into_bytes()
}

pub(super) fn current_doc_key(object_type: &str, object_id: &str) -> Vec<u8> {
    current_doc_key_from_object_id_bytes(object_type, object_id.as_bytes())
}

pub(super) fn current_doc_key_from_object_id_bytes(object_type: &str, object_id: &[u8]) -> Vec<u8> {
    let mut key =
        Vec::with_capacity(CURRENT_DOC_KEY_PREFIX.len() + object_type.len() + 1 + object_id.len());
    key.extend_from_slice(CURRENT_DOC_KEY_PREFIX.as_bytes());
    key.extend_from_slice(object_type.as_bytes());
    key.push(b':');
    key.extend_from_slice(object_id);
    key
}

pub(super) fn current_route_index_prefix(
    object_type: &str,
    route_constraints: &BTreeMap<String, String>,
) -> Vec<u8> {
    format!(
        "{CURRENT_ROUTE_KEY_PREFIX}{}:{}:",
        object_type,
        sync_tail_route_index_name(object_type, route_constraints)
    )
    .into_bytes()
}

pub(super) fn current_route_index_key(
    object_type: &str,
    route_constraints: &BTreeMap<String, String>,
    object_id: &str,
) -> Vec<u8> {
    let mut key = current_route_index_prefix(object_type, route_constraints);
    key.extend_from_slice(object_id.as_bytes());
    key
}

pub(super) fn sync_tail_op_key(op_id: u64) -> Vec<u8> {
    format!("{SYNC_TAIL_OP_KEY_PREFIX}{op_id:020}").into_bytes()
}

pub(super) fn sync_tail_refs_key(op_id: u64) -> Vec<u8> {
    format!("{SYNC_TAIL_REFS_KEY_PREFIX}{op_id:020}").into_bytes()
}

pub(super) fn sync_tail_object_index_name(object_type: &str) -> String {
    sync_tail_index_name("object", object_type, &BTreeMap::new())
}

pub(super) fn sync_tail_clear_index_name(object_type: &str) -> String {
    sync_tail_index_name("clear", object_type, &BTreeMap::new())
}

pub(super) fn sync_tail_route_index_name(
    object_type: &str,
    route_constraints: &BTreeMap<String, String>,
) -> String {
    sync_tail_index_name("route", object_type, route_constraints)
}

fn sync_tail_index_name(
    kind: &str,
    object_type: &str,
    route_constraints: &BTreeMap<String, String>,
) -> String {
    let mut name = String::with_capacity(
        kind.len()
            + object_type.len()
            + route_constraints
                .iter()
                .map(|(key, value)| key.len() + value.len() + 12)
                .sum::<usize>()
            + 24,
    );
    push_len_prefixed(&mut name, kind);
    push_len_prefixed(&mut name, object_type);
    name.push_str(&route_constraints.len().to_string());
    name.push('|');
    for (key, value) in route_constraints {
        push_len_prefixed(&mut name, key);
        push_len_prefixed(&mut name, value);
    }
    name
}

fn push_len_prefixed(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
    output.push('|');
}

pub(super) fn sync_tail_index_entry_key(index_key: &str, global_op_id: u64) -> Vec<u8> {
    format!("{SYNC_TAIL_INDEX_ENTRY_KEY_PREFIX}{index_key}:{global_op_id:020}").into_bytes()
}

pub(super) fn sync_tail_index_entry_prefix(index_key: &str) -> Vec<u8> {
    format!("{SYNC_TAIL_INDEX_ENTRY_KEY_PREFIX}{index_key}:").into_bytes()
}

pub(super) fn sync_tail_global_op_id_from_key(key: &[u8], prefix: &[u8]) -> Option<u64> {
    std::str::from_utf8(key.get(prefix.len()..)?)
        .ok()?
        .parse()
        .ok()
}

pub(super) fn batch_key(end_lsn: PostgresLsn) -> Vec<u8> {
    format!("{BATCH_KEY_PREFIX}{:020}", end_lsn.to_u64()).into_bytes()
}
