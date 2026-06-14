use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
};

use libmdbx::{NoWriteMap, TransactionKind, WriteFlags};
use serde::{Deserialize, Serialize};

use super::{
    accumulators::{
        collect_checkpoint_accumulator_deltas_for_op, persist_checkpoint_accumulator_deltas,
    },
    current_state::{
        apply_materialized_sync_state_op, materialize_sync_state_op, materialize_sync_state_ops,
    },
    derive::DerivedSyncTailOp,
    error::ReplicationIngestError,
    keys::{
        sync_tail_clear_index_name, sync_tail_global_op_id_from_key, sync_tail_index_entry_key,
        sync_tail_index_entry_prefix, sync_tail_object_index_name, sync_tail_op_key,
        sync_tail_refs_key, sync_tail_route_index_name, META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY,
        META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY, META_SYNC_TAIL_LAST_OP_ID_KEY,
        META_SYNC_TAIL_RETAINED_FLOOR_KEY,
    },
    metrics::ReplicationIngestMetricCounters,
};
use crate::sync_rules::RustExecutionPlan;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedSyncTailOp {
    pub op_id: u64,
    pub operation: PersistedSyncTailOperation,
    pub object_type: Option<String>,
    pub object_id: Option<String>,
    pub route_fields: BTreeMap<String, String>,
    pub data_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_route_fields: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_data_json: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedSyncTailOperation {
    Clear,
    Put,
    Remove,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSyncTailOps {
    pub latest_op_id: u64,
    pub ops: Vec<PersistedSyncTailOp>,
}

pub(super) fn persist_task_tail_ops(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    derived_ops: Vec<DerivedSyncTailOp>,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    let derived_ops = materialize_sync_state_ops(txn, table, derived_ops, plan, metrics)?;

    if derived_ops.is_empty() {
        return Ok(());
    }

    let mut last_op_id = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
        "sync tail last op id",
    )?
    .unwrap_or(0);

    let mut checkpoint_accumulator_deltas = BTreeMap::new();

    for derived_op in derived_ops {
        last_op_id += 1;
        let persisted = PersistedSyncTailOp {
            op_id: last_op_id,
            operation: derived_op.operation,
            object_type: derived_op.object_type,
            object_id: derived_op.object_id,
            route_fields: derived_op.route_fields,
            data_json: derived_op.data_json,
            previous_route_fields: derived_op.previous_route_fields,
            previous_data_json: derived_op.previous_data_json,
        };
        txn.put(
            table,
            sync_tail_op_key(last_op_id),
            serde_json::to_vec(&persisted).map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "serialize sync tail op {last_op_id}: {error}"
                ))
            })?,
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist sync tail op {last_op_id}: {error}"))
        })?;
        let index_keys = sync_tail_index_keys_for_op(&persisted, plan);
        persist_sync_tail_index_entries(txn, table, &persisted, &index_keys, metrics)?;
        txn.put(
            table,
            sync_tail_refs_key(last_op_id),
            serde_json::to_vec(&index_keys).map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "serialize sync tail index references {last_op_id}: {error}"
                ))
            })?,
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist sync tail index references {last_op_id}: {error}"
            ))
        })?;
        collect_checkpoint_accumulator_deltas_for_op(
            &mut checkpoint_accumulator_deltas,
            &persisted,
            plan,
        )?;
        metrics.tail_ops_written.fetch_add(1, Ordering::Relaxed);
    }

    txn.put(
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY,
        last_op_id.to_string(),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!("persist sync tail last op id: {error}"))
    })?;
    txn.put(
        table,
        META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY,
        last_op_id.to_string(),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!("persist sync tail indexed-through op id: {error}"))
    })?;
    persist_checkpoint_accumulator_deltas(txn, table, &checkpoint_accumulator_deltas)?;

    Ok(())
}

pub(super) fn persist_snapshot_current_ops_without_tail(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    derived_ops: Vec<DerivedSyncTailOp>,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    if derived_ops.is_empty() {
        return Ok(());
    }

    let mut snapshot_op_count = 0_u64;

    for derived_op in derived_ops {
        let materialized_op = materialize_sync_state_op(txn, table, derived_op)?;
        apply_materialized_sync_state_op(txn, table, &materialized_op, plan, metrics)?;

        if materialized_op.operation != PersistedSyncTailOperation::Put {
            continue;
        }

        snapshot_op_count = snapshot_op_count.checked_add(1).ok_or_else(|| {
            ReplicationIngestError::CorruptBatch("snapshot cursor floor overflow".to_owned())
        })?;
    }

    advance_sync_tail_snapshot_floor(txn, table, snapshot_op_count)
}

pub(super) fn prune_sync_tail(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    retain_ops: u64,
    delete_chunk_ops: u64,
) -> Result<(), ReplicationIngestError> {
    let last_op_id = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
        "sync tail last op id",
    )?
    .unwrap_or(0);
    let retained_floor = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_RETAINED_FLOOR_KEY.to_vec(),
        "sync tail retained floor",
    )?
    .unwrap_or(0);
    let snapshot_floor = read_optional_u64(
        txn,
        table,
        META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY.to_vec(),
        "initial snapshot cursor floor",
    )?
    .unwrap_or(0);
    let desired_floor = last_op_id.saturating_sub(retain_ops);
    if desired_floor <= retained_floor {
        return Ok(());
    }

    // The chunk size bounds each inner deletion pass, not the total catch-up
    // performed for a source transaction. Capping total deletions here lets a
    // sustained stream that appends at least one chunk per transaction grow
    // the retained tail without bound. Keep all chunks in this MDBX
    // transaction so the retained floor cannot become visible separately from
    // the corresponding op and index deletions.
    let delete_chunk_ops = delete_chunk_ops.max(1);
    // Initial snapshot rows reserve cursor ids but are represented only in
    // current state, never as tail records. Jump over that synthetic prefix;
    // walking it would make pruning latency proportional to snapshot size.
    if desired_floor > snapshot_floor {
        let mut chunk_start = (retained_floor + 1).max(snapshot_floor + 1);
        while chunk_start <= desired_floor {
            let chunk_end = desired_floor.min(chunk_start.saturating_add(delete_chunk_ops - 1));
            for op_id in chunk_start..=chunk_end {
                if let Some(bytes) = txn
                    .get::<Vec<u8>>(table, &sync_tail_refs_key(op_id))
                    .map_err(|error| {
                        ReplicationIngestError::Mdbx(format!(
                            "read sync tail index references {op_id}: {error}"
                        ))
                    })?
                {
                    let index_keys: BTreeSet<String> =
                        serde_json::from_slice(&bytes).map_err(|error| {
                            ReplicationIngestError::CorruptBatch(format!(
                                "sync tail index references {op_id} are invalid: {error}"
                            ))
                        })?;
                    for index_key in index_keys {
                        txn.del(table, sync_tail_index_entry_key(&index_key, op_id), None)
                            .map_err(|error| {
                                ReplicationIngestError::Mdbx(format!(
                                    "delete sync tail index entry {index_key}/{op_id}: {error}"
                                ))
                            })?;
                    }
                }
                txn.del(table, sync_tail_refs_key(op_id), None)
                    .map_err(|error| {
                        ReplicationIngestError::Mdbx(format!(
                            "delete sync tail index references {op_id}: {error}"
                        ))
                    })?;
                txn.del(table, sync_tail_op_key(op_id), None)
                    .map_err(|error| {
                        ReplicationIngestError::Mdbx(format!(
                            "delete sync tail op {op_id}: {error}"
                        ))
                    })?;
            }
            if chunk_end == desired_floor {
                break;
            }
            chunk_start = chunk_end + 1;
        }
    }
    txn.put(
        table,
        META_SYNC_TAIL_RETAINED_FLOOR_KEY,
        desired_floor.to_string(),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!("persist sync tail retained floor: {error}"))
    })?;
    Ok(())
}

pub(super) fn advance_sync_tail_snapshot_floor(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    snapshot_op_count: u64,
) -> Result<(), ReplicationIngestError> {
    if snapshot_op_count == 0 {
        return Ok(());
    }

    let current_floor = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
        "sync tail last op id",
    )?
    .unwrap_or(0);
    let snapshot_floor = current_floor
        .checked_add(snapshot_op_count)
        .ok_or_else(|| {
            ReplicationIngestError::CorruptBatch("snapshot cursor floor overflow".to_owned())
        })?;

    txn.put(
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY,
        snapshot_floor.to_string(),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!("persist sync tail last op id: {error}"))
    })?;
    txn.put(
        table,
        META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY,
        snapshot_floor.to_string(),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!("persist sync tail indexed-through op id: {error}"))
    })?;

    Ok(())
}

/// Restore this op's sync-tail index entries during the legacy backfill. The
/// backfill walks a strictly ascending `(indexed_through+1)..=last_op_id` range in
/// one transaction and advances `indexed_through` atomically (a mid-backfill crash
/// rolls the whole transaction back), so each op id is guaranteed absent from every
/// bucket index. Write directly — the former per-op O(n) presence scan made the
/// first post-upgrade serve O(n*m) under the single writer lock.
pub(super) fn persist_sync_tail_index_entries_if_missing(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    op: &PersistedSyncTailOp,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    for index_key in sync_tail_index_keys_for_op(op, plan) {
        put_sync_tail_index_entry(txn, table, &index_key, op.op_id, metrics)?;
    }

    Ok(())
}

fn persist_sync_tail_index_entries(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    op: &PersistedSyncTailOp,
    index_keys: &BTreeSet<String>,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    for index_key in index_keys {
        put_sync_tail_index_entry(txn, table, index_key, op.op_id, metrics)?;
    }

    Ok(())
}

fn put_sync_tail_index_entry(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    index_key: &str,
    global_op_id: u64,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    txn.put(
        table,
        sync_tail_index_entry_key(index_key, global_op_id),
        global_op_id.to_be_bytes(),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!(
            "persist sync tail index entry {index_key}/{global_op_id}: {error}"
        ))
    })?;

    metrics
        .tail_index_entries_written
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

pub fn sync_tail_index_keys_for_bucket(
    object_type: &str,
    route_constraints: &BTreeMap<String, String>,
) -> Vec<String> {
    if route_constraints.is_empty() {
        return vec![sync_tail_object_index_name(object_type)];
    }

    vec![
        sync_tail_clear_index_name(object_type),
        sync_tail_route_index_name(object_type, route_constraints),
    ]
}

fn sync_tail_index_keys_for_op(
    op: &PersistedSyncTailOp,
    plan: &RustExecutionPlan,
) -> BTreeSet<String> {
    let Some(object_type) = op.object_type.as_deref() else {
        return BTreeSet::new();
    };

    let mut keys = BTreeSet::from([sync_tail_object_index_name(object_type)]);
    match op.operation {
        PersistedSyncTailOperation::Clear => {
            keys.insert(sync_tail_clear_index_name(object_type));
        }
        PersistedSyncTailOperation::Put | PersistedSyncTailOperation::Remove => {
            for route_constraints in
                plan.required_route_indexes_for_row(object_type, &op.route_fields)
            {
                keys.insert(sync_tail_route_index_name(object_type, &route_constraints));
            }
            if let Some(previous_route_fields) = &op.previous_route_fields {
                for route_constraints in
                    plan.required_route_indexes_for_row(object_type, previous_route_fields)
                {
                    keys.insert(sync_tail_route_index_name(object_type, &route_constraints));
                }
            }
        }
    }

    keys
}

pub(super) fn scan_sync_tail_index_entries<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    index_key: &str,
    first_global_op_id: u64,
    last_global_op_id: u64,
    global_op_ids: &mut Vec<u64>,
) -> Result<(), ReplicationIngestError> {
    visit_sync_tail_index_entries(
        txn,
        table,
        index_key,
        first_global_op_id,
        last_global_op_id,
        |global_op_id| {
            global_op_ids.push(global_op_id);
            Ok(true)
        },
    )?;
    Ok(())
}

pub(super) fn scan_sync_tail_index_entries_bounded<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    index_key: &str,
    first_global_op_id: u64,
    last_global_op_id: u64,
    global_op_ids: &mut BTreeSet<u64>,
    remaining_scan_entries: &mut u64,
) -> Result<bool, ReplicationIngestError> {
    visit_sync_tail_index_entries(
        txn,
        table,
        index_key,
        first_global_op_id,
        last_global_op_id,
        |global_op_id| {
            if *remaining_scan_entries == 0 {
                return Ok(false);
            }
            *remaining_scan_entries -= 1;
            global_op_ids.insert(global_op_id);
            Ok(true)
        },
    )
}

fn visit_sync_tail_index_entries<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    index_key: &str,
    first_global_op_id: u64,
    last_global_op_id: u64,
    mut visit: impl FnMut(u64) -> Result<bool, ReplicationIngestError>,
) -> Result<bool, ReplicationIngestError> {
    if first_global_op_id > last_global_op_id {
        return Ok(true);
    }

    let prefix = sync_tail_index_entry_prefix(index_key);
    let start_key = sync_tail_index_entry_key(index_key, first_global_op_id);
    let mut cursor = txn.cursor(table).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("open sync tail index cursor: {error}"))
    })?;
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(&start_key) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan sync tail index {index_key}: {error}"))
        })?;
        if !key.starts_with(&prefix) {
            break;
        }
        let Some(global_op_id) = sync_tail_global_op_id_from_key(&key, &prefix) else {
            continue;
        };
        if global_op_id > last_global_op_id {
            break;
        }
        let stored_global_op_id = parse_be_u64(&value, "sync tail index entry")?;
        if stored_global_op_id != global_op_id {
            return Err(ReplicationIngestError::CorruptBatch(format!(
                "sync tail index entry key {global_op_id} points at op {stored_global_op_id}"
            )));
        }
        if !visit(global_op_id)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn read_sync_tail_op<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    op_id: u64,
) -> Result<PersistedSyncTailOp, ReplicationIngestError> {
    let bytes = txn
        .get::<Vec<u8>>(table, &sync_tail_op_key(op_id))
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("read sync tail op {op_id}: {error}"))
        })?
        .ok_or_else(|| {
            ReplicationIngestError::CorruptBatch(format!(
                "sync tail index points at missing op {op_id}"
            ))
        })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!(
            "sync tail op {op_id} is not valid JSON: {error}"
        ))
    })
}

pub(super) fn read_sync_tail_op_bounded<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    op_id: u64,
    remaining_bytes: &mut u64,
) -> Result<PersistedSyncTailOp, ReplicationIngestError> {
    let bytes = txn
        .get::<Cow<'_, [u8]>>(table, &sync_tail_op_key(op_id))
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("read sync tail op {op_id}: {error}"))
        })?
        .ok_or_else(|| {
            ReplicationIngestError::CorruptBatch(format!(
                "sync tail index points at missing op {op_id}"
            ))
        })?;
    let serialized_len = bytes.len() as u64;
    if serialized_len > *remaining_bytes {
        return Err(ReplicationIngestError::ResourceLimit(format!(
            "serialized sync tail op {op_id} needs {serialized_len} bytes but only {remaining_bytes} remain in the per-request budget"
        )));
    }
    *remaining_bytes -= serialized_len;
    serde_json::from_slice(&bytes).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!(
            "sync tail op {op_id} is not valid JSON: {error}"
        ))
    })
}

/// Read a decimal-string-encoded `u64` metadata value. Op-id counters are
/// persisted via `u64::to_string`, so they must be decoded as decimal text.
/// They must NOT be length-sniffed as big-endian: an op id with exactly 8 decimal digits
/// (10_000_000..=99_999_999) is also 8 bytes and would be silently misread.
pub(super) fn read_optional_u64<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    key: Vec<u8>,
    label: &str,
) -> Result<Option<u64>, ReplicationIngestError> {
    txn.get::<Vec<u8>>(table, &key)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("read {label}: {error}")))?
        .map(|bytes| parse_decimal_u64(&bytes, label))
        .transpose()
}

/// Read a fixed 8-byte big-endian `u64` value (the sync-tail index entries are
/// persisted via `u64::to_be_bytes`). Retained for the encoding round-trip test;
/// production reads entries in bulk via `parse_be_u64` in `scan_sync_tail_index_entries`.
#[cfg(test)]
pub(super) fn read_optional_be_u64<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    key: Vec<u8>,
    label: &str,
) -> Result<Option<u64>, ReplicationIngestError> {
    txn.get::<Vec<u8>>(table, &key)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("read {label}: {error}")))?
        .map(|bytes| parse_be_u64(&bytes, label))
        .transpose()
}

fn parse_decimal_u64(bytes: &[u8], label: &str) -> Result<u64, ReplicationIngestError> {
    std::str::from_utf8(bytes)
        .map_err(|error| {
            ReplicationIngestError::CorruptBatch(format!("{label} metadata is not UTF-8: {error}"))
        })?
        .parse::<u64>()
        .map_err(|error| {
            ReplicationIngestError::CorruptBatch(format!("{label} metadata is invalid: {error}"))
        })
}

fn parse_be_u64(bytes: &[u8], label: &str) -> Result<u64, ReplicationIngestError> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        ReplicationIngestError::CorruptBatch(format!(
            "{label} metadata is not an 8-byte big-endian u64 (got {} bytes)",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(array))
}
