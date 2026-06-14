use std::collections::{BTreeMap, HashMap};

use libmdbx::{NoWriteMap, TransactionKind, WriteFlags};
use pg_walstream::RowData;

use super::{
    current_state::{CurrentDocumentWrite, PersistedBucketedDocument},
    derive::sync_rule_error_to_ingest_error,
    error::ReplicationIngestError,
    keys::{
        CURRENT_CHECKPOINT_ACCUMULATOR_KEY_PREFIX, SYNC_TAIL_CHECKPOINT_ACCUMULATOR_KEY_PREFIX,
    },
    tail_log::{PersistedSyncTailOp, PersistedSyncTailOperation},
};
use crate::{
    protocol::messages::{put_checksum, remove_checksum, source_subkey_for_object},
    sync_rules::{CompiledTablePlan, JsonColumnTypes, ResolvedSyncBucket, RustExecutionPlan},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistedCheckpointAccumulator {
    pub checksum: u32,
    pub count: u64,
}

impl PersistedCheckpointAccumulator {
    pub(super) fn add(&mut self, delta: Self) -> Result<(), ReplicationIngestError> {
        self.checksum = self.checksum.wrapping_add(delta.checksum);
        self.count = self.count.checked_add(delta.count).ok_or_else(|| {
            ReplicationIngestError::CorruptBatch(
                "sync checkpoint accumulator count overflow".to_owned(),
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod accumulator_arithmetic_tests {
    use super::*;

    #[test]
    fn add_wraps_the_u32_checksum_and_sums_the_count() {
        let mut acc = PersistedCheckpointAccumulator {
            checksum: u32::MAX,
            count: 5,
        };
        acc.add(PersistedCheckpointAccumulator {
            checksum: 3,
            count: 10,
        })
        .expect("add within range");
        // The checksum is a rolling u32 sum: u32::MAX + 3 wraps to 2.
        assert_eq!(acc.checksum, 2);
        assert_eq!(acc.count, 15);
    }

    #[test]
    fn add_rejects_count_overflow_instead_of_wrapping() {
        let mut acc = PersistedCheckpointAccumulator {
            checksum: 0,
            count: u64::MAX,
        };
        let error = acc
            .add(PersistedCheckpointAccumulator {
                checksum: 0,
                count: 1,
            })
            .expect_err("count overflow must be rejected, not silently wrapped");
        assert!(matches!(error, ReplicationIngestError::CorruptBatch(_)));
        // The count is left at its pre-add value (checked_add did not commit).
        assert_eq!(acc.count, u64::MAX);
    }

    #[test]
    fn persist_current_accumulator_deltas_clamps_count_underflow_to_zero() {
        use crate::replication::ingest::store::ReplicationMdbxStore;
        use std::collections::BTreeMap;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let store = ReplicationMdbxStore::new(dir.path()).expect("store");
        let txn = store.db.begin_rw_txn().expect("rw txn");
        let table = txn.open_table(None).expect("table");

        let key = "acc-underflow".to_owned();
        write_current_checkpoint_accumulator(
            &txn,
            &table,
            &key,
            PersistedCheckpointAccumulator {
                checksum: 100,
                count: 2,
            },
        )
        .expect("seed accumulator");

        // Remove more (5) than the seeded count (2): the deliberate clamp keeps
        // the ingest loop alive instead of erroring on accounting drift.
        let mut deltas = BTreeMap::new();
        deltas.insert(
            key.clone(),
            CurrentAccumulatorDelta {
                add: PersistedCheckpointAccumulator::default(),
                remove: PersistedCheckpointAccumulator {
                    checksum: 10,
                    count: 5,
                },
            },
        );
        persist_current_accumulator_deltas(&txn, &table, &deltas)
            .expect("underflow must clamp, not error");

        let after =
            read_current_checkpoint_accumulator(&txn, &table, &key).expect("read accumulator");
        assert_eq!(after.count, 0, "count underflow must clamp to zero");
    }
}

pub(super) fn persist_checkpoint_accumulator_deltas(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    deltas: &BTreeMap<String, PersistedCheckpointAccumulator>,
) -> Result<(), ReplicationIngestError> {
    for (accumulator_key, delta) in deltas {
        let mut accumulator = read_checkpoint_accumulator(txn, table, accumulator_key)?;
        accumulator.add(*delta)?;
        write_checkpoint_accumulator(txn, table, accumulator_key, accumulator)?;
    }
    Ok(())
}

pub(super) fn collect_checkpoint_accumulator_deltas_for_op(
    deltas: &mut BTreeMap<String, PersistedCheckpointAccumulator>,
    op: &PersistedSyncTailOp,
    plan: &RustExecutionPlan,
) -> Result<(), ReplicationIngestError> {
    add_previous_put_accumulator_deltas(op, plan, deltas)?;
    add_remove_accumulator_deltas(op, plan, deltas)?;
    Ok(())
}

fn add_previous_put_accumulator_deltas(
    op: &PersistedSyncTailOp,
    plan: &RustExecutionPlan,
    deltas: &mut BTreeMap<String, PersistedCheckpointAccumulator>,
) -> Result<(), ReplicationIngestError> {
    if !matches!(
        op.operation,
        PersistedSyncTailOperation::Put | PersistedSyncTailOperation::Remove
    ) {
        return Ok(());
    }
    let Some(object_type) = op.object_type.as_deref() else {
        return Ok(());
    };
    let Some(object_id) = op.object_id.as_deref() else {
        return Ok(());
    };
    let Some(previous_data_json) = op.previous_data_json.as_deref().or_else(|| {
        (op.operation == PersistedSyncTailOperation::Remove)
            .then_some(op.data_json.as_deref())
            .flatten()
    }) else {
        return Ok(());
    };
    let previous_route_fields = op
        .previous_route_fields
        .as_ref()
        .unwrap_or(&op.route_fields);

    for bucket in plan.accumulator_buckets_for_row(object_type, previous_route_fields) {
        if !bucket.matches_object_routes_and_data(
            object_type,
            previous_route_fields,
            previous_data_json,
        ) {
            continue;
        }
        let Ok(projected_data) = bucket.project_document_json(object_type, previous_data_json)
        else {
            continue;
        };
        add_checkpoint_accumulator_delta(
            deltas,
            &bucket,
            PersistedCheckpointAccumulator {
                checksum: put_checksum(object_type, object_id, &projected_data),
                count: 1,
            },
        )?;
    }
    Ok(())
}

fn add_remove_accumulator_deltas(
    op: &PersistedSyncTailOp,
    plan: &RustExecutionPlan,
    deltas: &mut BTreeMap<String, PersistedCheckpointAccumulator>,
) -> Result<(), ReplicationIngestError> {
    if op.operation != PersistedSyncTailOperation::Remove {
        return Ok(());
    }
    let Some(object_type) = op.object_type.as_deref() else {
        return Ok(());
    };
    let Some(object_id) = op.object_id.as_deref() else {
        return Ok(());
    };

    for bucket in plan.accumulator_buckets_for_row(object_type, &op.route_fields) {
        let matches_bucket = op.data_json.as_deref().map_or_else(
            || bucket.matches_object_and_routes(object_type, &op.route_fields),
            |data_json| {
                bucket.matches_object_routes_and_data(object_type, &op.route_fields, data_json)
            },
        );
        if !matches_bucket {
            continue;
        }
        add_checkpoint_accumulator_delta(
            deltas,
            &bucket,
            PersistedCheckpointAccumulator {
                checksum: remove_checksum(&source_subkey_for_object(object_type, object_id)),
                count: 1,
            },
        )?;
    }
    Ok(())
}

fn add_checkpoint_accumulator_delta(
    deltas: &mut BTreeMap<String, PersistedCheckpointAccumulator>,
    bucket: &ResolvedSyncBucket,
    delta: PersistedCheckpointAccumulator,
) -> Result<(), ReplicationIngestError> {
    for key in sync_tail_checkpoint_accumulator_keys_for_bucket(bucket) {
        deltas.entry(key).or_default().add(delta)?;
    }
    Ok(())
}

pub fn sync_tail_checkpoint_accumulator_keys_for_bucket(
    bucket: &ResolvedSyncBucket,
) -> Vec<String> {
    sync_checkpoint_accumulator_keys_for_bucket(bucket)
}

pub fn sync_current_checkpoint_accumulator_keys_for_bucket(
    bucket: &ResolvedSyncBucket,
) -> Vec<String> {
    sync_checkpoint_accumulator_keys_for_bucket(bucket)
}

fn sync_checkpoint_accumulator_keys_for_bucket(bucket: &ResolvedSyncBucket) -> Vec<String> {
    let mut keys = bucket
        .queries()
        .iter()
        .map(|query| query.checkpoint_accumulator_key().to_owned())
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys
}

fn tail_checkpoint_accumulator_key(accumulator_key: &str) -> Vec<u8> {
    checkpoint_accumulator_key(SYNC_TAIL_CHECKPOINT_ACCUMULATOR_KEY_PREFIX, accumulator_key)
}

fn current_checkpoint_accumulator_key(accumulator_key: &str) -> Vec<u8> {
    checkpoint_accumulator_key(CURRENT_CHECKPOINT_ACCUMULATOR_KEY_PREFIX, accumulator_key)
}

fn checkpoint_accumulator_key(prefix: &str, accumulator_key: &str) -> Vec<u8> {
    format!("{prefix}{accumulator_key}").into_bytes()
}

pub(super) fn read_checkpoint_accumulator<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    accumulator_key: &str,
) -> Result<PersistedCheckpointAccumulator, ReplicationIngestError> {
    read_checkpoint_accumulator_with_key(
        txn,
        table,
        &tail_checkpoint_accumulator_key(accumulator_key),
        accumulator_key,
    )
}

pub(super) fn read_current_checkpoint_accumulator<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    accumulator_key: &str,
) -> Result<PersistedCheckpointAccumulator, ReplicationIngestError> {
    read_checkpoint_accumulator_with_key(
        txn,
        table,
        &current_checkpoint_accumulator_key(accumulator_key),
        accumulator_key,
    )
}

fn read_checkpoint_accumulator_with_key<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    storage_key: &[u8],
    accumulator_key: &str,
) -> Result<PersistedCheckpointAccumulator, ReplicationIngestError> {
    let Some(bytes) = txn.get::<Vec<u8>>(table, storage_key).map_err(|error| {
        ReplicationIngestError::Mdbx(format!(
            "read sync checkpoint accumulator {accumulator_key}: {error}"
        ))
    })?
    else {
        return Ok(PersistedCheckpointAccumulator::default());
    };
    decode_checkpoint_accumulator(&bytes)
}

fn write_checkpoint_accumulator(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    accumulator_key: &str,
    accumulator: PersistedCheckpointAccumulator,
) -> Result<(), ReplicationIngestError> {
    write_checkpoint_accumulator_with_key(
        txn,
        table,
        &tail_checkpoint_accumulator_key(accumulator_key),
        accumulator_key,
        accumulator,
    )
}

fn write_current_checkpoint_accumulator(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    accumulator_key: &str,
    accumulator: PersistedCheckpointAccumulator,
) -> Result<(), ReplicationIngestError> {
    write_checkpoint_accumulator_with_key(
        txn,
        table,
        &current_checkpoint_accumulator_key(accumulator_key),
        accumulator_key,
        accumulator,
    )
}

fn write_checkpoint_accumulator_with_key(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    storage_key: &[u8],
    accumulator_key: &str,
    accumulator: PersistedCheckpointAccumulator,
) -> Result<(), ReplicationIngestError> {
    txn.put(
        table,
        storage_key,
        encode_checkpoint_accumulator(accumulator),
        WriteFlags::UPSERT,
    )
    .map_err(|error| {
        ReplicationIngestError::Mdbx(format!(
            "persist sync checkpoint accumulator {accumulator_key}: {error}"
        ))
    })
}

fn encode_checkpoint_accumulator(accumulator: PersistedCheckpointAccumulator) -> [u8; 12] {
    let mut bytes = [0_u8; 12];
    bytes[..4].copy_from_slice(&accumulator.checksum.to_be_bytes());
    bytes[4..].copy_from_slice(&accumulator.count.to_be_bytes());
    bytes
}

fn decode_checkpoint_accumulator(
    bytes: &[u8],
) -> Result<PersistedCheckpointAccumulator, ReplicationIngestError> {
    if bytes.len() != 12 {
        return Err(ReplicationIngestError::CorruptBatch(format!(
            "sync checkpoint accumulator should be 12 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(PersistedCheckpointAccumulator {
        checksum: u32::from_be_bytes(bytes[..4].try_into().expect("checksum slice length")),
        count: u64::from_be_bytes(bytes[4..].try_into().expect("count slice length")),
    })
}

pub(super) fn add_current_document_accumulator(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    document: CurrentDocumentWrite<'_>,
    plan: &RustExecutionPlan,
) -> Result<(), ReplicationIngestError> {
    persist_current_document_accumulator_delta(
        txn,
        table,
        document,
        plan,
        CurrentAccumulatorDeltaDirection::Add,
    )
}

pub(super) fn remove_current_document_accumulator(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    document: &PersistedBucketedDocument,
    plan: &RustExecutionPlan,
) -> Result<(), ReplicationIngestError> {
    persist_current_document_accumulator_delta(
        txn,
        table,
        CurrentDocumentWrite {
            object_type: &document.object_type,
            object_id: &document.object_id,
            route_fields: &document.route_fields,
            data_json: &document.data_json,
        },
        plan,
        CurrentAccumulatorDeltaDirection::Remove,
    )
}

#[derive(Clone, Copy)]
pub(super) enum CurrentAccumulatorDeltaDirection {
    Add,
    Remove,
}

#[derive(Clone, Copy, Default)]
pub(super) struct CurrentAccumulatorDelta {
    add: PersistedCheckpointAccumulator,
    remove: PersistedCheckpointAccumulator,
}

#[derive(Clone)]
pub(super) struct CurrentAccumulatorTarget {
    bucket: ResolvedSyncBucket,
    keys: Vec<String>,
    projection_key: String,
}

pub(super) struct SnapshotAccumulatorInput<'a> {
    pub(super) table_plan: &'a CompiledTablePlan,
    pub(super) row: &'a RowData,
    pub(super) column_types: Option<&'a JsonColumnTypes>,
    pub(super) document: CurrentDocumentWrite<'a>,
}

fn persist_current_document_accumulator_delta(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    document: CurrentDocumentWrite<'_>,
    plan: &RustExecutionPlan,
    direction: CurrentAccumulatorDeltaDirection,
) -> Result<(), ReplicationIngestError> {
    let mut deltas = BTreeMap::new();
    let targets =
        current_accumulator_targets_for_row(plan, document.object_type, document.route_fields);
    collect_current_document_accumulator_delta_for_targets(
        &mut deltas,
        document,
        &targets,
        direction,
    )?;
    persist_current_accumulator_deltas(txn, table, &deltas)
}

pub(super) fn current_accumulator_targets_for_row(
    plan: &RustExecutionPlan,
    object_type: &str,
    route_fields: &BTreeMap<String, String>,
) -> Vec<CurrentAccumulatorTarget> {
    plan.accumulator_buckets_for_row(object_type, route_fields)
        .into_iter()
        .map(|bucket| {
            let keys = sync_current_checkpoint_accumulator_keys_for_bucket(&bucket);
            let projection_key = bucket.projection_key().to_owned();
            CurrentAccumulatorTarget {
                bucket,
                keys,
                projection_key,
            }
        })
        .collect()
}

fn collect_current_document_accumulator_delta_for_targets(
    deltas: &mut BTreeMap<String, CurrentAccumulatorDelta>,
    document: CurrentDocumentWrite<'_>,
    targets: &[CurrentAccumulatorTarget],
    direction: CurrentAccumulatorDeltaDirection,
) -> Result<(), ReplicationIngestError> {
    collect_current_accumulator_deltas(deltas, document, targets, direction, |target| {
        target
            .bucket
            .project_document_json(document.object_type, document.data_json)
            .map_err(sync_rule_error_to_ingest_error)
    })
}

pub(super) fn collect_current_document_accumulator_delta_for_snapshot_targets(
    deltas: &mut BTreeMap<String, CurrentAccumulatorDelta>,
    input: SnapshotAccumulatorInput<'_>,
    targets: &[CurrentAccumulatorTarget],
    direction: CurrentAccumulatorDeltaDirection,
) -> Result<(), ReplicationIngestError> {
    let document = input.document;
    collect_current_accumulator_deltas(deltas, document, targets, direction, |target| {
        input
            .table_plan
            .project_row_json_from_serialized(
                input.row,
                target.bucket.projection(),
                document.data_json,
                input.column_types,
            )
            .map_err(sync_rule_error_to_ingest_error)
    })
}

fn collect_current_accumulator_deltas<F>(
    deltas: &mut BTreeMap<String, CurrentAccumulatorDelta>,
    document: CurrentDocumentWrite<'_>,
    targets: &[CurrentAccumulatorTarget],
    direction: CurrentAccumulatorDeltaDirection,
    mut project_for_target: F,
) -> Result<(), ReplicationIngestError>
where
    F: FnMut(&CurrentAccumulatorTarget) -> Result<String, ReplicationIngestError>,
{
    let mut checksums_by_projection = HashMap::<&str, u32>::new();
    for target in targets {
        let bucket = &target.bucket;
        if !bucket.matches_object_routes_and_data(
            document.object_type,
            document.route_fields,
            document.data_json,
        ) {
            continue;
        }
        let checksum = if let Some(checksum) =
            checksums_by_projection.get(target.projection_key.as_str())
        {
            *checksum
        } else {
            let projected_data = project_for_target(target)?;
            let checksum = put_checksum(document.object_type, document.object_id, &projected_data);
            checksums_by_projection.insert(target.projection_key.as_str(), checksum);
            checksum
        };
        add_current_accumulator_checksum_delta(deltas, target, checksum, direction)?;
    }
    Ok(())
}

fn add_current_accumulator_checksum_delta(
    deltas: &mut BTreeMap<String, CurrentAccumulatorDelta>,
    target: &CurrentAccumulatorTarget,
    checksum: u32,
    direction: CurrentAccumulatorDeltaDirection,
) -> Result<(), ReplicationIngestError> {
    for key in &target.keys {
        let delta = deltas.entry(key.clone()).or_default();
        match direction {
            CurrentAccumulatorDeltaDirection::Add => {
                delta.add.checksum = delta.add.checksum.wrapping_add(checksum);
                delta.add.count = delta.add.count.checked_add(1).ok_or_else(|| {
                    ReplicationIngestError::CorruptBatch(
                        "current checkpoint accumulator add count overflow".to_owned(),
                    )
                })?;
            }
            CurrentAccumulatorDeltaDirection::Remove => {
                delta.remove.checksum = delta.remove.checksum.wrapping_add(checksum);
                delta.remove.count = delta.remove.count.checked_add(1).ok_or_else(|| {
                    ReplicationIngestError::CorruptBatch(
                        "current checkpoint accumulator remove count overflow".to_owned(),
                    )
                })?;
            }
        }
    }
    Ok(())
}

pub(super) fn persist_current_accumulator_deltas(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    deltas: &BTreeMap<String, CurrentAccumulatorDelta>,
) -> Result<(), ReplicationIngestError> {
    for (key, delta) in deltas {
        let mut accumulator = read_current_checkpoint_accumulator(txn, table, key)?;
        accumulator.add(delta.add)?;
        // A count underflow means accounting drifted (removes exceeded adds).
        // Reset to a consistent EMPTY accumulator rather than failing the batch
        // (a hard error wedges the ingest loop — every retry replays the same
        // underflow) or leaving count=0 paired with a now-nonzero checksum, which
        // would advertise a checkpoint whose count and checksum disagree. Only
        // subtract the checksum on the non-underflow path.
        accumulator.count = match accumulator.count.checked_sub(delta.remove.count) {
            Some(count) => {
                accumulator.checksum = accumulator.checksum.wrapping_sub(delta.remove.checksum);
                count
            }
            None => {
                tracing::warn!(
                    accumulator = %key,
                    count = accumulator.count,
                    remove = delta.remove.count,
                    "current checkpoint accumulator count underflow; resetting to empty"
                );
                accumulator.checksum = 0;
                0
            }
        };
        write_current_checkpoint_accumulator(txn, table, key, accumulator)?;
    }
    Ok(())
}
