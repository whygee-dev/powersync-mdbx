use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env, fs,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use libmdbx::{
    Database, DatabaseOptions, NoWriteMap, ReadWriteOptions, SyncMode, TransactionKind, WriteFlags,
};
use pg_walstream::RowData;
use tokio::sync::watch;

#[cfg(test)]
use super::keys::META_SYNC_STATE_JSON_KEY;
use super::{
    accumulators::{
        collect_checkpoint_accumulator_deltas_for_op,
        collect_current_document_accumulator_delta_for_snapshot_targets,
        current_accumulator_targets_for_row, persist_checkpoint_accumulator_deltas,
        persist_current_accumulator_deltas, read_checkpoint_accumulator,
        read_current_checkpoint_accumulator, CurrentAccumulatorDelta,
        CurrentAccumulatorDeltaDirection, CurrentAccumulatorTarget, PersistedCheckpointAccumulator,
        SnapshotAccumulatorInput,
    },
    batch_codec::ReplicationCommitBatch,
    current_state::{
        collect_current_documents_for_prefix_bounded, collect_current_route_documents_bounded,
        put_new_current_document_with_route_indexes, scan_current_document_keys_for_prefix,
        scan_current_documents_for_prefix, scan_current_route_documents,
        scan_current_route_index_keys, CurrentDocumentWrite, CurrentWriteContext,
        PersistedBucketedDocument,
    },
    derive::{
        derive_parameter_lookup_ops, derive_sync_tail_ops_with_options,
        sync_rule_error_to_ingest_error,
    },
    error::ReplicationIngestError,
    keys::{
        batch_key, current_doc_prefix, current_route_index_prefix, sync_tail_op_key,
        META_INITIAL_SNAPSHOT_BOOTSTRAP_INTENT_KEY, META_INITIAL_SNAPSHOT_COMPLETE_KEY,
        META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY, META_INITIAL_SNAPSHOT_SOURCE_IDENTITY_KEY,
        META_LAST_COMMIT_END_LSN_KEY, META_LAYOUT_VERSION_KEY,
        META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY, META_SYNC_TAIL_LAST_OP_ID_KEY,
        META_SYNC_TAIL_RETAINED_FLOOR_KEY,
    },
    lookup_state::{
        apply_parameter_lookup_ops, put_parameter_lookup_row, scan_parameter_lookup_entries,
    },
    metrics::{ReplicationIngestMetricCounters, ReplicationIngestMetrics},
    tail_log::{
        advance_sync_tail_snapshot_floor, persist_snapshot_current_ops_without_tail,
        persist_sync_tail_index_entries_if_missing, persist_task_tail_ops, prune_sync_tail,
        read_optional_u64, read_sync_tail_op, read_sync_tail_op_bounded,
        scan_sync_tail_index_entries, scan_sync_tail_index_entries_bounded, IndexedSyncTailOps,
        PersistedSyncTailOp,
    },
};
#[cfg(test)]
use crate::protocol::messages::TaskRow;
#[cfg(test)]
use crate::replication::ingest::keys::CURRENT_DOC_KEY_PREFIX;
use crate::replication::postgres::PostgresLsn;
use crate::sync_rules::{
    execution_plan, storage_contract_id, JsonColumnTypes, ResolvedSyncBucket, RustExecutionPlan,
};

const DEFAULT_INGEST_PATH: &str = "./data/powersync-rust-mdbx-ingest";
const MDBX_MAX_SIZE_ENV: &str = "POWERSYNC_RUST_MDBX_MAX_SIZE_BYTES";
const INGEST_PATH_ENV: &str = "POWERSYNC_RUST_MDBX_INGEST_PATH";
const TAIL_PATH_ENV: &str = "POWERSYNC_RUST_MDBX_TAIL_PATH";
const MDBX_PATH_ENV: &str = "POWERSYNC_RUST_MDBX_PATH";
const INGEST_LAYOUT_FORMAT_VERSION: &str = "global-op-index-retention-v3";
const DEFAULT_TAIL_RETAIN_OPS: u64 = 1_000_000;
const DEFAULT_TAIL_PRUNE_BATCH_OPS: u64 = 10_000;
const CURSOR_IDS_PER_MILLISECOND: u64 = 1 << 20;
const DEFAULT_SYNC_READ_MAX_ENTRIES: u64 = 250_000;
const DEFAULT_SYNC_READ_MAX_BYTES: u64 = 128 * 1024 * 1024;

type MdbxDatabase = Database<NoWriteMap>;

#[derive(Debug, Clone, Copy)]
pub struct PersistBatchOptions {
    pub persist_raw_batch: bool,
    pub assume_new_inserts: bool,
    pub snapshot_without_tail: bool,
}

#[derive(Debug)]
pub struct BucketReadSnapshot {
    pub latest_op_id: u64,
    pub tail_ops: Vec<PersistedSyncTailOp>,
    pub current_accumulator: PersistedCheckpointAccumulator,
    pub tail_accumulator: PersistedCheckpointAccumulator,
    pub current_documents: Vec<PersistedBucketedDocument>,
    pub reset_required: bool,
}

#[derive(Debug, Clone)]
pub struct BucketReadRequest {
    pub bucket: ResolvedSyncBucket,
    pub index_keys: Vec<String>,
    pub current_accumulator_keys: Vec<String>,
    pub tail_accumulator_keys: Vec<String>,
    pub after: u64,
}

impl Default for PersistBatchOptions {
    fn default() -> Self {
        Self {
            persist_raw_batch: persist_raw_batches_enabled(),
            assume_new_inserts: false,
            snapshot_without_tail: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SnapshotRowOrder {
    Any,
    ColumnName,
}

// Default off: raw batch blobs are a debugging aid with no production
// reader (`load_batch` is test-only), but they double write volume in the
// ingest txn and are never pruned. The benchmark harness already ran with
// this disabled.
fn persist_raw_batches_enabled() -> bool {
    env::var("POWERSYNC_RUST_PERSIST_RAW_BATCHES")
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(false)
}

#[derive(Debug)]
pub struct ReplicationMdbxStore {
    pub(super) db: MdbxDatabase,
    path: PathBuf,
    task_tail_advance_tx: watch::Sender<u64>,
    metrics: ReplicationIngestMetricCounters,
    verified_layout_version: std::sync::RwLock<Option<String>>,
}

impl ReplicationMdbxStore {
    pub fn new_from_env() -> Result<Self, ReplicationIngestError> {
        Self::new(resolve_ingest_path_from_env())
    }

    pub fn shared_from_env() -> Result<Arc<Self>, ReplicationIngestError> {
        Self::shared(resolve_ingest_path_from_env())
    }

    pub fn shared(path: impl AsRef<Path>) -> Result<Arc<Self>, ReplicationIngestError> {
        let path = path.as_ref().to_path_buf();
        let registry = shared_store_registry();
        let mut registry = registry
            .lock()
            .expect("shared replication MDBX store registry mutex should not be poisoned");

        if let Some(store) = registry.get(&path).and_then(Weak::upgrade) {
            return Ok(store);
        }

        let store = Arc::new(Self::new(&path)?);
        registry.insert(path, Arc::downgrade(&store));
        Ok(store)
    }

    pub fn new(path: impl AsRef<Path>) -> Result<Self, ReplicationIngestError> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("create {}: {error}", path.display()))
        })?;

        let options = DatabaseOptions {
            max_tables: Some(1),
            mode: libmdbx::Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                min_size: None,
                max_size: mdbx_max_size_from_env(),
                growth_step: None,
                shrink_threshold: None,
            }),
            no_rdahead: true,
            ..Default::default()
        };

        let db = MdbxDatabase::open_with_options(&path, options).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open {}: {error}", path.display()))
        })?;
        let (task_tail_advance_tx, _) = watch::channel(0);

        let store = Self {
            db,
            path,
            task_tail_advance_tx,
            metrics: ReplicationIngestMetricCounters::default(),
            verified_layout_version: std::sync::RwLock::new(None),
        };
        store.ensure_layout_version()?;
        let initial_task_tail = store.task_tail_last_op_id()?.unwrap_or(0);
        let _ = store.task_tail_advance_tx.send(initial_task_tail);
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn metrics_snapshot(&self) -> ReplicationIngestMetrics {
        self.metrics.snapshot()
    }

    pub fn record_protocol_encode(&self, elapsed_ms: u64, bytes_sent: u64) {
        self.metrics
            .protocol_encode_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        self.metrics
            .bytes_sent
            .fetch_add(bytes_sent, Ordering::Relaxed);
    }

    pub fn record_replication_decode(&self, elapsed_ms: u64) {
        self.metrics
            .replication_decode_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
    }

    pub fn record_source_snapshot_scan(&self, elapsed_ms: u64) {
        self.metrics
            .source_snapshot_scan_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
    }

    pub fn persist_batch(
        &self,
        batch: &ReplicationCommitBatch,
    ) -> Result<(), ReplicationIngestError> {
        self.persist_batch_with_plan(batch, execution_plan())
    }

    pub fn persist_batch_with_plan(
        &self,
        batch: &ReplicationCommitBatch,
        plan: &RustExecutionPlan,
    ) -> Result<(), ReplicationIngestError> {
        self.persist_batch_with_plan_and_options(batch, plan, PersistBatchOptions::default())
    }

    pub fn persist_batch_with_plan_and_options(
        &self,
        batch: &ReplicationCommitBatch,
        plan: &RustExecutionPlan,
        options: PersistBatchOptions,
    ) -> Result<(), ReplicationIngestError> {
        self.persist_batch_with_plan_options_and_tail_retention(
            batch,
            plan,
            options,
            positive_u64_env("POWERSYNC_RUST_TAIL_RETAIN_OPS", DEFAULT_TAIL_RETAIN_OPS),
            positive_u64_env(
                "POWERSYNC_RUST_TAIL_PRUNE_BATCH_OPS",
                DEFAULT_TAIL_PRUNE_BATCH_OPS,
            ),
        )
    }

    pub(super) fn persist_batch_with_plan_options_and_tail_retention(
        &self,
        batch: &ReplicationCommitBatch,
        plan: &RustExecutionPlan,
        options: PersistBatchOptions,
        tail_retain_ops: u64,
        tail_delete_chunk_ops: u64,
    ) -> Result<(), ReplicationIngestError> {
        self.ensure_layout_version_for(plan.storage_contract_id())?;
        let encode_started = Instant::now();
        let encoded = options.persist_raw_batch.then(|| batch.encode());
        if encoded.is_some() {
            self.metrics.raw_batch_encode_ms.fetch_add(
                encode_started.elapsed().as_millis() as u64,
                Ordering::Relaxed,
            );
        }
        let sync_rule_eval_started = Instant::now();
        let derived_sync_ops =
            derive_sync_tail_ops_with_options(batch, plan, options.assume_new_inserts)?;
        let derived_lookup_ops = derive_parameter_lookup_ops(batch, plan)?;
        let write_started = Instant::now();
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;

        // Feedback can be lost after MDBX commits, causing PostgreSQL to
        // redeliver an already-durable transaction. The comparison and LSN
        // update share this write transaction with the state changes.
        if read_last_persisted_end_lsn(&txn, &table)?
            .is_some_and(|persisted_lsn| batch.end_lsn <= persisted_lsn)
        {
            return Ok(());
        }

        self.metrics
            .rows_seen
            .fetch_add(batch.changes.len() as u64, Ordering::Relaxed);
        self.metrics.sync_rule_eval_ms.fetch_add(
            sync_rule_eval_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );

        if let Some(encoded) = encoded {
            txn.put(
                &table,
                batch_key(batch.end_lsn),
                encoded,
                WriteFlags::UPSERT,
            )
            .map_err(|error| ReplicationIngestError::Mdbx(format!("persist batch: {error}")))?;
            self.metrics
                .raw_batches_persisted
                .fetch_add(1, Ordering::Relaxed);
        }
        txn.put(
            &table,
            META_LAST_COMMIT_END_LSN_KEY,
            batch.end_lsn.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist last commit LSN: {error}"))
        })?;
        apply_parameter_lookup_ops(&txn, &table, &derived_lookup_ops, plan)?;
        if options.snapshot_without_tail {
            persist_snapshot_current_ops_without_tail(
                &txn,
                &table,
                derived_sync_ops,
                plan,
                &self.metrics,
            )?;
        } else {
            persist_task_tail_ops(&txn, &table, derived_sync_ops, plan, &self.metrics)?;
            prune_sync_tail(&txn, &table, tail_retain_ops, tail_delete_chunk_ops)?;
        }
        txn.commit()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("commit batch txn: {error}")))?;
        self.metrics.mdbx_write_txn_ms.fetch_add(
            write_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .batches_persisted
            .fetch_add(1, Ordering::Relaxed);

        if let Some(last_op_id) = self.task_tail_last_op_id()? {
            let _ = self.task_tail_advance_tx.send(last_op_id);
        }

        Ok(())
    }

    pub fn persist_initial_snapshot_rows_with_plan(
        &self,
        source_table: &str,
        rows: Vec<RowData>,
        end_lsn: PostgresLsn,
        plan: &RustExecutionPlan,
    ) -> Result<(), ReplicationIngestError> {
        self.persist_initial_snapshot_rows(
            source_table,
            rows,
            end_lsn,
            plan,
            SnapshotRowOrder::Any,
            None,
        )
    }

    pub fn persist_initial_snapshot_rows_with_presorted_columns(
        &self,
        source_table: &str,
        rows: Vec<RowData>,
        end_lsn: PostgresLsn,
        plan: &RustExecutionPlan,
    ) -> Result<(), ReplicationIngestError> {
        self.persist_initial_snapshot_rows(
            source_table,
            rows,
            end_lsn,
            plan,
            SnapshotRowOrder::ColumnName,
            None,
        )
    }

    pub fn persist_initial_snapshot_rows_with_presorted_columns_and_types(
        &self,
        source_table: &str,
        rows: Vec<RowData>,
        end_lsn: PostgresLsn,
        plan: &RustExecutionPlan,
        column_types: &JsonColumnTypes,
    ) -> Result<(), ReplicationIngestError> {
        self.persist_initial_snapshot_rows(
            source_table,
            rows,
            end_lsn,
            plan,
            SnapshotRowOrder::ColumnName,
            Some(column_types),
        )
    }

    fn persist_initial_snapshot_rows(
        &self,
        source_table: &str,
        rows: Vec<RowData>,
        end_lsn: PostgresLsn,
        plan: &RustExecutionPlan,
        row_order: SnapshotRowOrder,
        column_types: Option<&JsonColumnTypes>,
    ) -> Result<(), ReplicationIngestError> {
        self.ensure_layout_version_for(plan.storage_contract_id())?;
        let table_plan = plan
            .table_plan(source_table)
            .or_else(|| {
                source_table
                    .rsplit_once('.')
                    .and_then(|(_, table)| plan.table_plan(table))
            })
            .ok_or_else(|| {
                ReplicationIngestError::CorruptBatch(format!(
                    "initial snapshot source table {source_table} is not present in sync plan"
                ))
            })?;
        let lookup_table = plan.lookup_table_plan(source_table);
        let row_count = rows.len();
        let write_started = Instant::now();
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        txn.put(
            &table,
            META_LAST_COMMIT_END_LSN_KEY,
            end_lsn.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist last commit LSN: {error}"))
        })?;

        let mut current_accumulator_deltas = BTreeMap::<String, CurrentAccumulatorDelta>::new();
        let mut current_accumulator_target_cache =
            HashMap::<(String, BTreeMap<String, String>), Vec<CurrentAccumulatorTarget>>::new();
        let object_type = table_plan.object_type();
        for row in rows {
            if let Some(lookup_table) = lookup_table {
                put_parameter_lookup_row(&txn, &table, lookup_table, &row)?;
            }
            let object_id = table_plan
                .object_id_for_row(&row)
                .map_err(sync_rule_error_to_ingest_error)?;
            let route_fields = table_plan
                .route_fields_for_row(&row, true)
                .map_err(sync_rule_error_to_ingest_error)?;
            let data_json = match row_order {
                SnapshotRowOrder::Any => column_types.map_or_else(
                    || table_plan.serialize_full_row_json(&row),
                    |column_types| {
                        table_plan.serialize_full_row_json_with_column_types(&row, column_types)
                    },
                ),
                SnapshotRowOrder::ColumnName => column_types.map_or_else(
                    || table_plan.serialize_full_row_json_presorted(&row),
                    |column_types| {
                        table_plan
                            .serialize_full_row_json_presorted_with_column_types(&row, column_types)
                    },
                ),
            }
            .map_err(sync_rule_error_to_ingest_error)?;
            let route_indexes = plan.required_route_indexes_for_row(object_type, &route_fields);
            put_new_current_document_with_route_indexes(
                CurrentWriteContext {
                    txn: &txn,
                    table: &table,
                    metrics: &self.metrics,
                },
                CurrentDocumentWrite {
                    object_type,
                    object_id: &object_id,
                    route_fields: &route_fields,
                    data_json: &data_json,
                },
                &route_indexes,
            )?;
            let current_accumulator_targets = current_accumulator_target_cache
                .entry((object_type.to_owned(), route_fields.clone()))
                .or_insert_with(|| {
                    current_accumulator_targets_for_row(plan, object_type, &route_fields)
                });
            collect_current_document_accumulator_delta_for_snapshot_targets(
                &mut current_accumulator_deltas,
                SnapshotAccumulatorInput {
                    table_plan,
                    row: &row,
                    column_types,
                    document: CurrentDocumentWrite {
                        object_type,
                        object_id: &object_id,
                        route_fields: &route_fields,
                        data_json: &data_json,
                    },
                },
                current_accumulator_targets,
                CurrentAccumulatorDeltaDirection::Add,
            )?;
        }
        persist_current_accumulator_deltas(&txn, &table, &current_accumulator_deltas)?;
        advance_sync_tail_snapshot_floor(&txn, &table, row_count as u64)?;
        txn.commit().map_err(|error| {
            ReplicationIngestError::Mdbx(format!("commit initial snapshot batch txn: {error}"))
        })?;
        self.metrics
            .rows_seen
            .fetch_add(row_count as u64, Ordering::Relaxed);
        self.metrics.mdbx_write_txn_ms.fetch_add(
            write_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .batches_persisted
            .fetch_add(1, Ordering::Relaxed);

        if let Some(last_op_id) = self.task_tail_last_op_id()? {
            let _ = self.task_tail_advance_tx.send(last_op_id);
        }

        Ok(())
    }

    pub fn persist_initial_snapshot_lookup_rows(
        &self,
        source_table: &str,
        rows: Vec<RowData>,
        end_lsn: PostgresLsn,
        plan: &RustExecutionPlan,
    ) -> Result<(), ReplicationIngestError> {
        self.ensure_layout_version_for(plan.storage_contract_id())?;
        let lookup_table = plan.lookup_table_plan(source_table).ok_or_else(|| {
            ReplicationIngestError::CorruptBatch(format!(
                "initial snapshot lookup source table {source_table} is not present in sync plan"
            ))
        })?;
        let write_started = Instant::now();
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        txn.put(
            &table,
            META_LAST_COMMIT_END_LSN_KEY,
            end_lsn.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist last commit LSN: {error}"))
        })?;
        for row in &rows {
            put_parameter_lookup_row(&txn, &table, lookup_table, row)?;
        }
        txn.commit().map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "commit initial snapshot lookup batch txn: {error}"
            ))
        })?;
        self.metrics
            .rows_seen
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        self.metrics.mdbx_write_txn_ms.fetch_add(
            write_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .batches_persisted
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn persist_initial_snapshot_marker_with_plan(
        &self,
        snapshot_lsn: PostgresLsn,
        plan: &RustExecutionPlan,
        source_identity: &str,
    ) -> Result<(), ReplicationIngestError> {
        self.ensure_layout_version_for(plan.storage_contract_id())?;
        let write_started = Instant::now();
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        txn.put(
            &table,
            META_LAST_COMMIT_END_LSN_KEY,
            snapshot_lsn.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist last commit LSN: {error}"))
        })?;
        // Written atomically with the LSN marker so a restart can detect a
        // completed snapshot and skip re-running it (avoiding double-counted
        // additive deltas).
        txn.put(
            &table,
            META_INITIAL_SNAPSHOT_COMPLETE_KEY,
            snapshot_lsn.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist initial snapshot completion marker: {error}"
            ))
        })?;
        txn.put(
            &table,
            META_INITIAL_SNAPSHOT_SOURCE_IDENTITY_KEY,
            source_identity,
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist initial snapshot source identity: {error}"
            ))
        })?;
        let snapshot_floor = read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
            "sync tail last op id",
        )?
        .unwrap_or(0);
        txn.put(
            &table,
            META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY,
            snapshot_floor.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist snapshot cursor floor: {error}"))
        })?;
        txn.del(&table, META_INITIAL_SNAPSHOT_BOOTSTRAP_INTENT_KEY, None)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!("clear snapshot bootstrap intent: {error}"))
            })?;
        txn.commit().map_err(|error| {
            ReplicationIngestError::Mdbx(format!("commit initial snapshot marker txn: {error}"))
        })?;
        self.metrics.mdbx_write_txn_ms.fetch_add(
            write_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .batches_persisted
            .fetch_add(1, Ordering::Relaxed);

        if let Some(last_op_id) = self.task_tail_last_op_id()? {
            let _ = self.task_tail_advance_tx.send(last_op_id);
        }

        Ok(())
    }

    /// Whether the initial Postgres snapshot has already been fully persisted.
    /// Lets the snapshot be idempotent across restarts: a completed snapshot must
    /// not be re-run, or its additive count/accumulator deltas would be applied
    /// twice and corrupt bucket checksums and counts.
    pub fn is_initial_snapshot_complete(&self) -> Result<bool, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let completed = txn
            .get::<Vec<u8>>(&table, META_INITIAL_SNAPSHOT_COMPLETE_KEY)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!(
                    "read initial snapshot completion marker: {error}"
                ))
            })?;
        let identity = txn
            .get::<Vec<u8>>(&table, META_INITIAL_SNAPSHOT_SOURCE_IDENTITY_KEY)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!(
                    "read initial snapshot source identity: {error}"
                ))
            })?;
        Ok(completed.is_some() && identity.is_some())
    }

    pub fn initial_snapshot_source_identity(
        &self,
    ) -> Result<Option<String>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        txn.get::<Vec<u8>>(&table, META_INITIAL_SNAPSHOT_SOURCE_IDENTITY_KEY)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!(
                    "read initial snapshot source identity: {error}"
                ))
            })?
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "snapshot source identity is not UTF-8: {error}"
                ))
            })
    }

    pub fn initial_snapshot_bootstrap_intent(
        &self,
    ) -> Result<Option<String>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        txn.get::<Vec<u8>>(&table, META_INITIAL_SNAPSHOT_BOOTSTRAP_INTENT_KEY)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!("read snapshot bootstrap intent: {error}"))
            })?
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "snapshot bootstrap intent is not UTF-8: {error}"
                ))
            })
    }

    pub fn reset_incomplete_initial_snapshot(
        &self,
        intent: &str,
        plan: &RustExecutionPlan,
    ) -> Result<(), ReplicationIngestError> {
        let expected_contract = plan.storage_contract_id();
        let persisted_layout_version =
            format!("{INGEST_LAYOUT_FORMAT_VERSION}:{expected_contract}");
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let cursor_generation_base = next_cursor_generation_base(&txn, &table)?;
        txn.clear_table(&table).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("clear incomplete snapshot: {error}"))
        })?;
        txn.put(
            &table,
            META_LAYOUT_VERSION_KEY,
            persisted_layout_version,
            WriteFlags::UPSERT,
        )
        .map_err(|error| ReplicationIngestError::Mdbx(format!("write layout version: {error}")))?;
        txn.put(
            &table,
            META_INITIAL_SNAPSHOT_BOOTSTRAP_INTENT_KEY,
            intent,
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist snapshot bootstrap intent: {error}"))
        })?;
        seed_cursor_generation(&txn, &table, cursor_generation_base)?;
        txn.commit().map_err(|error| {
            ReplicationIngestError::Mdbx(format!("commit incomplete snapshot reset: {error}"))
        })?;
        *self
            .verified_layout_version
            .write()
            .expect("verified layout version lock should not be poisoned") =
            Some(expected_contract.to_owned());
        let _ = self.task_tail_advance_tx.send(cursor_generation_base);
        Ok(())
    }

    pub fn last_persisted_end_lsn(&self) -> Result<Option<PostgresLsn>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        read_last_persisted_end_lsn(&txn, &table)
    }

    pub fn load_batch(
        &self,
        end_lsn: PostgresLsn,
    ) -> Result<Option<ReplicationCommitBatch>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let encoded = txn
            .get::<Vec<u8>>(&table, &batch_key(end_lsn))
            .map_err(|error| ReplicationIngestError::Mdbx(format!("read batch: {error}")))?;

        encoded
            .as_deref()
            .map(ReplicationCommitBatch::decode)
            .transpose()
    }

    pub fn read_parameter_lookup_rows(
        &self,
        lookup_id: &str,
        key_values: &[String],
        max_entries: usize,
    ) -> Result<Vec<BTreeMap<String, String>>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        scan_parameter_lookup_entries(&txn, &table, lookup_id, key_values, max_entries)
    }

    #[cfg(test)]
    pub(crate) fn write_sync_state_snapshot_for_test(
        &self,
        bytes: &[u8],
    ) -> Result<(), ReplicationIngestError> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        txn.put(&table, META_SYNC_STATE_JSON_KEY, bytes, WriteFlags::UPSERT)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!(
                    "persist sync state snapshot for test: {error}"
                ))
            })?;
        txn.commit().map_err(|error| {
            ReplicationIngestError::Mdbx(format!("commit sync state snapshot for test: {error}"))
        })?;
        Ok(())
    }

    pub fn task_tail_last_op_id(&self) -> Result<Option<u64>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
            "sync tail last op id",
        )
    }

    pub async fn wait_for_task_tail_advance(
        &self,
        after: u64,
        timeout: Duration,
    ) -> Result<Option<u64>, ReplicationIngestError> {
        if let Some(latest) = self.task_tail_last_op_id()? {
            if latest > after {
                return Ok(Some(latest));
            }
        }

        let mut receiver = self.task_tail_advance_tx.subscribe();
        if *receiver.borrow() > after {
            return Ok(Some(*receiver.borrow()));
        }

        match tokio::time::timeout(timeout, async move {
            loop {
                if receiver.changed().await.is_err() {
                    return None;
                }
                let latest = *receiver.borrow_and_update();
                if latest > after {
                    return Some(latest);
                }
            }
        })
        .await
        {
            Ok(latest) => Ok(latest),
            Err(_) => Ok(None),
        }
    }

    pub fn load_task_tail_ops_since(
        &self,
        after: u64,
    ) -> Result<Vec<PersistedSyncTailOp>, ReplicationIngestError> {
        let Some(last_op_id) = self.task_tail_last_op_id()? else {
            return Ok(Vec::new());
        };

        if after >= last_op_id {
            return Ok(Vec::new());
        }

        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let mut ops = Vec::with_capacity(last_op_id.saturating_sub(after) as usize);

        for op_id in (after + 1)..=last_op_id {
            let Some(bytes) = txn
                .get::<Vec<u8>>(&table, &sync_tail_op_key(op_id))
                .map_err(|error| {
                    ReplicationIngestError::Mdbx(format!("read sync tail op {op_id}: {error}"))
                })?
            else {
                continue;
            };

            let op = serde_json::from_slice(&bytes).map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "sync tail op {op_id} is not valid JSON: {error}"
                ))
            })?;
            ops.push(op);
        }

        Ok(ops)
    }

    pub fn indexed_task_tail_last_op_id(
        &self,
        index_keys: &[String],
    ) -> Result<u64, ReplicationIngestError> {
        if index_keys.is_empty() {
            return Ok(0);
        }
        self.ensure_sync_tail_indexes_current()?;

        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;

        read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
            "sync tail last op id",
        )
        .map(|value| value.unwrap_or(0))
    }

    pub fn load_indexed_task_tail_ops_since(
        &self,
        index_keys: &[String],
        after: u64,
    ) -> Result<IndexedSyncTailOps, ReplicationIngestError> {
        let started = Instant::now();
        if index_keys.is_empty() {
            return Ok(IndexedSyncTailOps {
                latest_op_id: 0,
                ops: Vec::new(),
            });
        }
        self.ensure_sync_tail_indexes_current()?;

        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let latest_op_id = read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
            "sync tail last op id",
        )?
        .unwrap_or(0);
        if after >= latest_op_id {
            return Ok(IndexedSyncTailOps {
                latest_op_id,
                ops: Vec::new(),
            });
        }

        let mut global_op_ids = Vec::new();

        for index_key in index_keys {
            scan_sync_tail_index_entries(
                &txn,
                &table,
                index_key,
                after + 1,
                latest_op_id,
                &mut global_op_ids,
            )?;
        }

        global_op_ids.sort_unstable();
        global_op_ids.dedup();
        let ops = global_op_ids
            .into_iter()
            .map(|global_op_id| read_sync_tail_op(&txn, &table, global_op_id))
            .collect::<Result<Vec<_>, _>>()?;

        self.metrics
            .tail_scan_ms
            .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(IndexedSyncTailOps { latest_op_id, ops })
    }

    pub fn read_bucket_snapshot(
        &self,
        bucket: &ResolvedSyncBucket,
        index_keys: &[String],
        current_accumulator_keys: &[String],
        tail_accumulator_keys: &[String],
        after: u64,
    ) -> Result<BucketReadSnapshot, ReplicationIngestError> {
        let request = BucketReadRequest {
            bucket: bucket.clone(),
            index_keys: index_keys.to_vec(),
            current_accumulator_keys: current_accumulator_keys.to_vec(),
            tail_accumulator_keys: tail_accumulator_keys.to_vec(),
            after,
        };
        let mut snapshots = self.read_bucket_snapshots(std::slice::from_ref(&request))?;
        Ok(snapshots.remove(0))
    }

    pub fn read_bucket_snapshots(
        &self,
        requests: &[BucketReadRequest],
    ) -> Result<Vec<BucketReadSnapshot>, ReplicationIngestError> {
        let max_entries = positive_u64_env(
            "POWERSYNC_RUST_MAX_SYNC_READ_ENTRIES",
            DEFAULT_SYNC_READ_MAX_ENTRIES,
        );
        self.read_bucket_snapshots_with_limits(
            requests,
            max_entries,
            max_entries,
            positive_u64_env(
                "POWERSYNC_RUST_MAX_SYNC_READ_BYTES",
                DEFAULT_SYNC_READ_MAX_BYTES,
            ),
        )
    }

    pub(super) fn read_bucket_snapshots_with_limits(
        &self,
        requests: &[BucketReadRequest],
        max_entries: u64,
        max_index_scan_entries: u64,
        max_bytes: u64,
    ) -> Result<Vec<BucketReadSnapshot>, ReplicationIngestError> {
        self.ensure_sync_tail_indexes_current()?;
        let started = Instant::now();
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let mut budget = SyncReadBudget {
            remaining_entries: max_entries,
            remaining_index_scan_entries: max_index_scan_entries,
            remaining_bytes: max_bytes,
        };
        let snapshots = requests
            .iter()
            .map(|request| read_bucket_snapshot_in_transaction(&txn, &table, request, &mut budget))
            .collect::<Result<Vec<_>, _>>()?;
        self.metrics
            .tail_scan_ms
            .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(snapshots)
    }

    pub fn checkpoint_accumulator_for_bucket(
        &self,
        accumulator_keys: &[String],
    ) -> Result<PersistedCheckpointAccumulator, ReplicationIngestError> {
        if accumulator_keys.is_empty() {
            return Ok(PersistedCheckpointAccumulator::default());
        }
        self.ensure_sync_tail_indexes_current()?;

        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let mut accumulator = PersistedCheckpointAccumulator::default();
        for key in accumulator_keys {
            accumulator.add(read_checkpoint_accumulator(&txn, &table, key)?)?;
        }
        Ok(accumulator)
    }

    pub fn current_checkpoint_accumulator_for_bucket(
        &self,
        accumulator_keys: &[String],
    ) -> Result<PersistedCheckpointAccumulator, ReplicationIngestError> {
        if accumulator_keys.is_empty() {
            return Ok(PersistedCheckpointAccumulator::default());
        }

        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let mut accumulator = PersistedCheckpointAccumulator::default();
        for key in accumulator_keys {
            accumulator.add(read_current_checkpoint_accumulator(&txn, &table, key)?)?;
        }
        Ok(accumulator)
    }

    pub fn ensure_sync_tail_indexes_current(&self) -> Result<(), ReplicationIngestError> {
        // Fast path under a read-only txn: MDBX allows a single writer, so
        // taking the RW txn here would serialize every serving-path read
        // against the replication ingest loop. Backfill is only needed for
        // legacy stores written before inline indexing.
        {
            let txn = self
                .db
                .begin_ro_txn()
                .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
            let table = txn.open_table(None).map_err(|error| {
                ReplicationIngestError::Mdbx(format!("open default table: {error}"))
            })?;
            let last_op_id = read_optional_u64(
                &txn,
                &table,
                META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
                "sync tail last op id",
            )?
            .unwrap_or(0);
            let indexed_through = read_optional_u64(
                &txn,
                &table,
                META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY.to_vec(),
                "sync tail indexed-through op id",
            )?
            .unwrap_or(0);
            if indexed_through >= last_op_id {
                return Ok(());
            }
        }

        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let last_op_id = read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
            "sync tail last op id",
        )?
        .unwrap_or(0);
        let indexed_through = read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY.to_vec(),
            "sync tail indexed-through op id",
        )?
        .unwrap_or(0);

        // Re-checked under the writer lock: another writer may have finished
        // the backfill between the two transactions.
        if indexed_through >= last_op_id {
            return Ok(());
        }

        let mut checkpoint_accumulator_deltas = BTreeMap::new();
        for op_id in (indexed_through + 1)..=last_op_id {
            let op = read_sync_tail_op(&txn, &table, op_id)?;
            persist_sync_tail_index_entries_if_missing(
                &txn,
                &table,
                &op,
                execution_plan(),
                &self.metrics,
            )?;
            collect_checkpoint_accumulator_deltas_for_op(
                &mut checkpoint_accumulator_deltas,
                &op,
                execution_plan(),
            )?;
        }
        persist_checkpoint_accumulator_deltas(&txn, &table, &checkpoint_accumulator_deltas)?;
        txn.put(
            &table,
            META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY,
            last_op_id.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist sync tail indexed-through op id: {error}"
            ))
        })?;
        txn.commit().map_err(|error| {
            ReplicationIngestError::Mdbx(format!("commit sync tail index backfill: {error}"))
        })?;
        Ok(())
    }

    #[cfg(test)]
    pub fn load_current_task_rows(&self) -> Result<Vec<TaskRow>, ReplicationIngestError> {
        self.load_current_documents().and_then(|documents| {
            documents
                .into_iter()
                .filter(|document| document.object_type == "tasks")
                .map(|document| {
                    task_row_from_document_json(&document.object_id, &document.data_json)
                })
                .collect()
        })
    }

    #[cfg(test)]
    pub fn load_current_documents(
        &self,
    ) -> Result<Vec<PersistedBucketedDocument>, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        scan_current_documents_for_prefix(&txn, &table, CURRENT_DOC_KEY_PREFIX.as_bytes())
    }

    pub fn load_current_documents_for_bucket(
        &self,
        bucket: &ResolvedSyncBucket,
    ) -> Result<Vec<PersistedBucketedDocument>, ReplicationIngestError> {
        let started = Instant::now();
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let mut documents_by_key = BTreeMap::new();

        for query in bucket.queries() {
            if query.route_constraints().is_empty() {
                for document in scan_current_documents_for_prefix(
                    &txn,
                    &table,
                    &current_doc_prefix(query.object_type()),
                )? {
                    documents_by_key
                        .entry((document.object_type.clone(), document.object_id.clone()))
                        .or_insert(document);
                }
            } else {
                let index_prefix =
                    current_route_index_prefix(query.object_type(), query.route_constraints());
                for document in
                    scan_current_route_documents(&txn, &table, &index_prefix, query.object_type())?
                {
                    documents_by_key
                        .entry((document.object_type.clone(), document.object_id.clone()))
                        .or_insert(document);
                }
            }
        }

        self.metrics
            .cold_snapshot_scan_ms
            .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(documents_by_key.into_values().collect())
    }

    pub fn current_document_count_for_bucket(
        &self,
        bucket: &ResolvedSyncBucket,
    ) -> Result<u64, ReplicationIngestError> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RO txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let mut document_keys = BTreeSet::new();

        for query in bucket.queries() {
            if query.route_constraints().is_empty() {
                scan_current_document_keys_for_prefix(
                    &txn,
                    &table,
                    &current_doc_prefix(query.object_type()),
                    &mut document_keys,
                )?;
            } else {
                let index_prefix =
                    current_route_index_prefix(query.object_type(), query.route_constraints());
                scan_current_route_index_keys(
                    &txn,
                    &table,
                    &index_prefix,
                    query.object_type(),
                    &mut document_keys,
                )?;
            }
        }

        Ok(document_keys.len() as u64)
    }

    fn ensure_layout_version(&self) -> Result<(), ReplicationIngestError> {
        let expected_layout_version = storage_contract_id();
        self.ensure_layout_version_for(&expected_layout_version)
    }

    fn ensure_layout_version_for(
        &self,
        expected_layout_version: &str,
    ) -> Result<(), ReplicationIngestError> {
        // Called once per persisted batch: skip the RW txn entirely once this
        // store instance has verified the expected version.
        if self
            .verified_layout_version
            .read()
            .expect("verified layout version lock should not be poisoned")
            .as_deref()
            == Some(expected_layout_version)
        {
            return Ok(());
        }
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("begin RW txn: {error}")))?;
        let table = txn.open_table(None).map_err(|error| {
            ReplicationIngestError::Mdbx(format!("open default table: {error}"))
        })?;
        let current = txn
            .get::<Vec<u8>>(&table, META_LAYOUT_VERSION_KEY)
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!("read layout version: {error}"))
            })?;
        let current = current
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "layout version metadata is not UTF-8: {error}"
                ))
            })?;
        let persisted_layout_version =
            format!("{INGEST_LAYOUT_FORMAT_VERSION}:{expected_layout_version}");

        if current.as_deref() != Some(persisted_layout_version.as_str()) {
            let cursor_generation_base = current
                .is_some()
                .then(|| next_cursor_generation_base(&txn, &table))
                .transpose()?;
            txn.clear_table(&table).map_err(|error| {
                ReplicationIngestError::Mdbx(format!("clear layout table: {error}"))
            })?;
            txn.put(
                &table,
                META_LAYOUT_VERSION_KEY,
                &persisted_layout_version,
                WriteFlags::UPSERT,
            )
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!("write layout version: {error}"))
            })?;
            if let Some(cursor_generation_base) = cursor_generation_base {
                seed_cursor_generation(&txn, &table, cursor_generation_base)?;
            }
        }

        txn.commit()
            .map_err(|error| ReplicationIngestError::Mdbx(format!("commit layout txn: {error}")))?;
        *self
            .verified_layout_version
            .write()
            .expect("verified layout version lock should not be poisoned") =
            Some(expected_layout_version.to_owned());
        Ok(())
    }

    pub fn reset_for_layout_version(
        &self,
        expected_layout_version: &str,
    ) -> Result<(), ReplicationIngestError> {
        self.ensure_layout_version_for(expected_layout_version)?;
        let _ = self.task_tail_advance_tx.send(0);
        Ok(())
    }
}

fn next_cursor_generation_base<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
) -> Result<u64, ReplicationIngestError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("system clock is before the Unix epoch: {error}"))
        })?;
    let time_base = u64::try_from(elapsed.as_millis())
        .ok()
        .and_then(|millis| millis.checked_mul(CURSOR_IDS_PER_MILLISECOND))
        .filter(|base| *base <= i64::MAX as u64)
        .ok_or_else(|| {
            ReplicationIngestError::Mdbx(
                "system time cannot be represented in the signed 64-bit sync cursor space"
                    .to_owned(),
            )
        })?;
    let previous = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
        "previous sync tail last op id",
    )?
    .unwrap_or(0);
    let after_previous = previous
        .checked_add(1)
        .ok_or_else(|| ReplicationIngestError::Mdbx("sync cursor space is exhausted".to_owned()))?;
    Ok(time_base.max(after_previous))
}

fn seed_cursor_generation(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    cursor_generation_base: u64,
) -> Result<(), ReplicationIngestError> {
    for (key, label) in [
        (META_SYNC_TAIL_LAST_OP_ID_KEY, "sync tail last op id"),
        (
            META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY,
            "sync tail indexed-through op id",
        ),
        (
            META_SYNC_TAIL_RETAINED_FLOOR_KEY,
            "sync tail retained floor",
        ),
    ] {
        txn.put(
            table,
            key,
            cursor_generation_base.to_string(),
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("persist {label} generation base: {error}"))
        })?;
    }
    Ok(())
}

#[cfg(test)]
fn task_row_from_document_json(
    object_id: &str,
    data_json: &str,
) -> Result<TaskRow, ReplicationIngestError> {
    let value: serde_json::Value = serde_json::from_str(data_json).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!(
            "sync state snapshot document {object_id} is not valid JSON: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        ReplicationIngestError::CorruptBatch(format!(
            "sync state snapshot document {object_id} is not a JSON object"
        ))
    })?;

    Ok(TaskRow {
        id: required_json_string(object, "id", object_id)?,
        org_id: required_json_string(object, "org_id", object_id)?,
        project_id: required_json_string(object, "project_id", object_id)?,
        title: required_json_string(object, "title", object_id)?,
        status: required_json_string(object, "status", object_id)?,
        priority: required_json_u32(object, "priority", object_id)?,
        assignee_id: required_json_string(object, "assignee_id", object_id)?,
        story_points: required_json_u32(object, "story_points", object_id)?,
        updated_at: required_json_string(object, "updated_at", object_id)?,
        summary: required_json_string(object, "summary", object_id)?,
    })
}

#[cfg(test)]
fn required_json_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    object_id: &str,
) -> Result<String, ReplicationIngestError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            ReplicationIngestError::CorruptBatch(format!(
                "sync state snapshot document {object_id} is missing string field {field}"
            ))
        })
}

#[cfg(test)]
fn required_json_u32(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    object_id: &str,
) -> Result<u32, ReplicationIngestError> {
    let value = object.get(field).ok_or_else(|| {
        ReplicationIngestError::CorruptBatch(format!(
            "sync state snapshot document {object_id} is missing numeric field {field}"
        ))
    })?;

    if let Some(number) = value.as_u64() {
        return u32::try_from(number).map_err(|error| {
            ReplicationIngestError::CorruptBatch(format!(
                "sync state snapshot document {object_id} field {field} is out of range: {error}"
            ))
        });
    }

    if let Some(text) = value.as_str() {
        return text.parse::<u32>().map_err(|error| {
            ReplicationIngestError::CorruptBatch(format!(
                "sync state snapshot document {object_id} field {field} is not a valid u32: {error}"
            ))
        });
    }

    Err(ReplicationIngestError::CorruptBatch(format!(
        "sync state snapshot document {object_id} field {field} is neither a string nor a number"
    )))
}

fn resolve_ingest_path_from_env() -> PathBuf {
    env::var(INGEST_PATH_ENV)
        .map(PathBuf::from)
        .or_else(|_| env::var(TAIL_PATH_ENV).map(|value| PathBuf::from(value).join("ingest")))
        .or_else(|_| env::var(MDBX_PATH_ENV).map(|value| PathBuf::from(value).join("ingest")))
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_INGEST_PATH))
}

fn read_bucket_snapshot_in_transaction<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    request: &BucketReadRequest,
    budget: &mut SyncReadBudget,
) -> Result<BucketReadSnapshot, ReplicationIngestError> {
    let latest_op_id = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
        "sync tail last op id",
    )?
    .unwrap_or(0);
    let snapshot_floor = read_optional_u64(
        txn,
        table,
        META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY.to_vec(),
        "initial snapshot cursor floor",
    )?
    .unwrap_or(0);
    let retained_floor = read_optional_u64(
        txn,
        table,
        META_SYNC_TAIL_RETAINED_FLOOR_KEY.to_vec(),
        "sync tail retained floor",
    )?
    .unwrap_or(0);
    let mut reset_required = request.after > latest_op_id
        || (request.after > 0
            && (request.after < snapshot_floor || request.after < retained_floor));
    let mut effective_after = if reset_required { 0 } else { request.after };

    let mut current_accumulator = PersistedCheckpointAccumulator::default();
    for key in &request.current_accumulator_keys {
        current_accumulator.add(read_current_checkpoint_accumulator(txn, table, key)?)?;
    }
    let mut tail_accumulator = PersistedCheckpointAccumulator::default();
    for key in &request.tail_accumulator_keys {
        tail_accumulator.add(read_checkpoint_accumulator(txn, table, key)?)?;
    }

    let mut global_op_ids = BTreeSet::new();
    if effective_after > 0 && effective_after < latest_op_id {
        for index_key in &request.index_keys {
            let complete = scan_sync_tail_index_entries_bounded(
                txn,
                table,
                index_key,
                effective_after + 1,
                latest_op_id,
                &mut global_op_ids,
                &mut budget.remaining_index_scan_entries,
            )?;
            if !complete {
                // The indexed delta is too dense for this request's remaining
                // scan-work budget. A clearing snapshot is bounded by current
                // bucket cardinality and avoids partially serving the delta.
                reset_required = true;
                effective_after = 0;
                global_op_ids.clear();
                break;
            }
        }
    }
    let entry_upper_bound = if effective_after == 0 {
        current_accumulator.count.saturating_add(1)
    } else {
        global_op_ids.len() as u64
    };
    if entry_upper_bound > budget.remaining_entries {
        return Err(ReplicationIngestError::ResourceLimit(format!(
            "bucket {} needs at most {entry_upper_bound} entries but only {} remain in the per-request budget",
            request.bucket.bucket_name(),
            budget.remaining_entries
        )));
    }
    budget.remaining_entries -= entry_upper_bound;

    let mut tail_ops = Vec::with_capacity(global_op_ids.len());
    for op_id in global_op_ids {
        tail_ops.push(read_sync_tail_op_bounded(
            txn,
            table,
            op_id,
            &mut budget.remaining_bytes,
        )?);
    }

    let mut documents_by_key = BTreeMap::new();
    let mut seen_document_keys = BTreeSet::new();
    if effective_after == 0 {
        for query in request.bucket.queries() {
            if query.route_constraints().is_empty() {
                collect_current_documents_for_prefix_bounded(
                    txn,
                    table,
                    &current_doc_prefix(query.object_type()),
                    &mut seen_document_keys,
                    &mut documents_by_key,
                    &mut budget.remaining_bytes,
                )?;
            } else {
                let index_prefix =
                    current_route_index_prefix(query.object_type(), query.route_constraints());
                collect_current_route_documents_bounded(
                    txn,
                    table,
                    &index_prefix,
                    query.object_type(),
                    &mut seen_document_keys,
                    &mut documents_by_key,
                    &mut budget.remaining_bytes,
                )?;
            }
        }
    }

    let current_documents = documents_by_key.into_values().collect::<Vec<_>>();

    Ok(BucketReadSnapshot {
        latest_op_id,
        tail_ops,
        current_accumulator,
        tail_accumulator,
        current_documents,
        reset_required,
    })
}

struct SyncReadBudget {
    remaining_entries: u64,
    remaining_index_scan_entries: u64,
    remaining_bytes: u64,
}

fn read_last_persisted_end_lsn<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
) -> Result<Option<PostgresLsn>, ReplicationIngestError> {
    txn.get::<Vec<u8>>(table, META_LAST_COMMIT_END_LSN_KEY)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("read last commit LSN: {error}")))?
        .map(|bytes| {
            let value = String::from_utf8(bytes).map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "last commit LSN metadata is not UTF-8: {error}"
                ))
            })?;
            value
                .parse::<PostgresLsn>()
                .map_err(|error| ReplicationIngestError::InvalidPersistedLsn(error.to_string()))
        })
        .transpose()
}

fn shared_store_registry(
) -> &'static Mutex<std::collections::HashMap<PathBuf, Weak<ReplicationMdbxStore>>> {
    static REGISTRY: OnceLock<
        Mutex<std::collections::HashMap<PathBuf, Weak<ReplicationMdbxStore>>>,
    > = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn mdbx_max_size_from_env() -> Option<isize> {
    env::var(MDBX_MAX_SIZE_ENV)
        .ok()
        .and_then(|value| value.parse::<isize>().ok())
        .filter(|value| *value > 0)
}

fn positive_u64_env(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
