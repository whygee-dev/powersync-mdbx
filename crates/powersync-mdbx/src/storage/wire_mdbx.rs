use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use tracing::info;

use crate::{
    protocol::messages::{
        bucket_sync_stream_bson_chunk_iter, bucket_sync_stream_ndjson_chunk_iter,
        protocol_checksum_i32, put_checksum, remove_checksum, source_subkey_for_object,
        BucketSyncView, OplogEntry, OplogOperation, SyncChunk,
    },
    replication::ingest::{
        sync_current_checkpoint_accumulator_keys_for_bucket,
        sync_tail_checkpoint_accumulator_keys_for_bucket, sync_tail_index_keys_for_bucket,
        BucketReadRequest, BucketReadSnapshot, PersistedBucketedDocument,
        PersistedCheckpointAccumulator, PersistedSyncTailOp, PersistedSyncTailOperation,
        ReplicationMdbxStore,
    },
    sync_rules::{execution_plan, resolve_bucket_request, ResolvedSyncBucket, RustExecutionPlan},
};

use super::{Storage, StorageError, StreamEncoding, SyncBucketCursors, SyncChunkSource};

#[derive(Debug)]
pub struct WireMdbxStorage {
    ingest_store: Arc<ReplicationMdbxStore>,
}

impl WireMdbxStorage {
    pub fn new_from_env() -> Self {
        let ingest_store = ReplicationMdbxStore::shared_from_env()
            .expect("failed to open replication ingest MDBX store for wire-mdbx storage");
        Self::from_ingest_store(ingest_store)
    }

    pub fn new(snapshot_path: impl AsRef<Path>, tail_path: impl AsRef<Path>) -> Self {
        let tail_path = tail_path.as_ref().to_path_buf();
        let ingest_path = tail_path.join("replication-ingest");
        Self::new_with_ingest(snapshot_path, tail_path, ingest_path)
    }

    pub fn new_with_ingest(
        _snapshot_path: impl AsRef<Path>,
        _tail_path: impl AsRef<Path>,
        ingest_path: impl AsRef<Path>,
    ) -> Self {
        let ingest_store = ReplicationMdbxStore::shared(ingest_path)
            .expect("failed to open replication ingest MDBX store for wire-mdbx storage");
        Self::from_ingest_store(ingest_store)
    }

    fn from_ingest_store(ingest_store: Arc<ReplicationMdbxStore>) -> Self {
        let storage_contract_fingerprint = execution_plan().storage_contract_fingerprint();
        info!(
            backend = "wire-mdbx",
            ingest_path = %ingest_store.path().display(),
            storage_contract_fingerprint,
            read_path = "ingest-derived-compiled-tail",
            "initialized compiler-driven wire-mdbx storage"
        );

        Self { ingest_store }
    }
}

impl Default for WireMdbxStorage {
    fn default() -> Self {
        Self::new_from_env()
    }
}

impl Storage for WireMdbxStorage {
    fn is_ready(&self) -> Result<bool, StorageError> {
        self.ingest_store
            .is_initial_snapshot_complete()
            .map_err(|error| StorageError(format!("failed to read snapshot readiness: {error}")))
    }

    fn sync_chunk_source_for_buckets_with_plan(
        &self,
        buckets: &SyncBucketCursors,
        plan: &RustExecutionPlan,
        encoding: StreamEncoding,
    ) -> Result<SyncChunkSource, StorageError> {
        self.dynamic_task_tail_source_for_buckets(buckets, plan, encoding)
    }

    fn read_parameter_lookup_rows(
        &self,
        lookup_id: &str,
        key_values: &[String],
        max_entries: usize,
    ) -> Result<Vec<BTreeMap<String, String>>, StorageError> {
        self.ingest_store
            .read_parameter_lookup_rows(lookup_id, key_values, max_entries)
            .map_err(|error| StorageError(format!("failed to read parameter lookup rows: {error}")))
    }

    fn latest_sync_bucket_cursors_with_plan(
        &self,
        buckets: &SyncBucketCursors,
        plan: &RustExecutionPlan,
    ) -> Result<Option<SyncBucketCursors>, StorageError> {
        Ok(Some(
            self.latest_sync_bucket_cursors_for_plan(buckets, plan)?,
        ))
    }

    fn wait_for_new_sync_bucket_cursors_with_plan<'a>(
        &'a self,
        buckets: &'a SyncBucketCursors,
        plan: &'a RustExecutionPlan,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Option<SyncBucketCursors>, StorageError>> + Send + 'a>>
    {
        Box::pin(async move {
            let current_latest = self.latest_sync_bucket_cursors_for_plan(buckets, plan)?;
            if current_latest != *buckets {
                return Ok(Some(current_latest));
            }

            let baseline_global = self.latest_sync_op_id()?;
            let _ = self
                .ingest_store
                .wait_for_task_tail_advance(baseline_global, timeout)
                .await;
            let latest = self.latest_sync_bucket_cursors_for_plan(buckets, plan)?;
            Ok((latest != *buckets).then_some(latest))
        })
    }

    fn diagnostics_json(&self) -> Option<serde_json::Value> {
        let last_persisted_end_lsn = self
            .ingest_store
            .last_persisted_end_lsn()
            .ok()
            .flatten()
            .map(|lsn| lsn.to_string());
        Some(serde_json::json!({
            "backend": "wire-mdbx",
            "ingest_path": self.ingest_store.path().display().to_string(),
            "last_persisted_end_lsn": last_persisted_end_lsn,
            "metrics": self.ingest_store.metrics_snapshot()
        }))
    }
}

impl WireMdbxStorage {
    fn latest_sync_op_id(&self) -> Result<u64, StorageError> {
        Ok(self
            .ingest_store
            .task_tail_last_op_id()
            .map_err(|error| {
                StorageError(format!("failed to read dynamic task tail cursor: {error}"))
            })?
            .unwrap_or(0))
    }

    fn latest_sync_bucket_cursors_for_plan(
        &self,
        buckets: &SyncBucketCursors,
        plan: &RustExecutionPlan,
    ) -> Result<SyncBucketCursors, StorageError> {
        let resolved_buckets = resolved_sync_buckets(buckets, plan);
        let mut pairs = Vec::with_capacity(resolved_buckets.len());
        for bucket in &resolved_buckets {
            let indexed_latest = self
                .ingest_store
                .indexed_task_tail_last_op_id(&bucket_tail_index_keys(&bucket.resolved))
                .map_err(|error| {
                    StorageError(format!("failed to read indexed sync tail cursor: {error}"))
                })?;
            let current_latest = self
                .ingest_store
                .current_checkpoint_accumulator_for_bucket(
                    &sync_current_checkpoint_accumulator_keys_for_bucket(&bucket.resolved),
                )
                .map_err(|error| {
                    StorageError(format!(
                        "failed to read current checkpoint accumulator for bucket: {error}"
                    ))
                })?
                .count;
            let latest = indexed_latest.max(current_latest);
            pairs.push((
                bucket.resolved.bucket_name().to_owned(),
                bucket.after.max(latest),
            ));
        }
        Ok(SyncBucketCursors::from_pairs(pairs))
    }

    fn dynamic_task_tail_source_for_buckets(
        &self,
        buckets: &SyncBucketCursors,
        plan: &RustExecutionPlan,
        encoding: StreamEncoding,
    ) -> Result<SyncChunkSource, StorageError> {
        let resolved_buckets = resolved_sync_buckets(buckets, plan);
        let min_after = resolved_buckets
            .iter()
            .map(|bucket| bucket.after)
            .min()
            .unwrap_or(0);
        let read_requests = resolved_buckets
            .iter()
            .map(|bucket| BucketReadRequest {
                bucket: bucket.resolved.clone(),
                index_keys: bucket_tail_index_keys(&bucket.resolved),
                current_accumulator_keys: sync_current_checkpoint_accumulator_keys_for_bucket(
                    &bucket.resolved,
                ),
                tail_accumulator_keys: sync_tail_checkpoint_accumulator_keys_for_bucket(
                    &bucket.resolved,
                ),
                after: bucket.after,
            })
            .collect::<Vec<_>>();
        let read_snapshots = self
            .ingest_store
            .read_bucket_snapshots(&read_requests)
            .map_err(|error| StorageError(format!("failed to read bucket snapshots: {error}")))?;

        let mut bucket_views = Vec::with_capacity(resolved_buckets.len());
        let mut final_cursor_pairs = Vec::with_capacity(resolved_buckets.len());
        for (bucket, snapshot) in resolved_buckets.iter().zip(read_snapshots) {
            let payload = build_bucket_payload(bucket, snapshot)?;
            final_cursor_pairs.push((
                bucket.resolved.bucket_name().to_owned(),
                payload.global_last_op_id,
            ));

            bucket_views.push(BucketSyncView {
                bucket_name: bucket.resolved.bucket_name().to_owned(),
                stream_name: bucket.resolved.stream_name().to_owned(),
                is_default: bucket.resolved.is_default(),
                current_entries: payload.current_entries,
                tail_entries: payload.tail_entries,
                last_op_id: payload.global_last_op_id,
                snapshot_floor_op_id: payload.snapshot_floor_op_id,
                snapshot_clear_checksum: Some(payload.snapshot_clear_checksum),
                force_snapshot_clear: payload.force_snapshot_clear,
                checkpoint_checksum: Some(payload.checkpoint_checksum),
                checkpoint_count: Some(payload.checkpoint_count),
                after: Some(payload.bounded_after),
            });
        }
        let stream_last_op_id = bucket_views
            .iter()
            .map(|bucket| bucket.last_op_id)
            .max()
            .unwrap_or(0);

        let chunks: Box<dyn Iterator<Item = SyncChunk> + Send> = match encoding {
            StreamEncoding::Ndjson => Box::new(bucket_sync_stream_ndjson_chunk_iter(
                bucket_views,
                stream_last_op_id,
            )),
            StreamEncoding::Bson => Box::new(bucket_sync_stream_bson_chunk_iter(
                bucket_views,
                stream_last_op_id,
            )),
        };
        let store = Arc::clone(&self.ingest_store);
        let mut chunks = chunks;
        let metered_chunks = std::iter::from_fn(move || {
            let protocol_started = Instant::now();
            let chunk = chunks.next()?;
            store.record_protocol_encode(
                protocol_started.elapsed().as_millis() as u64,
                chunk.bytes.len() as u64,
            );
            Some(chunk)
        });

        info!(
            backend = "wire-mdbx",
            ingest_path = %self.ingest_store.path().display(),
            storage_contract_fingerprint = %plan.storage_contract_fingerprint(),
            encoding = ?encoding,
            requested_buckets = resolved_buckets.len(),
            requested_after = min_after,
            dynamic_tail_last_op_id = stream_last_op_id,
            stream_last_op_id,
            read_path = "ingest-derived-compiled-tail",
            "served sync chunks from compiler-driven MDBX state"
        );

        Ok(SyncChunkSource {
            chunk_count_hint: None,
            chunks: Box::new(metered_chunks),
            final_cursors: Some(SyncBucketCursors::from_pairs(final_cursor_pairs)),
        })
    }
}

#[derive(Debug, Clone)]
struct BucketPayload {
    current_entries: Vec<OplogEntry>,
    tail_entries: Vec<OplogEntry>,
    checkpoint_count: u64,
    checkpoint_checksum: i32,
    snapshot_clear_checksum: u32,
    force_snapshot_clear: bool,
    bounded_after: u64,
    global_last_op_id: u64,
    snapshot_floor_op_id: u64,
}

fn build_bucket_payload(
    bucket: &ResolvedBucketCursor,
    snapshot: BucketReadSnapshot,
) -> Result<BucketPayload, StorageError> {
    let indexed_latest = snapshot.latest_op_id;
    let effective_after = if snapshot.reset_required {
        0
    } else {
        bucket.after
    };
    let checkpoint_entries = if effective_after == 0 {
        current_entries_for_bucket(&snapshot.current_documents, &bucket.resolved)?
    } else {
        Vec::new()
    };
    let current_checkpoint = if effective_after == 0 {
        checkpoint_accumulator_for_entries(&checkpoint_entries)
    } else {
        snapshot.current_accumulator
    };
    let superseded_remainder = snapshot.tail_accumulator;
    let checkpoint_sum = current_checkpoint
        .checksum
        .wrapping_add(superseded_remainder.checksum);
    let checkpoint_count = current_checkpoint.count + superseded_remainder.count;
    let global_last_op_id = indexed_latest.max(current_checkpoint.count);
    let bounded_after = effective_after.min(global_last_op_id);
    let snapshot_floor_op_id = if effective_after == 0 {
        global_last_op_id
    } else {
        0
    };
    let current_entries = if bounded_after < snapshot_floor_op_id {
        checkpoint_entries
    } else {
        Vec::new()
    };
    let tail_entries = snapshot
        .tail_ops
        .iter()
        .filter_map(|op| {
            tail_op_effect_for_bucket(op, &bucket.resolved).map(|operation| (op, operation))
        })
        .map(|(op, operation)| {
            task_tail_oplog_entry_with_op_id(op, operation, &bucket.resolved, op.op_id)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(BucketPayload {
        current_entries,
        tail_entries,
        checkpoint_count,
        checkpoint_checksum: protocol_checksum_i32(checkpoint_sum),
        snapshot_clear_checksum: superseded_remainder.checksum,
        force_snapshot_clear: snapshot.reset_required,
        bounded_after,
        global_last_op_id,
        snapshot_floor_op_id,
    })
}

#[derive(Debug, Clone)]
struct ResolvedBucketCursor {
    resolved: ResolvedSyncBucket,
    after: u64,
}

fn resolved_sync_buckets(
    buckets: &SyncBucketCursors,
    plan: &RustExecutionPlan,
) -> Vec<ResolvedBucketCursor> {
    let mut resolved = buckets
        .buckets
        .iter()
        .filter_map(|bucket| {
            resolve_bucket_request_for_plan(plan, &bucket.name).map(|resolved| {
                ResolvedBucketCursor {
                    resolved,
                    after: bucket.after,
                }
            })
        })
        .collect::<Vec<_>>();

    if buckets.buckets.is_empty() && buckets.default_when_empty {
        resolved.extend(
            plan.default_bucket_requests()
                .into_iter()
                .map(|resolved| ResolvedBucketCursor { resolved, after: 0 }),
        );
    }

    resolved
}

fn resolve_bucket_request_for_plan(
    plan: &RustExecutionPlan,
    name: &str,
) -> Option<ResolvedSyncBucket> {
    plan.resolve_bucket_request(name)
        .or_else(|| resolve_bucket_request(name))
}

fn current_entries_for_bucket(
    documents: &[PersistedBucketedDocument],
    bucket: &ResolvedSyncBucket,
) -> Result<Vec<OplogEntry>, StorageError> {
    documents
        .iter()
        .filter(|document| document_matches_bucket(document, bucket))
        .map(|document| document_put_entry(document, bucket))
        .collect()
}

fn checkpoint_accumulator_for_entries(entries: &[OplogEntry]) -> PersistedCheckpointAccumulator {
    PersistedCheckpointAccumulator {
        checksum: entries.iter().fold(0_u32, |checksum, entry| {
            checksum.wrapping_add(entry.checksum)
        }),
        count: entries.len() as u64,
    }
}

fn bucket_tail_index_keys(bucket: &ResolvedSyncBucket) -> Vec<String> {
    let mut keys = bucket
        .queries()
        .iter()
        .flat_map(|query| {
            sync_tail_index_keys_for_bucket(query.object_type(), query.route_constraints())
        })
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys
}

fn document_matches_bucket(
    document: &PersistedBucketedDocument,
    bucket: &ResolvedSyncBucket,
) -> bool {
    bucket.matches_object_routes_and_data(
        &document.object_type,
        &document.route_fields,
        &document.data_json,
    )
}

#[cfg(test)]
fn tail_op_matches_bucket(op: &PersistedSyncTailOp, bucket: &ResolvedSyncBucket) -> bool {
    tail_op_effect_for_bucket(op, bucket).is_some()
}

fn tail_op_effect_for_bucket(
    op: &PersistedSyncTailOp,
    bucket: &ResolvedSyncBucket,
) -> Option<PersistedSyncTailOperation> {
    match op.operation {
        PersistedSyncTailOperation::Clear => Some(PersistedSyncTailOperation::Clear),
        PersistedSyncTailOperation::Put => {
            let object_type = op.object_type.as_deref()?;
            let matches_after = op.data_json.as_deref().is_some_and(|data_json| {
                bucket.matches_object_routes_and_data(object_type, &op.route_fields, data_json)
            });
            let matches_before = op.previous_data_json.as_deref().is_some_and(|data_json| {
                bucket.matches_object_routes_and_data(
                    object_type,
                    op.previous_route_fields
                        .as_ref()
                        .unwrap_or(&op.route_fields),
                    data_json,
                )
            });
            if matches_after {
                Some(PersistedSyncTailOperation::Put)
            } else if matches_before {
                Some(PersistedSyncTailOperation::Remove)
            } else {
                None
            }
        }
        PersistedSyncTailOperation::Remove => op.object_type.as_deref().and_then(|object_type| {
            op.data_json.as_deref().map_or_else(
                || {
                    bucket
                        .matches_object_and_routes(object_type, &op.route_fields)
                        .then_some(PersistedSyncTailOperation::Remove)
                },
                |data_json| {
                    bucket
                        .matches_object_routes_and_data(object_type, &op.route_fields, data_json)
                        .then_some(PersistedSyncTailOperation::Remove)
                },
            )
        }),
    }
}

fn task_tail_oplog_entry_with_op_id(
    op: &PersistedSyncTailOp,
    operation: PersistedSyncTailOperation,
    bucket: &ResolvedSyncBucket,
    op_id: u64,
) -> Result<OplogEntry, StorageError> {
    match operation {
        PersistedSyncTailOperation::Clear => Ok(OplogEntry {
            op_id: op_id.to_string(),
            op: OplogOperation::Clear,
            object_type: None,
            object_id: None,
            data: None,
            checksum: 0,
            subkey: None,
        }),
        PersistedSyncTailOperation::Put => {
            let object_type = op.object_type.as_deref().unwrap_or(bucket.object_type());
            let object_id = op.object_id.as_deref();
            let data = op
                .data_json
                .as_ref()
                .map(|data_json| {
                    bucket
                        .project_document_json(object_type, data_json)
                        .map_err(|error| {
                            StorageError(format!(
                                "failed to project persisted sync tail op {} for bucket {}: {}",
                                op.op_id,
                                bucket.bucket_name(),
                                error
                            ))
                        })
                })
                .transpose()?;
            Ok(OplogEntry {
                op_id: op_id.to_string(),
                op: OplogOperation::Put,
                object_type: op.object_type.clone(),
                object_id: op.object_id.clone(),
                checksum: object_id
                    .zip(data.as_deref())
                    .map_or(0, |(object_id, data)| {
                        put_checksum(object_type, object_id, data)
                    }),
                data,
                subkey: object_id.map(|object_id| source_subkey_for_object(object_type, object_id)),
            })
        }
        PersistedSyncTailOperation::Remove => {
            let object_type = op.object_type.as_deref().unwrap_or(bucket.object_type());
            let object_id = op.object_id.as_deref();
            let subkey =
                object_id.map(|object_id| source_subkey_for_object(object_type, object_id));
            Ok(OplogEntry {
                op_id: op_id.to_string(),
                op: OplogOperation::Remove,
                object_type: op.object_type.clone(),
                object_id: op.object_id.clone(),
                data: None,
                checksum: subkey.as_deref().map_or(0, remove_checksum),
                subkey,
            })
        }
    }
}

fn document_put_entry(
    document: &PersistedBucketedDocument,
    bucket: &ResolvedSyncBucket,
) -> Result<OplogEntry, StorageError> {
    let data = bucket
        .project_document_json(&document.object_type, &document.data_json)
        .map_err(|error| {
            StorageError(format!(
                "failed to project persisted document {} for bucket {}: {}",
                document.object_id,
                bucket.bucket_name(),
                error
            ))
        })?;
    Ok(OplogEntry {
        op_id: "0".to_owned(),
        op: OplogOperation::Put,
        object_type: Some(document.object_type.clone()),
        object_id: Some(document.object_id.clone()),
        checksum: put_checksum(&document.object_type, &document.object_id, &data),
        data: Some(data),
        subkey: Some(source_subkey_for_object(
            &document.object_type,
            &document.object_id,
        )),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};
    use tempfile::TempDir;

    use crate::replication::ingest::{ReplicationCommitBatch, ReplicationMdbxStore};
    use crate::storage::{Storage, SyncBucketCursors};
    use crate::sync_rules::{
        compile_sync_rules_source, execution_plan, lower_canonical_semantic_plan,
        org_comments_bucket_name, org_memberships_bucket_name, org_projects_bucket_name,
        org_tasks_bucket_name, owner_projects_bucket_name, project_tasks_bucket_name,
        region_organizations_bucket_name, task_comments_bucket_name, DEFAULT_TASKS_BUCKET_NAME,
    };

    use super::*;

    fn body_for_buckets(
        storage: &WireMdbxStorage,
        cursors: &SyncBucketCursors,
        encoding: StreamEncoding,
    ) -> bytes::Bytes {
        storage
            .sync_body_source_for_buckets_with_plan(cursors, execution_plan(), encoding)
            .expect("sync body source should read")
            .body
    }

    #[test]
    fn wire_mdbx_storage_plain_reads_expand_default_buckets_without_fixture_fallback() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let storage = WireMdbxStorage::new(snapshot_dir.path(), tail_dir.path());
        let default_cursors = SyncBucketCursors::default();
        let ndjson_body = String::from_utf8(
            body_for_buckets(&storage, &default_cursors, StreamEncoding::Ndjson).to_vec(),
        )
        .expect("ndjson body");
        let bson_body = body_for_buckets(&storage, &default_cursors, StreamEncoding::Bson);

        assert!(ndjson_body.contains(DEFAULT_TASKS_BUCKET_NAME));
        assert!(ndjson_body.contains("\"last_op_id\":\"0\""));
        assert!(!ndjson_body.contains("task-org-001-0001-0001"));
        assert!(
            !bson_body.is_empty(),
            "bson stream should contain checkpoint framing"
        );
    }

    #[test]
    fn wire_mdbx_explicit_empty_bucket_set_does_not_expand_default_buckets() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let storage = WireMdbxStorage::new(snapshot_dir.path(), tail_dir.path());
        let cursors = SyncBucketCursors::from_pairs(std::iter::empty::<(&str, u64)>());

        let body = String::from_utf8(
            body_for_buckets(&storage, &cursors, StreamEncoding::Ndjson).to_vec(),
        )
        .expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid sync line"))
            .collect::<Vec<_>>();

        assert_eq!(
            lines[0]["checkpoint"]["buckets"].as_array().unwrap().len(),
            0
        );
        assert!(!body.contains(DEFAULT_TASKS_BUCKET_NAME));
    }

    #[test]
    fn wire_mdbx_storage_serves_ingest_derived_task_tail_chunks() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 44,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(400),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(401),
            end_lsn: crate::replication::postgres::PostgresLsn(402),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-insert")),
                    ("org_id", ColumnValue::text("org-001")),
                    ("project_id", ColumnValue::text("project-org-001-0001")),
                    ("title", ColumnValue::text("Runtime insert benchmark row")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("3")),
                    ("assignee_id", ColumnValue::text("user-org-001-0001")),
                    ("story_points", ColumnValue::text("5")),
                    ("updated_at", ColumnValue::text("2026-01-03T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:insert")),
                ]),
                Lsn::from(402_u64),
            )],
        };

        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );

        let chunks = storage
            .sync_chunk_source_for_buckets_with_plan(
                &SyncBucketCursors::default(),
                execution_plan(),
                StreamEncoding::Ndjson,
            )
            .expect("chunk source should read")
            .chunks
            .collect::<Vec<_>>();
        let body = chunks
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        let body = String::from_utf8(body).expect("ndjson body");

        assert!(body.contains("\"checkpoint\":{\"last_op_id\":\""));
        assert!(body.contains("\"op\":\"PUT\""));
        assert!(body.contains("\"object_id\":\"task-runtime-insert\""));
        assert!(body.contains("Runtime insert benchmark row"));
        assert!(storage
            .sync_hold_open_body_source_for_buckets_with_plan(
                &SyncBucketCursors::default(),
                execution_plan(),
                StreamEncoding::Ndjson,
            )
            .expect("hold-open body source should read")
            .is_none());
    }

    #[test]
    fn wire_mdbx_storage_preserves_global_cursor_gaps_across_irrelevant_tail_ops() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 45,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(410),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(411),
            end_lsn: crate::replication::postgres::PostgresLsn(412),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "projects",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("project-runtime-a")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("code", ColumnValue::text("PRJ-RT-A")),
                        ("name", ColumnValue::text("Runtime Project A")),
                        ("status", ColumnValue::text("active")),
                        ("priority", ColumnValue::text("1")),
                        ("owner_id", ColumnValue::text("user-runtime-a")),
                        ("updated_at", ColumnValue::text("2026-04-20T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:a")),
                    ]),
                    Lsn::from(412_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-a")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime-a")),
                        ("title", ColumnValue::text("Runtime Task A")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("1")),
                        ("assignee_id", ColumnValue::text("user-runtime-a")),
                        ("story_points", ColumnValue::text("3")),
                        ("updated_at", ColumnValue::text("2026-04-20T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:task:a")),
                    ]),
                    Lsn::from(412_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "projects",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("project-runtime-b")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("code", ColumnValue::text("PRJ-RT-B")),
                        ("name", ColumnValue::text("Runtime Project B")),
                        ("status", ColumnValue::text("active")),
                        ("priority", ColumnValue::text("2")),
                        ("owner_id", ColumnValue::text("user-runtime-b")),
                        ("updated_at", ColumnValue::text("2026-04-20T00:00:01Z")),
                        ("summary", ColumnValue::text("runtime:project:b")),
                    ]),
                    Lsn::from(412_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-b")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime-b")),
                        ("title", ColumnValue::text("Runtime Task B")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("2")),
                        ("assignee_id", ColumnValue::text("user-runtime-b")),
                        ("story_points", ColumnValue::text("5")),
                        ("updated_at", ColumnValue::text("2026-04-20T00:00:01Z")),
                        ("summary", ColumnValue::text("runtime:task:b")),
                    ]),
                    Lsn::from(412_u64),
                ),
            ],
        };

        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let cursors = SyncBucketCursors::from_pairs([(DEFAULT_TASKS_BUCKET_NAME, 0)]);

        assert_eq!(
            Storage::latest_sync_bucket_cursors_with_plan(&storage, &cursors, execution_plan())
                .expect("latest cursors should read")
                .and_then(|latest| latest.max_after()),
            Some(4)
        );

        let body = String::from_utf8(
            body_for_buckets(&storage, &cursors, StreamEncoding::Ndjson).to_vec(),
        )
        .expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid sync line"))
            .collect::<Vec<_>>();

        assert_eq!(
            lines[0]["checkpoint"]["last_op_id"].as_str(),
            Some("4"),
            "checkpoint should use the global tail cursor"
        );
        assert_eq!(
            lines[1]["data"]["next_after"].as_str(),
            Some("4"),
            "full snapshot should end at the global tail cursor"
        );
        let op_ids = lines[1]["data"]["data"]
            .as_array()
            .expect("data entries")
            .iter()
            .filter_map(|entry| {
                entry
                    .get("object_id")
                    .and_then(serde_json::Value::as_str)
                    .zip(entry.get("op_id").and_then(serde_json::Value::as_str))
            })
            .collect::<Vec<_>>();
        assert!(op_ids.contains(&("task-runtime-a", "3")));
        assert!(op_ids.contains(&("task-runtime-b", "4")));
        assert_eq!(
            lines[2]["checkpoint_complete"]["last_op_id"].as_str(),
            Some("4"),
            "checkpoint_complete should stay aligned with the global tail cursor"
        );

        let warm_cursors = SyncBucketCursors::from_pairs([(DEFAULT_TASKS_BUCKET_NAME, 4)]);
        let warm_body = String::from_utf8(
            body_for_buckets(&storage, &warm_cursors, StreamEncoding::Ndjson).to_vec(),
        )
        .expect("warm ndjson body");
        let warm_lines = warm_body
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).expect("valid warm sync line")
            })
            .collect::<Vec<_>>();
        assert_eq!(warm_lines.len(), 2);
        assert_eq!(
            warm_lines[0]["checkpoint"]["last_op_id"].as_str(),
            Some("4"),
            "warm reconnect should retain the global tail cursor"
        );
        assert_eq!(
            warm_lines[1]["checkpoint_complete"]["last_op_id"].as_str(),
            Some("4"),
        );
    }

    #[test]
    fn wire_mdbx_storage_full_snapshot_uses_current_ingest_state_not_tail_history() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let insert_batch = ReplicationCommitBatch {
            transaction_id: 45,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(500),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(501),
            end_lsn: crate::replication::postgres::PostgresLsn(502),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-snapshot")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime")),
                        ("title", ColumnValue::text("Runtime snapshot row")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("4")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("8")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:snapshot")),
                    ]),
                    Lsn::from(502_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-delete")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime")),
                        ("title", ColumnValue::text("Runtime delete row")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("2")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("3")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:delete")),
                    ]),
                    Lsn::from(502_u64),
                ),
            ],
        };
        ingest_store
            .persist_batch(&insert_batch)
            .expect("persist insert batch");

        let update_delete_batch = ReplicationCommitBatch {
            transaction_id: 46,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(503),
            begin_commit_time_micros: 3,
            commit_lsn: crate::replication::postgres::PostgresLsn(504),
            end_lsn: crate::replication::postgres::PostgresLsn(505),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::update(
                    "public",
                    "tasks",
                    7,
                    Some(RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-snapshot")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime")),
                        ("title", ColumnValue::text("Runtime snapshot row")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("4")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("8")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:snapshot")),
                    ])),
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-snapshot")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime")),
                        ("title", ColumnValue::text("Runtime snapshot row updated")),
                        ("status", ColumnValue::text("in_progress")),
                        ("priority", ColumnValue::text("5")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("13")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:01:00Z")),
                        ("summary", ColumnValue::text("runtime:snapshot:updated")),
                    ]),
                    pg_walstream::ReplicaIdentity::Default,
                    Vec::new(),
                    Lsn::from(505_u64),
                ),
                ChangeEvent::delete(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-runtime-delete")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-runtime")),
                        ("title", ColumnValue::text("Runtime delete row")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("2")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("3")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:delete")),
                    ]),
                    pg_walstream::ReplicaIdentity::Default,
                    Vec::new(),
                    Lsn::from(505_u64),
                ),
            ],
        };
        ingest_store
            .persist_batch(&update_delete_batch)
            .expect("persist update/delete batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );

        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::default(),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            data_entries
                .iter()
                .filter(|entry| {
                    entry.get("object_id").and_then(serde_json::Value::as_str)
                        == Some("task-runtime-snapshot")
                })
                .count(),
            1,
        );
        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str)
                == Some("task-runtime-snapshot")
                && entry
                    .get("data")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|json| json.contains("Runtime snapshot row updated"))
                && entry
                    .get("data")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|json| json.contains("\"status\":\"in_progress\""))
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str)
                != Some("task-runtime-delete")
        }));
        assert!(
            !body.contains("task-org-001-0001-0001"),
            "full snapshot should not fall back to fixture-seeded benchmark rows once ingest state exists"
        );
    }

    #[test]
    fn wire_mdbx_incremental_reads_do_not_require_current_state_snapshot() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");

        let initial_batch = ReplicationCommitBatch {
            transaction_id: 90,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(800),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(801),
            end_lsn: crate::replication::postgres::PostgresLsn(802),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-base")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime base row")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("1")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("2")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:base")),
                ]),
                Lsn::from(802_u64),
            )],
        };
        ingest_store
            .persist_batch(&initial_batch)
            .expect("persist initial batch");

        let incremental_batch = ReplicationCommitBatch {
            transaction_id: 91,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(803),
            begin_commit_time_micros: 3,
            commit_lsn: crate::replication::postgres::PostgresLsn(804),
            end_lsn: crate::replication::postgres::PostgresLsn(805),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-delta")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime delta row")),
                    ("status", ColumnValue::text("in_progress")),
                    ("priority", ColumnValue::text("2")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("3")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:01:00Z")),
                    ("summary", ColumnValue::text("runtime:delta")),
                ]),
                Lsn::from(805_u64),
            )],
        };
        ingest_store
            .persist_batch(&incremental_batch)
            .expect("persist incremental batch");
        ingest_store
            .write_sync_state_snapshot_for_test(b"not-json")
            .expect("corrupt current state snapshot");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let body = String::from_utf8(
            body_for_buckets(
                &storage,
                &SyncBucketCursors::from_pairs([(DEFAULT_TASKS_BUCKET_NAME, 1)]),
                StreamEncoding::Ndjson,
            )
            .to_vec(),
        )
        .expect("incremental ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            lines.len(),
            3,
            "incremental sync should stay checkpoint/data/complete"
        );
        assert_eq!(
            data_entries.len(),
            1,
            "incremental sync should emit only the delta row"
        );
        assert_eq!(
            data_entries[0]
                .get("object_id")
                .and_then(serde_json::Value::as_str),
            Some("task-runtime-delta")
        );
    }

    #[test]
    fn wire_mdbx_storage_serves_project_scoped_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 47,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(600),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(601),
            end_lsn: crate::replication::postgres::PostgresLsn(602),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-project-a")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-a")),
                        ("title", ColumnValue::text("Project A task")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("1")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("2")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:a")),
                    ]),
                    Lsn::from(602_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-project-b")),
                        ("org_id", ColumnValue::text("org-runtime")),
                        ("project_id", ColumnValue::text("project-b")),
                        ("title", ColumnValue::text("Project B task")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("2")),
                        ("assignee_id", ColumnValue::text("user-runtime")),
                        ("story_points", ColumnValue::text("3")),
                        ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:b")),
                    ]),
                    Lsn::from(602_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let project_bucket = project_tasks_bucket_name("project-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(project_bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(lines.iter().any(|line| {
            line.get("checkpoint")
                .and_then(|checkpoint| checkpoint.get("buckets"))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .any(|bucket| {
                    bucket.get("bucket").and_then(serde_json::Value::as_str)
                        == Some(project_bucket.as_str())
                })
        }));
        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("task-project-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("task-project-a")
        }));
    }

    #[test]
    fn wire_mdbx_storage_serves_org_scoped_tasks_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 471,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(660),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(661),
            end_lsn: crate::replication::postgres::PostgresLsn(662),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-org-a")),
                        ("org_id", ColumnValue::text("org-a")),
                        ("project_id", ColumnValue::text("project-a")),
                        ("title", ColumnValue::text("Org A task")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("1")),
                        ("assignee_id", ColumnValue::text("user-a")),
                        ("story_points", ColumnValue::text("2")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:org:a")),
                    ]),
                    Lsn::from(662_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "tasks",
                    7,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("task-org-b")),
                        ("org_id", ColumnValue::text("org-b")),
                        ("project_id", ColumnValue::text("project-b")),
                        ("title", ColumnValue::text("Org B task")),
                        ("status", ColumnValue::text("todo")),
                        ("priority", ColumnValue::text("2")),
                        ("assignee_id", ColumnValue::text("user-b")),
                        ("story_points", ColumnValue::text("3")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:org:b")),
                    ]),
                    Lsn::from(662_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let bucket = org_tasks_bucket_name("org-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("task-org-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("task-org-a")
        }));
    }

    #[test]
    fn wire_mdbx_storage_serves_org_scoped_projects_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 48,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(700),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(701),
            end_lsn: crate::replication::postgres::PostgresLsn(702),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "projects",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("project-org-a")),
                        ("org_id", ColumnValue::text("org-a")),
                        ("code", ColumnValue::text("PRJ-A")),
                        ("name", ColumnValue::text("Project A")),
                        ("status", ColumnValue::text("active")),
                        ("priority", ColumnValue::text("1")),
                        ("owner_id", ColumnValue::text("user-a")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:a")),
                    ]),
                    Lsn::from(702_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "projects",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("project-org-b")),
                        ("org_id", ColumnValue::text("org-b")),
                        ("code", ColumnValue::text("PRJ-B")),
                        ("name", ColumnValue::text("Project B")),
                        ("status", ColumnValue::text("active")),
                        ("priority", ColumnValue::text("2")),
                        ("owner_id", ColumnValue::text("user-b")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:b")),
                    ]),
                    Lsn::from(702_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let project_bucket = org_projects_bucket_name("org-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(project_bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("project-org-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("project-org-a")
        }));
    }

    #[test]
    fn wire_mdbx_storage_serves_owner_scoped_projects_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 482,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(760),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(761),
            end_lsn: crate::replication::postgres::PostgresLsn(762),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "projects",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("project-owner-a")),
                        ("org_id", ColumnValue::text("org-a")),
                        ("code", ColumnValue::text("PRJ-A")),
                        ("name", ColumnValue::text("Project A")),
                        ("status", ColumnValue::text("active")),
                        ("priority", ColumnValue::text("1")),
                        ("owner_id", ColumnValue::text("owner-a")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:a")),
                    ]),
                    Lsn::from(762_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "projects",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("project-owner-b")),
                        ("org_id", ColumnValue::text("org-b")),
                        ("code", ColumnValue::text("PRJ-B")),
                        ("name", ColumnValue::text("Project B")),
                        ("status", ColumnValue::text("active")),
                        ("priority", ColumnValue::text("2")),
                        ("owner_id", ColumnValue::text("owner-b")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("summary", ColumnValue::text("runtime:project:b")),
                    ]),
                    Lsn::from(762_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let bucket = owner_projects_bucket_name("owner-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("project-owner-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("project-owner-a")
        }));
    }

    #[test]
    fn wire_mdbx_storage_serves_task_scoped_comments_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 49,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(800),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(801),
            end_lsn: crate::replication::postgres::PostgresLsn(802),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "comments",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("comment-task-a")),
                        ("org_id", ColumnValue::text("org-a")),
                        ("task_id", ColumnValue::text("task-a")),
                        ("owner_id", ColumnValue::text("user-a")),
                        ("author_id", ColumnValue::text("author-a")),
                        ("body", ColumnValue::text("Comment for task A")),
                        ("created_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:01Z")),
                    ]),
                    Lsn::from(802_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "comments",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("comment-task-b")),
                        ("org_id", ColumnValue::text("org-b")),
                        ("task_id", ColumnValue::text("task-b")),
                        ("owner_id", ColumnValue::text("user-b")),
                        ("author_id", ColumnValue::text("author-b")),
                        ("body", ColumnValue::text("Comment for task B")),
                        ("created_at", ColumnValue::text("2026-04-12T00:00:02Z")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:03Z")),
                    ]),
                    Lsn::from(802_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let comment_bucket = task_comments_bucket_name("task-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(comment_bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("comment-task-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("comment-task-a")
        }));
    }

    #[test]
    fn wire_mdbx_storage_serves_org_scoped_comments_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 493,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(860),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(861),
            end_lsn: crate::replication::postgres::PostgresLsn(862),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "comments",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("comment-org-a")),
                        ("org_id", ColumnValue::text("org-a")),
                        ("task_id", ColumnValue::text("task-a")),
                        ("owner_id", ColumnValue::text("owner-a")),
                        ("author_id", ColumnValue::text("author-a")),
                        ("body", ColumnValue::text("Comment for org A")),
                        ("created_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:01Z")),
                    ]),
                    Lsn::from(862_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "comments",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("comment-org-b")),
                        ("org_id", ColumnValue::text("org-b")),
                        ("task_id", ColumnValue::text("task-b")),
                        ("owner_id", ColumnValue::text("owner-b")),
                        ("author_id", ColumnValue::text("author-b")),
                        ("body", ColumnValue::text("Comment for org B")),
                        ("created_at", ColumnValue::text("2026-04-12T00:00:02Z")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:03Z")),
                    ]),
                    Lsn::from(862_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let bucket = org_comments_bucket_name("org-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("comment-org-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("comment-org-a")
        }));
    }

    #[test]
    fn wire_mdbx_storage_serves_org_scoped_memberships_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 50,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(900),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(901),
            end_lsn: crate::replication::postgres::PostgresLsn(902),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "memberships",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("membership-org-a")),
                        ("org_id", ColumnValue::text("org-a")),
                        ("user_id", ColumnValue::text("user-a")),
                        ("owner_id", ColumnValue::text("owner-a")),
                        ("role", ColumnValue::text("member")),
                        ("display_name", ColumnValue::text("Member A")),
                        ("email", ColumnValue::text("a@example.com")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                    ]),
                    Lsn::from(902_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "memberships",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("membership-org-b")),
                        ("org_id", ColumnValue::text("org-b")),
                        ("user_id", ColumnValue::text("user-b")),
                        ("owner_id", ColumnValue::text("owner-b")),
                        ("role", ColumnValue::text("admin")),
                        ("display_name", ColumnValue::text("Member B")),
                        ("email", ColumnValue::text("b@example.com")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:01Z")),
                    ]),
                    Lsn::from(902_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let membership_bucket = org_memberships_bucket_name("org-b");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(membership_bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("membership-org-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("membership-org-a")
        }));
    }

    #[test]
    fn bucket_aware_reads_do_not_fall_back_to_default_fixture_when_ingest_is_empty() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let bucket = org_projects_bucket_name("org-empty");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();

        assert!(lines.iter().any(|line| {
            line.get("checkpoint")
                .and_then(|checkpoint| checkpoint.get("buckets"))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .any(|checkpoint_bucket| {
                    checkpoint_bucket
                        .get("bucket")
                        .and_then(serde_json::Value::as_str)
                        == Some(bucket.as_str())
                })
        }));
        assert!(!body.contains(DEFAULT_TASKS_BUCKET_NAME));
        assert!(!body.contains("task-org-001-0001-0001"));
    }

    #[test]
    fn compiler_driven_read_path_projects_same_table_streams_per_bucket() {
        let plan = lower_canonical_semantic_plan(
            compile_sync_rules_source(include_str!(
                "../../tests/fixtures/sync_plan/projection_variants.sync-rules"
            ))
            .expect("projection variants should compile"),
        )
        .expect("projection variants plan should lower");
        let queue_bucket = plan
            .resolve_bucket_request(r#"1#tickets_by_queue|0["queue-a"]"#)
            .expect("queue bucket");
        let status_bucket = plan
            .resolve_bucket_request(r#"1#tickets_by_status|0["open"]"#)
            .expect("status bucket");
        let document = PersistedBucketedDocument {
            object_type: "tickets".to_owned(),
            object_id: "ticket-1".to_owned(),
            route_fields: BTreeMap::from([
                (String::from("queue_id"), String::from("queue-a")),
                (String::from("status"), String::from("open")),
            ]),
            data_json: serde_json::json!({
                "id": "ticket-1",
                "queue_id": "queue-a",
                "status": "open",
                "title": "Fix bug"
            })
            .to_string(),
        };
        let tail_op = PersistedSyncTailOp {
            op_id: 7,
            operation: PersistedSyncTailOperation::Put,
            object_type: Some("tickets".to_owned()),
            object_id: Some("ticket-1".to_owned()),
            route_fields: document.route_fields.clone(),
            data_json: Some(document.data_json.clone()),
            previous_route_fields: None,
            previous_data_json: None,
        };

        let queue_entries =
            current_entries_for_bucket(std::slice::from_ref(&document), &queue_bucket)
                .expect("queue entries should project");
        let status_entries =
            current_entries_for_bucket(std::slice::from_ref(&document), &status_bucket)
                .expect("status entries should project");
        let queue_tail = task_tail_oplog_entry_with_op_id(
            &tail_op,
            PersistedSyncTailOperation::Put,
            &queue_bucket,
            1,
        )
        .expect("queue tail entry should project");
        let status_tail = task_tail_oplog_entry_with_op_id(
            &tail_op,
            PersistedSyncTailOperation::Put,
            &status_bucket,
            1,
        )
        .expect("status tail entry should project");
        let expected_queue = serde_json::json!({
            "ticket_id": "ticket-1",
            "queue_id": "queue-a",
            "title": "Fix bug"
        })
        .to_string();
        let expected_status = serde_json::json!({
            "ticket_id": "ticket-1",
            "status": "open"
        })
        .to_string();

        assert_eq!(
            queue_entries[0].data.as_deref(),
            Some(expected_queue.as_str())
        );
        assert_eq!(
            status_entries[0].data.as_deref(),
            Some(expected_status.as_str())
        );
        assert_eq!(queue_tail.data.as_deref(), Some(expected_queue.as_str()));
        assert_eq!(status_tail.data.as_deref(), Some(expected_status.as_str()));
    }

    #[test]
    fn compiler_driven_multi_query_stream_serves_multiple_object_types_from_one_bucket() {
        let plan = lower_canonical_semantic_plan(
            compile_sync_rules_source(
                r#"
config:
  edition: 3
streams:
  inbox:
    queries:
      - SELECT id AS task_id, org_id, title FROM tasks WHERE org_id = subscription.parameter('org_id')
      - SELECT id AS comment_id, task_id, org_id, body FROM comments WHERE org_id = subscription.parameter('org_id')
"#,
            )
            .expect("multi-query stream should compile"),
        )
        .expect("multi-query stream should lower");
        let bucket = plan
            .resolve_bucket_request(r#"1#inbox|0["org-a"]"#)
            .expect("inbox bucket");
        let documents = vec![
            PersistedBucketedDocument {
                object_type: "tasks".to_owned(),
                object_id: "task-a".to_owned(),
                route_fields: BTreeMap::from([(String::from("org_id"), String::from("org-a"))]),
                data_json: serde_json::json!({
                    "id": "task-a",
                    "org_id": "org-a",
                    "title": "Task A",
                    "body": "not selected"
                })
                .to_string(),
            },
            PersistedBucketedDocument {
                object_type: "comments".to_owned(),
                object_id: "comment-a".to_owned(),
                route_fields: BTreeMap::from([(String::from("org_id"), String::from("org-a"))]),
                data_json: serde_json::json!({
                    "id": "comment-a",
                    "task_id": "task-a",
                    "org_id": "org-a",
                    "body": "Comment A",
                    "title": "not selected"
                })
                .to_string(),
            },
        ];

        let entries = current_entries_for_bucket(&documents, &bucket)
            .expect("multi-query entries should project");
        let payloads = entries
            .iter()
            .map(|entry| entry.data.as_deref().unwrap_or_default())
            .collect::<Vec<_>>();

        assert_eq!(entries.len(), 2);
        assert!(payloads.contains(
            &serde_json::json!({
                "task_id": "task-a",
                "org_id": "org-a",
                "title": "Task A"
            })
            .to_string()
            .as_str()
        ));
        assert!(payloads.contains(
            &serde_json::json!({
                "comment_id": "comment-a",
                "task_id": "task-a",
                "org_id": "org-a",
                "body": "Comment A"
            })
            .to_string()
            .as_str()
        ));
    }

    #[test]
    fn residual_filter_update_transition_emits_remove() {
        let plan = lower_canonical_semantic_plan(
            compile_sync_rules_source(
                r#"
config:
  edition: 3
streams:
  issues:
    query: SELECT * FROM issues WHERE project_id = subscription.parameter('project_id') AND deleted_at IS NULL
"#,
            )
            .expect("filtered stream should compile"),
        )
        .expect("filtered stream should lower");
        let bucket = plan
            .resolve_bucket_request(r#"1#issues|0["project-a"]"#)
            .expect("issue bucket");
        let remove = PersistedSyncTailOp {
            op_id: 1,
            operation: PersistedSyncTailOperation::Remove,
            object_type: Some("issues".to_owned()),
            object_id: Some("issue-a".to_owned()),
            route_fields: BTreeMap::from([(String::from("project_id"), String::from("project-a"))]),
            data_json: None,
            previous_route_fields: None,
            previous_data_json: None,
        };
        let filtered_put = PersistedSyncTailOp {
            op_id: 2,
            operation: PersistedSyncTailOperation::Put,
            object_type: Some("issues".to_owned()),
            object_id: Some("issue-a".to_owned()),
            route_fields: BTreeMap::from([(String::from("project_id"), String::from("project-a"))]),
            data_json: Some(
                serde_json::json!({
                    "id": "issue-a",
                    "project_id": "project-a",
                    "deleted_at": "2026-01-01T00:00:00Z"
                })
                .to_string(),
            ),
            previous_route_fields: Some(BTreeMap::from([(
                String::from("project_id"),
                String::from("project-a"),
            )])),
            previous_data_json: Some(
                serde_json::json!({
                    "id": "issue-a",
                    "project_id": "project-a",
                    "deleted_at": null
                })
                .to_string(),
            ),
        };

        assert!(tail_op_matches_bucket(&remove, &bucket));
        assert_eq!(
            tail_op_effect_for_bucket(&filtered_put, &bucket),
            Some(PersistedSyncTailOperation::Remove)
        );
        let entry = task_tail_oplog_entry_with_op_id(
            &filtered_put,
            PersistedSyncTailOperation::Remove,
            &bucket,
            filtered_put.op_id,
        )
        .expect("transition entry");
        assert!(matches!(entry.op, OplogOperation::Remove));
    }

    #[test]
    fn wire_mdbx_storage_serves_region_scoped_organizations_bucket_from_ingest_state() {
        let snapshot_dir = TempDir::new().expect("snapshot temp directory should exist");
        let tail_dir = TempDir::new().expect("tail temp directory should exist");
        let ingest_dir = TempDir::new().expect("ingest temp directory should exist");
        let ingest_store = ReplicationMdbxStore::shared(ingest_dir.path()).expect("ingest store");
        let batch = ReplicationCommitBatch {
            transaction_id: 504,
            begin_final_lsn: crate::replication::postgres::PostgresLsn(960),
            begin_commit_time_micros: 1,
            commit_lsn: crate::replication::postgres::PostgresLsn(961),
            end_lsn: crate::replication::postgres::PostgresLsn(962),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert(
                    "public",
                    "organizations",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("org-region-a")),
                        ("name", ColumnValue::text("Org A")),
                        ("owner_id", ColumnValue::text("owner-a")),
                        ("plan", ColumnValue::text("growth")),
                        ("region", ColumnValue::text("us-east-1")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                    ]),
                    Lsn::from(962_u64),
                ),
                ChangeEvent::insert(
                    "public",
                    "organizations",
                    8,
                    RowData::from_pairs(vec![
                        ("id", ColumnValue::text("org-region-b")),
                        ("name", ColumnValue::text("Org B")),
                        ("owner_id", ColumnValue::text("owner-b")),
                        ("plan", ColumnValue::text("enterprise")),
                        ("region", ColumnValue::text("eu-west-1")),
                        ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                    ]),
                    Lsn::from(962_u64),
                ),
            ],
        };
        ingest_store.persist_batch(&batch).expect("persist batch");

        let storage = WireMdbxStorage::new_with_ingest(
            snapshot_dir.path(),
            tail_dir.path(),
            ingest_dir.path(),
        );
        let bucket = region_organizations_bucket_name("eu-west-1");
        let body = body_for_buckets(
            &storage,
            &SyncBucketCursors::from_pairs([(bucket.as_str(), 0)]),
            StreamEncoding::Ndjson,
        );
        let body = String::from_utf8(body.to_vec()).expect("ndjson body");
        let lines = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("sync line json"))
            .collect::<Vec<_>>();
        let data_entries = lines
            .iter()
            .filter_map(|line| line.get("data"))
            .flat_map(|payload| {
                payload
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();

        assert!(data_entries.iter().any(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) == Some("org-region-b")
        }));
        assert!(data_entries.iter().all(|entry| {
            entry.get("object_id").and_then(serde_json::Value::as_str) != Some("org-region-a")
        }));
    }
}
