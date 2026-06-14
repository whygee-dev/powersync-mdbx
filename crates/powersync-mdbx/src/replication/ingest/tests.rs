use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use libmdbx::WriteFlags;
use pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, ReplicaIdentity, RowData};
use tempfile::TempDir;

use super::keys::{
    current_doc_key, current_route_index_key, sync_tail_index_entry_key, sync_tail_op_key,
    META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY, META_SYNC_TAIL_LAST_OP_ID_KEY,
};
use super::tail_log::{read_optional_be_u64, read_optional_u64};
use super::*;
use crate::protocol::messages::{put_checksum, remove_checksum, source_subkey_for_object};
use crate::replication::postgres::PostgresLsn;
use crate::replication::runner::ReplicationStreamEvent;
use crate::sync_rules::execution_plan;

#[test]
fn decoder_builds_insert_batch_from_relation_and_commit_sequence() {
    let mut decoder = PgOutputBatchDecoder::new();

    assert!(decoder
        .push_stream_event(&ReplicationStreamEvent::Begin {
            final_lsn: PostgresLsn(10),
            xid: 42,
            commit_time_micros: 1234,
        })
        .expect("begin")
        .is_none());
    assert!(decoder
        .push_stream_event(&ReplicationStreamEvent::XLogData {
            wal_start: PostgresLsn(11),
            wal_end: PostgresLsn(12),
            server_time_micros: 0,
            data: relation_message(7, "public", "users", "name"),
        })
        .expect("relation")
        .is_none());
    assert!(decoder
        .push_stream_event(&ReplicationStreamEvent::XLogData {
            wal_start: PostgresLsn(12),
            wal_end: PostgresLsn(13),
            server_time_micros: 0,
            data: insert_message(7, &[Some("1"), Some("Alice")]),
        })
        .expect("insert")
        .is_none());

    let batch = decoder
        .push_stream_event(&ReplicationStreamEvent::Commit {
            lsn: PostgresLsn(20),
            end_lsn: PostgresLsn(21),
            commit_time_micros: 9999,
        })
        .expect("commit")
        .expect("batch");

    assert_eq!(batch.transaction_id, 42);
    assert_eq!(batch.begin_final_lsn, PostgresLsn(10));
    assert_eq!(batch.commit_lsn, PostgresLsn(20));
    assert_eq!(batch.end_lsn, PostgresLsn(21));
    assert_eq!(batch.change_count(), 1);

    let change = &batch.changes[0];
    match &change.event_type {
        EventType::Insert {
            schema,
            table,
            relation_oid,
            data,
        } => {
            assert_eq!(&**schema, "public");
            assert_eq!(&**table, "users");
            assert_eq!(*relation_oid, 7);
            assert_eq!(data.get("id").and_then(ColumnValue::as_str), Some("1"));
            assert_eq!(
                data.get("name").and_then(ColumnValue::as_str),
                Some("Alice")
            );
        }
        other => panic!("expected insert change, got {other:?}"),
    }
}

#[test]
fn decoder_builds_update_delete_and_truncate_batch() {
    let mut decoder = PgOutputBatchDecoder::new();

    decoder
        .push_stream_event(&ReplicationStreamEvent::Begin {
            final_lsn: PostgresLsn(30),
            xid: 77,
            commit_time_micros: 111,
        })
        .expect("begin");
    decoder
        .push_stream_event(&ReplicationStreamEvent::XLogData {
            wal_start: PostgresLsn(31),
            wal_end: PostgresLsn(32),
            server_time_micros: 0,
            data: relation_message(9, "public", "tasks", "status"),
        })
        .expect("relation");
    decoder
        .push_stream_event(&ReplicationStreamEvent::XLogData {
            wal_start: PostgresLsn(32),
            wal_end: PostgresLsn(33),
            server_time_micros: 0,
            data: update_message(9, &[Some("1"), Some("todo")], &[Some("1"), Some("done")]),
        })
        .expect("update");
    decoder
        .push_stream_event(&ReplicationStreamEvent::XLogData {
            wal_start: PostgresLsn(33),
            wal_end: PostgresLsn(34),
            server_time_micros: 0,
            data: delete_message(9, &[Some("1"), Some("done")]),
        })
        .expect("delete");
    decoder
        .push_stream_event(&ReplicationStreamEvent::XLogData {
            wal_start: PostgresLsn(34),
            wal_end: PostgresLsn(35),
            server_time_micros: 0,
            data: truncate_message(&[9]),
        })
        .expect("truncate");

    let batch = decoder
        .push_stream_event(&ReplicationStreamEvent::Commit {
            lsn: PostgresLsn(40),
            end_lsn: PostgresLsn(41),
            commit_time_micros: 222,
        })
        .expect("commit")
        .expect("batch");

    assert_eq!(batch.change_count(), 3);
    match &batch.changes[0].event_type {
        EventType::Update {
            old_data,
            new_data,
            key_columns,
            ..
        } => {
            let old = old_data.as_ref().expect("old row");
            assert_eq!(
                old.get("status").and_then(ColumnValue::as_str),
                Some("todo")
            );
            assert_eq!(
                new_data.get("status").and_then(ColumnValue::as_str),
                Some("done")
            );
            assert_eq!(key_columns.len(), 1);
            assert_eq!(&*key_columns[0], "id");
        }
        other => panic!("expected update change, got {other:?}"),
    }
    match &batch.changes[1].event_type {
        EventType::Delete { old_data, .. } => {
            assert_eq!(old_data.get("id").and_then(ColumnValue::as_str), Some("1"));
        }
        other => panic!("expected delete change, got {other:?}"),
    }
    match &batch.changes[2].event_type {
        EventType::Truncate(tables) => {
            assert_eq!(tables.len(), 1);
            assert_eq!(&*tables[0], "public.tasks");
        }
        other => panic!("expected truncate change, got {other:?}"),
    }
}

#[test]
fn mdbx_store_round_trips_persisted_batches() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 9,
        begin_final_lsn: PostgresLsn(100),
        begin_commit_time_micros: 111,
        commit_lsn: PostgresLsn(200),
        end_lsn: PostgresLsn(201),
        commit_time_micros: 222,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "users",
            7,
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("1")),
                ("name", ColumnValue::text("Alice")),
            ]),
            Lsn::from(201_u64),
        )],
    };

    store
        .persist_batch_with_plan_and_options(
            &batch,
            execution_plan(),
            PersistBatchOptions {
                persist_raw_batch: true,
                ..PersistBatchOptions::default()
            },
        )
        .expect("persist");
    assert_eq!(
        store.last_persisted_end_lsn().expect("lsn"),
        Some(PostgresLsn(201))
    );

    let loaded = store
        .load_batch(PostgresLsn(201))
        .expect("load")
        .expect("present");
    assert_eq!(loaded.transaction_id, 9);
    assert_eq!(loaded.commit_lsn, PostgresLsn(200));
    assert_eq!(loaded.end_lsn, PostgresLsn(201));
    assert_eq!(loaded.change_count(), 1);

    match &loaded.changes[0].event_type {
        EventType::Insert { data, .. } => {
            assert_eq!(
                data.get("name").and_then(ColumnValue::as_str),
                Some("Alice")
            );
        }
        other => panic!("expected insert change, got {other:?}"),
    }
}

#[test]
fn mdbx_store_ignores_redelivered_and_stale_commit_batches() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 10,
        begin_final_lsn: PostgresLsn(200),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(201),
        end_lsn: PostgresLsn(202),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "tasks",
            7,
            task_row_data("task-once", "org-a", "project-a"),
            Lsn::from(202_u64),
        )],
    };

    store.persist_batch(&batch).expect("persist first delivery");
    let first_metrics = store.metrics_snapshot();
    let first_tail = store.task_tail_last_op_id().expect("first tail");

    store
        .persist_batch(&batch)
        .expect("ignore exact redelivery");
    let mut stale = batch;
    stale.transaction_id = 9;
    stale.end_lsn = PostgresLsn(201);
    stale.changes = vec![ChangeEvent::insert(
        "public",
        "tasks",
        7,
        task_row_data("task-stale", "org-a", "project-a"),
        Lsn::from(201_u64),
    )];
    store.persist_batch(&stale).expect("ignore stale delivery");

    assert_eq!(store.task_tail_last_op_id().expect("tail"), first_tail);
    assert_eq!(store.metrics_snapshot().rows_seen, first_metrics.rows_seen);
    let bucket = execution_plan()
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name("project-a"))
        .expect("project bucket");
    let documents = store
        .load_current_documents_for_bucket(&bucket)
        .expect("current documents");
    assert_eq!(
        documents
            .iter()
            .map(|document| document.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-once"]
    );
}

#[test]
fn tail_retention_catches_up_past_one_delete_chunk_atomically() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    for op_id in 1..=5_u64 {
        let batch = ReplicationCommitBatch {
            transaction_id: op_id as u32,
            begin_final_lsn: PostgresLsn(op_id * 10),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(op_id * 10 + 1),
            end_lsn: PostgresLsn(op_id * 10 + 2),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data(&format!("task-{op_id}"), "org-retain", "project-retain"),
                Lsn::from(op_id * 10 + 2),
            )],
        };
        store.persist_batch(&batch).expect("persist tail op");
    }

    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("table");
    super::tail_log::prune_sync_tail(&txn, &table, 2, 2).expect("prune tail");
    txn.commit().expect("commit prune");

    let txn = store.db.begin_ro_txn().expect("ro txn");
    let table = txn.open_table(None).expect("table");
    for op_id in 1..=3 {
        assert!(txn
            .get::<Vec<u8>>(&table, &sync_tail_op_key(op_id))
            .expect("read pruned op")
            .is_none());
        assert!(txn
            .get::<Vec<u8>>(&table, &super::keys::sync_tail_refs_key(op_id))
            .expect("read pruned refs")
            .is_none());
    }
    assert!(txn
        .get::<Vec<u8>>(&table, &sync_tail_op_key(4))
        .expect("read retained op")
        .is_some());
    assert_eq!(
        read_optional_u64(
            &txn,
            &table,
            super::keys::META_SYNC_TAIL_RETAINED_FLOOR_KEY.to_vec(),
            "retained floor",
        )
        .expect("read floor"),
        Some(3)
    );
    drop(txn);

    let bucket = execution_plan()
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(
            "project-retain",
        ))
        .expect("project bucket");
    let snapshot = store
        .read_bucket_snapshot(
            &bucket,
            &sync_tail_index_keys_for_bucket(
                "tasks",
                &BTreeMap::from([("project_id".to_owned(), "project-retain".to_owned())]),
            ),
            &sync_current_checkpoint_accumulator_keys_for_bucket(&bucket),
            &sync_tail_checkpoint_accumulator_keys_for_bucket(&bucket),
            1,
        )
        .expect("read snapshot below retained floor");
    assert!(snapshot.reset_required);
    assert_eq!(snapshot.latest_op_id, 5);
    assert_eq!(snapshot.current_documents.len(), 5);
    assert!(snapshot.tail_ops.is_empty());
}

#[test]
fn tail_retention_rolls_back_earlier_chunks_when_a_later_chunk_fails() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let object_index = super::keys::sync_tail_object_index_name("tasks");
    for op_id in 1..=4_u64 {
        let batch = ReplicationCommitBatch {
            transaction_id: op_id as u32,
            begin_final_lsn: PostgresLsn(op_id * 10),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(op_id * 10 + 1),
            end_lsn: PostgresLsn(op_id * 10 + 2),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data(&format!("task-rollback-{op_id}"), "org-a", "project-a"),
                Lsn::from(op_id * 10 + 2),
            )],
        };
        store.persist_batch(&batch).expect("persist tail op");
    }

    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("table");
    txn.put(
        &table,
        super::keys::sync_tail_refs_key(3),
        b"invalid-json",
        WriteFlags::UPSERT,
    )
    .expect("corrupt later refs");
    txn.commit().expect("commit corruption");

    {
        let txn = store.db.begin_rw_txn().expect("rw txn");
        let table = txn.open_table(None).expect("table");
        assert!(matches!(
            super::tail_log::prune_sync_tail(&txn, &table, 1, 2),
            Err(ReplicationIngestError::CorruptBatch(_))
        ));
    }

    let txn = store.db.begin_ro_txn().expect("ro txn");
    let table = txn.open_table(None).expect("table");
    for op_id in 1..=2 {
        assert!(txn
            .get::<Vec<u8>>(&table, &sync_tail_op_key(op_id))
            .expect("read rolled-back op")
            .is_some());
        assert!(txn
            .get::<Vec<u8>>(&table, &super::keys::sync_tail_refs_key(op_id))
            .expect("read rolled-back refs")
            .is_some());
        assert!(txn
            .get::<Vec<u8>>(
                &table,
                &super::keys::sync_tail_index_entry_key(&object_index, op_id),
            )
            .expect("read rolled-back index entry")
            .is_some());
    }
    assert_eq!(
        read_optional_u64(
            &txn,
            &table,
            super::keys::META_SYNC_TAIL_RETAINED_FLOOR_KEY.to_vec(),
            "retained floor",
        )
        .expect("read floor"),
        None
    );
}

#[test]
fn tail_retention_skips_the_synthetic_initial_snapshot_cursor_range() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let snapshot_floor = 1_000_000_000_000_u64;

    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("table");
    txn.put(
        &table,
        super::keys::META_SYNC_TAIL_LAST_OP_ID_KEY,
        snapshot_floor.to_string(),
        WriteFlags::UPSERT,
    )
    .expect("seed last op id");
    txn.put(
        &table,
        super::keys::META_INITIAL_SNAPSHOT_CURSOR_FLOOR_KEY,
        snapshot_floor.to_string(),
        WriteFlags::UPSERT,
    )
    .expect("seed snapshot floor");
    super::tail_log::prune_sync_tail(&txn, &table, 2, 2).expect("prune synthetic range");
    txn.commit().expect("commit prune");

    let txn = store.db.begin_ro_txn().expect("ro txn");
    let table = txn.open_table(None).expect("table");
    assert_eq!(
        read_optional_u64(
            &txn,
            &table,
            super::keys::META_SYNC_TAIL_RETAINED_FLOOR_KEY.to_vec(),
            "retained floor",
        )
        .expect("read floor"),
        Some(snapshot_floor - 2)
    );
}

#[test]
fn tail_retention_stays_at_target_across_repeated_large_transactions() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let object_index = super::keys::sync_tail_object_index_name("tasks");

    for round in 0..2_u64 {
        let first_object_id = round * 7 + 1;
        let end_lsn = (round + 1) * 100;
        let batch = ReplicationCommitBatch {
            transaction_id: (round + 1) as u32,
            begin_final_lsn: PostgresLsn(end_lsn - 2),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(end_lsn - 1),
            end_lsn: PostgresLsn(end_lsn),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: (first_object_id..first_object_id + 7)
                .map(|object_id| {
                    ChangeEvent::insert(
                        "public",
                        "tasks",
                        7,
                        task_row_data(
                            &format!("task-retained-{object_id}"),
                            "org-retain",
                            "project-retain",
                        ),
                        Lsn::from(end_lsn),
                    )
                })
                .collect(),
        };
        store
            .persist_batch_with_plan_options_and_tail_retention(
                &batch,
                execution_plan(),
                PersistBatchOptions::default(),
                2,
                2,
            )
            .expect("persist and prune large batch");

        let expected_last = (round + 1) * 7;
        let expected_floor = expected_last - 2;
        let txn = store.db.begin_ro_txn().expect("ro txn");
        let table = txn.open_table(None).expect("table");
        assert_eq!(
            read_optional_u64(
                &txn,
                &table,
                super::keys::META_SYNC_TAIL_RETAINED_FLOOR_KEY.to_vec(),
                "retained floor",
            )
            .expect("read floor"),
            Some(expected_floor)
        );
        for op_id in 1..=expected_floor {
            assert!(txn
                .get::<Vec<u8>>(&table, &sync_tail_op_key(op_id))
                .expect("read pruned op")
                .is_none());
            assert!(txn
                .get::<Vec<u8>>(&table, &super::keys::sync_tail_refs_key(op_id))
                .expect("read pruned refs")
                .is_none());
            assert!(txn
                .get::<Vec<u8>>(
                    &table,
                    &super::keys::sync_tail_index_entry_key(&object_index, op_id),
                )
                .expect("read pruned index entry")
                .is_none());
        }
        for op_id in expected_floor + 1..=expected_last {
            assert!(txn
                .get::<Vec<u8>>(&table, &sync_tail_op_key(op_id))
                .expect("read retained op")
                .is_some());
        }
    }
}

#[test]
fn initial_snapshot_can_seed_current_state_without_tail_history() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let snapshot_batch = ReplicationCommitBatch {
        transaction_id: 10,
        begin_final_lsn: PostgresLsn(300),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(301),
        end_lsn: PostgresLsn(302),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data("task-snapshot-1", "org-a", "project-a"),
                Lsn::from(302_u64),
            ),
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data("task-snapshot-2", "org-a", "project-a"),
                Lsn::from(302_u64),
            ),
        ],
    };

    store
        .persist_batch_with_plan_and_options(
            &snapshot_batch,
            plan,
            PersistBatchOptions {
                persist_raw_batch: false,
                assume_new_inserts: true,
                snapshot_without_tail: true,
            },
        )
        .expect("persist snapshot batch");

    let metrics = store.metrics_snapshot();
    assert_eq!(metrics.tail_ops_written, 0);
    assert_eq!(metrics.tail_index_entries_written, 0);
    assert_eq!(store.task_tail_last_op_id().expect("last op"), Some(2));
    let object_index = sync_tail_index_keys_for_bucket("tasks", &BTreeMap::new());
    assert_eq!(
        store
            .indexed_task_tail_last_op_id(&object_index)
            .expect("indexed latest"),
        2
    );
    assert_eq!(
        store
            .load_indexed_task_tail_ops_since(&object_index, 0)
            .expect("snapshot tail")
            .ops
            .len(),
        0,
        "initial snapshot rows should not be duplicated as tail history"
    );

    let live_batch = ReplicationCommitBatch {
        transaction_id: 11,
        begin_final_lsn: PostgresLsn(303),
        begin_commit_time_micros: 3,
        commit_lsn: PostgresLsn(304),
        end_lsn: PostgresLsn(305),
        commit_time_micros: 4,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "tasks",
            7,
            task_row_data("task-live-1", "org-a", "project-a"),
            Lsn::from(305_u64),
        )],
    };
    store
        .persist_batch(&live_batch)
        .expect("persist live batch");

    let live_tail = store
        .load_indexed_task_tail_ops_since(&object_index, 2)
        .expect("live tail");
    assert_eq!(live_tail.latest_op_id, 3);
    assert_eq!(live_tail.ops.len(), 1);
    assert_eq!(live_tail.ops[0].op_id, 3);
    assert_eq!(live_tail.ops[0].object_id.as_deref(), Some("task-live-1"));
}

#[test]
fn indexed_bucket_reads_preserve_global_op_ids_across_gaps_and_unions() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 12,
        begin_final_lsn: PostgresLsn(310),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(311),
        end_lsn: PostgresLsn(312),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data("task-a-1", "org-a", "project-a"),
                Lsn::from(312_u64),
            ),
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data("task-b-1", "org-b", "project-b"),
                Lsn::from(312_u64),
            ),
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data("task-a-2", "org-a", "project-a"),
                Lsn::from(312_u64),
            ),
        ],
    };
    store.persist_batch(&batch).expect("persist batch");

    let project_a_indexes = sync_tail_index_keys_for_bucket(
        "tasks",
        &BTreeMap::from([("project_id".to_owned(), "project-a".to_owned())]),
    );
    let project_a = store
        .load_indexed_task_tail_ops_since(&project_a_indexes, 0)
        .expect("project-a indexed ops");
    assert_eq!(project_a.latest_op_id, 3);
    assert_eq!(
        project_a.ops.iter().map(|op| op.op_id).collect::<Vec<_>>(),
        vec![1, 3]
    );

    let after_gap = store
        .load_indexed_task_tail_ops_since(&project_a_indexes, 1)
        .expect("project-a ops after global cursor 1");
    assert_eq!(after_gap.latest_op_id, 3);
    assert_eq!(
        after_gap.ops.iter().map(|op| op.op_id).collect::<Vec<_>>(),
        vec![3]
    );

    let mut overlapping_indexes = sync_tail_index_keys_for_bucket("tasks", &BTreeMap::new());
    overlapping_indexes.extend(project_a_indexes);
    let union = store
        .load_indexed_task_tail_ops_since(&overlapping_indexes, 0)
        .expect("overlapping index union");
    assert_eq!(
        union.ops.iter().map(|op| op.op_id).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "overlapping indexes must be sorted and deduplicated by global op id"
    );
}

#[test]
fn initial_snapshot_completion_marker_signals_idempotency_across_restart() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();

    assert!(
        !store
            .is_initial_snapshot_complete()
            .expect("read completion marker"),
        "a fresh store must not report a completed initial snapshot"
    );

    store
        .persist_initial_snapshot_marker_with_plan(PostgresLsn(402), plan, "test-source")
        .expect("persist snapshot marker");

    assert!(
        store
            .is_initial_snapshot_complete()
            .expect("read completion marker"),
        "after the snapshot marker is persisted the snapshot must report complete so a restart skips re-running it"
    );
    assert_eq!(
        store
            .initial_snapshot_source_identity()
            .expect("read source identity")
            .as_deref(),
        Some("test-source")
    );
}

#[test]
fn interrupted_snapshot_reset_is_owned_atomic_and_moves_to_a_new_cursor_epoch() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let intent = "v1:owned-bootstrap";

    store
        .reset_incomplete_initial_snapshot(intent, plan)
        .expect("start owned bootstrap");
    let first_base = store
        .task_tail_last_op_id()
        .expect("first cursor base")
        .expect("seeded cursor base");
    assert_eq!(
        store
            .initial_snapshot_bootstrap_intent()
            .expect("bootstrap intent")
            .as_deref(),
        Some(intent)
    );

    store
        .persist_initial_snapshot_rows_with_plan(
            "tasks",
            vec![task_row_data(
                "partial-snapshot",
                "org-partial",
                "project-partial",
            )],
            PostgresLsn(0),
            plan,
        )
        .expect("persist partial snapshot");
    let partial_tail = store
        .task_tail_last_op_id()
        .expect("partial cursor")
        .expect("partial cursor value");
    assert!(partial_tail > first_base);
    assert_eq!(
        store
            .load_current_documents()
            .expect("partial current documents")
            .len(),
        1
    );

    store
        .reset_incomplete_initial_snapshot(intent, plan)
        .expect("recover interrupted bootstrap");
    let recovered_base = store
        .task_tail_last_op_id()
        .expect("recovered cursor base")
        .expect("recovered cursor base value");
    assert!(recovered_base > partial_tail);
    assert!(store
        .load_current_documents()
        .expect("reset current documents")
        .is_empty());
    assert_eq!(store.last_persisted_end_lsn().expect("reset LSN"), None);
    assert!(!store
        .is_initial_snapshot_complete()
        .expect("reset completion marker"));

    store
        .persist_initial_snapshot_marker_with_plan(PostgresLsn(500), plan, "test-source")
        .expect("complete recovered snapshot");
    assert_eq!(
        store
            .initial_snapshot_bootstrap_intent()
            .expect("cleared bootstrap intent"),
        None
    );
    assert!(store
        .is_initial_snapshot_complete()
        .expect("completion marker"));
}

#[test]
fn direct_initial_snapshot_rows_seed_current_state_without_tail_history() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();

    store
        .persist_initial_snapshot_rows_with_plan(
            "tasks",
            vec![
                task_row_data("task-direct-snapshot-1", "org-a", "project-a"),
                task_row_data("task-direct-snapshot-2", "org-a", "project-a"),
            ],
            PostgresLsn(0),
            plan,
        )
        .expect("persist direct snapshot rows");
    store
        .persist_initial_snapshot_marker_with_plan(PostgresLsn(402), plan, "test-source")
        .expect("persist direct snapshot marker");

    let metrics = store.metrics_snapshot();
    assert_eq!(metrics.rows_seen, 2);
    assert_eq!(metrics.rows_synced, 2);
    assert_eq!(metrics.tail_ops_written, 0);
    assert_eq!(metrics.tail_index_entries_written, 0);
    assert_eq!(
        store.last_persisted_end_lsn().expect("lsn"),
        Some(PostgresLsn(402))
    );

    let object_index = sync_tail_index_keys_for_bucket("tasks", &BTreeMap::new());
    assert_eq!(
        store
            .indexed_task_tail_last_op_id(&object_index)
            .expect("indexed latest"),
        2
    );
    assert_eq!(
        store
            .load_indexed_task_tail_ops_since(&object_index, 0)
            .expect("snapshot tail")
            .ops
            .len(),
        0,
        "direct initial snapshot rows should not be duplicated as tail history"
    );

    let current = store.load_current_documents().expect("current docs");
    assert_eq!(current.len(), 2);
    assert_eq!(store.task_tail_last_op_id().expect("last op"), Some(2));
    let project_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name("project-a"))
        .expect("project bucket");
    let current_accumulator = store
        .current_checkpoint_accumulator_for_bucket(
            &sync_current_checkpoint_accumulator_keys_for_bucket(&project_bucket),
        )
        .expect("current accumulator");
    assert_eq!(current_accumulator.count, 2);
}

#[test]
fn mdbx_store_rejects_truncate_before_persisting_the_transaction() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 12,
        begin_final_lsn: PostgresLsn(300),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(301),
        end_lsn: PostgresLsn(302),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-insert")),
                    ("org_id", ColumnValue::text("org-001")),
                    ("project_id", ColumnValue::text("project-org-001-0001")),
                    ("title", ColumnValue::text("Runtime insert")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("3")),
                    ("assignee_id", ColumnValue::text("user-org-001-0001")),
                    ("story_points", ColumnValue::text("5")),
                    ("updated_at", ColumnValue::text("2026-01-03T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:insert")),
                ]),
                Lsn::from(302_u64),
            ),
            ChangeEvent::delete(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![("id", ColumnValue::text("task-sentinel-delete-0001"))]),
                ReplicaIdentity::Default,
                vec![Arc::from("id")],
                Lsn::from(302_u64),
            ),
            ChangeEvent::truncate(vec![Arc::from("public.tasks")], Lsn::from(302_u64)),
        ],
    };

    assert_eq!(
        store.persist_batch(&batch),
        Err(ReplicationIngestError::UnsupportedPgoutputMessage(
            "truncate on a materialized table"
        ))
    );
    assert_eq!(store.task_tail_last_op_id().expect("tail metadata"), None);
    assert!(store
        .load_current_task_rows()
        .expect("task state rows")
        .is_empty());
    assert_eq!(
        store.last_persisted_end_lsn().expect("commit metadata"),
        None
    );
}

#[test]
fn mdbx_store_backfills_sync_tail_indexes_for_legacy_tail_ops() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let legacy_op = PersistedSyncTailOp {
        op_id: 1,
        operation: PersistedSyncTailOperation::Put,
        object_type: Some("tasks".to_owned()),
        object_id: Some("task-legacy".to_owned()),
        route_fields: BTreeMap::from([("project_id".to_owned(), "project-legacy".to_owned())]),
        data_json: Some(
            serde_json::json!({"id": "task-legacy", "project_id": "project-legacy"}).to_string(),
        ),
        previous_route_fields: None,
        previous_data_json: None,
    };

    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("default table");
    txn.put(
        &table,
        sync_tail_op_key(1),
        serde_json::to_vec(&legacy_op).expect("legacy op json"),
        WriteFlags::UPSERT,
    )
    .expect("legacy op put");
    txn.put(
        &table,
        META_SYNC_TAIL_LAST_OP_ID_KEY,
        "1",
        WriteFlags::UPSERT,
    )
    .expect("legacy tail metadata put");
    txn.commit().expect("legacy commit");

    let index_keys = sync_tail_index_keys_for_bucket(
        "tasks",
        &BTreeMap::from([("project_id".to_owned(), "project-legacy".to_owned())]),
    );
    assert_eq!(
        store
            .indexed_task_tail_last_op_id(&index_keys)
            .expect("indexed last op id"),
        1
    );
    let indexed = store
        .load_indexed_task_tail_ops_since(&index_keys, 0)
        .expect("indexed ops");
    assert_eq!(indexed.latest_op_id, 1);
    assert_eq!(indexed.ops, vec![legacy_op]);

    let txn = store.db.begin_ro_txn().expect("ro txn");
    let table = txn.open_table(None).expect("default table");
    assert_eq!(
        read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY.to_vec(),
            "sync tail indexed-through op id"
        )
        .expect("indexed-through metadata"),
        Some(1)
    );
}

#[test]
fn numeric_metadata_round_trips_at_eight_digit_op_ids() {
    // Regression: an op id with exactly 8 decimal digits (10_000_000..=99_999_999)
    // is also 8 bytes wide, so a length-sniffing decoder misread the decimal-string
    // metadata as big-endian. The two encodings are now decoded explicitly.
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let eight_digit: u64 = 12_345_678;

    {
        let txn = store.db.begin_rw_txn().expect("rw txn");
        let table = txn.open_table(None).expect("default table");
        // Op-id counters are persisted as decimal text...
        txn.put(
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY,
            eight_digit.to_string(),
            WriteFlags::UPSERT,
        )
        .expect("put decimal metadata");
        // ...sync-tail index entries as fixed 8-byte big-endian.
        txn.put(
            &table,
            b"meta:test_be_entry".as_slice(),
            eight_digit.to_be_bytes(),
            WriteFlags::UPSERT,
        )
        .expect("put big-endian entry");
        txn.commit().expect("commit");
    }

    let txn = store.db.begin_ro_txn().expect("ro txn");
    let table = txn.open_table(None).expect("default table");
    assert_eq!(
        read_optional_u64(
            &txn,
            &table,
            META_SYNC_TAIL_LAST_OP_ID_KEY.to_vec(),
            "sync tail last op id",
        )
        .expect("decimal metadata"),
        Some(eight_digit),
        "8-digit decimal metadata must not be misread as big-endian",
    );
    assert_eq!(
        read_optional_be_u64(
            &txn,
            &table,
            b"meta:test_be_entry".to_vec(),
            "test be entry"
        )
        .expect("big-endian entry"),
        Some(eight_digit),
    );
}

#[test]
fn mdbx_store_backfill_recomputes_checkpoint_accumulators_for_multiple_ops_in_same_bucket() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let table_plan = plan.table_plan("tasks").expect("tasks table plan");
    let project_id = "project-backfill-accumulator";
    let route_fields = BTreeMap::from([("project_id".to_owned(), project_id.to_owned())]);
    let old_task_1 = task_row_data_with_title(
        "task-backfill-accumulator-1",
        "org-backfill-accumulator",
        project_id,
        "old title",
    );
    let new_task_1 = task_row_data_with_title(
        "task-backfill-accumulator-1",
        "org-backfill-accumulator",
        project_id,
        "new title",
    );
    let old_task_2 = task_row_data_with_title(
        "task-backfill-accumulator-2",
        "org-backfill-accumulator",
        project_id,
        "delete me",
    );
    let old_task_1_json = table_plan
        .serialize_full_row_json(&old_task_1)
        .expect("old task 1 json");
    let new_task_1_json = table_plan
        .serialize_full_row_json(&new_task_1)
        .expect("new task 1 json");
    let old_task_2_json = table_plan
        .serialize_full_row_json(&old_task_2)
        .expect("old task 2 json");
    let ops = vec![
        PersistedSyncTailOp {
            op_id: 1,
            operation: PersistedSyncTailOperation::Put,
            object_type: Some("tasks".to_owned()),
            object_id: Some("task-backfill-accumulator-1".to_owned()),
            route_fields: route_fields.clone(),
            data_json: Some(new_task_1_json),
            previous_route_fields: Some(route_fields.clone()),
            previous_data_json: Some(old_task_1_json.clone()),
        },
        PersistedSyncTailOp {
            op_id: 2,
            operation: PersistedSyncTailOperation::Remove,
            object_type: Some("tasks".to_owned()),
            object_id: Some("task-backfill-accumulator-2".to_owned()),
            route_fields: route_fields.clone(),
            data_json: Some(old_task_2_json.clone()),
            previous_route_fields: None,
            previous_data_json: None,
        },
    ];
    persist_legacy_tail_ops(&store, &ops);

    let index_keys = sync_tail_index_keys_for_bucket("tasks", &route_fields);
    let indexed = store
        .load_indexed_task_tail_ops_since(&index_keys, 0)
        .expect("indexed ops");
    assert_eq!(indexed.ops, ops);

    let project_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(project_id))
        .expect("project bucket");
    let accumulator = store
        .checkpoint_accumulator_for_bucket(&sync_tail_checkpoint_accumulator_keys_for_bucket(
            &project_bucket,
        ))
        .expect("checkpoint accumulator");
    let expected_checksum = put_checksum("tasks", "task-backfill-accumulator-1", &old_task_1_json)
        .wrapping_add(put_checksum(
            "tasks",
            "task-backfill-accumulator-2",
            &old_task_2_json,
        ))
        .wrapping_add(remove_checksum(&source_subkey_for_object(
            "tasks",
            "task-backfill-accumulator-2",
        )));

    assert_eq!(accumulator.count, 3);
    assert_eq!(accumulator.checksum, expected_checksum);
}

#[test]
fn mdbx_store_persists_project_docs_for_org_bucket_materialization() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 13,
        begin_final_lsn: PostgresLsn(320),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(321),
        end_lsn: PostgresLsn(322),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "projects",
            8,
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("project-runtime-1")),
                ("org_id", ColumnValue::text("org-001")),
                ("code", ColumnValue::text("PRJ-001")),
                ("name", ColumnValue::text("Runtime project")),
                ("status", ColumnValue::text("active")),
                ("priority", ColumnValue::text("4")),
                ("owner_id", ColumnValue::text("user-org-001")),
                ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                ("summary", ColumnValue::text("runtime:project")),
            ]),
            Lsn::from(322_u64),
        )],
    };

    store.persist_batch(&batch).expect("persist");

    let ops = store.load_task_tail_ops_since(0).expect("sync tail ops");
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].object_type.as_deref(), Some("projects"));
    assert_eq!(
        ops[0].route_fields.get("org_id").map(String::as_str),
        Some("org-001")
    );
    assert_eq!(
        ops[0].route_fields.get("owner_id").map(String::as_str),
        Some("user-org-001")
    );

    let documents = store
        .load_current_documents()
        .expect("sync state documents");
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].object_type, "projects");
    assert_eq!(
        documents[0].route_fields.get("org_id").map(String::as_str),
        Some("org-001")
    );
    assert_eq!(
        documents[0]
            .route_fields
            .get("owner_id")
            .map(String::as_str),
        Some("user-org-001")
    );
}

#[test]
fn mdbx_store_persists_comment_docs_for_task_bucket_materialization() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 14,
        begin_final_lsn: PostgresLsn(330),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(331),
        end_lsn: PostgresLsn(332),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "comments",
            8,
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("comment-runtime-1")),
                ("org_id", ColumnValue::text("org-001")),
                ("task_id", ColumnValue::text("task-001")),
                ("owner_id", ColumnValue::text("user-owner-1")),
                ("author_id", ColumnValue::text("user-author-1")),
                ("body", ColumnValue::text("runtime:comment")),
                ("created_at", ColumnValue::text("2026-04-12T00:00:00Z")),
                ("updated_at", ColumnValue::text("2026-04-12T00:00:01Z")),
            ]),
            Lsn::from(332_u64),
        )],
    };

    store.persist_batch(&batch).expect("persist");

    let ops = store.load_task_tail_ops_since(0).expect("sync tail ops");
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].object_type.as_deref(), Some("comments"));
    assert_eq!(
        ops[0].route_fields.get("task_id").map(String::as_str),
        Some("task-001")
    );
    assert_eq!(
        ops[0].route_fields.get("org_id").map(String::as_str),
        Some("org-001")
    );

    let documents = store
        .load_current_documents()
        .expect("sync state documents");
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].object_type, "comments");
    assert_eq!(
        documents[0].route_fields.get("task_id").map(String::as_str),
        Some("task-001")
    );
    assert_eq!(
        documents[0].route_fields.get("org_id").map(String::as_str),
        Some("org-001")
    );
}

#[test]
fn mdbx_store_persists_membership_docs_for_org_bucket_materialization() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 15,
        begin_final_lsn: PostgresLsn(340),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(341),
        end_lsn: PostgresLsn(342),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "memberships",
            8,
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("membership-runtime-1")),
                ("org_id", ColumnValue::text("org-001")),
                ("user_id", ColumnValue::text("user-001")),
                ("owner_id", ColumnValue::text("user-owner-1")),
                ("role", ColumnValue::text("member")),
                ("display_name", ColumnValue::text("Runtime Member")),
                ("email", ColumnValue::text("runtime@example.com")),
                ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
            ]),
            Lsn::from(342_u64),
        )],
    };

    store.persist_batch(&batch).expect("persist");

    let ops = store.load_task_tail_ops_since(0).expect("sync tail ops");
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].object_type.as_deref(), Some("memberships"));
    assert_eq!(
        ops[0].route_fields.get("org_id").map(String::as_str),
        Some("org-001")
    );

    let documents = store
        .load_current_documents()
        .expect("sync state documents");
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].object_type, "memberships");
    assert_eq!(
        documents[0].route_fields.get("org_id").map(String::as_str),
        Some("org-001")
    );
}

#[test]
fn mdbx_store_persists_organization_docs_for_region_bucket_materialization() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 16,
        begin_final_lsn: PostgresLsn(350),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(351),
        end_lsn: PostgresLsn(352),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![ChangeEvent::insert(
            "public",
            "organizations",
            8,
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("org-runtime-1")),
                ("name", ColumnValue::text("Runtime Org")),
                ("owner_id", ColumnValue::text("user-owner-1")),
                ("plan", ColumnValue::text("enterprise")),
                ("region", ColumnValue::text("eu-west-1")),
                ("updated_at", ColumnValue::text("2026-04-12T00:00:00Z")),
            ]),
            Lsn::from(352_u64),
        )],
    };

    store.persist_batch(&batch).expect("persist");

    let ops = store.load_task_tail_ops_since(0).expect("sync tail ops");
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].object_type.as_deref(), Some("organizations"));
    assert_eq!(
        ops[0].route_fields.get("region").map(String::as_str),
        Some("eu-west-1")
    );

    let documents = store
        .load_current_documents()
        .expect("sync state documents");
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].object_type, "organizations");
    assert_eq!(
        documents[0].route_fields.get("region").map(String::as_str),
        Some("eu-west-1")
    );
}

#[tokio::test]
async fn mdbx_store_waits_for_task_tail_advance_without_polling() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::shared(directory.path()).expect("shared store");
    let store_for_persist = Arc::clone(&store);

    let persist = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let batch = ReplicationCommitBatch {
            transaction_id: 17,
            begin_final_lsn: PostgresLsn(350),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(351),
            end_lsn: PostgresLsn(352),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-notify")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime notify")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("1")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("2")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:notify")),
                ]),
                Lsn::from(352_u64),
            )],
        };
        store_for_persist
            .persist_batch(&batch)
            .expect("persist batch");
    });

    let latest = store
        .wait_for_task_tail_advance(0, Duration::from_millis(250))
        .await
        .expect("wait for task tail advance should succeed");

    persist.await.expect("persist task should join");
    assert_eq!(latest, Some(1));
}

#[test]
fn mdbx_store_materializes_current_task_rows_without_fixture_seed() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 18,
        begin_final_lsn: PostgresLsn(400),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(401),
        end_lsn: PostgresLsn(402),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-1")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime one")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("1")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("2")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:1")),
                ]),
                Lsn::from(402_u64),
            ),
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-runtime-2")),
                    ("org_id", ColumnValue::text("org-runtime")),
                    ("project_id", ColumnValue::text("project-runtime")),
                    ("title", ColumnValue::text("Runtime two")),
                    ("status", ColumnValue::text("done")),
                    ("priority", ColumnValue::text("2")),
                    ("assignee_id", ColumnValue::text("user-runtime")),
                    ("story_points", ColumnValue::text("3")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:01Z")),
                    ("summary", ColumnValue::text("runtime:2")),
                ]),
                Lsn::from(402_u64),
            ),
        ],
    };

    store.persist_batch(&batch).expect("persist");

    let rows = store.load_current_task_rows().expect("task state rows");
    let ids = rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>();
    assert_eq!(ids, vec!["task-runtime-1", "task-runtime-2"]);
    assert!(
        rows.iter().all(|row| !row.id.starts_with("task-org-")),
        "task state snapshot should not rely on fixture-seeded benchmark rows"
    );

    let documents = store
        .load_current_documents()
        .expect("task state documents");
    assert_eq!(
        documents
            .iter()
            .map(|document| (
                document.object_type.as_str(),
                document.object_id.as_str(),
                document.route_fields.get("project_id").map(String::as_str)
            ))
            .collect::<Vec<_>>(),
        vec![
            ("tasks", "task-runtime-1", Some("project-runtime")),
            ("tasks", "task-runtime-2", Some("project-runtime")),
        ]
    );
}

#[test]
fn mdbx_store_uses_exact_route_indexes_for_current_and_tail() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let batch = ReplicationCommitBatch {
        transaction_id: 19,
        begin_final_lsn: PostgresLsn(410),
        begin_commit_time_micros: 1,
        commit_lsn: PostgresLsn(411),
        end_lsn: PostgresLsn(412),
        commit_time_micros: 2,
        column_types_by_table: BTreeMap::new(),
        changes: vec![
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-route-a")),
                    ("org_id", ColumnValue::text("org-route")),
                    ("project_id", ColumnValue::text("project-a")),
                    ("title", ColumnValue::text("Route A")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("1")),
                    ("assignee_id", ColumnValue::text("user-route")),
                    ("story_points", ColumnValue::text("2")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:00Z")),
                    ("summary", ColumnValue::text("runtime:route:a")),
                ]),
                Lsn::from(412_u64),
            ),
            ChangeEvent::insert(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![
                    ("id", ColumnValue::text("task-route-b")),
                    ("org_id", ColumnValue::text("org-route")),
                    ("project_id", ColumnValue::text("project-b")),
                    ("title", ColumnValue::text("Route B")),
                    ("status", ColumnValue::text("todo")),
                    ("priority", ColumnValue::text("1")),
                    ("assignee_id", ColumnValue::text("user-route")),
                    ("story_points", ColumnValue::text("2")),
                    ("updated_at", ColumnValue::text("2026-04-11T00:00:01Z")),
                    ("summary", ColumnValue::text("runtime:route:b")),
                ]),
                Lsn::from(412_u64),
            ),
        ],
    };

    store.persist_batch(&batch).expect("persist");

    let plan = execution_plan();
    let project_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name("project-a"))
        .expect("project bucket");
    let org_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::org_tasks_bucket_name("org-route"))
        .expect("org bucket");
    let project_documents = store
        .load_current_documents_for_bucket(&project_bucket)
        .expect("project current documents");
    let org_documents = store
        .load_current_documents_for_bucket(&org_bucket)
        .expect("org current documents");

    assert_eq!(
        project_documents
            .iter()
            .map(|document| document.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-route-a"]
    );
    assert_eq!(org_documents.len(), 2);
    assert_eq!(
        store.metrics_snapshot().tail_index_entries_written,
        6,
        "two task puts should write object + project + org indexes, not all route-field subsets"
    );
}

#[test]
fn mdbx_store_current_route_index_values_cover_routed_document_reads() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();

    store
        .persist_initial_snapshot_rows_with_plan(
            "tasks",
            vec![
                task_row_data("task-covering-route-a", "org-covering", "project-covering"),
                task_row_data("task-covering-route-b", "org-covering", "project-covering"),
            ],
            PostgresLsn(0),
            plan,
        )
        .expect("persist snapshot rows");
    let metrics = store.metrics_snapshot();
    assert!(metrics.current_index_puts > 0);
    assert!(metrics.current_index_value_bytes >= metrics.current_index_puts);

    let route_constraints =
        BTreeMap::from([("project_id".to_owned(), "project-covering".to_owned())]);
    let route_key_b = current_route_index_key("tasks", &route_constraints, "task-covering-route-b");

    {
        let txn = store.db.begin_rw_txn().expect("rw txn");
        let table = txn.open_table(None).expect("table");
        txn.del(
            &table,
            current_doc_key("tasks", "task-covering-route-a"),
            None,
        )
        .expect("delete primary current doc");
        txn.put(&table, route_key_b, [], WriteFlags::UPSERT)
            .expect("simulate legacy empty route index value");
        txn.commit().expect("commit");
    }

    let bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(
            "project-covering",
        ))
        .expect("project bucket");
    let mut documents = store
        .load_current_documents_for_bucket(&bucket)
        .expect("current documents");
    documents.sort_by(|left, right| left.object_id.cmp(&right.object_id));

    assert_eq!(
        documents
            .iter()
            .map(|document| document.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-covering-route-a", "task-covering-route-b"]
    );
    assert_eq!(
        store
            .current_document_count_for_bucket(&bucket)
            .expect("current count"),
        2
    );
}

#[test]
fn mdbx_store_update_moves_current_document_between_project_buckets() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let table_plan = plan.table_plan("tasks").expect("tasks table plan");
    let old_task = task_row_data_with_title(
        "task-current-route-move",
        "org-current-route",
        "project-current-route-a",
        "old project",
    );
    let new_task = task_row_data_with_title(
        "task-current-route-move",
        "org-current-route",
        "project-current-route-b",
        "new project",
    );
    let new_task_json = table_plan
        .serialize_full_row_json(&new_task)
        .expect("new task json");

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 24,
            begin_final_lsn: PostgresLsn(440),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(441),
            end_lsn: PostgresLsn(442),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                old_task,
                Lsn::from(442_u64),
            )],
        })
        .expect("persist initial row");
    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 25,
            begin_final_lsn: PostgresLsn(443),
            begin_commit_time_micros: 3,
            commit_lsn: PostgresLsn(444),
            end_lsn: PostgresLsn(445),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::update(
                "public",
                "tasks",
                7,
                None,
                new_task,
                pg_walstream::ReplicaIdentity::Default,
                Vec::new(),
                Lsn::from(445_u64),
            )],
        })
        .expect("persist update");

    let old_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(
            "project-current-route-a",
        ))
        .expect("old project bucket");
    let new_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(
            "project-current-route-b",
        ))
        .expect("new project bucket");

    assert!(store
        .load_current_documents_for_bucket(&old_bucket)
        .expect("old project docs")
        .is_empty());
    let new_documents = store
        .load_current_documents_for_bucket(&new_bucket)
        .expect("new project docs");
    assert_eq!(new_documents.len(), 1);
    assert_eq!(new_documents[0].object_id, "task-current-route-move");
    assert_eq!(
        store
            .current_checkpoint_accumulator_for_bucket(
                &sync_current_checkpoint_accumulator_keys_for_bucket(&old_bucket),
            )
            .expect("old current accumulator"),
        PersistedCheckpointAccumulator::default()
    );
    assert_eq!(
        store
            .current_checkpoint_accumulator_for_bucket(
                &sync_current_checkpoint_accumulator_keys_for_bucket(&new_bucket),
            )
            .expect("new current accumulator"),
        PersistedCheckpointAccumulator {
            count: 1,
            checksum: put_checksum("tasks", "task-current-route-move", &new_task_json),
        }
    );
}

#[test]
fn mdbx_store_delete_clears_materialized_current_document_bucket_state() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let project_id = "project-current-delete";
    let old_task = task_row_data_with_title(
        "task-current-delete",
        "org-current-delete",
        project_id,
        "delete me",
    );

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 26,
            begin_final_lsn: PostgresLsn(446),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(447),
            end_lsn: PostgresLsn(448),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                old_task,
                Lsn::from(448_u64),
            )],
        })
        .expect("persist initial row");
    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 27,
            begin_final_lsn: PostgresLsn(449),
            begin_commit_time_micros: 3,
            commit_lsn: PostgresLsn(450),
            end_lsn: PostgresLsn(451),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::delete(
                "public",
                "tasks",
                7,
                RowData::from_pairs(vec![("id", ColumnValue::text("task-current-delete"))]),
                pg_walstream::ReplicaIdentity::Default,
                vec![Arc::from("id")],
                Lsn::from(451_u64),
            )],
        })
        .expect("persist delete");

    let project_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(project_id))
        .expect("project bucket");
    assert!(store
        .load_current_documents_for_bucket(&project_bucket)
        .expect("project docs")
        .is_empty());
    assert_eq!(
        store
            .current_checkpoint_accumulator_for_bucket(
                &sync_current_checkpoint_accumulator_keys_for_bucket(&project_bucket),
            )
            .expect("current accumulator"),
        PersistedCheckpointAccumulator::default()
    );
}

#[test]
fn mdbx_store_persists_checkpoint_accumulators_for_superseded_bucket_entries() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let table_plan = plan.table_plan("tasks").expect("tasks table plan");
    let old_task_1 = task_row_data_with_title(
        "task-accumulator-1",
        "org-accumulator",
        "project-accumulator",
        "old title",
    );
    let old_task_2 = task_row_data_with_title(
        "task-accumulator-2",
        "org-accumulator",
        "project-accumulator",
        "delete me",
    );
    let new_task_1 = task_row_data_with_title(
        "task-accumulator-1",
        "org-accumulator",
        "project-accumulator",
        "new title",
    );
    let old_task_1_json = table_plan
        .serialize_full_row_json(&old_task_1)
        .expect("old task 1 json");
    let old_task_2_json = table_plan
        .serialize_full_row_json(&old_task_2)
        .expect("old task 2 json");

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 20,
            begin_final_lsn: PostgresLsn(420),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(421),
            end_lsn: PostgresLsn(422),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert("public", "tasks", 7, old_task_1, Lsn::from(422_u64)),
                ChangeEvent::insert("public", "tasks", 7, old_task_2.clone(), Lsn::from(422_u64)),
            ],
        })
        .expect("persist initial rows");
    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 21,
            begin_final_lsn: PostgresLsn(423),
            begin_commit_time_micros: 3,
            commit_lsn: PostgresLsn(424),
            end_lsn: PostgresLsn(425),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::update(
                    "public",
                    "tasks",
                    7,
                    None,
                    new_task_1,
                    pg_walstream::ReplicaIdentity::Default,
                    Vec::new(),
                    Lsn::from(425_u64),
                ),
                ChangeEvent::delete(
                    "public",
                    "tasks",
                    7,
                    old_task_2,
                    pg_walstream::ReplicaIdentity::Default,
                    Vec::new(),
                    Lsn::from(425_u64),
                ),
            ],
        })
        .expect("persist update/delete");

    let project_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(
            "project-accumulator",
        ))
        .expect("project bucket");
    let accumulator = store
        .checkpoint_accumulator_for_bucket(&sync_tail_checkpoint_accumulator_keys_for_bucket(
            &project_bucket,
        ))
        .expect("checkpoint accumulator");
    let expected_checksum = put_checksum("tasks", "task-accumulator-1", &old_task_1_json)
        .wrapping_add(put_checksum(
            "tasks",
            "task-accumulator-2",
            &old_task_2_json,
        ))
        .wrapping_add(remove_checksum(&source_subkey_for_object(
            "tasks",
            "task-accumulator-2",
        )));

    assert_eq!(accumulator.count, 3);
    assert_eq!(accumulator.checksum, expected_checksum);
}

#[test]
fn mdbx_store_persists_current_checkpoint_accumulators() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let plan = execution_plan();
    let table_plan = plan.table_plan("tasks").expect("tasks table plan");
    let old_task_1 = task_row_data_with_title(
        "task-current-accumulator-1",
        "org-current-accumulator",
        "project-current-accumulator",
        "old title",
    );
    let old_task_2 = task_row_data_with_title(
        "task-current-accumulator-2",
        "org-current-accumulator",
        "project-current-accumulator",
        "delete me",
    );
    let new_task_1 = task_row_data_with_title(
        "task-current-accumulator-1",
        "org-current-accumulator",
        "project-current-accumulator",
        "new title",
    );
    let new_task_1_json = table_plan
        .serialize_full_row_json(&new_task_1)
        .expect("new task 1 json");

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 22,
            begin_final_lsn: PostgresLsn(430),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(431),
            end_lsn: PostgresLsn(432),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::insert("public", "tasks", 7, old_task_1, Lsn::from(432_u64)),
                ChangeEvent::insert("public", "tasks", 7, old_task_2.clone(), Lsn::from(432_u64)),
            ],
        })
        .expect("persist initial rows");
    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 23,
            begin_final_lsn: PostgresLsn(433),
            begin_commit_time_micros: 3,
            commit_lsn: PostgresLsn(434),
            end_lsn: PostgresLsn(435),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: vec![
                ChangeEvent::update(
                    "public",
                    "tasks",
                    7,
                    None,
                    new_task_1,
                    pg_walstream::ReplicaIdentity::Default,
                    Vec::new(),
                    Lsn::from(435_u64),
                ),
                ChangeEvent::delete(
                    "public",
                    "tasks",
                    7,
                    old_task_2,
                    pg_walstream::ReplicaIdentity::Default,
                    Vec::new(),
                    Lsn::from(435_u64),
                ),
            ],
        })
        .expect("persist update/delete");

    let project_bucket = plan
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(
            "project-current-accumulator",
        ))
        .expect("project bucket");
    let accumulator = store
        .current_checkpoint_accumulator_for_bucket(
            &sync_current_checkpoint_accumulator_keys_for_bucket(&project_bucket),
        )
        .expect("current checkpoint accumulator");

    assert_eq!(accumulator.count, 1);
    assert_eq!(
        accumulator.checksum,
        put_checksum("tasks", "task-current-accumulator-1", &new_task_1_json)
    );
}

#[test]
fn bucket_read_budget_charges_indexed_ops_not_global_delta_per_bucket() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let projects = [
        "project-budget-a",
        "project-budget-b",
        "project-budget-c",
        "project-budget-d",
    ];

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 30,
            begin_final_lsn: PostgresLsn(500),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(501),
            end_lsn: PostgresLsn(502),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: projects
                .iter()
                .enumerate()
                .map(|(project_index, project_id)| {
                    ChangeEvent::insert(
                        "public",
                        "tasks",
                        7,
                        task_row_data(
                            &format!("task-budget-{project_index}-baseline"),
                            "org-budget",
                            project_id,
                        ),
                        Lsn::from(502_u64),
                    )
                })
                .collect(),
        })
        .expect("persist baseline");
    let after = store
        .task_tail_last_op_id()
        .expect("tail metadata")
        .expect("baseline tail cursor");
    assert_eq!(after, 4);

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 31,
            begin_final_lsn: PostgresLsn(503),
            begin_commit_time_micros: 3,
            commit_lsn: PostgresLsn(504),
            end_lsn: PostgresLsn(505),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: projects
                .iter()
                .enumerate()
                .flat_map(|(project_index, project_id)| {
                    (0..3).map(move |task_index| {
                        ChangeEvent::insert(
                            "public",
                            "tasks",
                            7,
                            task_row_data(
                                &format!("task-budget-{project_index}-tail-{task_index}"),
                                "org-budget",
                                project_id,
                            ),
                            Lsn::from(505_u64),
                        )
                    })
                })
                .collect(),
        })
        .expect("persist sparse churn");

    let requests = projects
        .iter()
        .map(|project_id| project_bucket_read_request(project_id, after))
        .collect::<Vec<_>>();
    let snapshots = store
        .read_bucket_snapshots_with_limits(&requests, 20, 20, 1_000_000)
        .expect("read sparse bucket deltas");

    assert_eq!(snapshots.len(), projects.len());
    for snapshot in snapshots {
        assert_eq!(snapshot.latest_op_id, 16);
        assert!(!snapshot.reset_required);
        assert_eq!(snapshot.tail_ops.len(), 3);
        assert!(snapshot.current_documents.is_empty());
    }
}

#[test]
fn bucket_read_scan_budget_resets_dense_delta_without_partial_tail() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let project_id = "project-dense-budget";

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 32,
            begin_final_lsn: PostgresLsn(510),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(511),
            end_lsn: PostgresLsn(512),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data("task-dense-baseline", "org-dense", project_id),
                Lsn::from(512_u64),
            )],
        })
        .expect("persist baseline");
    let after = store
        .task_tail_last_op_id()
        .expect("tail metadata")
        .expect("baseline tail cursor");

    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 33,
            begin_final_lsn: PostgresLsn(513),
            begin_commit_time_micros: 3,
            commit_lsn: PostgresLsn(514),
            end_lsn: PostgresLsn(515),
            commit_time_micros: 4,
            column_types_by_table: BTreeMap::new(),
            changes: (0..6)
                .map(|task_index| {
                    ChangeEvent::insert(
                        "public",
                        "tasks",
                        7,
                        task_row_data(
                            &format!("task-dense-tail-{task_index}"),
                            "org-dense",
                            project_id,
                        ),
                        Lsn::from(515_u64),
                    )
                })
                .collect(),
        })
        .expect("persist dense churn");

    let snapshot = store
        .read_bucket_snapshots_with_limits(
            &[project_bucket_read_request(project_id, after)],
            10,
            3,
            1_000_000,
        )
        .expect("read clearing snapshot")
        .remove(0);

    assert_eq!(snapshot.latest_op_id, 7);
    assert!(snapshot.reset_required);
    assert!(snapshot.tail_ops.is_empty());
    assert_eq!(snapshot.current_documents.len(), 7);
}

#[test]
fn bucket_read_byte_budget_rejects_current_value_before_decoding() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("default table");
    txn.put(
        &table,
        current_doc_key("tasks", "task-oversized-current"),
        vec![b'x'; 128],
        WriteFlags::UPSERT,
    )
    .expect("put oversized invalid current value");
    txn.commit().expect("commit");

    let bucket = execution_plan()
        .resolve_bucket_request(crate::sync_rules::DEFAULT_TASKS_BUCKET_NAME)
        .expect("default tasks bucket");
    let request = BucketReadRequest {
        index_keys: sync_tail_index_keys_for_bucket("tasks", &BTreeMap::new()),
        current_accumulator_keys: sync_current_checkpoint_accumulator_keys_for_bucket(&bucket),
        tail_accumulator_keys: sync_tail_checkpoint_accumulator_keys_for_bucket(&bucket),
        bucket,
        after: 0,
    };

    assert!(matches!(
        store.read_bucket_snapshots_with_limits(&[request], 10, 10, 16),
        Err(ReplicationIngestError::ResourceLimit(_))
    ));
}

#[test]
fn bucket_read_byte_budget_rejects_tail_value_before_decoding() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let request = project_bucket_read_request("project-oversized-tail", 1);
    let route_index = request.index_keys.last().expect("route index");
    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("default table");
    txn.put(
        &table,
        sync_tail_op_key(2),
        vec![b'x'; 128],
        WriteFlags::UPSERT,
    )
    .expect("put oversized invalid tail value");
    txn.put(
        &table,
        sync_tail_index_entry_key(route_index, 2),
        2_u64.to_be_bytes(),
        WriteFlags::UPSERT,
    )
    .expect("put tail index entry");
    txn.put(
        &table,
        META_SYNC_TAIL_LAST_OP_ID_KEY,
        "2",
        WriteFlags::UPSERT,
    )
    .expect("put tail cursor");
    txn.put(
        &table,
        META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY,
        "2",
        WriteFlags::UPSERT,
    )
    .expect("put indexed-through cursor");
    txn.commit().expect("commit");

    assert!(matches!(
        store.read_bucket_snapshots_with_limits(&[request], 10, 10, 16),
        Err(ReplicationIngestError::ResourceLimit(_))
    ));
}

#[test]
fn bucket_read_byte_budget_charges_deduplicated_tail_value_once() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    let bucket = execution_plan()
        .resolve_bucket_request(crate::sync_rules::DEFAULT_TASKS_BUCKET_NAME)
        .expect("default tasks bucket");
    let route_fields = BTreeMap::from([(
        "project_id".to_owned(),
        "project-deduplicated-tail".to_owned(),
    )]);
    let mut index_keys = sync_tail_index_keys_for_bucket("tasks", &BTreeMap::new());
    index_keys.extend(sync_tail_index_keys_for_bucket("tasks", &route_fields));
    index_keys.sort();
    index_keys.dedup();
    let op = PersistedSyncTailOp {
        op_id: 2,
        operation: PersistedSyncTailOperation::Put,
        object_type: Some("tasks".to_owned()),
        object_id: Some("task-deduplicated-tail".to_owned()),
        route_fields,
        data_json: Some("{\"id\":\"task-deduplicated-tail\"}".to_owned()),
        previous_route_fields: None,
        previous_data_json: None,
    };
    let encoded = serde_json::to_vec(&op).expect("tail op json");

    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("default table");
    txn.put(&table, sync_tail_op_key(2), &encoded, WriteFlags::UPSERT)
        .expect("put tail op");
    for index_key in &index_keys {
        txn.put(
            &table,
            sync_tail_index_entry_key(index_key, 2),
            2_u64.to_be_bytes(),
            WriteFlags::UPSERT,
        )
        .expect("put tail index entry");
    }
    txn.put(
        &table,
        META_SYNC_TAIL_LAST_OP_ID_KEY,
        "2",
        WriteFlags::UPSERT,
    )
    .expect("put tail cursor");
    txn.put(
        &table,
        META_SYNC_TAIL_INDEXED_THROUGH_OP_ID_KEY,
        "2",
        WriteFlags::UPSERT,
    )
    .expect("put indexed-through cursor");
    txn.commit().expect("commit");

    let request = BucketReadRequest {
        index_keys,
        current_accumulator_keys: sync_current_checkpoint_accumulator_keys_for_bucket(&bucket),
        tail_accumulator_keys: sync_tail_checkpoint_accumulator_keys_for_bucket(&bucket),
        bucket,
        after: 1,
    };
    let snapshot = store
        .read_bucket_snapshots_with_limits(&[request], 10, 10, encoded.len() as u64)
        .expect("deduplicated tail read")
        .remove(0);

    assert_eq!(snapshot.tail_ops, vec![op]);
}

#[test]
fn bucket_read_byte_budget_charges_deduplicated_current_value_once() {
    let directory = TempDir::new().expect("temp dir");
    let store = ReplicationMdbxStore::new(directory.path()).expect("store");
    store
        .persist_batch(&ReplicationCommitBatch {
            transaction_id: 34,
            begin_final_lsn: PostgresLsn(520),
            begin_commit_time_micros: 1,
            commit_lsn: PostgresLsn(521),
            end_lsn: PostgresLsn(522),
            commit_time_micros: 2,
            column_types_by_table: BTreeMap::new(),
            changes: vec![ChangeEvent::insert(
                "public",
                "tasks",
                7,
                task_row_data(
                    "task-deduplicated-current",
                    "org-deduplicated-current",
                    "project-deduplicated-current",
                ),
                Lsn::from(522_u64),
            )],
        })
        .expect("persist current document");

    let txn = store.db.begin_ro_txn().expect("ro txn");
    let table = txn.open_table(None).expect("default table");
    let document_key = current_doc_key("tasks", "task-deduplicated-current");
    let serialized_len = txn
        .get::<Vec<u8>>(&table, &document_key)
        .expect("read current value")
        .expect("current value")
        .len() as u64;
    let mut remaining_bytes = serialized_len;
    let mut seen_document_keys = BTreeSet::new();
    let mut documents_by_key = BTreeMap::new();
    super::current_state::collect_current_documents_for_prefix_bounded(
        &txn,
        &table,
        &super::keys::current_doc_prefix("tasks"),
        &mut seen_document_keys,
        &mut documents_by_key,
        &mut remaining_bytes,
    )
    .expect("collect direct current document");
    super::current_state::collect_current_route_documents_bounded(
        &txn,
        &table,
        &super::keys::current_route_index_prefix(
            "tasks",
            &BTreeMap::from([(
                "project_id".to_owned(),
                "project-deduplicated-current".to_owned(),
            )]),
        ),
        "tasks",
        &mut seen_document_keys,
        &mut documents_by_key,
        &mut remaining_bytes,
    )
    .expect("deduplicate routed current document");

    assert_eq!(remaining_bytes, 0);
    assert_eq!(documents_by_key.len(), 1);
}

fn task_row_data(id: &str, org_id: &str, project_id: &str) -> RowData {
    task_row_data_with_title(id, org_id, project_id, id)
}

fn task_row_data_with_title(id: &str, org_id: &str, project_id: &str, title: &str) -> RowData {
    RowData::from_pairs(vec![
        ("id", ColumnValue::text(id)),
        ("org_id", ColumnValue::text(org_id)),
        ("project_id", ColumnValue::text(project_id)),
        ("title", ColumnValue::text(title)),
        ("status", ColumnValue::text("todo")),
        ("priority", ColumnValue::text("1")),
        ("assignee_id", ColumnValue::text("user-a")),
        ("story_points", ColumnValue::text("1")),
        ("updated_at", ColumnValue::text("2026-01-01T00:00:00Z")),
        ("summary", ColumnValue::text("test")),
    ])
}

fn project_bucket_read_request(project_id: &str, after: u64) -> BucketReadRequest {
    let bucket = execution_plan()
        .resolve_bucket_request(&crate::sync_rules::project_tasks_bucket_name(project_id))
        .expect("project bucket");
    BucketReadRequest {
        index_keys: sync_tail_index_keys_for_bucket(
            "tasks",
            &BTreeMap::from([("project_id".to_owned(), project_id.to_owned())]),
        ),
        current_accumulator_keys: sync_current_checkpoint_accumulator_keys_for_bucket(&bucket),
        tail_accumulator_keys: sync_tail_checkpoint_accumulator_keys_for_bucket(&bucket),
        bucket,
        after,
    }
}

fn persist_legacy_tail_ops(store: &ReplicationMdbxStore, ops: &[PersistedSyncTailOp]) {
    let txn = store.db.begin_rw_txn().expect("rw txn");
    let table = txn.open_table(None).expect("default table");
    for op in ops {
        txn.put(
            &table,
            sync_tail_op_key(op.op_id),
            serde_json::to_vec(op).expect("legacy op json"),
            WriteFlags::UPSERT,
        )
        .expect("legacy op put");
    }
    let last_op_id = ops.iter().map(|op| op.op_id).max().unwrap_or(0);
    txn.put(
        &table,
        META_SYNC_TAIL_LAST_OP_ID_KEY,
        last_op_id.to_string(),
        WriteFlags::UPSERT,
    )
    .expect("legacy tail metadata put");
    txn.commit().expect("legacy commit");
}

fn relation_message(
    relation_id: u32,
    schema: &str,
    table: &str,
    second_column_name: &str,
) -> Bytes {
    let mut data = vec![b'R'];
    data.extend_from_slice(&relation_id.to_be_bytes());
    data.extend_from_slice(&write_cstring(schema));
    data.extend_from_slice(&write_cstring(table));
    data.push(b'd');
    data.extend_from_slice(&(2_u16).to_be_bytes());

    data.push(0x01);
    data.extend_from_slice(&write_cstring("id"));
    data.extend_from_slice(&(23_u32).to_be_bytes());
    data.extend_from_slice(&(-1_i32).to_be_bytes());

    data.push(0x00);
    data.extend_from_slice(&write_cstring(second_column_name));
    data.extend_from_slice(&(25_u32).to_be_bytes());
    data.extend_from_slice(&(-1_i32).to_be_bytes());

    Bytes::from(data)
}

fn insert_message(relation_id: u32, values: &[Option<&str>]) -> Bytes {
    let mut data = vec![b'I'];
    data.extend_from_slice(&relation_id.to_be_bytes());
    data.push(b'N');
    encode_tuple(&mut data, values);
    Bytes::from(data)
}

fn update_message(
    relation_id: u32,
    old_values: &[Option<&str>],
    new_values: &[Option<&str>],
) -> Bytes {
    let mut data = vec![b'U'];
    data.extend_from_slice(&relation_id.to_be_bytes());
    data.push(b'K');
    encode_tuple(&mut data, old_values);
    data.push(b'N');
    encode_tuple(&mut data, new_values);
    Bytes::from(data)
}

fn delete_message(relation_id: u32, old_values: &[Option<&str>]) -> Bytes {
    let mut data = vec![b'D'];
    data.extend_from_slice(&relation_id.to_be_bytes());
    data.push(b'K');
    encode_tuple(&mut data, old_values);
    Bytes::from(data)
}

fn truncate_message(relation_ids: &[u32]) -> Bytes {
    let mut data = vec![b'T'];
    data.extend_from_slice(&(relation_ids.len() as u32).to_be_bytes());
    data.push(0x00);
    for relation_id in relation_ids {
        data.extend_from_slice(&relation_id.to_be_bytes());
    }
    Bytes::from(data)
}

fn encode_tuple(data: &mut Vec<u8>, values: &[Option<&str>]) {
    data.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for value in values {
        match value {
            Some(text) => {
                data.push(b't');
                data.extend_from_slice(&(text.len() as u32).to_be_bytes());
                data.extend_from_slice(text.as_bytes());
            }
            None => data.push(b'n'),
        }
    }
}

fn write_cstring(value: &str) -> Vec<u8> {
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    bytes
}
