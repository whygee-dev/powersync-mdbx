use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationIngestMetrics {
    pub rows_seen: u64,
    pub rows_synced: u64,
    pub replication_decode_ms: u64,
    pub raw_batch_encode_ms: u64,
    pub sync_rule_eval_ms: u64,
    pub mdbx_write_txn_ms: u64,
    pub source_snapshot_scan_ms: u64,
    pub batches_persisted: u64,
    pub raw_batches_persisted: u64,
    pub current_puts: u64,
    pub current_deletes: u64,
    pub current_index_puts: u64,
    pub current_index_value_bytes: u64,
    pub current_index_deletes: u64,
    pub tail_ops_written: u64,
    pub tail_index_entries_written: u64,
    pub cold_snapshot_scan_ms: u64,
    pub tail_scan_ms: u64,
    pub protocol_encode_ms: u64,
    pub bytes_sent: u64,
}

#[derive(Debug, Default)]
pub(super) struct ReplicationIngestMetricCounters {
    pub(super) rows_seen: AtomicU64,
    pub(super) rows_synced: AtomicU64,
    pub(super) replication_decode_ms: AtomicU64,
    pub(super) raw_batch_encode_ms: AtomicU64,
    pub(super) sync_rule_eval_ms: AtomicU64,
    pub(super) mdbx_write_txn_ms: AtomicU64,
    pub(super) source_snapshot_scan_ms: AtomicU64,
    pub(super) batches_persisted: AtomicU64,
    pub(super) raw_batches_persisted: AtomicU64,
    pub(super) current_puts: AtomicU64,
    pub(super) current_deletes: AtomicU64,
    pub(super) current_index_puts: AtomicU64,
    pub(super) current_index_value_bytes: AtomicU64,
    pub(super) current_index_deletes: AtomicU64,
    pub(super) tail_ops_written: AtomicU64,
    pub(super) tail_index_entries_written: AtomicU64,
    pub(super) cold_snapshot_scan_ms: AtomicU64,
    pub(super) tail_scan_ms: AtomicU64,
    pub(super) protocol_encode_ms: AtomicU64,
    pub(super) bytes_sent: AtomicU64,
}

impl ReplicationIngestMetricCounters {
    pub(super) fn snapshot(&self) -> ReplicationIngestMetrics {
        ReplicationIngestMetrics {
            rows_seen: self.rows_seen.load(Ordering::Relaxed),
            rows_synced: self.rows_synced.load(Ordering::Relaxed),
            replication_decode_ms: self.replication_decode_ms.load(Ordering::Relaxed),
            raw_batch_encode_ms: self.raw_batch_encode_ms.load(Ordering::Relaxed),
            sync_rule_eval_ms: self.sync_rule_eval_ms.load(Ordering::Relaxed),
            mdbx_write_txn_ms: self.mdbx_write_txn_ms.load(Ordering::Relaxed),
            source_snapshot_scan_ms: self.source_snapshot_scan_ms.load(Ordering::Relaxed),
            batches_persisted: self.batches_persisted.load(Ordering::Relaxed),
            raw_batches_persisted: self.raw_batches_persisted.load(Ordering::Relaxed),
            current_puts: self.current_puts.load(Ordering::Relaxed),
            current_deletes: self.current_deletes.load(Ordering::Relaxed),
            current_index_puts: self.current_index_puts.load(Ordering::Relaxed),
            current_index_value_bytes: self.current_index_value_bytes.load(Ordering::Relaxed),
            current_index_deletes: self.current_index_deletes.load(Ordering::Relaxed),
            tail_ops_written: self.tail_ops_written.load(Ordering::Relaxed),
            tail_index_entries_written: self.tail_index_entries_written.load(Ordering::Relaxed),
            cold_snapshot_scan_ms: self.cold_snapshot_scan_ms.load(Ordering::Relaxed),
            tail_scan_ms: self.tail_scan_ms.load(Ordering::Relaxed),
            protocol_encode_ms: self.protocol_encode_ms.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }
}
