use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use pg_walstream::{
    ChangeEvent, LogicalReplicationMessage, LogicalReplicationParser, Lsn, RelationInfo,
    ReplicaIdentity, ReplicationState,
};

use super::{batch_codec::ReplicationCommitBatch, error::ReplicationIngestError};
use crate::replication::postgres::PostgresLsn;
use crate::replication::runner::ReplicationStreamEvent;
use crate::sync_rules::{JsonColumnType, JsonColumnTypes};

pub struct PgOutputBatchDecoder {
    parser: LogicalReplicationParser,
    state: ReplicationState,
    transaction: Option<OpenTransaction>,
}

#[derive(Debug)]
struct OpenTransaction {
    transaction_id: u32,
    begin_final_lsn: PostgresLsn,
    begin_commit_time_micros: i64,
    changes: Vec<ChangeEvent>,
}

impl PgOutputBatchDecoder {
    pub fn new() -> Self {
        Self {
            parser: LogicalReplicationParser::with_protocol_version(1),
            state: ReplicationState::new(),
            transaction: None,
        }
    }

    pub fn push_stream_event(
        &mut self,
        event: &ReplicationStreamEvent,
    ) -> Result<Option<ReplicationCommitBatch>, ReplicationIngestError> {
        match event {
            ReplicationStreamEvent::KeepAlive { wal_end, .. } => {
                self.state.update_received_lsn(wal_end.to_u64());
                Ok(None)
            }
            ReplicationStreamEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => {
                if let Some(current) = &self.transaction {
                    return Err(ReplicationIngestError::BeginWhileTransactionOpen {
                        existing_xid: current.transaction_id,
                        new_xid: *xid,
                    });
                }

                self.transaction = Some(OpenTransaction {
                    transaction_id: *xid,
                    begin_final_lsn: *final_lsn,
                    begin_commit_time_micros: *commit_time_micros,
                    changes: Vec::new(),
                });
                Ok(None)
            }
            ReplicationStreamEvent::XLogData { wal_end, data, .. } => {
                self.state.update_received_lsn(wal_end.to_u64());
                self.decode_wal_payload(*wal_end, data)?;
                Ok(None)
            }
            ReplicationStreamEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            } => {
                self.state.update_received_lsn(end_lsn.to_u64());
                let Some(transaction) = self.transaction.take() else {
                    return Err(ReplicationIngestError::CommitWithoutBegin { end_lsn: *end_lsn });
                };

                Ok(Some(ReplicationCommitBatch {
                    transaction_id: transaction.transaction_id,
                    begin_final_lsn: transaction.begin_final_lsn,
                    begin_commit_time_micros: transaction.begin_commit_time_micros,
                    commit_lsn: *lsn,
                    end_lsn: *end_lsn,
                    commit_time_micros: *commit_time_micros,
                    column_types_by_table: current_relation_column_types(&self.state),
                    changes: transaction.changes,
                }))
            }
            ReplicationStreamEvent::Message { .. } | ReplicationStreamEvent::StoppedAt { .. } => {
                Ok(None)
            }
        }
    }

    fn decode_wal_payload(
        &mut self,
        wal_end: PostgresLsn,
        data: &Bytes,
    ) -> Result<(), ReplicationIngestError> {
        let decoded = self
            .parser
            .parse_wal_message(data)
            .map_err(|error| ReplicationIngestError::PgWalstream(error.to_string()))?;

        match decoded.message {
            LogicalReplicationMessage::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                self.state.add_relation(RelationInfo::new(
                    relation_id,
                    namespace,
                    relation_name,
                    replica_identity,
                    columns,
                ));
                Ok(())
            }
            LogicalReplicationMessage::Insert { relation_id, tuple } => {
                let relation = self.relation(relation_id)?;
                let change = ChangeEvent::insert(
                    Arc::clone(&relation.namespace),
                    Arc::clone(&relation.relation_name),
                    relation_id,
                    tuple.to_row_data(&relation),
                    Lsn::from(wal_end.to_u64()),
                );
                self.push_change("insert", change)
            }
            LogicalReplicationMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
                ..
            } => {
                let relation = self.relation(relation_id)?;
                let old_data = old_tuple.map(|tuple| tuple.to_row_data(&relation));
                let change = ChangeEvent::update(
                    Arc::clone(&relation.namespace),
                    Arc::clone(&relation.relation_name),
                    relation_id,
                    old_data,
                    new_tuple.to_row_data(&relation),
                    replica_identity(&relation)?,
                    key_columns(&relation),
                    Lsn::from(wal_end.to_u64()),
                );
                self.push_change("update", change)
            }
            LogicalReplicationMessage::Delete {
                relation_id,
                old_tuple,
                ..
            } => {
                let relation = self.relation(relation_id)?;
                let change = ChangeEvent::delete(
                    Arc::clone(&relation.namespace),
                    Arc::clone(&relation.relation_name),
                    relation_id,
                    old_tuple.to_row_data(&relation),
                    replica_identity(&relation)?,
                    key_columns(&relation),
                    Lsn::from(wal_end.to_u64()),
                );
                self.push_change("delete", change)
            }
            LogicalReplicationMessage::Truncate { relation_ids, .. } => {
                let mut tables = Vec::with_capacity(relation_ids.len());
                for relation_id in relation_ids {
                    let relation = self.relation(relation_id)?;
                    tables.push(Arc::from(relation.full_name()));
                }
                let change = ChangeEvent::truncate(tables, Lsn::from(wal_end.to_u64()));
                self.push_change("truncate", change)
            }
            LogicalReplicationMessage::Type { .. }
            | LogicalReplicationMessage::Origin { .. }
            | LogicalReplicationMessage::Message { .. } => Ok(()),
            other => Err(ReplicationIngestError::UnsupportedPgoutputMessage(
                logical_message_kind(&other),
            )),
        }
    }

    fn push_change(
        &mut self,
        change_kind: &'static str,
        change: ChangeEvent,
    ) -> Result<(), ReplicationIngestError> {
        let Some(transaction) = self.transaction.as_mut() else {
            return Err(ReplicationIngestError::ChangeOutsideTransaction { change_kind });
        };
        transaction.changes.push(change);
        Ok(())
    }

    fn relation(&self, relation_id: u32) -> Result<RelationInfo, ReplicationIngestError> {
        self.state
            .get_relation(relation_id)
            .cloned()
            .ok_or(ReplicationIngestError::MissingRelation { relation_id })
    }
}

impl Default for PgOutputBatchDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn replica_identity(relation: &RelationInfo) -> Result<ReplicaIdentity, ReplicationIngestError> {
    ReplicaIdentity::from_byte(relation.replica_identity).ok_or(
        ReplicationIngestError::InvalidReplicaIdentity {
            relation_id: relation.relation_id,
            raw: relation.replica_identity,
        },
    )
}

fn key_columns(relation: &RelationInfo) -> Vec<Arc<str>> {
    relation
        .get_key_columns()
        .into_iter()
        .map(|column| Arc::clone(&column.name))
        .collect()
}

fn current_relation_column_types(state: &ReplicationState) -> BTreeMap<String, JsonColumnTypes> {
    state
        .relations
        .values()
        .map(|relation| {
            (
                relation.relation_name.to_string(),
                relation
                    .columns
                    .iter()
                    .map(|column| {
                        (
                            column.name.to_string(),
                            json_column_type_from_type_oid(column.type_id),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

fn json_column_type_from_type_oid(type_id: u32) -> JsonColumnType {
    match type_id {
        16 => JsonColumnType::Boolean,
        20 | 21 | 23 | 700 | 701 | 1700 => JsonColumnType::Number,
        1082 | 1114 | 1184 => JsonColumnType::Timestamp,
        114 | 3802 => JsonColumnType::Json,
        _ => JsonColumnType::String,
    }
}

fn logical_message_kind(message: &LogicalReplicationMessage) -> &'static str {
    match message {
        LogicalReplicationMessage::Begin { .. } => "begin",
        LogicalReplicationMessage::Commit { .. } => "commit",
        LogicalReplicationMessage::Relation { .. } => "relation",
        LogicalReplicationMessage::Insert { .. } => "insert",
        LogicalReplicationMessage::Update { .. } => "update",
        LogicalReplicationMessage::Delete { .. } => "delete",
        LogicalReplicationMessage::Truncate { .. } => "truncate",
        LogicalReplicationMessage::Type { .. } => "type",
        LogicalReplicationMessage::Origin { .. } => "origin",
        LogicalReplicationMessage::Message { .. } => "message",
        LogicalReplicationMessage::StreamStart { .. } => "stream-start",
        LogicalReplicationMessage::StreamStop => "stream-stop",
        LogicalReplicationMessage::StreamCommit { .. } => "stream-commit",
        LogicalReplicationMessage::StreamAbort { .. } => "stream-abort",
        LogicalReplicationMessage::BeginPrepare { .. } => "begin-prepare",
        LogicalReplicationMessage::Prepare { .. } => "prepare",
        LogicalReplicationMessage::CommitPrepared { .. } => "commit-prepared",
        LogicalReplicationMessage::RollbackPrepared { .. } => "rollback-prepared",
        LogicalReplicationMessage::StreamPrepare { .. } => "stream-prepare",
    }
}

#[cfg(test)]
mod transaction_assembly_tests {
    use super::PgOutputBatchDecoder;
    use crate::replication::ingest::error::ReplicationIngestError;
    use crate::replication::postgres::PostgresLsn;
    use crate::replication::runner::ReplicationStreamEvent;

    // The pgoutput byte parsing lives in the `pg_walstream` dependency and is
    // covered end-to-end by the live-Postgres smoke tests; here we cover the
    // repo-owned transaction state machine that those binary-free events drive.
    #[test]
    fn assembles_begin_commit_into_a_batch_and_rejects_malformed_boundaries() {
        let mut decoder = PgOutputBatchDecoder::new();

        let commit = ReplicationStreamEvent::Commit {
            lsn: PostgresLsn(10),
            end_lsn: PostgresLsn(11),
            commit_time_micros: 0,
        };
        // A Commit with no open transaction is rejected, not silently dropped.
        assert!(matches!(
            decoder.push_stream_event(&commit),
            Err(ReplicationIngestError::CommitWithoutBegin { .. })
        ));

        let begin = ReplicationStreamEvent::Begin {
            final_lsn: PostgresLsn(10),
            xid: 42,
            commit_time_micros: 0,
        };
        // Begin opens a transaction; a second Begin while one is open is rejected.
        assert!(matches!(decoder.push_stream_event(&begin), Ok(None)));
        assert!(matches!(
            decoder.push_stream_event(&begin),
            Err(ReplicationIngestError::BeginWhileTransactionOpen {
                existing_xid: 42,
                new_xid: 42,
            })
        ));

        // Commit closes the transaction into a batch carrying the xid and end LSN.
        let batch = decoder
            .push_stream_event(&commit)
            .expect("commit should not error")
            .expect("commit should yield a batch");
        assert_eq!(batch.transaction_id, 42);
        assert_eq!(batch.end_lsn, PostgresLsn(11));
        assert!(batch.changes.is_empty());
    }
}
