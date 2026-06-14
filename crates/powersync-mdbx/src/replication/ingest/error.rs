use crate::replication::postgres::PostgresLsn;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationIngestError {
    #[error("pgoutput decode failed: {0}")]
    PgWalstream(String),
    #[error("missing relation metadata for relation {relation_id}")]
    MissingRelation { relation_id: u32 },
    #[error("relation {relation_id} has unsupported replica identity byte 0x{raw:02x}")]
    InvalidReplicaIdentity { relation_id: u32, raw: u8 },
    #[error("{change_kind} change arrived outside an open transaction")]
    ChangeOutsideTransaction { change_kind: &'static str },
    #[error("commit at {end_lsn} arrived without a matching begin")]
    CommitWithoutBegin { end_lsn: PostgresLsn },
    #[error("received begin for xid={new_xid} while xid={existing_xid} is still open")]
    BeginWhileTransactionOpen { existing_xid: u32, new_xid: u32 },
    #[error("pgoutput message kind {0} is not yet supported in the Rust ingest seam")]
    UnsupportedPgoutputMessage(&'static str),
    #[error("MDBX replication ingest store failed: {0}")]
    Mdbx(String),
    #[error("replication batch payload is corrupt: {0}")]
    CorruptBatch(String),
    #[error("invalid persisted replication LSN metadata: {0}")]
    InvalidPersistedLsn(String),
    #[error("replication read resource limit exceeded: {0}")]
    ResourceLimit(String),
}
