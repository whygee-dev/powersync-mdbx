use std::collections::BTreeMap;

use bytes::BytesMut;
use pg_walstream::ChangeEvent;

use super::error::ReplicationIngestError;
use crate::replication::postgres::PostgresLsn;
use crate::sync_rules::JsonColumnTypes;

#[derive(Debug, Clone)]
pub struct ReplicationCommitBatch {
    pub transaction_id: u32,
    pub begin_final_lsn: PostgresLsn,
    pub begin_commit_time_micros: i64,
    pub commit_lsn: PostgresLsn,
    pub end_lsn: PostgresLsn,
    pub commit_time_micros: i64,
    pub column_types_by_table: BTreeMap<String, JsonColumnTypes>,
    pub changes: Vec<ChangeEvent>,
}

impl ReplicationCommitBatch {
    pub fn change_count(&self) -> usize {
        self.changes.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&self.transaction_id.to_be_bytes());
        buffer.extend_from_slice(&self.begin_final_lsn.to_u64().to_be_bytes());
        buffer.extend_from_slice(&self.begin_commit_time_micros.to_be_bytes());
        buffer.extend_from_slice(&self.commit_lsn.to_u64().to_be_bytes());
        buffer.extend_from_slice(&self.end_lsn.to_u64().to_be_bytes());
        buffer.extend_from_slice(&self.commit_time_micros.to_be_bytes());
        buffer.extend_from_slice(&(self.changes.len() as u32).to_be_bytes());

        for change in &self.changes {
            let mut encoded = BytesMut::new();
            change.encode(&mut encoded);
            buffer.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&encoded);
        }

        buffer.to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplicationIngestError> {
        let mut cursor = 0usize;
        let transaction_id = read_u32(bytes, &mut cursor)?;
        let begin_final_lsn = PostgresLsn(read_u64(bytes, &mut cursor)?);
        let begin_commit_time_micros = read_i64(bytes, &mut cursor)?;
        let commit_lsn = PostgresLsn(read_u64(bytes, &mut cursor)?);
        let end_lsn = PostgresLsn(read_u64(bytes, &mut cursor)?);
        let commit_time_micros = read_i64(bytes, &mut cursor)?;
        let change_count = read_u32(bytes, &mut cursor)? as usize;
        let mut changes = Vec::with_capacity(change_count);

        for index in 0..change_count {
            let encoded_len = read_u32(bytes, &mut cursor)? as usize;
            let encoded_end = cursor.checked_add(encoded_len).ok_or_else(|| {
                ReplicationIngestError::CorruptBatch(format!(
                    "encoded change {index} length overflow ({encoded_len})"
                ))
            })?;
            let encoded = bytes.get(cursor..encoded_end).ok_or_else(|| {
                ReplicationIngestError::CorruptBatch(format!(
                    "encoded change {index} exceeds batch bounds"
                ))
            })?;
            cursor = encoded_end;
            changes.push(
                ChangeEvent::decode(encoded)
                    .map_err(|error| ReplicationIngestError::PgWalstream(error.to_string()))?,
            );
        }

        if cursor != bytes.len() {
            return Err(ReplicationIngestError::CorruptBatch(format!(
                "expected batch payload to end at {cursor}, found trailing {} bytes",
                bytes.len().saturating_sub(cursor)
            )));
        }

        Ok(Self {
            transaction_id,
            begin_final_lsn,
            begin_commit_time_micros,
            commit_lsn,
            end_lsn,
            commit_time_micros,
            column_types_by_table: BTreeMap::new(),
            changes,
        })
    }
}

pub(super) fn push_u32(
    bytes: &mut Vec<u8>,
    value: usize,
    label: &str,
) -> Result<(), ReplicationIngestError> {
    let value = u32::try_from(value).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!("{label} exceeds u32 length: {error}"))
    })?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

pub(super) fn push_len_prefixed_bytes(
    output: &mut Vec<u8>,
    value: &[u8],
    label: &str,
) -> Result<(), ReplicationIngestError> {
    push_u32(output, value.len(), label)?;
    output.extend_from_slice(value);
    Ok(())
}

pub(super) fn read_len_prefixed_string(
    bytes: &[u8],
    cursor: &mut usize,
    label: &str,
) -> Result<String, ReplicationIngestError> {
    let len = read_u32(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| ReplicationIngestError::CorruptBatch(format!("{label} cursor overflow")))?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        ReplicationIngestError::CorruptBatch(format!("missing {label} at offset {}", *cursor))
    })?;
    *cursor = end;
    String::from_utf8(slice.to_vec()).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!("{label} is not UTF-8: {error}"))
    })
}

pub(super) fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, ReplicationIngestError> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| ReplicationIngestError::CorruptBatch("u32 cursor overflow".to_owned()))?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        ReplicationIngestError::CorruptBatch(format!("missing u32 at offset {}", *cursor))
    })?;
    *cursor = end;
    Ok(u32::from_be_bytes(
        slice.try_into().expect("u32 slice length"),
    ))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ReplicationIngestError> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| ReplicationIngestError::CorruptBatch("u64 cursor overflow".to_owned()))?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        ReplicationIngestError::CorruptBatch(format!("missing u64 at offset {}", *cursor))
    })?;
    *cursor = end;
    Ok(u64::from_be_bytes(
        slice.try_into().expect("u64 slice length"),
    ))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, ReplicationIngestError> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| ReplicationIngestError::CorruptBatch("i64 cursor overflow".to_owned()))?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        ReplicationIngestError::CorruptBatch(format!("missing i64 at offset {}", *cursor))
    })?;
    *cursor = end;
    Ok(i64::from_be_bytes(
        slice.try_into().expect("i64 slice length"),
    ))
}
