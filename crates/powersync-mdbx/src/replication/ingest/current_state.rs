use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
};

use libmdbx::{NoWriteMap, TransactionKind, WriteFlags};
use serde::{Deserialize, Serialize};

use super::{
    accumulators::{add_current_document_accumulator, remove_current_document_accumulator},
    batch_codec::{push_len_prefixed_bytes, push_u32, read_len_prefixed_string, read_u32},
    derive::DerivedSyncTailOp,
    error::ReplicationIngestError,
    keys::{
        current_doc_key, current_doc_key_from_object_id_bytes, current_doc_prefix,
        current_route_index_key, CURRENT_DOC_BINARY_MAGIC, CURRENT_DOC_KEY_PREFIX,
    },
    metrics::ReplicationIngestMetricCounters,
    tail_log::PersistedSyncTailOperation,
};
use crate::sync_rules::RustExecutionPlan;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedBucketedDocument {
    pub object_type: String,
    pub object_id: String,
    pub route_fields: BTreeMap<String, String>,
    pub data_json: String,
}

#[derive(Clone, Copy)]
pub(super) struct CurrentWriteContext<'a> {
    pub(super) txn: &'a libmdbx::Transaction<'a, libmdbx::RW, NoWriteMap>,
    pub(super) table: &'a libmdbx::Table<'a>,
    pub(super) metrics: &'a ReplicationIngestMetricCounters,
}

#[derive(Clone, Copy)]
pub(super) struct CurrentDocumentWrite<'a> {
    pub(super) object_type: &'a str,
    pub(super) object_id: &'a str,
    pub(super) route_fields: &'a BTreeMap<String, String>,
    pub(super) data_json: &'a str,
}

pub(super) fn materialize_sync_state_ops(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    derived_ops: Vec<DerivedSyncTailOp>,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<Vec<DerivedSyncTailOp>, ReplicationIngestError> {
    if derived_ops.is_empty() {
        return Ok(Vec::new());
    }

    let mut materialized_ops = Vec::with_capacity(derived_ops.len());
    for derived_op in derived_ops {
        let materialized_op = materialize_sync_state_op(txn, table, derived_op)?;
        apply_materialized_sync_state_op(txn, table, &materialized_op, plan, metrics)?;
        materialized_ops.push(materialized_op);
    }

    Ok(materialized_ops)
}

pub(super) fn materialize_sync_state_op(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    mut derived_op: DerivedSyncTailOp,
) -> Result<DerivedSyncTailOp, ReplicationIngestError> {
    if derived_op.operation == PersistedSyncTailOperation::Put && !derived_op.assume_new {
        let Some(object_type) = derived_op.object_type.as_deref() else {
            return Ok(derived_op);
        };
        let Some(object_id) = derived_op.object_id.as_deref() else {
            return Ok(derived_op);
        };
        if let Some(document) = read_current_document(txn, table, object_type, object_id)? {
            derived_op.previous_route_fields = Some(document.route_fields);
            derived_op.previous_data_json = Some(document.data_json);
        }
        return Ok(derived_op);
    }

    if derived_op.operation != PersistedSyncTailOperation::Remove || derived_op.data_json.is_some()
    {
        return Ok(derived_op);
    }

    let Some(object_id) = derived_op.object_id.as_deref() else {
        return Err(ReplicationIngestError::CorruptBatch(
            "sync REMOVE operation is missing object_id".to_owned(),
        ));
    };

    if let Some(object_type) = derived_op.object_type.as_deref() {
        if let Some(document) = read_current_document(txn, table, object_type, object_id)? {
            derived_op.object_type = Some(document.object_type.clone());
            derived_op.route_fields = document.route_fields.clone();
            derived_op.data_json = Some(document.data_json.clone());
        }
    }

    Ok(derived_op)
}

pub(super) fn apply_materialized_sync_state_op(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    derived_op: &DerivedSyncTailOp,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    match derived_op.operation {
        PersistedSyncTailOperation::Clear => {
            if let Some(object_type) = derived_op.object_type.as_deref() {
                clear_current_documents_for_object(txn, table, object_type, plan, metrics)?;
            } else {
                clear_current_documents_for_prefix(
                    txn,
                    table,
                    CURRENT_DOC_KEY_PREFIX.as_bytes(),
                    plan,
                    metrics,
                )?;
            }
        }
        PersistedSyncTailOperation::Put => {
            let Some(object_type) = derived_op.object_type.as_deref() else {
                return Err(ReplicationIngestError::CorruptBatch(
                    "sync PUT operation is missing object_type".to_owned(),
                ));
            };
            let Some(object_id) = derived_op.object_id.as_deref() else {
                return Err(ReplicationIngestError::CorruptBatch(
                    "sync PUT operation is missing object_id".to_owned(),
                ));
            };
            let Some(data_json) = derived_op.data_json.as_deref() else {
                return Err(ReplicationIngestError::CorruptBatch(
                    "sync PUT operation is missing JSON payload".to_owned(),
                ));
            };
            put_current_document(
                CurrentWriteContext {
                    txn,
                    table,
                    metrics,
                },
                CurrentDocumentWrite {
                    object_type,
                    object_id,
                    route_fields: &derived_op.route_fields,
                    data_json,
                },
                plan,
                derived_op.assume_new,
                materialized_previous_document(derived_op, object_type, object_id).as_ref(),
            )?;
        }
        PersistedSyncTailOperation::Remove => {
            let Some(object_type) = derived_op.object_type.as_deref() else {
                return Ok(());
            };
            let Some(object_id) = derived_op.object_id.as_deref() else {
                return Err(ReplicationIngestError::CorruptBatch(
                    "sync REMOVE operation is missing object_id".to_owned(),
                ));
            };
            delete_current_document(
                txn,
                table,
                object_type,
                object_id,
                plan,
                metrics,
                materialized_current_document(derived_op, object_type, object_id).as_ref(),
            )?;
        }
    }

    Ok(())
}

fn put_current_document(
    ctx: CurrentWriteContext<'_>,
    document: CurrentDocumentWrite<'_>,
    plan: &RustExecutionPlan,
    assume_new: bool,
    previous_document: Option<&PersistedBucketedDocument>,
) -> Result<(), ReplicationIngestError> {
    if !assume_new {
        if let Some(existing) = previous_document {
            delete_current_route_indexes(ctx.txn, ctx.table, existing, plan, ctx.metrics)?;
            remove_current_document_accumulator(ctx.txn, ctx.table, existing, plan)?;
        }
    }

    let route_indexes =
        plan.required_route_indexes_for_row(document.object_type, document.route_fields);
    put_new_current_document_with_route_indexes(ctx, document, &route_indexes)?;
    add_current_document_accumulator(ctx.txn, ctx.table, document, plan)
}

pub(super) fn put_new_current_document_with_route_indexes(
    ctx: CurrentWriteContext<'_>,
    document: CurrentDocumentWrite<'_>,
    route_indexes: &[BTreeMap<String, String>],
) -> Result<(), ReplicationIngestError> {
    let encoded_document = encode_current_document(
        document.object_type,
        document.object_id,
        document.route_fields,
        document.data_json,
    )?;
    ctx.txn
        .put(
            ctx.table,
            current_doc_key(document.object_type, document.object_id),
            &encoded_document,
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist current document {}.{}: {error}",
                document.object_type, document.object_id
            ))
        })?;
    ctx.metrics.current_puts.fetch_add(1, Ordering::Relaxed);
    ctx.metrics.rows_synced.fetch_add(1, Ordering::Relaxed);

    for route_constraints in route_indexes {
        ctx.txn
            .put(
                ctx.table,
                current_route_index_key(
                    document.object_type,
                    route_constraints,
                    document.object_id,
                ),
                &encoded_document,
                WriteFlags::UPSERT,
            )
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!(
                    "persist current route index {}.{}: {error}",
                    document.object_type, document.object_id
                ))
            })?;
        ctx.metrics
            .current_index_puts
            .fetch_add(1, Ordering::Relaxed);
        ctx.metrics
            .current_index_value_bytes
            .fetch_add(encoded_document.len() as u64, Ordering::Relaxed);
    }

    Ok(())
}

fn delete_current_document(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    object_type: &str,
    object_id: &str,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
    materialized_existing: Option<&PersistedBucketedDocument>,
) -> Result<(), ReplicationIngestError> {
    if let Some(existing) = materialized_existing {
        delete_current_route_indexes(txn, table, existing, plan, metrics)?;
        remove_current_document_accumulator(txn, table, existing, plan)?;
    }
    let key = current_doc_key(object_type, object_id);
    if txn.del(table, &key, None).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("delete current document: {error}"))
    })? {
        metrics.current_deletes.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn materialized_previous_document(
    derived_op: &DerivedSyncTailOp,
    object_type: &str,
    object_id: &str,
) -> Option<PersistedBucketedDocument> {
    let data_json = derived_op.previous_data_json.as_ref()?;
    Some(PersistedBucketedDocument {
        object_type: object_type.to_owned(),
        object_id: object_id.to_owned(),
        route_fields: derived_op
            .previous_route_fields
            .as_ref()
            .unwrap_or(&derived_op.route_fields)
            .clone(),
        data_json: data_json.clone(),
    })
}

fn materialized_current_document(
    derived_op: &DerivedSyncTailOp,
    object_type: &str,
    object_id: &str,
) -> Option<PersistedBucketedDocument> {
    let data_json = derived_op.data_json.as_ref()?;
    Some(PersistedBucketedDocument {
        object_type: object_type.to_owned(),
        object_id: object_id.to_owned(),
        route_fields: derived_op.route_fields.clone(),
        data_json: data_json.clone(),
    })
}

fn delete_current_route_indexes(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    document: &PersistedBucketedDocument,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    for route_constraints in
        plan.required_route_indexes_for_row(&document.object_type, &document.route_fields)
    {
        if txn
            .del(
                table,
                current_route_index_key(
                    &document.object_type,
                    &route_constraints,
                    &document.object_id,
                ),
                None,
            )
            .map_err(|error| {
                ReplicationIngestError::Mdbx(format!("delete current route index: {error}"))
            })?
        {
            metrics
                .current_index_deletes
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

fn clear_current_documents_for_object(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    object_type: &str,
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    clear_current_documents_for_prefix(txn, table, &current_doc_prefix(object_type), plan, metrics)
}

fn clear_current_documents_for_prefix(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
    plan: &RustExecutionPlan,
    metrics: &ReplicationIngestMetricCounters,
) -> Result<(), ReplicationIngestError> {
    let mut cursor = txn
        .cursor(table)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("open current cursor: {error}")))?;
    let mut removals = Vec::new();
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current documents for clear: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        let document = decode_current_document(&key, &value)?;
        let mut keys = Vec::with_capacity(1);
        keys.push(key);
        keys.extend(current_route_keys_for_document(&document, plan));
        removals.push((document, keys));
    }
    drop(cursor);
    for (document, keys) in removals {
        remove_current_document_accumulator(txn, table, &document, plan)?;
        for key in keys {
            if txn.del(table, &key, None).map_err(|error| {
                ReplicationIngestError::Mdbx(format!("clear current key: {error}"))
            })? {
                metrics.current_deletes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

fn current_route_keys_for_document(
    document: &PersistedBucketedDocument,
    plan: &RustExecutionPlan,
) -> Vec<Vec<u8>> {
    plan.required_route_indexes_for_row(&document.object_type, &document.route_fields)
        .into_iter()
        .map(|route_constraints| {
            current_route_index_key(
                &document.object_type,
                &route_constraints,
                &document.object_id,
            )
        })
        .collect()
}

fn read_current_document<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    object_type: &str,
    object_id: &str,
) -> Result<Option<PersistedBucketedDocument>, ReplicationIngestError> {
    read_current_document_by_key(txn, table, &current_doc_key(object_type, object_id))
}

pub(super) fn read_current_document_by_key<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    key: &[u8],
) -> Result<Option<PersistedBucketedDocument>, ReplicationIngestError> {
    txn.get::<Vec<u8>>(table, key)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("read current document: {error}")))?
        .map(|bytes| decode_current_document(key, &bytes))
        .transpose()
}

pub(super) fn scan_current_documents_for_prefix<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
) -> Result<Vec<PersistedBucketedDocument>, ReplicationIngestError> {
    let mut cursor = txn
        .cursor(table)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("open current cursor: {error}")))?;
    let mut documents = Vec::new();
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current documents: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        documents.push(decode_current_document(&key, &value)?);
    }
    Ok(documents)
}

pub(super) fn collect_current_documents_for_prefix_bounded<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
    seen_document_keys: &mut BTreeSet<Vec<u8>>,
    documents_by_key: &mut BTreeMap<(String, String), PersistedBucketedDocument>,
    remaining_bytes: &mut u64,
) -> Result<(), ReplicationIngestError> {
    let mut cursor = txn
        .cursor(table)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("open current cursor: {error}")))?;
    for item in cursor.iter_from::<Cow<'_, [u8]>, Cow<'_, [u8]>>(prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current documents: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        if seen_document_keys.contains(key.as_ref()) {
            continue;
        }
        consume_serialized_read_bytes(remaining_bytes, value.len())?;
        let document = decode_current_document(&key, &value)?;
        seen_document_keys.insert(key.into_owned());
        documents_by_key.insert(
            (document.object_type.clone(), document.object_id.clone()),
            document,
        );
    }
    Ok(())
}

pub(super) fn scan_current_document_keys_for_prefix<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
    document_keys: &mut BTreeSet<Vec<u8>>,
) -> Result<(), ReplicationIngestError> {
    let mut cursor = txn
        .cursor(table)
        .map_err(|error| ReplicationIngestError::Mdbx(format!("open current cursor: {error}")))?;
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(prefix) {
        let (key, _) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current document keys: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        document_keys.insert(key);
    }
    Ok(())
}

pub(super) fn scan_current_route_documents<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
    object_type: &str,
) -> Result<Vec<PersistedBucketedDocument>, ReplicationIngestError> {
    let mut cursor = txn.cursor(table).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("open current route cursor: {error}"))
    })?;
    let mut route_entries = Vec::new();
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current route index: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        route_entries.push((key, value));
    }
    drop(cursor);

    let mut documents = Vec::with_capacity(route_entries.len());
    for (key, value) in route_entries {
        if let Some(document) =
            current_document_from_route_entry(txn, table, object_type, prefix, &key, &value)?
        {
            documents.push(document);
        }
    }
    Ok(documents)
}

pub(super) fn collect_current_route_documents_bounded<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
    object_type: &str,
    seen_document_keys: &mut BTreeSet<Vec<u8>>,
    documents_by_key: &mut BTreeMap<(String, String), PersistedBucketedDocument>,
    remaining_bytes: &mut u64,
) -> Result<(), ReplicationIngestError> {
    let mut cursor = txn.cursor(table).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("open current route cursor: {error}"))
    })?;
    for item in cursor.iter_from::<Cow<'_, [u8]>, Cow<'_, [u8]>>(prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current route index: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        let document_key = current_document_key_from_route_entry_without_decoding(
            object_type,
            prefix,
            &key,
            &value,
        );
        if seen_document_keys.contains(&document_key) {
            continue;
        }

        let document = if value.starts_with(CURRENT_DOC_BINARY_MAGIC) {
            consume_serialized_read_bytes(remaining_bytes, value.len())?;
            decode_current_document(&key, &value)?
        } else {
            let bytes = txn
                .get::<Cow<'_, [u8]>>(table, &document_key)
                .map_err(|error| {
                    ReplicationIngestError::Mdbx(format!("read current document: {error}"))
                })?
                .ok_or_else(|| {
                    ReplicationIngestError::CorruptBatch(format!(
                        "current route index points at missing document {}",
                        String::from_utf8_lossy(&document_key)
                    ))
                })?;
            consume_serialized_read_bytes(remaining_bytes, bytes.len())?;
            decode_current_document(&document_key, &bytes)?
        };
        seen_document_keys.insert(document_key);
        documents_by_key.insert(
            (document.object_type.clone(), document.object_id.clone()),
            document,
        );
    }
    Ok(())
}

pub(super) fn scan_current_route_index_keys<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    prefix: &[u8],
    object_type: &str,
    document_keys: &mut BTreeSet<Vec<u8>>,
) -> Result<(), ReplicationIngestError> {
    let mut cursor = txn.cursor(table).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("open current route cursor: {error}"))
    })?;
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan current route index keys: {error}"))
        })?;
        if !key.starts_with(prefix) {
            break;
        }
        document_keys.insert(current_document_key_from_route_entry_value(
            object_type,
            prefix,
            &key,
            &value,
        )?);
    }
    Ok(())
}

fn current_document_from_route_entry<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    object_type: &str,
    prefix: &[u8],
    key: &[u8],
    value: &[u8],
) -> Result<Option<PersistedBucketedDocument>, ReplicationIngestError> {
    if value.starts_with(CURRENT_DOC_BINARY_MAGIC) {
        return decode_current_document(key, value).map(Some);
    }

    let document_key = current_document_key_from_route_entry(object_type, prefix, key, value);
    read_current_document_by_key(txn, table, &document_key)
}

fn current_document_key_from_route_entry_value(
    object_type: &str,
    prefix: &[u8],
    key: &[u8],
    value: &[u8],
) -> Result<Vec<u8>, ReplicationIngestError> {
    if value.starts_with(CURRENT_DOC_BINARY_MAGIC) {
        let document = decode_current_document(key, value)?;
        return Ok(current_doc_key(&document.object_type, &document.object_id));
    }

    Ok(current_document_key_from_route_entry(
        object_type,
        prefix,
        key,
        value,
    ))
}

fn current_document_key_from_route_entry_without_decoding(
    object_type: &str,
    prefix: &[u8],
    key: &[u8],
    value: &[u8],
) -> Vec<u8> {
    if value.starts_with(CURRENT_DOC_BINARY_MAGIC) {
        let object_id = key.strip_prefix(prefix).unwrap_or_default();
        return current_doc_key_from_object_id_bytes(object_type, object_id);
    }
    current_document_key_from_route_entry(object_type, prefix, key, value)
}

fn consume_serialized_read_bytes(
    remaining_bytes: &mut u64,
    serialized_len: usize,
) -> Result<(), ReplicationIngestError> {
    let serialized_len = serialized_len as u64;
    if serialized_len > *remaining_bytes {
        return Err(ReplicationIngestError::ResourceLimit(format!(
            "serialized MDBX value needs {serialized_len} bytes but only {remaining_bytes} remain in the per-request budget"
        )));
    }
    *remaining_bytes -= serialized_len;
    Ok(())
}

fn current_document_key_from_route_entry(
    object_type: &str,
    prefix: &[u8],
    key: &[u8],
    value: &[u8],
) -> Vec<u8> {
    if !value.is_empty() {
        return value.to_vec();
    }

    let object_id = key.strip_prefix(prefix).unwrap_or_default();
    current_doc_key_from_object_id_bytes(object_type, object_id)
}

fn encode_current_document(
    object_type: &str,
    object_id: &str,
    route_fields: &BTreeMap<String, String>,
    data_json: &str,
) -> Result<Vec<u8>, ReplicationIngestError> {
    let mut bytes = Vec::with_capacity(
        CURRENT_DOC_BINARY_MAGIC.len()
            + object_type.len()
            + object_id.len()
            + data_json.len()
            + route_fields
                .iter()
                .map(|(key, value)| key.len() + value.len() + 8)
                .sum::<usize>()
            + 24,
    );
    bytes.extend_from_slice(CURRENT_DOC_BINARY_MAGIC);
    push_len_prefixed_bytes(&mut bytes, object_type.as_bytes(), "object_type")?;
    push_len_prefixed_bytes(&mut bytes, object_id.as_bytes(), "object_id")?;
    push_u32(&mut bytes, route_fields.len(), "route field count")?;
    for (key, value) in route_fields {
        push_len_prefixed_bytes(&mut bytes, key.as_bytes(), "route field key")?;
        push_len_prefixed_bytes(&mut bytes, value.as_bytes(), "route field value")?;
    }
    push_len_prefixed_bytes(&mut bytes, data_json.as_bytes(), "data_json")?;
    Ok(bytes)
}

fn decode_current_document(
    key: &[u8],
    bytes: &[u8],
) -> Result<PersistedBucketedDocument, ReplicationIngestError> {
    if bytes.starts_with(CURRENT_DOC_BINARY_MAGIC) {
        return decode_binary_current_document(key, bytes);
    }

    serde_json::from_slice(bytes).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!(
            "current document {} is not valid JSON: {error}",
            String::from_utf8_lossy(key)
        ))
    })
}

fn decode_binary_current_document(
    key: &[u8],
    bytes: &[u8],
) -> Result<PersistedBucketedDocument, ReplicationIngestError> {
    let mut cursor = CURRENT_DOC_BINARY_MAGIC.len();
    let object_type = read_len_prefixed_string(bytes, &mut cursor, "object_type")?;
    let object_id = read_len_prefixed_string(bytes, &mut cursor, "object_id")?;
    let route_field_count = read_u32(bytes, &mut cursor)? as usize;
    let mut route_fields = BTreeMap::new();
    for _ in 0..route_field_count {
        let name = read_len_prefixed_string(bytes, &mut cursor, "route field key")?;
        let value = read_len_prefixed_string(bytes, &mut cursor, "route field value")?;
        route_fields.insert(name, value);
    }
    let data_json = read_len_prefixed_string(bytes, &mut cursor, "data_json")?;
    if cursor != bytes.len() {
        return Err(ReplicationIngestError::CorruptBatch(format!(
            "current document {} has {} trailing bytes",
            String::from_utf8_lossy(key),
            bytes.len().saturating_sub(cursor)
        )));
    }
    Ok(PersistedBucketedDocument {
        object_type,
        object_id,
        route_fields,
        data_json,
    })
}
