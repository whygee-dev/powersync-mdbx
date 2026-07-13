use std::collections::BTreeMap;

use libmdbx::{NoWriteMap, TransactionKind, WriteFlags};
use pg_walstream::{ColumnValue, RowData};

use super::{
    derive::{DerivedParameterLookupOp, ParameterLookupOperation},
    error::ReplicationIngestError,
    keys::{parameter_lookup_index_key, parameter_lookup_index_prefix, parameter_lookup_row_key},
};
use crate::sync_rules::{
    CompiledLookupTablePlan, LiteralValue, Operand, Predicate, RustExecutionPlan,
};

pub(super) fn apply_parameter_lookup_ops(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    operations: &[DerivedParameterLookupOp],
    plan: &RustExecutionPlan,
) -> Result<(), ReplicationIngestError> {
    for operation in operations {
        let lookup_table = plan
            .lookup_table_plan(&operation.source_table)
            .ok_or_else(|| {
                ReplicationIngestError::CorruptBatch(format!(
                    "lookup source table {} is not present in sync plan",
                    operation.source_table
                ))
            })?;
        apply_parameter_lookup_op(txn, table, lookup_table, operation)?;
    }
    Ok(())
}

pub(super) fn put_parameter_lookup_row(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    lookup_table: &CompiledLookupTablePlan,
    row: &RowData,
) -> Result<(), ReplicationIngestError> {
    let row_id = row_id(lookup_table.source_table.as_str(), row)?;
    let row_key = parameter_lookup_row_key(&lookup_table.source_table, &row_id);
    let previous = read_parameter_lookup_row(txn, table, &row_key)?;
    if let Some(previous) = &previous {
        delete_parameter_lookup_indexes(txn, table, lookup_table, &row_id, previous)?;
    }

    let document = parameter_lookup_row_document(lookup_table, row, previous.as_ref())?;
    let encoded = serde_json::to_vec(&document).map_err(|error| {
        ReplicationIngestError::CorruptBatch(format!(
            "serialize parameter lookup row {}.{}: {error}",
            lookup_table.source_table, row_id
        ))
    })?;
    txn.put(table, row_key, encoded, WriteFlags::UPSERT)
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist parameter lookup row {}.{}: {error}",
                lookup_table.source_table, row_id
            ))
        })?;
    put_parameter_lookup_indexes(txn, table, lookup_table, &row_id, &document)
}

pub(super) fn remove_parameter_lookup_row(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    lookup_table: &CompiledLookupTablePlan,
    row: &RowData,
) -> Result<(), ReplicationIngestError> {
    let row_id = row_id(lookup_table.source_table.as_str(), row)?;
    let row_key = parameter_lookup_row_key(&lookup_table.source_table, &row_id);
    let Some(document) = read_parameter_lookup_row(txn, table, &row_key)? else {
        return Ok(());
    };
    delete_parameter_lookup_indexes(txn, table, lookup_table, &row_id, &document)?;
    txn.del(table, row_key, None).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("delete parameter lookup row: {error}"))
    })?;
    Ok(())
}

pub(super) fn apply_parameter_lookup_op(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    lookup_table: &CompiledLookupTablePlan,
    operation: &DerivedParameterLookupOp,
) -> Result<(), ReplicationIngestError> {
    match &operation.operation {
        ParameterLookupOperation::Put { row } => {
            put_parameter_lookup_row(txn, table, lookup_table, row)
        }
        ParameterLookupOperation::Update { old_data, new_data } => {
            // A PK change is only visible when the old tuple carries `id`,
            // which requires `id` to be (part of) the replica identity; with
            // the conventional id-as-primary-key schema that always holds.
            let new_id = row_id(lookup_table.source_table.as_str(), new_data)?;
            if old_data
                .as_ref()
                .and_then(|row| row.get("id"))
                .and_then(column_value_to_string)
                .is_some_and(|old_id| old_id != new_id)
            {
                if let Some(old_data) = old_data {
                    remove_parameter_lookup_row(txn, table, lookup_table, old_data)?;
                }
            }
            put_parameter_lookup_row(txn, table, lookup_table, new_data)
        }
        ParameterLookupOperation::Remove { old_data } => {
            remove_parameter_lookup_row(txn, table, lookup_table, old_data)
        }
    }
}

pub(super) fn scan_parameter_lookup_entries<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    lookup_id: &str,
    key_values: &[String],
    max_entries: usize,
) -> Result<Vec<BTreeMap<String, String>>, ReplicationIngestError> {
    let key_values = key_values.iter().map(String::as_str).collect::<Vec<_>>();
    let prefix = parameter_lookup_index_prefix(lookup_id, &key_values);
    let mut cursor = txn.cursor(table).map_err(|error| {
        ReplicationIngestError::Mdbx(format!("open parameter lookup cursor: {error}"))
    })?;
    let mut entries = Vec::new();
    for item in cursor.iter_from::<Vec<u8>, Vec<u8>>(&prefix) {
        let (key, value) = item.map_err(|error| {
            ReplicationIngestError::Mdbx(format!("scan parameter lookup entries: {error}"))
        })?;
        if !key.starts_with(&prefix) {
            break;
        }
        if entries.len() >= max_entries {
            return Err(ReplicationIngestError::ResourceLimit(format!(
                "parameter lookup result exceeds the configured {max_entries}-row limit"
            )));
        }
        entries.push(serde_json::from_slice(&value).map_err(|error| {
            ReplicationIngestError::CorruptBatch(format!(
                "parameter lookup entry {} is not valid JSON: {error}",
                String::from_utf8_lossy(&key)
            ))
        })?);
    }
    Ok(entries)
}

type ParameterLookupRowDocument = BTreeMap<String, Option<String>>;

fn parameter_lookup_row_document(
    lookup_table: &CompiledLookupTablePlan,
    row: &RowData,
    previous: Option<&ParameterLookupRowDocument>,
) -> Result<ParameterLookupRowDocument, ReplicationIngestError> {
    let mut columns = std::collections::BTreeSet::from(["id".to_owned()]);
    for lookup in &lookup_table.lookups {
        columns.extend(lookup.referenced_columns());
    }

    columns
        .into_iter()
        .map(|column| {
            let value = match row.get(&column) {
                Some(value) => column_value_to_string(value),
                // pgoutput omits unchanged TOAST columns from an update's new
                // tuple, while a genuine SQL NULL arrives as a present null
                // value; an absent column therefore keeps its previously
                // materialized value instead of collapsing to null.
                None => previous.and_then(|document| document.get(&column).cloned().flatten()),
            };
            Ok((column, value))
        })
        .collect()
}

fn row_id(source_table: &str, row: &RowData) -> Result<String, ReplicationIngestError> {
    row.get("id")
        .and_then(column_value_to_string)
        .ok_or_else(|| {
            ReplicationIngestError::CorruptBatch(format!(
                "parameter lookup table {source_table} change is missing required id column id"
            ))
        })
}

fn column_value_to_string(value: &ColumnValue) -> Option<String> {
    (!value.is_null()).then(|| value.to_string())
}

fn read_parameter_lookup_row<K: TransactionKind>(
    txn: &libmdbx::Transaction<'_, K, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    key: &[u8],
) -> Result<Option<ParameterLookupRowDocument>, ReplicationIngestError> {
    txn.get::<Vec<u8>>(table, key)
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!("read parameter lookup row: {error}"))
        })?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                ReplicationIngestError::CorruptBatch(format!(
                    "parameter lookup row {} is not valid JSON: {error}",
                    String::from_utf8_lossy(key)
                ))
            })
        })
        .transpose()
}

fn put_parameter_lookup_indexes(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    lookup_table: &CompiledLookupTablePlan,
    row_id: &str,
    document: &ParameterLookupRowDocument,
) -> Result<(), ReplicationIngestError> {
    for lookup in &lookup_table.lookups {
        if !row_predicate_matches(lookup.row_predicate.as_ref(), document) {
            continue;
        }
        let Some(key_values) = lookup
            .key_bindings
            .iter()
            .map(|(column, _)| document.get(column).and_then(Option::as_deref))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let Some(payload) = lookup
            .selected
            .iter()
            .map(|column| {
                document
                    .get(&column.column)
                    .and_then(Option::as_ref)
                    .map(|value| (column.alias.clone(), value.clone()))
            })
            .collect::<Option<BTreeMap<_, _>>>()
        else {
            continue;
        };
        let encoded = serde_json::to_vec(&payload).map_err(|error| {
            ReplicationIngestError::CorruptBatch(format!(
                "serialize parameter lookup entry {}.{}: {error}",
                lookup.lookup_id, row_id
            ))
        })?;
        txn.put(
            table,
            parameter_lookup_index_key(&lookup.lookup_id, &key_values, row_id),
            encoded,
            WriteFlags::UPSERT,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "persist parameter lookup entry {}.{}: {error}",
                lookup.lookup_id, row_id
            ))
        })?;
    }
    Ok(())
}

fn delete_parameter_lookup_indexes(
    txn: &libmdbx::Transaction<'_, libmdbx::RW, NoWriteMap>,
    table: &libmdbx::Table<'_>,
    lookup_table: &CompiledLookupTablePlan,
    row_id: &str,
    document: &ParameterLookupRowDocument,
) -> Result<(), ReplicationIngestError> {
    for lookup in &lookup_table.lookups {
        if !row_predicate_matches(lookup.row_predicate.as_ref(), document) {
            continue;
        }
        let Some(key_values) = lookup
            .key_bindings
            .iter()
            .map(|(column, _)| document.get(column).and_then(Option::as_deref))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        txn.del(
            table,
            parameter_lookup_index_key(&lookup.lookup_id, &key_values, row_id),
            None,
        )
        .map_err(|error| {
            ReplicationIngestError::Mdbx(format!(
                "delete parameter lookup entry {}.{}: {error}",
                lookup.lookup_id, row_id
            ))
        })?;
    }
    Ok(())
}

/// Evaluate a lookup row predicate against the stored document. The lookup
/// grammar only produces `And` conjunctions of `IsNull { Column, .. }` and
/// `Eq { Column, String-literal }` (either operand order), and the snapshot
/// type guard pins every referenced column to a text-family type, so plain
/// string equality here matches what PostgreSQL evaluated on the live path.
/// An absent column is treated as SQL null, matching the row-context IS NULL
/// convention pinned by the eval goldens.
fn row_predicate_matches(
    predicate: Option<&Predicate>,
    document: &ParameterLookupRowDocument,
) -> bool {
    predicate.is_none_or(|predicate| row_predicate_eval(predicate, document))
}

fn row_predicate_eval(predicate: &Predicate, document: &ParameterLookupRowDocument) -> bool {
    match predicate {
        Predicate::And { terms } => terms.iter().all(|term| row_predicate_eval(term, document)),
        // Unreachable via parse_parameter_lookup_plan's grammar; never match
        // rather than guess at semantics the compiler cannot produce.
        Predicate::Or { .. } | Predicate::In { .. } => false,
        Predicate::IsNull { operand, negated } => {
            let Operand::Column { name } = operand else {
                return false;
            };
            let is_null = document.get(name).is_none_or(|value| value.is_none());
            is_null != *negated
        }
        Predicate::Eq { left, right } => {
            let (Some(left), Some(right)) =
                (operand_text(left, document), operand_text(right, document))
            else {
                // A null column never satisfies equality, exactly as in SQL.
                return false;
            };
            left == right
        }
    }
}

fn operand_text<'a>(
    operand: &'a Operand,
    document: &'a ParameterLookupRowDocument,
) -> Option<&'a str> {
    match operand {
        Operand::Column { name } => document.get(name).and_then(|value| value.as_deref()),
        Operand::Literal {
            value: LiteralValue::String(value),
        } => Some(value.as_str()),
        // Non-string literals and bindings are rejected by the lookup grammar.
        Operand::Literal { .. } | Operand::Binding { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_rules::parse_parameter_lookup_plan;
    use tempfile::TempDir;

    fn lookup_table() -> CompiledLookupTablePlan {
        CompiledLookupTablePlan {
            source_table: "Membership".to_owned(),
            lookups: vec![
                parse_parameter_lookup_plan(
                    "SELECT workspace_id AS workspace_id FROM Membership WHERE team_id = auth.user_id() AND kind = 'shared'",
                )
                .expect("lookup plan"),
                parse_parameter_lookup_plan(
                    "SELECT value AS value FROM Membership WHERE team_id = auth.user_id() AND kind = 'shared'",
                )
                .expect("second lookup plan"),
            ],
        }
    }

    fn row(id: &str, team_id: &str, kind: &str, workspace_id: &str, value: &str) -> RowData {
        RowData::from_pairs(vec![
            ("id", ColumnValue::text(id)),
            ("team_id", ColumnValue::text(team_id)),
            ("kind", ColumnValue::text(kind)),
            ("workspace_id", ColumnValue::text(workspace_id)),
            ("value", ColumnValue::text(value)),
        ])
    }

    fn put(
        store: &crate::replication::ingest::store::ReplicationMdbxStore,
        plan: &CompiledLookupTablePlan,
        row: &RowData,
    ) {
        let txn = store.db.begin_rw_txn().expect("rw txn");
        let table = txn.open_table(None).expect("default table");
        put_parameter_lookup_row(&txn, &table, plan, row).expect("put lookup row");
        txn.commit().expect("commit");
    }

    fn update(
        store: &crate::replication::ingest::store::ReplicationMdbxStore,
        plan: &CompiledLookupTablePlan,
        old_data: RowData,
        new_data: RowData,
    ) {
        let txn = store.db.begin_rw_txn().expect("rw txn");
        let table = txn.open_table(None).expect("default table");
        let operation = DerivedParameterLookupOp {
            source_table: plan.source_table.clone(),
            operation: ParameterLookupOperation::Update {
                old_data: Some(old_data),
                new_data,
            },
        };
        apply_parameter_lookup_op(&txn, &table, plan, &operation).expect("update lookup row");
        txn.commit().expect("commit");
    }

    #[test]
    fn parameter_lookup_rows_follow_key_and_predicate_transitions() {
        let directory = TempDir::new().expect("temp directory");
        let store = crate::replication::ingest::store::ReplicationMdbxStore::new(directory.path())
            .expect("store");
        let plan = lookup_table();
        let first = row("row:1", "team:1", "shared", "workspace:1", "first");
        put(&store, &plan, &first);

        let key = vec!["team:1".to_owned()];
        let rows = store
            .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &key, 10)
            .expect("read lookup rows");
        assert_eq!(
            rows,
            vec![BTreeMap::from([(
                "workspace_id".to_owned(),
                "workspace:1".to_owned()
            )])]
        );
        let second_rows = store
            .read_parameter_lookup_rows(&plan.lookups[1].lookup_id, &key, 10)
            .expect("read second lookup rows");
        assert_eq!(
            second_rows,
            vec![BTreeMap::from([("value".to_owned(), "first".to_owned())])]
        );

        let predicate_failed = row("row:1", "team:1", "private", "workspace:2", "second");
        update(&store, &plan, first.clone(), predicate_failed.clone());
        assert!(store
            .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &key, 10)
            .expect("read after predicate transition")
            .is_empty());

        let key_changed = row("row:1", "team:2", "shared", "workspace:3", "third");
        update(&store, &plan, predicate_failed, key_changed);
        assert!(store
            .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &key, 10)
            .expect("read old key")
            .is_empty());
        assert_eq!(
            store
                .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &["team:2".to_owned()], 10,)
                .expect("read new key"),
            vec![BTreeMap::from([(
                "workspace_id".to_owned(),
                "workspace:3".to_owned()
            )])]
        );
    }

    #[test]
    fn absent_columns_carry_forward_while_explicit_nulls_drop_entries() {
        let directory = TempDir::new().expect("temp directory");
        let store = crate::replication::ingest::store::ReplicationMdbxStore::new(directory.path())
            .expect("store");
        let plan = lookup_table();
        put(
            &store,
            &plan,
            &row("row:1", "team:1", "shared", "workspace:1", "first"),
        );
        let key = vec!["team:1".to_owned()];

        // pgoutput omits unchanged TOAST columns from an update's new tuple;
        // the prior materialized values must survive such an update.
        let toast_update = RowData::from_pairs(vec![
            ("id", ColumnValue::text("row:1")),
            ("team_id", ColumnValue::text("team:1")),
            ("kind", ColumnValue::text("shared")),
        ]);
        update(
            &store,
            &plan,
            row("row:1", "team:1", "shared", "workspace:1", "first"),
            toast_update,
        );
        assert_eq!(
            store
                .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &key, 10)
                .expect("read after unchanged-toast update"),
            vec![BTreeMap::from([(
                "workspace_id".to_owned(),
                "workspace:1".to_owned()
            )])]
        );
        assert_eq!(
            store
                .read_parameter_lookup_rows(&plan.lookups[1].lookup_id, &key, 10)
                .expect("read second lookup after unchanged-toast update"),
            vec![BTreeMap::from([("value".to_owned(), "first".to_owned())])]
        );

        // A present SQL NULL is not an omission: nulling the selected column
        // must drop the entry.
        let null_update = RowData::from_pairs(vec![
            ("id", ColumnValue::text("row:1")),
            ("team_id", ColumnValue::text("team:1")),
            ("kind", ColumnValue::text("shared")),
            ("workspace_id", ColumnValue::Null),
            ("value", ColumnValue::text("second")),
        ]);
        update(
            &store,
            &plan,
            row("row:1", "team:1", "shared", "workspace:1", "first"),
            null_update,
        );
        assert!(store
            .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &key, 10)
            .expect("read after explicit null")
            .is_empty());
        assert_eq!(
            store
                .read_parameter_lookup_rows(&plan.lookups[1].lookup_id, &key, 10)
                .expect("read second lookup after explicit null"),
            vec![BTreeMap::from([("value".to_owned(), "second".to_owned())])]
        );
    }

    #[test]
    fn parameter_lookup_keys_are_separator_safe_and_reads_are_bounded() {
        let first = parameter_lookup_index_key("lookup:1", &["a:b", "c"], "row:1");
        let second = parameter_lookup_index_key("lookup", &["1:a:b", "c"], "row:1");
        assert_ne!(first, second);
        assert_ne!(
            parameter_lookup_row_key("table:1", "row:2"),
            parameter_lookup_row_key("table", "1:row:2")
        );

        let directory = TempDir::new().expect("temp directory");
        let store = crate::replication::ingest::store::ReplicationMdbxStore::new(directory.path())
            .expect("store");
        let plan = lookup_table();
        put(
            &store,
            &plan,
            &row("one", "team", "shared", "workspace:1", "one"),
        );
        put(
            &store,
            &plan,
            &row("two", "team", "shared", "workspace:2", "two"),
        );
        let error = store
            .read_parameter_lookup_rows(&plan.lookups[0].lookup_id, &["team".to_owned()], 1)
            .expect_err("lookup reads must fail beyond max_entries");
        assert!(matches!(error, ReplicationIngestError::ResourceLimit(_)));
    }
}
