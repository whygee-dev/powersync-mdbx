use std::{env, str::FromStr, sync::Arc, time::Instant};

use futures_util::{StreamExt, TryStreamExt};
use pg_walstream::{
    ColumnValue, PgReplicationConnection, ReplicationSlotOptions, RowData, SlotType,
};
use sha2::{Digest, Sha256};
use tokio_postgres::{types::ToSql, GenericClient, IsolationLevel, Row};
use tracing::info;

use super::{ingest::ReplicationMdbxStore, postgres::PostgresLsn, PostgresReplicationConfig};
use crate::{
    control_plane::ServiceContext,
    replication::runtime::ReplicationBootstrap,
    sync_rules::{CompiledTablePlan, JsonColumnType, JsonColumnTypes},
};

#[derive(Debug, Clone, Default)]
pub struct InitialSnapshotSummary {
    pub enabled: bool,
    pub rows_scanned: usize,
    pub batches_persisted: usize,
    pub snapshot_lsn: Option<PostgresLsn>,
    pub elapsed_ms: u128,
}

pub fn initial_snapshot_enabled() -> bool {
    env::var("POWERSYNC_RUST_INITIAL_SNAPSHOT")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

pub async fn run_initial_snapshot_if_enabled(
    config: &PostgresReplicationConfig,
    store: &Arc<ReplicationMdbxStore>,
    service_context: &ServiceContext,
) -> Result<InitialSnapshotSummary, String> {
    let snapshot_complete = store
        .is_initial_snapshot_complete()
        .map_err(|error| format!("read initial snapshot completion marker: {error}"))?;

    if !snapshot_complete && !initial_snapshot_enabled() {
        return Err(
            "initial snapshot is disabled but MDBX has no completed snapshot; refusing to start logical replication from a partial database"
                .to_owned(),
        );
    }

    let started = Instant::now();
    let bootstrap = ReplicationBootstrap::from_config(config, PostgresLsn(0)).map_err(|error| {
        format!("build initial snapshot bootstrap from replication config: {error}")
    })?;
    let mut control = bootstrap
        .connect_control_plane()
        .await
        .map_err(|error| format!("connect initial snapshot control plane: {error}"))?;
    let plan = service_context.active_plan();
    let source_identity =
        initial_snapshot_source_identity(&control.client, config, plan.as_ref()).await?;
    let required_source_tables = plan
        .source_tables()
        .into_iter()
        .map(CompiledTablePlan::source_table)
        .collect::<Vec<_>>();
    bootstrap
        .ensure_publication_covers(&control.client, &required_source_tables)
        .await
        .map_err(|error| format!("validate initial snapshot publication: {error}"))?;

    if snapshot_complete {
        let persisted_identity = store
            .initial_snapshot_source_identity()
            .map_err(|error| format!("read completed snapshot source identity: {error}"))?;
        if persisted_identity.as_deref() != Some(source_identity.as_str()) {
            return Err(
                "completed MDBX snapshot belongs to a different PostgreSQL cluster/database, slot, publication, group, or sync-rule storage contract"
                    .to_owned(),
            );
        }
        control
            .shutdown()
            .await
            .map_err(|error| format!("shutdown initial snapshot control plane: {error}"))?;
        info!("initial snapshot identity matches configured source; skipping re-run");
        return Ok(InitialSnapshotSummary {
            enabled: true,
            ..InitialSnapshotSummary::default()
        });
    }

    let bootstrap_intent = initial_snapshot_bootstrap_intent(&source_identity);
    reconcile_incomplete_bootstrap(
        &control.client,
        store,
        config,
        plan.as_ref(),
        &bootstrap_intent,
    )
    .await?;

    let replication_uri = replication_connection_uri(&config.uri);
    let slot_name = config.slot_name.clone();
    let (exporter, snapshot_lsn, snapshot_name) =
        tokio::task::spawn_blocking(move || create_exported_snapshot(&replication_uri, &slot_name))
            .await
            .map_err(|error| format!("join exported snapshot slot creation: {error}"))??;

    let mut rows_scanned = 0usize;
    let mut batches_persisted = 0usize;
    let batch_size = initial_snapshot_batch_size();

    // The slot's exported snapshot and consistent point are one PostgreSQL
    // visibility boundary. Keep the replication connection alive until this
    // transaction commits; PostgreSQL invalidates the exported snapshot when the
    // exporting session ends.
    let snapshot_txn = control
        .client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|error| format!("begin initial snapshot transaction: {error}"))?;
    snapshot_txn
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            escape_sql_literal(&snapshot_name)
        ))
        .await
        .map_err(|error| format!("import replication slot snapshot: {error}"))?;

    for table_plan in plan.source_tables() {
        let mut batch_rows = Vec::with_capacity(batch_size);
        let (column_types, stream) = load_table_row_stream(&snapshot_txn, table_plan).await?;
        futures_util::pin_mut!(stream);
        while let Some(row) = stream.try_next().await.map_err(|error| {
            format!(
                "snapshot query stream {}: {error}",
                table_plan.source_table()
            )
        })? {
            batch_rows.push(row);
            rows_scanned += 1;
            if batch_rows.len() >= batch_size {
                let rows = std::mem::replace(&mut batch_rows, Vec::with_capacity(batch_size));
                store
                    .persist_initial_snapshot_rows_with_presorted_columns_and_types(
                        table_plan.source_table(),
                        rows,
                        PostgresLsn(0),
                        plan.as_ref(),
                        &column_types,
                    )
                    .map_err(|error| format!("persist initial snapshot batch: {error}"))?;
                batches_persisted += 1;
            }
        }

        if !batch_rows.is_empty() {
            let rows = std::mem::take(&mut batch_rows);
            store
                .persist_initial_snapshot_rows_with_presorted_columns_and_types(
                    table_plan.source_table(),
                    rows,
                    PostgresLsn(0),
                    plan.as_ref(),
                    &column_types,
                )
                .map_err(|error| format!("persist initial snapshot batch: {error}"))?;
            batches_persisted += 1;
        }
    }

    snapshot_txn
        .commit()
        .await
        .map_err(|error| format!("commit initial snapshot transaction: {error}"))?;
    drop(exporter);

    store.record_source_snapshot_scan(started.elapsed().as_millis() as u64);
    store
        .persist_initial_snapshot_marker_with_plan(snapshot_lsn, plan.as_ref(), &source_identity)
        .map_err(|error| format!("persist initial snapshot LSN marker: {error}"))?;
    batches_persisted += 1;

    control
        .shutdown()
        .await
        .map_err(|error| format!("shutdown initial snapshot control plane: {error}"))?;
    let summary = InitialSnapshotSummary {
        enabled: true,
        rows_scanned,
        batches_persisted,
        snapshot_lsn: Some(snapshot_lsn),
        elapsed_ms: started.elapsed().as_millis(),
    };
    info!(?summary, "initial snapshot persisted into MDBX");
    Ok(summary)
}

fn initial_snapshot_batch_size() -> usize {
    env::var("POWERSYNC_RUST_INITIAL_SNAPSHOT_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10_000)
}

#[derive(Debug)]
struct BootstrapSlotState {
    active: bool,
    logical: bool,
    pgoutput: bool,
    current_database: bool,
}

fn initial_snapshot_bootstrap_intent(source_identity: &str) -> String {
    hash_identity(&["powersync-mdbx-snapshot-bootstrap-v2", source_identity])
}

async fn initial_snapshot_source_identity(
    client: &tokio_postgres::Client,
    config: &PostgresReplicationConfig,
    plan: &crate::sync_rules::RustExecutionPlan,
) -> Result<String, String> {
    let row = client
        .query_one(
            "SELECT system_identifier::text, database_oid::text, current_database()
             FROM pg_control_system(), LATERAL (
                 SELECT oid AS database_oid
                 FROM pg_database
                 WHERE datname = current_database()
             ) AS current_db",
            &[],
        )
        .await
        .map_err(|error| format!("read PostgreSQL source identity: {error}"))?;
    let system_identifier: String = row.get(0);
    let database_oid: String = row.get(1);
    let database_name: String = row.get(2);
    Ok(source_identity_from_parts(
        &system_identifier,
        &database_oid,
        &database_name,
        config,
        plan,
    ))
}

fn source_identity_from_parts(
    system_identifier: &str,
    database_oid: &str,
    database_name: &str,
    config: &PostgresReplicationConfig,
    plan: &crate::sync_rules::RustExecutionPlan,
) -> String {
    hash_identity(&[
        "powersync-mdbx-source-identity-v1",
        system_identifier,
        database_oid,
        database_name,
        config.slot_name.as_str(),
        config.publication_name.as_str(),
        config.group_id.as_str(),
        plan.storage_contract_id(),
    ])
}

fn hash_identity(values: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for value in values {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    format!("v1:{:x}", hasher.finalize())
}

async fn reconcile_incomplete_bootstrap(
    client: &tokio_postgres::Client,
    store: &ReplicationMdbxStore,
    config: &PostgresReplicationConfig,
    plan: &crate::sync_rules::RustExecutionPlan,
    expected_intent: &str,
) -> Result<(), String> {
    let existing_intent = store
        .initial_snapshot_bootstrap_intent()
        .map_err(|error| format!("read initial snapshot bootstrap intent: {error}"))?;
    let slot = client
        .query_opt(
            "SELECT active,
                    slot_type = 'logical',
                    COALESCE(plugin = 'pgoutput', false),
                    COALESCE(database::text = current_database(), false)
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&config.slot_name],
        )
        .await
        .map_err(|error| {
            format!(
                "inspect initial snapshot slot {}: {error}",
                config.slot_name
            )
        })?
        .map(|row| BootstrapSlotState {
            active: row.get(0),
            logical: row.get(1),
            pgoutput: row.get(2),
            current_database: row.get(3),
        });

    match existing_intent.as_deref() {
        None if slot.is_some() => {
            return Err(format!(
                "replication slot {} already exists but MDBX has no completed snapshot or matching bootstrap intent; refusing to adopt or drop an unowned slot",
                config.slot_name
            ));
        }
        Some(intent) if intent != expected_intent => {
            return Err(
                "MDBX contains an incomplete snapshot for a different source, slot, publication, or sync-rule storage contract; refusing destructive recovery"
                    .to_owned(),
            );
        }
        _ => {}
    }

    if let Some(slot) = slot {
        if slot.active {
            return Err(format!(
                "replication slot {} belongs to an incomplete snapshot bootstrap but is active; refusing to interrupt its owner",
                config.slot_name
            ));
        }
        if !slot.logical || !slot.pgoutput || !slot.current_database {
            return Err(format!(
                "replication slot {} does not match the expected logical pgoutput slot in the current database; refusing to drop it",
                config.slot_name
            ));
        }
        client
            .query_one("SELECT pg_drop_replication_slot($1)", &[&config.slot_name])
            .await
            .map_err(|error| {
                format!(
                    "drop interrupted initial snapshot slot {}: {error}",
                    config.slot_name
                )
            })?;
    }

    // Persist ownership before CREATE_REPLICATION_SLOT. On a crash, the next
    // process may only drop the inactive slot when this exact source and
    // storage-contract fingerprint is present.
    store
        .reset_incomplete_initial_snapshot(expected_intent, plan)
        .map_err(|error| format!("reset incomplete initial snapshot: {error}"))?;
    Ok(())
}

fn replication_connection_uri(uri: &str) -> String {
    let separator = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{separator}replication=database")
}

fn create_exported_snapshot(
    uri: &str,
    slot_name: &str,
) -> Result<(PgReplicationConnection, PostgresLsn, String), String> {
    let connection = PgReplicationConnection::connect(uri)
        .map_err(|error| format!("connect replication protocol for snapshot export: {error}"))?;
    let result = connection
        .create_replication_slot_with_options(
            slot_name,
            SlotType::Logical,
            Some("pgoutput"),
            &ReplicationSlotOptions {
                snapshot: Some("export".to_owned()),
                ..ReplicationSlotOptions::default()
            },
        )
        .map_err(|error| {
            format!("create replication slot {slot_name} with EXPORT_SNAPSHOT: {error}")
        })?;
    let consistent_point = result
        .get_value(0, 1)
        .ok_or_else(|| "CREATE_REPLICATION_SLOT did not return consistent_point".to_owned())?;
    let snapshot_name = result
        .get_value(0, 2)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "CREATE_REPLICATION_SLOT did not return snapshot_name".to_owned())?;
    let snapshot_lsn = PostgresLsn::from_str(&consistent_point).map_err(|error| {
        format!("parse replication slot consistent point {consistent_point}: {error}")
    })?;
    Ok((connection, snapshot_lsn, snapshot_name))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

async fn load_table_row_stream<C: GenericClient>(
    client: &C,
    table_plan: &CompiledTablePlan,
) -> Result<
    (
        JsonColumnTypes,
        impl futures_util::Stream<Item = Result<RowData, String>>,
    ),
    String,
> {
    let (schema, table) = split_table_name(table_plan.source_table());
    let mut table_columns = table_columns(client, schema, table).await?;
    table_columns.sort_by(|left, right| left.name.cmp(&right.name));
    let columns = table_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let column_types = table_columns
        .iter()
        .map(|column| (column.name.clone(), column.json_type))
        .collect::<JsonColumnTypes>();
    let query = format!(
        "SELECT {} FROM {}{}",
        columns
            .iter()
            .map(|column| format!("{0}::text AS {0}", quote_identifier(column)))
            .collect::<Vec<_>>()
            .join(", "),
        quote_table(schema, table),
        order_by_clause(&columns)
    );
    let stream = client
        .query_raw(query.as_str(), std::iter::empty::<&(dyn ToSql + Sync)>())
        .await
        .map_err(|error| format!("snapshot query {}: {error}", table_plan.source_table()))?;
    let column_names = columns
        .iter()
        .map(|column| Arc::<str>::from(column.as_str()))
        .collect::<Vec<_>>();
    let stream = stream.map(move |row| match row {
        Ok(row) => row_to_row_data(row, &column_names),
        Err(error) => Err(error.to_string()),
    });
    Ok((column_types, stream))
}

fn row_to_row_data(row: Row, columns: &[Arc<str>]) -> Result<RowData, String> {
    let mut data = RowData::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let value = row
            .try_get::<usize, Option<String>>(index)
            .map_err(|error| format!("read snapshot column {column}: {error}"))?;
        data.push(
            Arc::clone(column),
            value
                .as_deref()
                .map(ColumnValue::text)
                .unwrap_or(ColumnValue::Null),
        );
    }
    Ok(data)
}

#[derive(Debug, Clone)]
struct SnapshotTableColumn {
    name: String,
    json_type: JsonColumnType,
}

async fn table_columns<C: GenericClient>(
    client: &C,
    schema: &str,
    table: &str,
) -> Result<Vec<SnapshotTableColumn>, String> {
    let rows = client
        .query(
            "SELECT column_name, udt_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await
        .map_err(|error| format!("load columns for {schema}.{table}: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let name: String = row.get(0);
            let udt_name: String = row.get(1);
            SnapshotTableColumn {
                name,
                json_type: json_column_type_from_postgres_type(&udt_name),
            }
        })
        .collect())
}

fn json_column_type_from_postgres_type(type_name: &str) -> JsonColumnType {
    match type_name {
        "int2" | "int4" | "int8" | "float4" | "float8" | "numeric" => JsonColumnType::Number,
        "bool" => JsonColumnType::Boolean,
        "date" | "timestamp" | "timestamptz" => JsonColumnType::Timestamp,
        "json" | "jsonb" => JsonColumnType::Json,
        _ => JsonColumnType::String,
    }
}

fn split_table_name(source_table: &str) -> (&str, &str) {
    source_table
        .split_once('.')
        .unwrap_or(("public", source_table))
}

fn quote_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn order_by_clause(columns: &[String]) -> String {
    if columns.iter().any(|column| column == "id") {
        format!(" ORDER BY {}", quote_identifier("id"))
    } else if columns.is_empty() {
        String::new()
    } else {
        // No single `id` column: order by every column (already name-sorted) so
        // batch boundaries within a snapshot are deterministic across runs.
        format!(
            " ORDER BY {}",
            columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_rules::execution_plan;

    #[test]
    fn bootstrap_intent_is_stable_scoped_and_credential_opaque() {
        let mut config = PostgresReplicationConfig {
            uri: "postgres://user:super-secret@localhost/app?sslmode=disable".to_owned(),
            slot_name: "powersync_slot".to_owned(),
            publication_name: "powersync_publication".to_owned(),
            group_id: "default".to_owned(),
        };
        let source_identity = source_identity_from_parts(
            "system-identifier",
            "16384",
            "app",
            &config,
            execution_plan(),
        );
        let first = initial_snapshot_bootstrap_intent(&source_identity);
        assert_eq!(first, initial_snapshot_bootstrap_intent(&source_identity));
        assert!(!first.contains("super-secret"));

        config.uri = "postgres://user:rotated-secret@localhost/app?sslmode=disable".to_owned();
        let after_credential_rotation = source_identity_from_parts(
            "system-identifier",
            "16384",
            "app",
            &config,
            execution_plan(),
        );
        assert_eq!(source_identity, after_credential_rotation);

        config.slot_name = "different_slot".to_owned();
        let different_source = source_identity_from_parts(
            "system-identifier",
            "16384",
            "app",
            &config,
            execution_plan(),
        );
        assert_ne!(first, initial_snapshot_bootstrap_intent(&different_source));
    }
}
