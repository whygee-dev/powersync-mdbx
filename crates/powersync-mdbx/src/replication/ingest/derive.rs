use std::collections::BTreeMap;

use pg_walstream::{EventType, RowData};

use super::{
    batch_codec::ReplicationCommitBatch, error::ReplicationIngestError,
    tail_log::PersistedSyncTailOperation,
};
use crate::sync_rules::{JsonColumnTypes, RustExecutionPlan};

#[derive(Debug, Clone)]
pub(super) struct DerivedParameterLookupOp {
    pub(super) source_table: String,
    pub(super) operation: ParameterLookupOperation,
}

#[derive(Debug, Clone)]
pub(super) enum ParameterLookupOperation {
    Put {
        row: RowData,
    },
    Update {
        old_data: Option<RowData>,
        new_data: RowData,
    },
    Remove {
        old_data: RowData,
    },
}

pub(super) fn derive_sync_tail_ops_with_options(
    batch: &ReplicationCommitBatch,
    plan: &RustExecutionPlan,
    assume_new_inserts: bool,
) -> Result<Vec<DerivedSyncTailOp>, ReplicationIngestError> {
    let mut ops = Vec::new();

    for change in &batch.changes {
        match &change.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } if schema.as_ref() == "public" => {
                if let Some(plan) = plan.table_plan(table.as_ref()) {
                    let column_types = batch.column_types_by_table.get(table.as_ref());
                    ops.push(derive_row_put(
                        plan.object_type(),
                        plan.object_id_for_row(data)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        plan.route_fields_for_row(data, true)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        serialize_row_for_tail(plan, data, column_types)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        assume_new_inserts,
                    )?);
                }
            }
            EventType::Update {
                schema,
                table,
                old_data,
                new_data,
                ..
            } if schema.as_ref() == "public" => {
                if let Some(plan) = plan.table_plan(table.as_ref()) {
                    let column_types = batch.column_types_by_table.get(table.as_ref());
                    if let Some(old_data) = old_data {
                        ops.push(derive_row_remove(
                            plan.object_type(),
                            plan.object_id_for_row(old_data)
                                .map_err(sync_rule_error_to_ingest_error)?,
                            plan.route_fields_for_row(old_data, false)
                                .map_err(sync_rule_error_to_ingest_error)?,
                        )?);
                    }
                    ops.push(derive_row_put(
                        plan.object_type(),
                        plan.object_id_for_row(new_data)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        plan.route_fields_for_row(new_data, true)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        serialize_row_for_tail(plan, new_data, column_types)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        false,
                    )?);
                }
            }
            EventType::Delete {
                schema,
                table,
                old_data,
                ..
            } if schema.as_ref() == "public" => {
                if let Some(plan) = plan.table_plan(table.as_ref()) {
                    ops.push(derive_row_remove(
                        plan.object_type(),
                        plan.object_id_for_row(old_data)
                            .map_err(sync_rule_error_to_ingest_error)?,
                        plan.route_fields_for_row(old_data, false)
                            .map_err(sync_rule_error_to_ingest_error)?,
                    )?);
                }
            }
            EventType::Truncate(tables) => {
                for table_name in tables {
                    if let Some(source_table) = table_name.as_ref().strip_prefix("public.") {
                        if plan.table_plan(source_table).is_some()
                            || plan.lookup_table_plan(source_table).is_some()
                        {
                            return Err(ReplicationIngestError::UnsupportedPgoutputMessage(
                                "truncate on a materialized table",
                            ));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ops)
}

pub(super) fn derive_parameter_lookup_ops(
    batch: &ReplicationCommitBatch,
    plan: &RustExecutionPlan,
) -> Result<Vec<DerivedParameterLookupOp>, ReplicationIngestError> {
    let mut operations = Vec::new();
    for change in &batch.changes {
        match &change.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } if schema.as_ref() == "public" => {
                if let Some(lookup_table) = plan.lookup_table_plan(table) {
                    operations.push(DerivedParameterLookupOp {
                        source_table: lookup_table.source_table.clone(),
                        operation: ParameterLookupOperation::Put { row: data.clone() },
                    });
                }
            }
            EventType::Update {
                schema,
                table,
                old_data,
                new_data,
                ..
            } if schema.as_ref() == "public" => {
                if let Some(lookup_table) = plan.lookup_table_plan(table) {
                    operations.push(DerivedParameterLookupOp {
                        source_table: lookup_table.source_table.clone(),
                        operation: ParameterLookupOperation::Update {
                            old_data: old_data.clone(),
                            new_data: new_data.clone(),
                        },
                    });
                }
            }
            EventType::Delete {
                schema,
                table,
                old_data,
                ..
            } if schema.as_ref() == "public" => {
                if let Some(lookup_table) = plan.lookup_table_plan(table) {
                    operations.push(DerivedParameterLookupOp {
                        source_table: lookup_table.source_table.clone(),
                        operation: ParameterLookupOperation::Remove {
                            old_data: old_data.clone(),
                        },
                    });
                }
            }
            EventType::Truncate(tables) => {
                for table_name in tables {
                    if let Some(source_table) = table_name.as_ref().strip_prefix("public.") {
                        if plan.lookup_table_plan(source_table).is_some() {
                            return Err(ReplicationIngestError::UnsupportedPgoutputMessage(
                                "truncate on a materialized table",
                            ));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(operations)
}

pub(super) fn sync_rule_error_to_ingest_error(
    error: crate::sync_rules::SyncRuleError,
) -> ReplicationIngestError {
    ReplicationIngestError::CorruptBatch(error.to_string())
}

fn serialize_row_for_tail(
    plan: &crate::sync_rules::CompiledTablePlan,
    data: &RowData,
    column_types: Option<&JsonColumnTypes>,
) -> Result<String, crate::sync_rules::SyncRuleError> {
    column_types.map_or_else(
        || plan.serialize_full_row_json(data),
        |column_types| plan.serialize_full_row_json_with_column_types(data, column_types),
    )
}

fn derive_row_put(
    object_type: &str,
    object_id: String,
    route_fields: BTreeMap<String, String>,
    data_json: String,
    assume_new: bool,
) -> Result<DerivedSyncTailOp, ReplicationIngestError> {
    Ok(DerivedSyncTailOp {
        operation: PersistedSyncTailOperation::Put,
        object_type: Some(object_type.to_owned()),
        object_id: Some(object_id),
        route_fields,
        data_json: Some(data_json),
        previous_route_fields: None,
        previous_data_json: None,
        assume_new,
    })
}

fn derive_row_remove(
    object_type: &str,
    object_id: String,
    route_fields: BTreeMap<String, String>,
) -> Result<DerivedSyncTailOp, ReplicationIngestError> {
    Ok(DerivedSyncTailOp {
        operation: PersistedSyncTailOperation::Remove,
        object_type: Some(object_type.to_owned()),
        object_id: Some(object_id),
        route_fields,
        data_json: None,
        previous_route_fields: None,
        previous_data_json: None,
        assume_new: false,
    })
}

#[derive(Debug, Clone)]
pub(super) struct DerivedSyncTailOp {
    pub(super) operation: PersistedSyncTailOperation,
    pub(super) object_type: Option<String>,
    pub(super) object_id: Option<String>,
    pub(super) route_fields: BTreeMap<String, String>,
    pub(super) data_json: Option<String>,
    pub(super) previous_route_fields: Option<BTreeMap<String, String>>,
    pub(super) previous_data_json: Option<String>,
    pub(super) assume_new: bool,
}
