use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use super::eval::stream_bucket_groups;

pub type JsonColumnTypes = BTreeMap<String, JsonColumnType>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum JsonColumnType {
    String,
    Number,
    Boolean,
    Timestamp,
    Json,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SyncBucketDescriptor {
    pub bucket_name: String,
    pub stream_name: String,
    pub is_default: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalSemanticPlan {
    pub version: u32,
    pub edition: u32,
    pub compatibility_version: u32,
    pub storage_version: u32,
    pub streams: Vec<CanonicalStream>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalStream {
    pub name: String,
    pub source_table: String,
    pub output_table: String,
    pub auto_subscribe: bool,
    pub bucket_parameters: Vec<CanonicalBucketParameter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_filter: Option<Predicate>,
    pub projection: CanonicalProjection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data_queries: Vec<CanonicalDataQuery>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalDataQuery {
    pub source_table: String,
    pub output_table: String,
    pub bucket_parameters: Vec<CanonicalBucketParameter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_filter: Option<Predicate>,
    pub projection: CanonicalProjection,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalBucketParameter {
    pub name: String,
    pub source_column: String,
    pub binding: CanonicalBinding,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CanonicalBucketGroup {
    pub bucket_parameters: Vec<CanonicalBucketParameter>,
    pub request_filter: Option<Predicate>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanonicalBinding {
    AuthParameter { name: String },
    SubscriptionParameter { name: String },
    RequestUserId,
    RequestJwt { claim: String },
    RequestParameter { name: String },
    RequestParameterArray { name: String },
    ParameterQueryColumn { name: String, query: String },
    BucketParameter { name: String },
}

/// A structured filter predicate. Replaces the raw-SQL-substring `row_filter` /
/// `request_filter` representation: the WHERE clause is classified into typed
/// predicates at compile time, and evaluated by two context visitors (see
/// `eval.rs`) instead of being re-parsed from a string at serve time.
///
/// `And` / `Or` are n-ary and flat (a singleton conjunction/disjunction is
/// unwrapped to its sole term). `Eq` and `In` keep their operands in source
/// order — the row visitor compares positionally, which preserves the
/// directional, string-anchored coercion of `json_values_equal`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Predicate {
    And { terms: Vec<Predicate> },
    Or { terms: Vec<Predicate> },
    IsNull { operand: Operand, negated: bool },
    Eq { left: Operand, right: Operand },
    In { left: Operand, right: Operand },
}

/// One side of a comparison predicate. Classified once at compile time with the
/// precedence binding → literal → column, so a row filter only ever yields
/// `Column`/`Literal` and a request filter only ever yields `Binding`/`Literal`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operand {
    Column { name: String },
    Literal { value: LiteralValue },
    Binding { binding: CanonicalBinding },
}

/// The literal kinds the evaluator can honor. SQL float literals are rejected at
/// compile time, so they are intentionally absent here — the type encodes that
/// invariant. Mirrors exactly what `sql_literal_to_json` used to produce.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Null,
}

impl LiteralValue {
    /// The `serde_json::Value` this literal compares as. Must reproduce exactly
    /// what `sql_literal_to_json` returned so `json_values_equal` is unchanged.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            LiteralValue::String(value) => serde_json::Value::String(value.clone()),
            LiteralValue::Integer(value) => serde_json::Value::Number((*value).into()),
            LiteralValue::Boolean(value) => serde_json::Value::Bool(*value),
            LiteralValue::Null => serde_json::Value::Null,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanonicalProjection {
    Star,
    StarWithComputed {
        computed: Vec<CanonicalComputedColumn>,
    },
    Columns {
        columns: Vec<CanonicalProjectedColumn>,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalComputedColumn {
    pub alias: String,
    pub expression: CanonicalComputedExpression,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalComputedExpression {
    pub terms: Vec<CanonicalComputedTerm>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanonicalComputedTerm {
    Column { source_column: String },
    Literal { value: String },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalProjectedColumn {
    pub source_column: String,
    pub alias: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RustExecutionPlan {
    pub(super) canonical: CanonicalSemanticPlan,
    pub(super) streams_by_name: HashMap<String, CanonicalStream>,
    pub(super) tables_by_source: HashMap<String, CompiledTablePlan>,
    pub(super) route_index_columns_by_object: HashMap<String, Vec<Vec<String>>>,
    pub(super) stream_bucket_groups_by_name: HashMap<String, Vec<StreamBucketGroup>>,
    pub(super) accumulator_queries_by_object: HashMap<String, Vec<AccumulatorQueryTemplate>>,
    pub(super) default_stream_names: Vec<String>,
    // Computed once at lowering time: the contract id embeds the full
    // canonical plan JSON, so recomputing it per persisted batch is costly.
    pub(super) storage_contract_id: String,
    pub(super) storage_contract_fingerprint: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledTablePlan {
    pub(super) source_table: String,
    pub(super) object_type: String,
    pub(super) route_columns: Vec<String>,
    pub(super) object_id_expression: Option<CanonicalComputedExpression>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct StreamBucketGroup {
    pub(super) index: usize,
    pub(super) bucket_parameters: Vec<CanonicalBucketParameter>,
    pub(super) request_filter: Option<Predicate>,
    pub(super) queries: Vec<CanonicalDataQuery>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct AccumulatorQueryTemplate {
    pub(super) stream_name: String,
    pub(super) object_type: String,
    pub(super) bucket_parameters: Vec<CanonicalBucketParameter>,
    pub(super) row_filter: Option<Predicate>,
    pub(super) request_filter: Option<Predicate>,
    pub(super) projection: CanonicalProjection,
    pub(super) projection_key: String,
    pub(super) is_default: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedSyncBucket {
    pub(super) bucket_name: String,
    pub(super) stream_name: String,
    pub(super) object_type: String,
    pub(super) route_constraints: BTreeMap<String, String>,
    pub(super) projection: CanonicalProjection,
    pub(super) projection_key: String,
    pub(super) queries: Vec<ResolvedSyncQuery>,
    pub(super) is_default: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedSyncQuery {
    pub(super) object_type: String,
    pub(super) route_constraints: BTreeMap<String, String>,
    pub(super) row_filter: Option<Predicate>,
    pub(super) request_filter: Option<Predicate>,
    pub(super) projection: CanonicalProjection,
    pub(super) checkpoint_accumulator_key: String,
}

impl ResolvedSyncQuery {
    pub fn object_type(&self) -> &str {
        &self.object_type
    }

    pub fn route_constraints(&self) -> &BTreeMap<String, String> {
        &self.route_constraints
    }

    pub fn row_filter(&self) -> Option<&Predicate> {
        self.row_filter.as_ref()
    }

    pub fn request_filter(&self) -> Option<&Predicate> {
        self.request_filter.as_ref()
    }

    pub fn projection(&self) -> &CanonicalProjection {
        &self.projection
    }

    pub fn checkpoint_accumulator_key(&self) -> &str {
        &self.checkpoint_accumulator_key
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StreamDefinition<'a> {
    pub name: &'a str,
    pub query: &'a str,
    pub auto_subscribe: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct SyncRuleError(pub(super) String);

impl CanonicalStream {
    pub(super) fn data_queries(&self) -> Vec<CanonicalDataQuery> {
        if self.data_queries.is_empty() {
            vec![CanonicalDataQuery {
                source_table: self.source_table.clone(),
                output_table: self.output_table.clone(),
                bucket_parameters: self.bucket_parameters.clone(),
                row_filter: self.row_filter.clone(),
                request_filter: self.request_filter.clone(),
                projection: self.projection.clone(),
            }]
        } else {
            self.data_queries.clone()
        }
    }

    pub fn bucket_parameter_groups(&self) -> Vec<Vec<CanonicalBucketParameter>> {
        stream_bucket_groups(self)
            .into_iter()
            .map(|group| group.bucket_parameters)
            .collect()
    }

    pub fn bucket_groups(&self) -> Vec<CanonicalBucketGroup> {
        stream_bucket_groups(self)
            .into_iter()
            .map(|group| CanonicalBucketGroup {
                bucket_parameters: group.bucket_parameters,
                request_filter: group.request_filter,
            })
            .collect()
    }
}
