//! Request/auth parameter resolution for sync-stream bucket requests.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{auth::TokenPayload, sync_rules::CanonicalBinding};

use super::flatten_json_map;

#[derive(Debug, Clone)]
pub struct ResolvedParameterContext {
    auth_parameters: BTreeMap<String, String>,
    request_parameters: BTreeMap<String, String>,
    jwt_claims: BTreeMap<String, String>,
    user_id: Option<String>,
}

impl ResolvedParameterContext {
    pub fn from_request(
        token: Option<&TokenPayload>,
        request_parameters: &serde_json::Map<String, Value>,
    ) -> Self {
        let mut request_map = BTreeMap::new();
        flatten_json_map("", request_parameters, &mut request_map);

        let mut jwt_claims = token
            .map(TokenPayload::flattened_claim_strings)
            .unwrap_or_default();
        let user_id = token.and_then(|payload| payload.user_id().map(str::to_owned));
        let mut auth_parameters = jwt_claims.clone();
        if let Some(user_id) = &user_id {
            auth_parameters
                .entry("user_id".to_owned())
                .or_insert_with(|| user_id.clone());
        }

        Self {
            auth_parameters,
            request_parameters: request_map,
            jwt_claims: std::mem::take(&mut jwt_claims),
            user_id,
        }
    }

    pub fn binding_value(
        &self,
        binding: &CanonicalBinding,
        subscription_parameters: &BTreeMap<String, String>,
    ) -> Option<String> {
        match binding {
            CanonicalBinding::AuthParameter { name } => self.auth_parameters.get(name).cloned(),
            CanonicalBinding::SubscriptionParameter { name } => {
                subscription_parameters.get(name).cloned()
            }
            CanonicalBinding::RequestUserId => self.user_id.clone(),
            CanonicalBinding::RequestJwt { claim } => self.jwt_claims.get(claim).cloned(),
            CanonicalBinding::RequestParameter { name } => {
                self.request_parameters.get(name).cloned()
            }
            CanonicalBinding::RequestParameterArray { name } => {
                self.request_parameters.get(name).cloned()
            }
            CanonicalBinding::ParameterQueryColumn { name, .. } => {
                subscription_parameters.get(name).cloned()
            }
            CanonicalBinding::BucketParameter { name } => {
                subscription_parameters.get(name).cloned()
            }
        }
    }

    pub fn binding_values(
        &self,
        binding: &CanonicalBinding,
        subscription_parameters: &BTreeMap<String, String>,
    ) -> Vec<String> {
        match binding {
            CanonicalBinding::RequestParameterArray { name } => self
                .request_parameters
                .get(name)
                .map(|value| parse_request_array_values(value))
                .unwrap_or_default(),
            _ => self
                .binding_value(binding, subscription_parameters)
                .into_iter()
                .collect(),
        }
    }
}

fn parse_request_array_values(value: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .map(|values| {
            values
                .into_iter()
                .filter_map(|value| match value {
                    serde_json::Value::String(value) => Some(value),
                    serde_json::Value::Number(value) => Some(value.to_string()),
                    serde_json::Value::Bool(value) => Some(value.to_string()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_else(|| vec![value.to_owned()])
}
