use axum::{
    body::Bytes,
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::AppState;
use crate::control_plane::{ControlPlaneError, SyncRulesMutationOptions};

#[derive(Debug, Default, Deserialize)]
struct DiagnosticsRequest {
    #[serde(default)]
    sync_rules_content: bool,
}

#[derive(Debug, Default, Deserialize)]
struct SyncRulesContentRequest {
    #[serde(default)]
    content: String,
    #[serde(default)]
    base_version: Option<u64>,
    #[serde(default)]
    intent_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AdminValidateRequest {
    #[serde(default)]
    sync_rules: String,
}

#[derive(Debug, Default, Deserialize)]
struct SyncRulesMutationRequest {
    #[serde(default)]
    base_version: Option<u64>,
    #[serde(default)]
    intent_token: Option<String>,
}

pub async fn current_sync_rules(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    json_response(
        StatusCode::OK,
        state.service_context().sync_rules_current_payload(),
    )
}

pub async fn validate_sync_rules(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    match parse_sync_rules_content(&headers, &body) {
        Ok(content) => json_response(
            StatusCode::OK,
            state
                .service_context()
                .sync_rules_validate_payload(&content),
        ),
        Err(error) => json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    }
}

pub async fn deploy_sync_rules(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    let request = match parse_sync_rules_deploy_request(&headers, &body) {
        Ok(request) => request,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    };
    match state
        .service_context()
        .deploy_sync_rules(&request.content, request.options)
    {
        Ok(payload) => json_response(StatusCode::OK, payload),
        Err(error) => control_plane_error_response(error),
    }
}

pub async fn reprocess_sync_rules(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    let options = match parse_mutation_options(&body) {
        Ok(options) => options,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    };
    match state.service_context().reprocess_sync_rules(options) {
        Ok(payload) => json_response(StatusCode::OK, payload),
        Err(error) => control_plane_error_response(error),
    }
}

pub async fn diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    let include_content = if body.is_empty() {
        false
    } else {
        serde_json::from_slice::<DiagnosticsRequest>(&body)
            .map(|request| request.sync_rules_content)
            .unwrap_or(false)
    };
    json_response(
        StatusCode::OK,
        state.service_context().diagnostics_payload(include_content),
    )
}

pub async fn schema(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    json_response(StatusCode::OK, state.service_context().schema_payload())
}

pub async fn validate_admin(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    let request = match serde_json::from_slice::<AdminValidateRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json!({ "error": format!("invalid JSON body: {error}") }),
            )
        }
    };
    json_response(
        StatusCode::OK,
        state
            .service_context()
            .admin_validate_payload(&request.sync_rules),
    )
}

pub async fn reprocess_admin(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    let options = match parse_mutation_options(&body) {
        Ok(options) => options,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    };
    match state.service_context().admin_reprocess(options) {
        Ok(payload) => json_response(StatusCode::OK, payload),
        Err(error) => control_plane_error_response(error),
    }
}

pub async fn execute_sql(
    State(state): State<AppState>,
    headers: HeaderMap,
    _body: Bytes,
) -> Response {
    if let Err(error) = state.service_context().authorize_api(&headers) {
        return auth_error_response(error);
    }
    json_response(
        StatusCode::OK,
        state.service_context().execute_sql_out_of_scope_payload(),
    )
}

fn parse_sync_rules_content(headers: &HeaderMap, body: &[u8]) -> Result<String, String> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/json") {
        let request = serde_json::from_slice::<SyncRulesContentRequest>(body)
            .map_err(|error| format!("invalid JSON body: {error}"))?;
        if request.content.is_empty() {
            Err("content is required".to_owned())
        } else {
            Ok(request.content)
        }
    } else {
        let content = std::str::from_utf8(body)
            .map_err(|error| format!("invalid UTF-8 body: {error}"))?
            .to_owned();
        if content.trim().is_empty() {
            Err("content is required".to_owned())
        } else {
            Ok(content)
        }
    }
}

struct ParsedDeploySyncRulesRequest {
    content: String,
    options: SyncRulesMutationOptions,
}

fn parse_sync_rules_deploy_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ParsedDeploySyncRulesRequest, String> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/json") {
        let request = serde_json::from_slice::<SyncRulesContentRequest>(body)
            .map_err(|error| format!("invalid JSON body: {error}"))?;
        if request.content.is_empty() {
            return Err("content is required".to_owned());
        }
        Ok(ParsedDeploySyncRulesRequest {
            content: request.content,
            options: SyncRulesMutationOptions {
                base_version: request.base_version,
                intent_token: request.intent_token,
            },
        })
    } else {
        parse_sync_rules_content(headers, body).map(|content| ParsedDeploySyncRulesRequest {
            content,
            options: SyncRulesMutationOptions::default(),
        })
    }
}

fn parse_mutation_options(body: &[u8]) -> Result<SyncRulesMutationOptions, String> {
    if body.is_empty() {
        return Ok(SyncRulesMutationOptions::default());
    }
    let request = serde_json::from_slice::<SyncRulesMutationRequest>(body)
        .map_err(|error| format!("invalid JSON body: {error}"))?;
    Ok(SyncRulesMutationOptions {
        base_version: request.base_version,
        intent_token: request.intent_token,
    })
}

fn json_response(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn auth_error_response(error: crate::auth::AuthFailure) -> Response {
    json_response(error.status, error.body)
}

fn control_plane_error_response(error: ControlPlaneError) -> Response {
    json_response(error.status(), error.body().clone())
}
