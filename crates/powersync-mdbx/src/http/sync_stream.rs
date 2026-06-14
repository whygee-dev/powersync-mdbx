use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::{Extension, Json, State},
    http::{
        header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER},
        HeaderMap, StatusCode,
    },
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::{
    stream::{self, BoxStream},
    StreamExt,
};
use serde::Deserialize;
use serde_json::Value;
use tracing::{error, info};

use crate::{
    auth::TokenPayload,
    control_plane::{extract_string_map, ResolvedParameterContext, ServiceContext},
    http::{AdmissionStartedAt, AppState},
    protocol::messages::{SyncChunk, SyncChunkKind},
    storage::{StreamEncoding, SyncBodySource, SyncBucketCursors, SyncChunkSource},
    sync_rules::{bucket_name_for_stream_group_values, request_filter_matches, RustExecutionPlan},
};

const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const BSON_STREAM_CONTENT_TYPE: &str = "application/vnd.powersync.bson-stream";
const DEBUG_EMISSION_PATH_HEADER: &str = "x-powersync-emission-path";
const DEBUG_REQUEST_MS_HEADER: &str = "x-powersync-request-ms";
const DEBUG_TOTAL_REQUEST_MS_HEADER: &str = "x-powersync-total-request-ms";
const DEBUG_PRE_HANDLER_MS_HEADER: &str = "x-powersync-pre-handler-ms";
const DEBUG_REQUEST_US_HEADER: &str = "x-powersync-request-us";
const DEBUG_TOTAL_REQUEST_US_HEADER: &str = "x-powersync-total-request-us";
const DEBUG_PRE_HANDLER_US_HEADER: &str = "x-powersync-pre-handler-us";
const MAX_SYNC_BUCKETS: usize = 256;
const MAX_STREAM_LIFETIME: Duration = Duration::from_secs(300);

pub async fn sync_stream(
    State(state): State<AppState>,
    Extension(admission_started_at): Extension<AdmissionStartedAt>,
    headers: HeaderMap,
    request: Result<Json<SyncStreamRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let request_started_at = Instant::now();
    if !state.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(CONTENT_TYPE, "application/json; charset=utf-8")],
            serde_json::json!({ "error": "sync storage is not ready" }).to_string(),
        )
            .into_response();
    }
    let token = match state.service_context().authorize_user(&headers) {
        Ok(token) => token,
        Err(error) => {
            return (
                error.status,
                [(CONTENT_TYPE, "application/json; charset=utf-8")],
                error.body.to_string(),
            )
                .into_response()
        }
    };
    let request = match request {
        Ok(Json(value)) => value,
        Err(rejection) => {
            return (
                rejection.status(),
                [(CONTENT_TYPE, "application/json; charset=utf-8")],
                serde_json::json!({ "error": "invalid sync request JSON" }).to_string(),
            )
                .into_response()
        }
    };
    let hold_open = request.should_hold_open();
    let stream_lifetime = Some(
        token
            .as_ref()
            .and_then(TokenPayload::remaining_lifetime)
            .map_or(MAX_STREAM_LIFETIME, |remaining| {
                remaining.min(MAX_STREAM_LIFETIME)
            }),
    );
    let active_plan = state.service_context().active_plan();
    let bucket_cursors = match request
        .bucket_cursors(state.service_context(), token.as_ref())
        .await
    {
        Ok(bucket_cursors) => bucket_cursors,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                [(CONTENT_TYPE, "application/json; charset=utf-8")],
                serde_json::json!({ "error": message }).to_string(),
            )
                .into_response()
        }
    };
    if bucket_cursors.buckets.len() > MAX_SYNC_BUCKETS {
        return (
            StatusCode::BAD_REQUEST,
            [(CONTENT_TYPE, "application/json; charset=utf-8")],
            serde_json::json!({
                "error": format!("sync request exceeds the {MAX_SYNC_BUCKETS}-bucket limit")
            })
            .to_string(),
        )
            .into_response();
    }
    let after = bucket_cursors.max_after();
    let binary_data = request.binary_data;
    info!(
        bucket_after = ?after,
        bucket_count = bucket_cursors.buckets.len(),
        hold_open,
        binary_data,
        user_id = token.as_ref().and_then(TokenPayload::user_id),
        "sync stream request received"
    );

    let sync_read_permit = match state.acquire_sync_read().await {
        Ok(permit) => permit,
        Err(()) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [
                    (CONTENT_TYPE, "application/json; charset=utf-8"),
                    (RETRY_AFTER, "1"),
                ],
                serde_json::json!({ "error": "sync read capacity is temporarily exhausted" })
                    .to_string(),
            )
                .into_response()
        }
    };

    match negotiate_content_type(&headers, binary_data) {
        Ok(NegotiatedContentType::Ndjson) => sync_response(
            state.storage(),
            active_plan.clone(),
            hold_open,
            bucket_cursors.clone(),
            StreamEncoding::Ndjson,
            SyncResponseRequestContext {
                admission_started_at: admission_started_at.0,
                request_started_at,
                stream_lifetime,
                sync_read_permit: Some(sync_read_permit),
            },
        ),
        Ok(NegotiatedContentType::Bson) => sync_response(
            state.storage(),
            active_plan,
            hold_open,
            bucket_cursors,
            StreamEncoding::Bson,
            SyncResponseRequestContext {
                admission_started_at: admission_started_at.0,
                request_started_at,
                stream_lifetime,
                sync_read_permit: Some(sync_read_permit),
            },
        ),
        Err(ContentNegotiationError::BadRequest(message)) => (
            StatusCode::BAD_REQUEST,
            [(CONTENT_TYPE, "text/plain; charset=utf-8")],
            message,
        )
            .into_response(),
        Err(ContentNegotiationError::NotAcceptable(message)) => (
            StatusCode::NOT_ACCEPTABLE,
            [(CONTENT_TYPE, "text/plain; charset=utf-8")],
            message,
        )
            .into_response(),
    }
}

fn sync_response(
    storage: &crate::SharedStorage,
    active_plan: Arc<RustExecutionPlan>,
    hold_open: bool,
    bucket_cursors: SyncBucketCursors,
    encoding: StreamEncoding,
    request_context: SyncResponseRequestContext,
) -> Response {
    let SyncResponseRequestContext {
        admission_started_at,
        request_started_at,
        stream_lifetime,
        mut sync_read_permit,
    } = request_context;
    let after = bucket_cursors.max_after();
    let (label, content_type) = match encoding {
        StreamEncoding::Ndjson => ("ndjson", NDJSON_CONTENT_TYPE),
        StreamEncoding::Bson => ("bson", BSON_STREAM_CONTENT_TYPE),
    };

    if hold_open {
        if after.is_some() {
            match storage.sync_hold_open_body_source_for_buckets_with_plan(
                &bucket_cursors,
                active_plan.as_ref(),
                encoding,
            ) {
                Ok(Some(source)) => {
                    let started_at = Instant::now();
                    info!(
                        bucket_after = ?after,
                        encoding = label,
                        emission_path = "preframed-body+pending",
                        chunk_count_hint = source.chunk_count_hint,
                        body_bytes = source.body.len(),
                        prepare_ms = started_at.elapsed().as_millis(),
                        request_ms = request_started_at.elapsed().as_millis(),
                        "prepared hold-open preframed sync body"
                    );
                    return hold_open_body_response(
                        source,
                        label,
                        after,
                        content_type,
                        admission_started_at,
                        request_started_at,
                        stream_lifetime,
                    );
                }
                Ok(None) => {}
                Err(storage_error) => return internal_storage_error_response(&storage_error),
            }
        }

        let started_at = Instant::now();
        let source = match storage.sync_chunk_source_for_buckets_with_plan(
            &bucket_cursors,
            active_plan.as_ref(),
            encoding,
        ) {
            Ok(source) => source,
            Err(storage_error) => return internal_storage_error_response(&storage_error),
        };
        info!(
            bucket_after = ?after,
            encoding = label,
            emission_path = "chunk-stream",
            chunk_count_hint = source.chunk_count_hint,
            prepare_ms = started_at.elapsed().as_millis(),
            request_ms = request_started_at.elapsed().as_millis(),
            "prepared sync chunks"
        );
        streaming_response(
            source,
            StreamingResponseContext {
                storage: storage.clone(),
                hold_open,
                active_plan,
                encoding_label: label,
                stream_encoding: encoding,
                current_cursors: bucket_cursors,
                content_type,
                admission_started_at,
                request_started_at,
                stream_lifetime,
                sync_read_permit: sync_read_permit.take(),
            },
        )
    } else {
        let started_at = Instant::now();
        let source = match storage.sync_chunk_source_for_buckets_with_plan(
            &bucket_cursors,
            active_plan.as_ref(),
            encoding,
        ) {
            Ok(source) => source,
            Err(storage_error) => return internal_storage_error_response(&storage_error),
        };
        if source.chunk_count_hint.is_some() {
            let chunk_count_hint = source.chunk_count_hint;
            let mut body = Vec::new();
            for chunk in source.chunks {
                body.extend_from_slice(&chunk.bytes);
            }
            let source = SyncBodySource {
                body: Bytes::from(body),
                chunk_count_hint,
            };
            info!(
                bucket_after = ?after,
                encoding = label,
                emission_path = "preframed-body",
                chunk_count_hint,
                body_bytes = source.body.len(),
                prepare_ms = started_at.elapsed().as_millis(),
                request_ms = request_started_at.elapsed().as_millis(),
                "prepared preframed sync body"
            );
            direct_body_response(
                source,
                label,
                after,
                content_type,
                admission_started_at,
                request_started_at,
            )
        } else {
            info!(
                bucket_after = ?after,
                encoding = label,
                emission_path = "chunk-stream",
                prepare_ms = started_at.elapsed().as_millis(),
                request_ms = request_started_at.elapsed().as_millis(),
                "prepared lazy sync chunks"
            );
            streaming_response(
                source,
                StreamingResponseContext {
                    storage: storage.clone(),
                    hold_open,
                    active_plan,
                    encoding_label: label,
                    stream_encoding: encoding,
                    current_cursors: bucket_cursors,
                    content_type,
                    admission_started_at,
                    request_started_at,
                    stream_lifetime,
                    sync_read_permit: None,
                },
            )
        }
    }
}

struct SyncResponseRequestContext {
    admission_started_at: Instant,
    request_started_at: Instant,
    stream_lifetime: Option<Duration>,
    sync_read_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

fn direct_body_response(
    source: SyncBodySource,
    encoding: &'static str,
    after: Option<u64>,
    content_type: &'static str,
    admission_started_at: Instant,
    request_started_at: Instant,
) -> Response {
    let started_at = Instant::now();
    let debug_timings = DebugTimings::capture(admission_started_at, request_started_at);
    info!(
        bucket_after = ?after,
        encoding,
        emission_path = "preframed-body",
        body_bytes = source.body.len(),
        ttfb_ms = started_at.elapsed().as_millis(),
        request_ms = debug_timings.handler_request_ms,
        total_request_ms = debug_timings.total_request_ms,
        pre_handler_ms = debug_timings.pre_handler_ms,
        "sync stream sending preframed response body"
    );

    build_sync_response_headers(
        Response::builder(),
        content_type,
        "preframed-body",
        debug_timings,
    )
    .body(Body::from(source.body))
    .expect("response should build")
}

fn hold_open_body_response(
    source: SyncBodySource,
    encoding: &'static str,
    after: Option<u64>,
    content_type: &'static str,
    admission_started_at: Instant,
    request_started_at: Instant,
    stream_lifetime: Option<Duration>,
) -> Response {
    let started_at = Instant::now();
    let body_bytes = source.body.len();
    let debug_timings = DebugTimings::capture(admission_started_at, request_started_at);
    let stream = stream::iter([Ok::<Bytes, Infallible>(source.body)])
        .chain(stream::pending::<Result<Bytes, Infallible>>())
        .boxed();
    let stream: BoxStream<'static, Result<Bytes, Infallible>> = match stream_lifetime {
        Some(lifetime) => stream.take_until(tokio::time::sleep(lifetime)).boxed(),
        None => stream,
    };

    info!(
        bucket_after = ?after,
        encoding,
        emission_path = "preframed-body+pending",
        body_bytes,
        ttfb_ms = started_at.elapsed().as_millis(),
        request_ms = debug_timings.handler_request_ms,
        total_request_ms = debug_timings.total_request_ms,
        pre_handler_ms = debug_timings.pre_handler_ms,
        "sync stream sending hold-open preframed response body"
    );

    build_sync_response_headers(
        Response::builder(),
        content_type,
        "preframed-body+pending",
        debug_timings,
    )
    .body(Body::from_stream(stream))
    .expect("response should build")
}

fn streaming_response(source: SyncChunkSource, context: StreamingResponseContext) -> Response {
    let current_cursors = match source.final_cursors.clone() {
        Some(final_cursors) => final_cursors,
        None => match context.storage.latest_sync_bucket_cursors_with_plan(
            &context.current_cursors,
            context.active_plan.as_ref(),
        ) {
            Ok(latest_cursors) => latest_cursors.unwrap_or_else(|| context.current_cursors.clone()),
            Err(storage_error) => return internal_storage_error_response(&storage_error),
        },
    };
    let stream = build_body_stream(
        source,
        BuildBodyStreamContext {
            storage: context.storage.clone(),
            hold_open: context.hold_open,
            active_plan: context.active_plan,
            encoding: context.encoding_label,
            stream_encoding: context.stream_encoding,
            current_cursors,
            request_started_at: context.request_started_at,
            stream_lifetime: context.stream_lifetime,
            sync_read_permit: context.sync_read_permit,
        },
    );
    let debug_timings =
        DebugTimings::capture(context.admission_started_at, context.request_started_at);

    build_sync_response_headers(
        Response::builder(),
        context.content_type,
        "chunk-stream",
        debug_timings,
    )
    .body(Body::from_stream(stream))
    .expect("response should build")
}

fn internal_storage_error_response(storage_error: &crate::storage::StorageError) -> Response {
    error!(detail = %storage_error, "sync stream storage read failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(CONTENT_TYPE, "application/json; charset=utf-8")],
        serde_json::json!({ "error": "internal storage error" }).to_string(),
    )
        .into_response()
}

struct StreamingResponseContext {
    storage: crate::SharedStorage,
    hold_open: bool,
    active_plan: Arc<RustExecutionPlan>,
    encoding_label: &'static str,
    stream_encoding: StreamEncoding,
    current_cursors: SyncBucketCursors,
    content_type: &'static str,
    admission_started_at: Instant,
    request_started_at: Instant,
    stream_lifetime: Option<Duration>,
    sync_read_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

fn build_sync_response_headers(
    builder: axum::http::response::Builder,
    content_type: &'static str,
    emission_path: &'static str,
    debug_timings: DebugTimings,
) -> axum::http::response::Builder {
    builder
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header("X-Accel-Buffering", "no")
        .header(CACHE_CONTROL, "no-store")
        .header(DEBUG_EMISSION_PATH_HEADER, emission_path)
        .header(
            DEBUG_REQUEST_MS_HEADER,
            debug_timings.handler_request_ms.to_string(),
        )
        .header(
            DEBUG_TOTAL_REQUEST_MS_HEADER,
            debug_timings.total_request_ms.to_string(),
        )
        .header(
            DEBUG_PRE_HANDLER_MS_HEADER,
            debug_timings.pre_handler_ms.to_string(),
        )
        .header(
            DEBUG_REQUEST_US_HEADER,
            debug_timings.handler_request_us.to_string(),
        )
        .header(
            DEBUG_TOTAL_REQUEST_US_HEADER,
            debug_timings.total_request_us.to_string(),
        )
        .header(
            DEBUG_PRE_HANDLER_US_HEADER,
            debug_timings.pre_handler_us.to_string(),
        )
}

#[derive(Clone, Copy, Debug)]
struct DebugTimings {
    handler_request_ms: u128,
    total_request_ms: u128,
    pre_handler_ms: u128,
    handler_request_us: u128,
    total_request_us: u128,
    pre_handler_us: u128,
}

impl DebugTimings {
    fn capture(admission_started_at: Instant, request_started_at: Instant) -> Self {
        let total_request = admission_started_at.elapsed();
        let handler_request = request_started_at.elapsed();
        let pre_handler = total_request.saturating_sub(handler_request);

        Self {
            handler_request_ms: handler_request.as_millis(),
            total_request_ms: total_request.as_millis(),
            pre_handler_ms: pre_handler.as_millis(),
            handler_request_us: handler_request.as_micros(),
            total_request_us: total_request.as_micros(),
            pre_handler_us: pre_handler.as_micros(),
        }
    }
}

fn build_body_stream(
    source: SyncChunkSource,
    context: BuildBodyStreamContext,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let started_at = Instant::now();
    let SyncChunkSource {
        chunks,
        chunk_count_hint,
        ..
    } = source;
    let current_after = context.current_cursors.max_after().unwrap_or(0);
    let state = HoldOpenStreamState {
        storage: context.storage,
        active_plan: context.active_plan,
        encoding: context.encoding,
        stream_encoding: context.stream_encoding,
        current_cursors: context.current_cursors,
        request_started_at: context.request_started_at,
        started_at,
        chunk_count_hint,
        pending_chunks: chunks,
        first_chunk_emitted: false,
        first_data_emitted: false,
        current_after,
        hold_open: context.hold_open,
        poll_interval: hold_open_poll_interval(),
        _sync_read_permit: context.sync_read_permit,
    };

    let stream = stream::unfold(state, |mut state| async move {
        loop {
            if let Some(chunk) = state.pending_chunks.next() {
                return Some((Ok::<Bytes, Infallible>(state.log_chunk(chunk)), state));
            }

            if !state.hold_open {
                return None;
            }

            let latest_cursors = match state
                .storage
                .wait_for_new_sync_bucket_cursors_with_plan(
                    &state.current_cursors,
                    state.active_plan.as_ref(),
                    state.poll_interval,
                )
                .await
            {
                Ok(Some(latest_cursors)) => latest_cursors,
                Ok(None) => continue,
                Err(storage_error) => {
                    error!(
                        detail = %storage_error,
                        encoding = state.encoding,
                        "sync stream hold-open cursor wait failed; ending stream"
                    );
                    return None;
                }
            };
            if latest_cursors == state.current_cursors {
                continue;
            }

            let source = match state.storage.sync_chunk_source_for_buckets_with_plan(
                &state.current_cursors,
                state.active_plan.as_ref(),
                state.stream_encoding,
            ) {
                Ok(source) => source,
                Err(storage_error) => {
                    error!(
                        detail = %storage_error,
                        encoding = state.encoding,
                        "sync stream hold-open follow-up read failed; ending stream"
                    );
                    return None;
                }
            };
            let next_chunk_count_hint = source.chunk_count_hint;
            let source_final_cursors = source.final_cursors.unwrap_or(latest_cursors);
            let next_after = source_final_cursors
                .max_after()
                .unwrap_or(state.current_after);
            let next_chunks = source.chunks;
            info!(
                bucket_after = ?Some(state.current_after),
                encoding = state.encoding,
                next_after,
                chunk_count_hint = next_chunk_count_hint,
                request_ms = state.request_started_at.elapsed().as_millis(),
                "sync stream detected hold-open follow-up checkpoint"
            );
            state.current_after = next_after;
            state.current_cursors = source_final_cursors;
            state.chunk_count_hint = next_chunk_count_hint;
            state.pending_chunks = next_chunks;
        }
    })
    .boxed();
    match context.stream_lifetime {
        Some(lifetime) => stream.take_until(tokio::time::sleep(lifetime)).boxed(),
        None => stream,
    }
}

struct BuildBodyStreamContext {
    storage: crate::SharedStorage,
    hold_open: bool,
    active_plan: Arc<RustExecutionPlan>,
    encoding: &'static str,
    stream_encoding: StreamEncoding,
    /// Latest known cursors at stream start (requested cursors advanced to the
    /// freshest storage position).
    current_cursors: SyncBucketCursors,
    request_started_at: Instant,
    stream_lifetime: Option<Duration>,
    sync_read_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct HoldOpenStreamState {
    storage: crate::SharedStorage,
    active_plan: Arc<RustExecutionPlan>,
    encoding: &'static str,
    stream_encoding: StreamEncoding,
    current_cursors: SyncBucketCursors,
    request_started_at: Instant,
    started_at: Instant,
    chunk_count_hint: Option<usize>,
    pending_chunks: crate::storage::SyncChunkIterator,
    first_chunk_emitted: bool,
    first_data_emitted: bool,
    current_after: u64,
    hold_open: bool,
    poll_interval: Duration,
    _sync_read_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl HoldOpenStreamState {
    fn log_chunk(&mut self, chunk: SyncChunk) -> Bytes {
        let after = self.current_cursors.max_after();
        if !self.first_chunk_emitted {
            self.first_chunk_emitted = true;
            info!(
                bucket_after = ?after,
                encoding = self.encoding,
                chunk_count_hint = self.chunk_count_hint,
                ttfb_ms = self.started_at.elapsed().as_millis(),
                request_ms = self.request_started_at.elapsed().as_millis(),
                "sync stream emitted first chunk"
            );
        }

        if !self.first_data_emitted && chunk.kind == SyncChunkKind::Data {
            self.first_data_emitted = true;
            info!(
                bucket_after = ?after,
                encoding = self.encoding,
                first_data_ms = self.started_at.elapsed().as_millis(),
                request_ms = self.request_started_at.elapsed().as_millis(),
                "sync stream emitted first data chunk"
            );
        }

        if chunk.kind == SyncChunkKind::CheckpointComplete {
            info!(
                bucket_after = ?after,
                encoding = self.encoding,
                total_emit_ms = self.started_at.elapsed().as_millis(),
                request_ms = self.request_started_at.elapsed().as_millis(),
                "sync stream emitted checkpoint-complete chunk"
            );
        }

        chunk.bytes
    }
}

fn hold_open_poll_interval() -> Duration {
    // This bounds the idle re-check cadence only: new commits wake hold-open
    // connections immediately through the ingest watch channel, so the 1s
    // default just caps how long an idle connection waits before re-checking.
    const DEFAULT_POLL_MS: u64 = 1_000;
    let poll_ms = std::env::var("POWERSYNC_RUST_HOLD_OPEN_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POLL_MS);
    Duration::from_millis(poll_ms)
}

fn negotiate_content_type(
    headers: &HeaderMap,
    binary_data: bool,
) -> Result<NegotiatedContentType, ContentNegotiationError> {
    let Some(accept) = headers.get(ACCEPT) else {
        return Ok(if binary_data {
            NegotiatedContentType::Bson
        } else {
            NegotiatedContentType::Ndjson
        });
    };

    let Ok(accept) = accept.to_str() else {
        return Err(ContentNegotiationError::BadRequest("invalid Accept header"));
    };

    if accept.contains(BSON_STREAM_CONTENT_TYPE) {
        return Ok(NegotiatedContentType::Bson);
    }

    if accept.contains(NDJSON_CONTENT_TYPE) || accept.contains("*/*") {
        return Ok(NegotiatedContentType::Ndjson);
    }

    Err(ContentNegotiationError::NotAcceptable(
        "supported content types: application/x-ndjson, application/vnd.powersync.bson-stream",
    ))
}

enum NegotiatedContentType {
    Ndjson,
    Bson,
}

#[derive(Debug)]
enum ContentNegotiationError {
    BadRequest(&'static str),
    NotAcceptable(&'static str),
}

#[derive(Debug, Default, Deserialize)]
pub struct SyncStreamRequest {
    #[serde(default)]
    binary_data: bool,
    #[serde(default)]
    raw_data: bool,
    #[serde(default)]
    buckets: Vec<SyncBucketRequest>,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    parameters: serde_json::Map<String, Value>,
    #[serde(default)]
    streams: Option<StreamSubscriptionRequest>,
    #[serde(default)]
    only: Vec<String>,
}

impl SyncStreamRequest {
    /// A held-open (streaming) connection re-emits checkpoints as new data
    /// commits, instead of returning the current state once and closing.
    /// PowerSync streaming clients set `raw_data: true`, so this server uses
    /// that as the streaming-connection marker; the benchmark's one-shot
    /// protocol probes opt out with a `probe-` client id. (`raw_data` otherwise
    /// selects oplog `data` encoding, which this server does not vary, so it is
    /// not honored for that purpose.)
    fn should_hold_open(&self) -> bool {
        let is_streaming_client = self.raw_data;
        let is_one_shot_probe = self.client_id.starts_with("probe-");
        is_streaming_client && !is_one_shot_probe
    }

    async fn bucket_cursors(
        &self,
        service_context: &ServiceContext,
        token: Option<&TokenPayload>,
    ) -> Result<SyncBucketCursors, String> {
        let existing = self
            .buckets
            .iter()
            .map(|bucket| {
                bucket
                    .after
                    .parse::<u64>()
                    .map(|after| (bucket.name.clone(), after))
                    .map_err(|_| {
                        format!(
                            "invalid after cursor {:?} for bucket {:?}",
                            bucket.after, bucket.name
                        )
                    })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if let Some(streams) = &self.streams {
            let existing_map = existing.iter().cloned().collect::<BTreeMap<_, _>>();
            let desired_buckets = self
                .resolve_stream_bucket_names(service_context, token, streams)
                .await?;
            return Ok(SyncBucketCursors::from_pairs(
                desired_buckets
                    .iter()
                    .map(|bucket_name| {
                        (
                            bucket_name.as_str(),
                            existing_map.get(bucket_name).copied().unwrap_or(0),
                        )
                    })
                    .collect::<Vec<_>>(),
            ));
        }

        let allowed_defaults = self.allowed_default_bucket_names(service_context, token);
        if self.only.is_empty() {
            if existing.is_empty() {
                return Ok(SyncBucketCursors::from_pairs(
                    allowed_defaults
                        .iter()
                        .map(|bucket_name| (bucket_name.as_str(), 0)),
                ));
            }
            self.validate_explicit_bucket_names(
                service_context,
                token,
                &allowed_defaults,
                existing.iter().map(|(bucket_name, _)| bucket_name.as_str()),
            )?;
            Ok(SyncBucketCursors::from_pairs(existing))
        } else {
            let existing_map = existing.iter().cloned().collect::<BTreeMap<_, _>>();
            self.validate_explicit_bucket_names(
                service_context,
                token,
                &allowed_defaults,
                self.only.iter().map(String::as_str),
            )?;
            Ok(SyncBucketCursors::from_pairs(
                self.only
                    .iter()
                    .map(|bucket_name| {
                        (
                            bucket_name.as_str(),
                            existing_map.get(bucket_name).copied().unwrap_or(0),
                        )
                    })
                    .collect::<Vec<_>>(),
            ))
        }
    }

    fn validate_explicit_bucket_names<'a>(
        &self,
        service_context: &ServiceContext,
        token: Option<&TokenPayload>,
        allowed_defaults: &BTreeSet<String>,
        bucket_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), String> {
        for bucket_name in bucket_names {
            if token.is_none() && service_context.allows_anonymous_sync() {
                if service_context
                    .resolve_bucket_request(bucket_name)
                    .is_none()
                {
                    return Err(format!("unknown bucket {bucket_name}"));
                }
                continue;
            }
            if !allowed_defaults.contains(bucket_name) {
                return Err(format!(
                    "explicit bucket {bucket_name} is not an authorized default bucket; use stream subscriptions for parameterized buckets"
                ));
            }
        }
        Ok(())
    }

    fn allowed_default_bucket_names(
        &self,
        service_context: &ServiceContext,
        token: Option<&TokenPayload>,
    ) -> BTreeSet<String> {
        let context = ResolvedParameterContext::from_request(token, &self.parameters);
        let empty_subscription_parameters = BTreeMap::new();
        service_context
            .active_plan()
            .default_bucket_requests_matching(|binding| {
                context.binding_value(binding, &empty_subscription_parameters)
            })
            .into_iter()
            .map(|bucket| bucket.bucket_name().to_owned())
            .collect()
    }

    async fn resolve_stream_bucket_names(
        &self,
        service_context: &ServiceContext,
        token: Option<&TokenPayload>,
        streams: &StreamSubscriptionRequest,
    ) -> Result<Vec<String>, String> {
        let context = ResolvedParameterContext::from_request(token, &self.parameters);
        let mut bucket_names = Vec::new();
        let include_defaults = streams.include_defaults.unwrap_or(true);
        let empty_subscription_parameters = BTreeMap::new();

        if include_defaults {
            bucket_names.extend(
                service_context
                    .active_plan()
                    .default_bucket_requests_matching(|binding| {
                        context.binding_value(binding, &empty_subscription_parameters)
                    })
                    .into_iter()
                    .map(|bucket| bucket.bucket_name().to_owned()),
            );
        }

        for subscription in &streams.subscriptions {
            let stream = service_context
                .stream(&subscription.stream)
                .ok_or_else(|| format!("unknown stream {}", subscription.stream))?;
            let subscription_parameters = extract_string_map(&subscription.parameters);
            for (group_index, group) in stream.bucket_groups().into_iter().enumerate() {
                if !request_filter_matches(group.request_filter.as_ref(), |binding| {
                    context.binding_value(binding, &subscription_parameters)
                }) {
                    continue;
                }
                if let Some(query) =
                    group
                        .bucket_parameters
                        .iter()
                        .find_map(|parameter| match &parameter.binding {
                            crate::sync_rules::CanonicalBinding::ParameterQueryColumn {
                                query,
                                ..
                            } => Some(query.clone()),
                            _ => None,
                        })
                {
                    let columns = group
                        .bucket_parameters
                        .iter()
                        .map(|parameter| parameter.name.clone())
                        .collect::<Vec<_>>();
                    let rows = service_context
                        .parameter_query_rows(
                            &query,
                            &columns,
                            token,
                            &self.parameters,
                            &subscription_parameters,
                        )
                        .await
                        .map_err(|error| {
                            error!(
                                stream = %subscription.stream,
                                reason = %error,
                                "parameter query evaluation failed"
                            );
                            format!(
                                "parameter query for stream {} could not be evaluated",
                                subscription.stream
                            )
                        })?;
                    for row in rows {
                        let values = columns
                            .iter()
                            .map(|column| {
                                row.get(column).cloned().ok_or_else(|| {
                                    format!(
                                        "parameter query for stream {} did not return {}",
                                        subscription.stream, column
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        bucket_names.push(bucket_name_for_stream_group_values(
                            &subscription.stream,
                            group_index,
                            &values,
                        ));
                    }
                } else {
                    let value_options = group
                        .bucket_parameters
                        .iter()
                        .map(|parameter| {
                            let values = context
                                .binding_values(&parameter.binding, &subscription_parameters);
                            if values.is_empty() {
                                Err(format!(
                                    "missing value for stream {} parameter {}",
                                    subscription.stream, parameter.name
                                ))
                            } else {
                                Ok(values)
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    // Parameter values can be client-controlled arrays, so the
                    // cartesian product must be bounded before materializing.
                    let product_size = value_options
                        .iter()
                        .map(Vec::len)
                        .fold(1_usize, usize::saturating_mul);
                    let limit = max_resolved_buckets_per_request();
                    if bucket_names.len().saturating_add(product_size) > limit {
                        return Err(format!(
                            "stream {} resolves more than {limit} buckets in one request",
                            subscription.stream
                        ));
                    }
                    for values in cartesian_product_values(&value_options) {
                        bucket_names.push(bucket_name_for_stream_group_values(
                            &subscription.stream,
                            group_index,
                            &values,
                        ));
                    }
                }
            }
        }

        if !self.only.is_empty() {
            bucket_names.retain(|bucket_name| self.only.iter().any(|only| only == bucket_name));
        }

        let limit = max_resolved_buckets_per_request();
        if bucket_names.len() > limit {
            return Err(format!(
                "request resolves {} buckets; the limit is {limit}",
                bucket_names.len()
            ));
        }

        Ok(bucket_names)
    }
}

fn max_resolved_buckets_per_request() -> usize {
    const DEFAULT_MAX_RESOLVED_BUCKETS: usize = 10_000;
    std::env::var("POWERSYNC_RUST_MAX_BUCKETS_PER_REQUEST")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_RESOLVED_BUCKETS)
}

fn cartesian_product_values(value_options: &[Vec<String>]) -> Vec<Vec<String>> {
    if value_options.is_empty() {
        return vec![Vec::new()];
    }
    let mut products = vec![Vec::new()];
    for options in value_options {
        let mut next = Vec::new();
        for product in &products {
            for option in options {
                let mut product = product.clone();
                product.push(option.clone());
                next.push(product);
            }
        }
        products = next;
    }
    products
}

#[derive(Debug, Default, Deserialize)]
struct SyncBucketRequest {
    name: String,
    after: String,
}

#[derive(Debug, Default, Deserialize)]
struct StreamSubscriptionRequest {
    #[serde(default)]
    include_defaults: Option<bool>,
    #[serde(default)]
    subscriptions: Vec<RequestedStreamSubscription>,
}

#[derive(Debug, Default, Deserialize)]
struct RequestedStreamSubscription {
    stream: String,
    #[serde(default)]
    parameters: Option<serde_json::Map<String, Value>>,
    #[serde(default, rename = "override_priority")]
    _override_priority: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use axum::http::HeaderValue;
    use http_body_util::BodyExt;

    use super::*;
    use crate::{
        protocol::messages::SyncChunk,
        storage::{Storage, StorageError, SyncBodySource, SyncBucketCursors, SyncChunkSource},
        sync_rules::DEFAULT_TASKS_BUCKET_NAME,
    };

    #[test]
    fn negotiate_content_type_defaults_to_bson_when_binary_flag_is_set_and_accept_missing() {
        let headers = HeaderMap::new();
        let negotiated =
            negotiate_content_type(&headers, true).expect("content negotiation should succeed");
        assert!(matches!(negotiated, NegotiatedContentType::Bson));
    }

    #[test]
    fn negotiate_content_type_defaults_to_ndjson_when_binary_flag_is_not_set_and_accept_missing() {
        let headers = HeaderMap::new();
        let negotiated =
            negotiate_content_type(&headers, false).expect("content negotiation should succeed");
        assert!(matches!(negotiated, NegotiatedContentType::Ndjson));
    }

    #[test]
    fn negotiate_content_type_prefers_explicit_accept_header_over_binary_flag() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/x-ndjson"));

        let negotiated =
            negotiate_content_type(&headers, true).expect("content negotiation should succeed");
        assert!(matches!(negotiated, NegotiatedContentType::Ndjson));
    }

    #[test]
    fn hold_open_requires_raw_data_for_non_probe_client() {
        let request = SyncStreamRequest {
            raw_data: true,
            client_id: "benchmark-client".to_owned(),
            buckets: vec![],
            ..Default::default()
        };

        assert!(request.should_hold_open());
    }

    #[test]
    fn hold_open_is_disabled_for_probe_client() {
        let request = SyncStreamRequest {
            raw_data: true,
            client_id: "probe-123".to_owned(),
            buckets: vec![],
            ..Default::default()
        };

        assert!(!request.should_hold_open());
    }

    #[tokio::test]
    async fn bucket_cursors_reject_unauthorized_explicit_bucket_requests() {
        let request = SyncStreamRequest {
            buckets: vec![
                SyncBucketRequest {
                    name: "unknown".to_owned(),
                    after: "7".to_owned(),
                },
                SyncBucketRequest {
                    name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
                    after: "42".to_owned(),
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            request
                .bucket_cursors(&test_service_context(), None)
                .await
                .expect_err("unauthorized bucket should fail closed"),
            "explicit bucket unknown is not an authorized default bucket; use stream subscriptions for parameterized buckets"
        );
    }

    #[tokio::test]
    async fn empty_default_request_resolves_authorized_default_buckets() {
        let cursors = SyncStreamRequest::default()
            .bucket_cursors(&test_service_context(), None)
            .await
            .expect("default request should resolve");

        assert_eq!(
            cursors,
            SyncBucketCursors::from_pairs([(DEFAULT_TASKS_BUCKET_NAME, 0)])
        );
    }

    #[tokio::test]
    async fn explicit_parameterized_bucket_cannot_bypass_stream_authorization() {
        let token = TokenPayload::new_for_tests(
            serde_json::json!({"sub": "user-1", "project_id": "project-a"}),
            Some("user-1".to_owned()),
        );
        let request = SyncStreamRequest {
            buckets: vec![SyncBucketRequest {
                name: "1#tasks_by_project|0[\"project-b\"]".to_owned(),
                after: "0".to_owned(),
            }],
            ..Default::default()
        };

        assert!(request
            .bucket_cursors(&test_service_context(), Some(&token))
            .await
            .expect_err("direct parameterized bucket must fail closed")
            .contains("is not an authorized default bucket"));
    }

    #[tokio::test]
    async fn empty_stream_subscription_does_not_fall_back_to_default_buckets() {
        let request = SyncStreamRequest {
            streams: Some(StreamSubscriptionRequest {
                include_defaults: Some(false),
                subscriptions: Vec::new(),
            }),
            ..Default::default()
        };

        let cursors = request
            .bucket_cursors(&test_service_context(), None)
            .await
            .expect("empty stream subscription should resolve to no buckets");

        assert!(cursors.buckets.is_empty());
        assert!(!cursors.default_when_empty);
    }

    #[tokio::test]
    async fn stream_subscriptions_expand_auth_parameters_into_buckets() {
        let token = TokenPayload::new_for_tests(
            serde_json::json!({
                "sub": "user-1",
                "project_id": "project-a",
                "org_id": "org-a"
            }),
            Some("user-1".to_owned()),
        );
        let request = SyncStreamRequest {
            parameters: serde_json::Map::from_iter([(
                "project_id".to_owned(),
                Value::String("project-request".to_owned()),
            )]),
            streams: Some(StreamSubscriptionRequest {
                include_defaults: Some(false),
                subscriptions: vec![
                    RequestedStreamSubscription {
                        stream: "tasks_by_project".to_owned(),
                        parameters: Some(serde_json::Map::from_iter([(
                            "project_id".to_owned(),
                            Value::String("project-subscription".to_owned()),
                        )])),
                        _override_priority: None,
                    },
                    RequestedStreamSubscription {
                        stream: "tasks_by_org".to_owned(),
                        parameters: Some(serde_json::Map::from_iter([(
                            "org_id".to_owned(),
                            Value::String("org-a".to_owned()),
                        )])),
                        _override_priority: None,
                    },
                ],
            }),
            ..Default::default()
        };

        assert_eq!(
            request
                .bucket_cursors(&test_service_context(), Some(&token))
                .await
                .expect("stream buckets should resolve"),
            SyncBucketCursors::from_pairs([
                ("1#tasks_by_project|0[\"project-a\"]", 0),
                ("1#tasks_by_org|0[\"org-a\"]", 0),
            ])
        );
    }

    #[tokio::test]
    async fn auth_parameter_streams_reject_client_parameter_spoofing() {
        let token = TokenPayload::new_for_tests(
            serde_json::json!({"sub": "user-1"}),
            Some("user-1".to_owned()),
        );
        let request = SyncStreamRequest {
            parameters: serde_json::Map::from_iter([(
                "project_id".to_owned(),
                Value::String("project-request".to_owned()),
            )]),
            streams: Some(StreamSubscriptionRequest {
                include_defaults: Some(false),
                subscriptions: vec![RequestedStreamSubscription {
                    stream: "tasks_by_project".to_owned(),
                    parameters: Some(serde_json::Map::from_iter([(
                        "project_id".to_owned(),
                        Value::String("project-subscription".to_owned()),
                    )])),
                    _override_priority: None,
                }],
            }),
            ..Default::default()
        };

        assert_eq!(
            request
                .bucket_cursors(&test_service_context(), Some(&token))
                .await
                .expect_err("client parameters must not satisfy auth.parameter"),
            "missing value for stream tasks_by_project parameter project_id"
        );
    }

    #[tokio::test]
    async fn hold_open_with_after_uses_preframed_body_fast_path() {
        let counters = Arc::new(TestStorageCounters::default());
        let storage: crate::SharedStorage = Arc::new(TestStorage {
            counters: Arc::clone(&counters),
        });
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit");

        let response = sync_response(
            &storage,
            Arc::new(crate::sync_rules::execution_plan().clone()),
            true,
            SyncBucketCursors::from_pairs([(DEFAULT_TASKS_BUCKET_NAME, 8098)]),
            StreamEncoding::Ndjson,
            SyncResponseRequestContext {
                admission_started_at: Instant::now(),
                request_started_at: Instant::now(),
                stream_lifetime: None,
                sync_read_permit: Some(permit),
            },
        );
        assert!(
            Arc::clone(&admission).try_acquire_owned().is_ok(),
            "preframed pending streams perform no follow-up reads and must release admission"
        );
        let mut body = response.into_body();
        let first_frame = body
            .frame()
            .await
            .expect("frame should exist")
            .expect("frame should succeed");
        let first_chunk = first_frame.into_data().expect("frame should contain data");

        assert_eq!(
            first_chunk,
            Bytes::from_static(b"{\"checkpoint_complete\":true}\n")
        );
        assert_eq!(counters.hold_open_body_calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.chunk_source_calls.load(Ordering::SeqCst), 0);

        let second_frame =
            tokio::time::timeout(std::time::Duration::from_millis(20), body.frame()).await;
        assert!(
            second_frame.is_err(),
            "hold-open body should stay open after first chunk"
        );
    }

    #[tokio::test]
    async fn hold_open_stream_ends_at_the_authenticated_token_deadline() {
        let counters = Arc::new(TestStorageCounters::default());
        let storage: crate::SharedStorage = Arc::new(TestStorage { counters });
        let response = sync_response(
            &storage,
            Arc::new(crate::sync_rules::execution_plan().clone()),
            true,
            SyncBucketCursors::from_pairs([(DEFAULT_TASKS_BUCKET_NAME, 8098)]),
            StreamEncoding::Ndjson,
            SyncResponseRequestContext {
                admission_started_at: Instant::now(),
                request_started_at: Instant::now(),
                stream_lifetime: Some(Duration::from_millis(5)),
                sync_read_permit: None,
            },
        );
        let mut body = response.into_body();
        body.frame()
            .await
            .expect("initial frame should exist")
            .expect("initial frame should succeed");

        let end = tokio::time::timeout(Duration::from_millis(100), body.frame())
            .await
            .expect("stream should end at the token deadline");
        assert!(end.is_none());
    }

    fn test_service_context() -> ServiceContext {
        let temp = tempfile::TempDir::new().expect("temp dir should exist");
        ServiceContext::new_for_tests(
            temp.path().join("sync-rules-state.json"),
            Vec::new(),
            None,
            Vec::new(),
        )
        .expect("service context should build")
    }

    #[tokio::test]
    async fn hold_open_without_after_keeps_chunk_stream_path() {
        let counters = Arc::new(TestStorageCounters::default());
        let storage: crate::SharedStorage = Arc::new(TestStorage {
            counters: Arc::clone(&counters),
        });
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit");

        let response = sync_response(
            &storage,
            Arc::new(crate::sync_rules::execution_plan().clone()),
            true,
            SyncBucketCursors::default(),
            StreamEncoding::Ndjson,
            SyncResponseRequestContext {
                admission_started_at: Instant::now(),
                request_started_at: Instant::now(),
                stream_lifetime: None,
                sync_read_permit: Some(permit),
            },
        );
        let mut body = response.into_body();
        assert!(
            Arc::clone(&admission).try_acquire_owned().is_err(),
            "follow-up-capable hold-open stream must retain admission"
        );
        let first_frame = body
            .frame()
            .await
            .expect("frame should exist")
            .expect("frame should succeed");
        let first_chunk = first_frame.into_data().expect("frame should contain data");

        assert_eq!(first_chunk, Bytes::from_static(b"{\"data\":true}\n"));
        assert_eq!(counters.chunk_source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.hold_open_body_calls.load(Ordering::SeqCst), 0);

        let second_frame =
            tokio::time::timeout(std::time::Duration::from_millis(20), body.frame()).await;
        assert!(
            second_frame.is_err(),
            "hold-open chunk stream should stay open after first chunk"
        );
        drop(body);
        assert!(
            Arc::clone(&admission).try_acquire_owned().is_ok(),
            "dropping the hold-open body must release admission"
        );
    }

    #[tokio::test]
    async fn hold_open_advances_requested_bucket_cursors_independently_before_follow_up_reads() {
        let storage_impl = Arc::new(MultiBucketTestStorage::default());
        let storage: crate::SharedStorage = storage_impl.clone();

        let response = sync_response(
            &storage,
            Arc::new(crate::sync_rules::execution_plan().clone()),
            true,
            SyncBucketCursors::from_pairs([
                (DEFAULT_TASKS_BUCKET_NAME, 10),
                ("1#tasks_by_project|0[\"project-a\"]", 5),
            ]),
            StreamEncoding::Ndjson,
            SyncResponseRequestContext {
                admission_started_at: Instant::now(),
                request_started_at: Instant::now(),
                stream_lifetime: None,
                sync_read_permit: None,
            },
        );
        let mut body = response.into_body();
        let first_frame = tokio::time::timeout(std::time::Duration::from_millis(50), body.frame())
            .await
            .expect("follow-up chunk should arrive")
            .expect("frame should exist")
            .expect("frame should succeed");
        let first_chunk = first_frame.into_data().expect("frame should contain data");
        assert_eq!(first_chunk, Bytes::from_static(b"{\"data\":true}\n"));

        let recorded = storage_impl
            .recorded_bucket_reads
            .lock()
            .expect("recorded bucket reads lock")
            .clone();
        assert_eq!(recorded.len(), 3);
        assert_eq!(
            recorded[0],
            SyncBucketCursors::from_pairs([
                (DEFAULT_TASKS_BUCKET_NAME, 10),
                ("1#tasks_by_project|0[\"project-a\"]", 5),
            ])
        );
        assert_eq!(
            recorded[1],
            SyncBucketCursors::from_pairs([
                (DEFAULT_TASKS_BUCKET_NAME, 10),
                ("1#tasks_by_project|0[\"project-a\"]", 5),
            ])
        );
        assert_eq!(
            recorded[2],
            SyncBucketCursors::from_pairs([
                (DEFAULT_TASKS_BUCKET_NAME, 10),
                ("1#tasks_by_project|0[\"project-a\"]", 6),
            ])
        );
    }

    #[derive(Default)]
    struct TestStorageCounters {
        chunk_source_calls: AtomicUsize,
        hold_open_body_calls: AtomicUsize,
    }

    struct TestStorage {
        counters: Arc<TestStorageCounters>,
    }

    impl Storage for TestStorage {
        fn sync_chunk_source_for_buckets_with_plan(
            &self,
            _buckets: &SyncBucketCursors,
            _plan: &RustExecutionPlan,
            _encoding: StreamEncoding,
        ) -> Result<SyncChunkSource, StorageError> {
            self.counters
                .chunk_source_calls
                .fetch_add(1, Ordering::SeqCst);

            Ok(SyncChunkSource {
                chunks: Box::new(
                    [SyncChunk {
                        bytes: Bytes::from_static(b"{\"data\":true}\n"),
                        kind: SyncChunkKind::Data,
                    }]
                    .into_iter(),
                ),
                chunk_count_hint: Some(1),
                final_cursors: None,
            })
        }

        fn sync_hold_open_body_source_for_buckets_with_plan(
            &self,
            _buckets: &SyncBucketCursors,
            _plan: &RustExecutionPlan,
            _encoding: StreamEncoding,
        ) -> Result<Option<SyncBodySource>, StorageError> {
            self.counters
                .hold_open_body_calls
                .fetch_add(1, Ordering::SeqCst);

            Ok(Some(SyncBodySource {
                body: Bytes::from_static(b"{\"checkpoint_complete\":true}\n"),
                chunk_count_hint: Some(1),
            }))
        }
    }

    #[derive(Default)]
    struct MultiBucketTestStorage {
        chunk_source_calls: AtomicUsize,
        wait_calls: AtomicUsize,
        recorded_bucket_reads: Mutex<Vec<SyncBucketCursors>>,
    }

    impl Storage for MultiBucketTestStorage {
        fn sync_chunk_source_for_buckets_with_plan(
            &self,
            buckets: &SyncBucketCursors,
            _plan: &RustExecutionPlan,
            _encoding: StreamEncoding,
        ) -> Result<SyncChunkSource, StorageError> {
            self.chunk_source_calls.fetch_add(1, Ordering::SeqCst);
            self.recorded_bucket_reads
                .lock()
                .expect("recorded bucket reads lock")
                .push(buckets.clone());

            let call = self.chunk_source_calls.load(Ordering::SeqCst);
            let chunks: Vec<SyncChunk> = if call < 3 {
                Vec::new()
            } else {
                vec![SyncChunk {
                    bytes: Bytes::from_static(b"{\"data\":true}\n"),
                    kind: SyncChunkKind::Data,
                }]
            };

            Ok(SyncChunkSource {
                chunk_count_hint: Some(chunks.len()),
                chunks: Box::new(chunks.into_iter()),
                final_cursors: None,
            })
        }

        fn latest_sync_bucket_cursors_with_plan(
            &self,
            buckets: &SyncBucketCursors,
            _plan: &RustExecutionPlan,
        ) -> Result<Option<SyncBucketCursors>, StorageError> {
            Ok(Some(buckets.clone()))
        }

        fn wait_for_new_sync_bucket_cursors_with_plan<'a>(
            &'a self,
            buckets: &'a SyncBucketCursors,
            _plan: &'a RustExecutionPlan,
            _timeout: std::time::Duration,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Option<SyncBucketCursors>, StorageError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let call = self.wait_calls.fetch_add(1, Ordering::SeqCst);
                Ok(match call {
                    0 => Some(SyncBucketCursors::from_pairs([
                        (DEFAULT_TASKS_BUCKET_NAME, 10),
                        ("1#tasks_by_project|0[\"project-a\"]", 6),
                    ])),
                    1 => Some(SyncBucketCursors::from_pairs([
                        (DEFAULT_TASKS_BUCKET_NAME, 11),
                        ("1#tasks_by_project|0[\"project-a\"]", 6),
                    ])),
                    2 => Some(buckets.clone()),
                    _ => None,
                })
            })
        }
    }
}
