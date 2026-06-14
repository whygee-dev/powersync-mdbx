use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use tracing::{debug, info};

use crate::control_plane::ServiceContext;

use super::{
    ingest::{
        PgOutputBatchDecoder, ReplicationCommitBatch, ReplicationIngestError, ReplicationMdbxStore,
    },
    postgres::PostgresLsn,
    runtime::{ReplicationBootstrap, ReplicationRuntimeError},
    snapshot::run_initial_snapshot_if_enabled,
    PostgresReplicationConfig,
};

const MAX_EVENTS_ENV: &str = "POWERSYNC_RUST_REPLICATION_MAX_EVENTS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationRunnerOptions {
    pub max_events: Option<usize>,
}

impl ReplicationRunnerOptions {
    pub fn from_env() -> Result<Self, ReplicationRunnerConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    pub fn from_lookup<F>(lookup: F) -> Result<Self, ReplicationRunnerConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let max_events = lookup(MAX_EVENTS_ENV)
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                value.parse::<usize>().map_err(|_| {
                    ReplicationRunnerConfigError::InvalidUnsignedEnv {
                        name: MAX_EVENTS_ENV,
                        value: value.to_owned(),
                    }
                })
            })
            .transpose()?;

        Ok(Self { max_events })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationStreamEvent {
    KeepAlive {
        wal_end: PostgresLsn,
        reply_requested: bool,
        server_time_micros: i64,
    },
    Begin {
        final_lsn: PostgresLsn,
        xid: u32,
        commit_time_micros: i64,
    },
    XLogData {
        wal_start: PostgresLsn,
        wal_end: PostgresLsn,
        server_time_micros: i64,
        data: Bytes,
    },
    Commit {
        lsn: PostgresLsn,
        end_lsn: PostgresLsn,
        commit_time_micros: i64,
    },
    Message {
        transactional: bool,
        lsn: PostgresLsn,
        prefix: String,
        content: Bytes,
    },
    StoppedAt {
        reached: PostgresLsn,
    },
}

impl ReplicationStreamEvent {
    pub fn durable_lsn(&self) -> Option<PostgresLsn> {
        match self {
            Self::Commit { end_lsn, .. } => Some(*end_lsn),
            Self::StoppedAt { reached } => Some(*reached),
            _ => None,
        }
    }

    pub fn checkpoint_lsn(&self) -> Option<PostgresLsn> {
        match self {
            Self::KeepAlive { wal_end, .. } | Self::XLogData { wal_end, .. } => Some(*wal_end),
            Self::Begin { final_lsn, .. } => Some(*final_lsn),
            Self::Commit { end_lsn, .. } => Some(*end_lsn),
            Self::Message { lsn, .. } | Self::StoppedAt { reached: lsn } => Some(*lsn),
        }
    }

    pub fn event_kind(&self) -> &'static str {
        match self {
            Self::KeepAlive { .. } => "keepalive",
            Self::Begin { .. } => "begin",
            Self::XLogData { .. } => "xlog-data",
            Self::Commit { .. } => "commit",
            Self::Message { .. } => "message",
            Self::StoppedAt { .. } => "stopped-at",
        }
    }
}

impl From<ReplicationEvent> for ReplicationStreamEvent {
    fn from(event: ReplicationEvent) -> Self {
        match event {
            ReplicationEvent::KeepAlive {
                wal_end,
                reply_requested,
                server_time_micros,
            } => Self::KeepAlive {
                wal_end: wal_end.into(),
                reply_requested,
                server_time_micros,
            },
            ReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => Self::Begin {
                final_lsn: final_lsn.into(),
                xid,
                commit_time_micros,
            },
            ReplicationEvent::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => Self::XLogData {
                wal_start: wal_start.into(),
                wal_end: wal_end.into(),
                server_time_micros,
                data,
            },
            ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            } => Self::Commit {
                lsn: lsn.into(),
                end_lsn: end_lsn.into(),
                commit_time_micros,
            },
            ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
            } => Self::Message {
                transactional,
                lsn: lsn.into(),
                prefix,
                content,
            },
            ReplicationEvent::StoppedAt { reached } => Self::StoppedAt {
                reached: reached.into(),
            },
        }
    }
}

pub struct ReplicationStream {
    client: ReplicationClient,
    start_lsn: PostgresLsn,
}

impl ReplicationStream {
    pub async fn start(config: &PostgresReplicationConfig) -> Result<Self, ReplicationRunnerError> {
        let bootstrap = ReplicationBootstrap::prepare(config).await?;
        let start_lsn = bootstrap.replication_plane.start_lsn.into();
        let client = bootstrap.connect_replication().await?;
        Ok(Self { client, start_lsn })
    }

    pub async fn start_existing(
        config: &PostgresReplicationConfig,
        durable_lsn: PostgresLsn,
    ) -> Result<Self, ReplicationRunnerError> {
        let bootstrap = ReplicationBootstrap::prepare_existing(config, durable_lsn).await?;
        let start_lsn = bootstrap.replication_plane.start_lsn.into();
        let client = bootstrap.connect_replication().await?;
        Ok(Self { client, start_lsn })
    }

    pub fn start_lsn(&self) -> PostgresLsn {
        self.start_lsn
    }

    pub async fn recv(&mut self) -> Result<Option<ReplicationStreamEvent>, ReplicationRunnerError> {
        self.client
            .recv()
            .await
            .map(|event| event.map(ReplicationStreamEvent::from))
            .map_err(ReplicationRuntimeError::ReplicationReceive)
            .map_err(ReplicationRunnerError::from)
    }

    pub fn update_applied_lsn(&self, lsn: PostgresLsn) {
        self.client.update_applied_lsn(lsn.into());
    }

    pub fn stop(&self) {
        self.client.stop();
    }

    pub async fn shutdown(&mut self) -> Result<(), ReplicationRunnerError> {
        self.client
            .shutdown()
            .await
            .map_err(ReplicationRuntimeError::ReplicationShutdown)
            .map_err(ReplicationRunnerError::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRunSummary {
    pub start_lsn: PostgresLsn,
    pub total_events: usize,
    pub keepalive_events: usize,
    pub transaction_begin_events: usize,
    pub xlog_data_events: usize,
    pub commit_events: usize,
    pub message_events: usize,
    pub stop_events: usize,
    pub persisted_batches: usize,
    pub persisted_change_events: usize,
    pub last_ack_lsn: Option<PostgresLsn>,
    pub last_persisted_lsn: Option<PostgresLsn>,
    pub stopped_by_limit: bool,
}

impl Default for ReplicationRunSummary {
    fn default() -> Self {
        Self {
            start_lsn: PostgresLsn(0),
            total_events: 0,
            keepalive_events: 0,
            transaction_begin_events: 0,
            xlog_data_events: 0,
            commit_events: 0,
            message_events: 0,
            stop_events: 0,
            persisted_batches: 0,
            persisted_change_events: 0,
            last_ack_lsn: None,
            last_persisted_lsn: None,
            stopped_by_limit: false,
        }
    }
}

impl ReplicationRunSummary {
    fn new(start_lsn: PostgresLsn) -> Self {
        Self {
            start_lsn,
            ..Self::default()
        }
    }

    fn record(&mut self, event: &ReplicationStreamEvent) {
        self.total_events += 1;
        match event {
            ReplicationStreamEvent::KeepAlive { .. } => self.keepalive_events += 1,
            ReplicationStreamEvent::Begin { .. } => self.transaction_begin_events += 1,
            ReplicationStreamEvent::XLogData { .. } => self.xlog_data_events += 1,
            ReplicationStreamEvent::Commit { .. } => self.commit_events += 1,
            ReplicationStreamEvent::Message { .. } => self.message_events += 1,
            ReplicationStreamEvent::StoppedAt { .. } => self.stop_events += 1,
        }
    }

    fn record_ack_lsn(&mut self, lsn: PostgresLsn) {
        self.last_ack_lsn = Some(lsn);
    }

    fn record_persisted_batch(&mut self, batch: &ReplicationCommitBatch) {
        self.persisted_batches += 1;
        self.persisted_change_events += batch.change_count();
        self.last_persisted_lsn = Some(batch.end_lsn);
    }

    fn should_stop_for_limit(&self, options: ReplicationRunnerOptions) -> bool {
        matches!(options.max_events, Some(limit) if self.total_events >= limit)
    }
}

pub struct ReplicationRunner {
    stream: ReplicationStream,
    options: ReplicationRunnerOptions,
    summary: ReplicationRunSummary,
}

impl ReplicationRunner {
    pub async fn connect(
        config: &PostgresReplicationConfig,
        options: ReplicationRunnerOptions,
    ) -> Result<Self, ReplicationRunnerError> {
        let stream = ReplicationStream::start(config).await?;
        let start_lsn = stream.start_lsn();

        Ok(Self {
            stream,
            options,
            summary: ReplicationRunSummary::new(start_lsn),
        })
    }

    pub async fn connect_existing(
        config: &PostgresReplicationConfig,
        options: ReplicationRunnerOptions,
        durable_lsn: PostgresLsn,
    ) -> Result<Self, ReplicationRunnerError> {
        let stream = ReplicationStream::start_existing(config, durable_lsn).await?;
        let start_lsn = stream.start_lsn();
        Ok(Self {
            stream,
            options,
            summary: ReplicationRunSummary::new(start_lsn),
        })
    }

    pub fn start_lsn(&self) -> PostgresLsn {
        self.summary.start_lsn
    }

    pub async fn run(self) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
        self.run_with_observer(|_| {}).await
    }

    pub async fn run_with_observer<F>(
        mut self,
        mut observe: F,
    ) -> Result<ReplicationRunSummary, ReplicationRunnerError>
    where
        F: FnMut(&ReplicationStreamEvent),
    {
        while let Some(event) = self.stream.recv().await? {
            observe(&event);
            self.summary.record(&event);

            if let Some(lsn) = event.durable_lsn() {
                self.stream.update_applied_lsn(lsn);
                self.summary.record_ack_lsn(lsn);
            }

            if self.summary.should_stop_for_limit(self.options) {
                self.summary.stopped_by_limit = true;
                self.stream.stop();
            }
        }

        self.stream.shutdown().await?;
        Ok(self.summary)
    }
}

pub async fn run_replication_tap(
    config: &PostgresReplicationConfig,
    options: ReplicationRunnerOptions,
) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
    let runner = ReplicationRunner::connect(config, options).await?;
    let start_lsn = runner.start_lsn();

    info!(
        slot = %config.slot_name,
        publication = %config.publication_name,
        group_id = %config.group_id,
        start_lsn = %start_lsn,
        max_events = ?options.max_events,
        "starting live PostgreSQL logical replication tap"
    );

    runner.run_with_observer(log_replication_event).await
}

/// Structured `debug!` line for one replication stream event. Shared by the live
/// tap and the MDBX ingest loops so the per-variant logging stays identical.
fn log_replication_event(event: &ReplicationStreamEvent) {
    match event {
        ReplicationStreamEvent::KeepAlive {
            wal_end,
            reply_requested,
            ..
        } => {
            debug!(wal_end = %wal_end, reply_requested, kind = event.event_kind(), "replication event")
        }
        ReplicationStreamEvent::Begin { final_lsn, xid, .. } => {
            debug!(final_lsn = %final_lsn, xid, kind = event.event_kind(), "replication event")
        }
        ReplicationStreamEvent::XLogData {
            wal_start,
            wal_end,
            data,
            ..
        } => debug!(
            wal_start = %wal_start,
            wal_end = %wal_end,
            payload_bytes = data.len(),
            kind = event.event_kind(),
            "replication event"
        ),
        ReplicationStreamEvent::Commit { lsn, end_lsn, .. } => debug!(
            lsn = %lsn,
            end_lsn = %end_lsn,
            kind = event.event_kind(),
            "replication event"
        ),
        ReplicationStreamEvent::Message {
            transactional,
            lsn,
            prefix,
            content,
        } => debug!(
            transactional,
            lsn = %lsn,
            prefix,
            payload_bytes = content.len(),
            kind = event.event_kind(),
            "replication event"
        ),
        ReplicationStreamEvent::StoppedAt { reached } => {
            debug!(reached = %reached, kind = event.event_kind(), "replication event")
        }
    }
}

pub async fn run_replication_ingest(
    config: &PostgresReplicationConfig,
    options: ReplicationRunnerOptions,
) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
    let service_context = ServiceContext::from_env().map_err(ReplicationRunnerError::Other)?;
    run_replication_ingest_with_context(config, options, service_context).await
}

pub async fn run_replication_ingest_with_context(
    config: &PostgresReplicationConfig,
    options: ReplicationRunnerOptions,
    service_context: ServiceContext,
) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
    let store = ReplicationMdbxStore::shared_from_env()?;
    run_replication_ingest_with_store_and_readiness(config, options, store, service_context, None)
        .await
}

pub async fn run_replication_ingest_with_context_and_readiness(
    config: &PostgresReplicationConfig,
    options: ReplicationRunnerOptions,
    service_context: ServiceContext,
    runtime_readiness: Arc<AtomicBool>,
) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
    let store = ReplicationMdbxStore::shared_from_env()?;
    run_replication_ingest_with_store_and_readiness(
        config,
        options,
        store,
        service_context,
        Some(runtime_readiness),
    )
    .await
}

/// Drive PostgreSQL logical replication into MDBX.
///
/// Ingest-loop invariants and remaining operational limits:
/// - This loop mirrors `run_with_observer`'s skeleton but is intentionally NOT
///   merged with it: the observer acks on-sight (`event.durable_lsn()`) whereas
///   ingest acks only after a durable persist (`batch.end_lsn`). The shared
///   logging is factored into `log_replication_event`; merging the rest would
///   entangle that ack-after-persist correctness property with the simple tap.
/// - `store.persist_batch_with_plan` is a synchronous MDBX commit (with fsync) run
///   directly on the async runtime; in `ServiceMode::Unified` this can briefly
///   stall the shared executor serving the API. A production build should
///   `spawn_blocking` the persist (or run ingest on a dedicated runtime).
/// - On `stop()`/`shutdown()` the final applied LSN may not be flushed upstream
///   (`update_applied_lsn` sets it for pgwire-replication's periodic feedback,
///   which exposes no synchronous final flush), so the slot's `confirmed_flush_lsn`
///   can lag the last persisted batch and re-deliver already-applied transactions
///   on restart, relying on the store's end-LSN idempotency check.
pub async fn run_replication_ingest_with_store(
    config: &PostgresReplicationConfig,
    options: ReplicationRunnerOptions,
    store: Arc<ReplicationMdbxStore>,
    service_context: ServiceContext,
) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
    run_replication_ingest_with_store_and_readiness(config, options, store, service_context, None)
        .await
}

async fn run_replication_ingest_with_store_and_readiness(
    config: &PostgresReplicationConfig,
    options: ReplicationRunnerOptions,
    store: Arc<ReplicationMdbxStore>,
    service_context: ServiceContext,
    runtime_readiness: Option<Arc<AtomicBool>>,
) -> Result<ReplicationRunSummary, ReplicationRunnerError> {
    let mut decoder = PgOutputBatchDecoder::new();
    let snapshot_summary = run_initial_snapshot_if_enabled(config, &store, &service_context)
        .await
        .map_err(ReplicationRunnerError::Other)?;
    if let Some(runtime_readiness) = runtime_readiness {
        runtime_readiness.store(true, Ordering::Release);
    }
    let durable_lsn = store.last_persisted_end_lsn()?.ok_or_else(|| {
        ReplicationRunnerError::Other(
            "initial snapshot completed without a durable resume LSN".to_owned(),
        )
    })?;
    let mut runner = ReplicationRunner::connect_existing(config, options, durable_lsn).await?;
    let start_lsn = runner.start_lsn();

    info!(
        slot = %config.slot_name,
        publication = %config.publication_name,
        group_id = %config.group_id,
        start_lsn = %start_lsn,
        max_events = ?options.max_events,
        store_path = %store.path().display(),
        initial_snapshot = ?snapshot_summary,
        "starting PostgreSQL logical replication ingest into MDBX"
    );

    while let Some(event) = runner.stream.recv().await? {
        log_replication_event(&event);

        runner.summary.record(&event);

        let decode_started = std::time::Instant::now();
        let decoded_batch = decoder.push_stream_event(&event)?;
        store.record_replication_decode(decode_started.elapsed().as_millis() as u64);

        if let Some(batch) = decoded_batch {
            let active_plan = service_context.active_plan();
            store.persist_batch_with_plan(&batch, active_plan.as_ref())?;
            runner.stream.update_applied_lsn(batch.end_lsn);
            runner.summary.record_ack_lsn(batch.end_lsn);
            runner.summary.record_persisted_batch(&batch);
            debug!(
                transaction_id = batch.transaction_id,
                commit_lsn = %batch.commit_lsn,
                end_lsn = %batch.end_lsn,
                change_count = batch.change_count(),
                kind = "persisted-batch",
                "replication ingest persisted commit batch"
            );
        }

        if runner.summary.should_stop_for_limit(runner.options) {
            runner.summary.stopped_by_limit = true;
            runner.stream.stop();
        }
    }

    runner.stream.shutdown().await?;
    Ok(runner.summary)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationRunnerConfigError {
    #[error("invalid unsigned integer value for {name}: {value}")]
    InvalidUnsignedEnv { name: &'static str, value: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationRunnerError {
    #[error("replication runner configuration failed: {0}")]
    Config(ReplicationRunnerConfigError),
    #[error("replication ingest failed: {0}")]
    Ingest(ReplicationIngestError),
    #[error("replication runtime failed: {0}")]
    Runtime(ReplicationRuntimeError),
    #[error("replication runner failed: {0}")]
    Other(String),
}

impl From<ReplicationRunnerConfigError> for ReplicationRunnerError {
    fn from(value: ReplicationRunnerConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<ReplicationRuntimeError> for ReplicationRunnerError {
    fn from(value: ReplicationRuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<ReplicationIngestError> for ReplicationRunnerError {
    fn from(value: ReplicationIngestError) -> Self {
        Self::Ingest(value)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pgwire_replication::{Lsn, ReplicationEvent};

    use super::{
        ReplicationRunSummary, ReplicationRunnerConfigError, ReplicationRunnerOptions,
        ReplicationStreamEvent,
    };
    use crate::replication::postgres::PostgresLsn;

    #[test]
    fn parses_runner_options_from_lookup() {
        let options = ReplicationRunnerOptions::from_lookup(|name| match name {
            "POWERSYNC_RUST_REPLICATION_MAX_EVENTS" => Some("42".to_owned()),
            _ => None,
        })
        .expect("options");

        assert_eq!(options.max_events, Some(42));
    }

    #[test]
    fn rejects_invalid_runner_max_events() {
        let error = ReplicationRunnerOptions::from_lookup(|name| match name {
            "POWERSYNC_RUST_REPLICATION_MAX_EVENTS" => Some("bogus".to_owned()),
            _ => None,
        })
        .expect_err("invalid options");

        assert_eq!(
            error,
            ReplicationRunnerConfigError::InvalidUnsignedEnv {
                name: "POWERSYNC_RUST_REPLICATION_MAX_EVENTS",
                value: "bogus".to_owned(),
            }
        );
    }

    #[test]
    fn normalizes_pgwire_commit_into_local_event_and_ack_lsn() {
        let event = ReplicationStreamEvent::from(ReplicationEvent::Commit {
            lsn: Lsn(10),
            end_lsn: Lsn(12),
            commit_time_micros: 99,
        });

        assert_eq!(
            event,
            ReplicationStreamEvent::Commit {
                lsn: PostgresLsn(10),
                end_lsn: PostgresLsn(12),
                commit_time_micros: 99,
            }
        );
        assert_eq!(event.durable_lsn(), Some(PostgresLsn(12)));
        assert_eq!(event.checkpoint_lsn(), Some(PostgresLsn(12)));
    }

    #[test]
    fn summary_records_counts_and_limit_decision() {
        let options = ReplicationRunnerOptions {
            max_events: Some(2),
        };
        let mut summary = ReplicationRunSummary::new(PostgresLsn(7));
        let begin = ReplicationStreamEvent::Begin {
            final_lsn: PostgresLsn(8),
            xid: 5,
            commit_time_micros: 10,
        };
        let commit = ReplicationStreamEvent::Commit {
            lsn: PostgresLsn(9),
            end_lsn: PostgresLsn(10),
            commit_time_micros: 11,
        };

        summary.record(&begin);
        assert!(!summary.should_stop_for_limit(options));
        summary.record(&commit);
        summary.record_ack_lsn(PostgresLsn(10));

        assert_eq!(summary.start_lsn, PostgresLsn(7));
        assert_eq!(summary.total_events, 2);
        assert_eq!(summary.transaction_begin_events, 1);
        assert_eq!(summary.commit_events, 1);
        assert_eq!(summary.last_ack_lsn, Some(PostgresLsn(10)));
        assert!(summary.should_stop_for_limit(options));
    }

    #[test]
    fn normalizes_xlog_message_payloads() {
        let payload = Bytes::from_static(b"pgoutput");
        let event = ReplicationStreamEvent::from(ReplicationEvent::XLogData {
            wal_start: Lsn(1),
            wal_end: Lsn(2),
            server_time_micros: 3,
            data: payload.clone(),
        });

        assert_eq!(
            event,
            ReplicationStreamEvent::XLogData {
                wal_start: PostgresLsn(1),
                wal_end: PostgresLsn(2),
                server_time_micros: 3,
                data: payload,
            }
        );
    }
}
