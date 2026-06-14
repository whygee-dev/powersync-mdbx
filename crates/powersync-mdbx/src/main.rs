use std::{
    future::IntoFuture,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use powersync_mdbx::{
    build_app_with_storage_and_context, build_app_with_storage_context_and_runtime_readiness,
    config::{env_or_config_port, load_config_from_env},
    control_plane::ServiceContext,
    replication::{
        runner::{
            run_replication_ingest_with_context, run_replication_ingest_with_context_and_readiness,
            ReplicationRunnerOptions,
        },
        RuntimePlan, ServiceMode,
    },
    server::{bind_listener, ListenerOptions},
    storage::{build_storage, StorageBackend},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "powersync_mdbx=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = load_config_from_env()?;
    let port = env_or_config_port(config.as_ref());

    let listener_options = ListenerOptions::from_env()?;
    let runtime_plan = RuntimePlan::from_env()?;
    runtime_plan.ensure_supported_bootstrap()?;
    let replication_options = ReplicationRunnerOptions::from_env()?;
    let storage_backend = StorageBackend::from_env();
    let service_context = ServiceContext::from_env()?;

    match runtime_plan.service_mode {
        ServiceMode::ApiOnly => {
            let listener = bind_listener(SocketAddr::from(([0, 0, 0, 0], port)), listener_options)?;
            let bound_addr = listener.local_addr()?;
            info!(
                port,
                ?bound_addr,
                ?listener_options,
                ?storage_backend,
                ?runtime_plan,
                "starting powersync_mdbx destination runtime"
            );

            axum::serve(
                listener,
                build_app_with_storage_and_context(
                    build_storage(storage_backend),
                    service_context.clone(),
                ),
            )
            .await?;
        }
        ServiceMode::ReplicationOnly => {
            let replication = runtime_plan
                .replication
                .as_ref()
                .expect("replication config required for replication-only mode");
            let summary = run_replication_ingest_with_context(
                replication,
                replication_options,
                service_context.clone(),
            )
            .await?;
            info!(?summary, "replication-only runner exited");
        }
        ServiceMode::Unified => {
            let listener = bind_listener(SocketAddr::from(([0, 0, 0, 0], port)), listener_options)?;
            let bound_addr = listener.local_addr()?;
            let replication = runtime_plan
                .replication
                .clone()
                .expect("replication config required for unified mode");
            info!(
                port,
                ?bound_addr,
                ?listener_options,
                ?storage_backend,
                ?runtime_plan,
                ?replication_options,
                "starting powersync_mdbx unified runtime"
            );

            let runtime_readiness = Arc::new(AtomicBool::new(false));
            let app = build_app_with_storage_context_and_runtime_readiness(
                build_storage(storage_backend),
                service_context.clone(),
                Arc::clone(&runtime_readiness),
            );
            let serve = axum::serve(listener, app).into_future();
            // The runner resumes from the replication slot's confirmed
            // position, so a transient Postgres error or stream end must not
            // take the HTTP server down with it.
            let replication_task = tokio::spawn(async move {
                let mut backoff = Duration::from_secs(1);
                const MAX_BACKOFF: Duration = Duration::from_secs(60);
                const BACKOFF_RESET_AFTER: Duration = Duration::from_secs(300);
                loop {
                    let started = Instant::now();
                    match run_replication_ingest_with_context_and_readiness(
                        &replication,
                        replication_options,
                        service_context.clone(),
                        Arc::clone(&runtime_readiness),
                    )
                    .await
                    {
                        Ok(summary) => {
                            warn!(?summary, ?backoff, "replication runner exited; restarting")
                        }
                        Err(error) => {
                            error!(%error, ?backoff, "replication runner failed; restarting")
                        }
                    }
                    if started.elapsed() >= BACKOFF_RESET_AFTER {
                        backoff = Duration::from_secs(1);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            });

            // The runner loops forever (restarting on error/exit), so it only
            // resolves by panicking — surface that instead of silently serving
            // with replication dead. Otherwise the HTTP server (or a signal)
            // ends the process.
            tokio::select! {
                result = serve => result?,
                joined = replication_task => {
                    return Err(match joined {
                        Ok(()) => "replication task exited unexpectedly".into(),
                        Err(error) => format!("replication task panicked: {error}").into(),
                    });
                }
            }
        }
    }

    Ok(())
}
