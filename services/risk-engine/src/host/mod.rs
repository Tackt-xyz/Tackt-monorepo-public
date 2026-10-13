//! The risk-engine host: everything that makes the deterministic library a
//! running service. All network and disk IO lives here; the processing path
//! (`processing/`, `persistence/`) stays free of it.

pub mod config;
pub mod egress;
pub mod engine;
pub mod history;
pub mod http;
pub mod ingress;
pub mod ledger;
pub mod wire;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use metrics_exporter_prometheus::PrometheusHandle;
use risk_engine::ingest::IngestEvent;
use risk_engine::persistence::{ApplyLoop, SnapshotStore};
use risk_engine::processing::RiskEvent;
use tackt_nats::{StartPosition, StreamSpec, subjects::RiskStreams};
use tackt_postgres::PgRepository;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

use config::Config;
use egress::EgressPublisher;
use wire::StreamKind;

/// Shared flags: `/ready` surfaces the first two; `draining` releases the
/// egress retry loop so shutdown cannot wedge behind a NATS outage.
#[derive(Default)]
pub struct HostState {
    /// `PostgreSQL` inputs for the account-credit calculator, separate from legacy processor snapshots.
    pub ledger: ledger::LedgerInputs,
    pub ready: AtomicBool,
    pub poisoned: AtomicBool,
    pub draining: AtomicBool,
}

/// Runs the host until SIGTERM/ctrl-c: checks required streams, publishes recovery
/// corrections, attaches cursor-positioned consumers, serves observability
/// endpoints, and drains gracefully with a final snapshot.
pub async fn run(
    config: Config,
    metrics: PrometheusHandle,
    store: SnapshotStore,
    apply_loop: ApplyLoop,
    corrections: Vec<RiskEvent>,
    cursors: BTreeMap<String, u64>,
) -> anyhow::Result<()> {
    let repository = if let Some(url) = config.migration_database_url.as_deref() {
        PgRepository::initialise(url, 1).await?;
        PgRepository::connect(&config.database_url, 2).await?
    } else {
        PgRepository::initialise(&config.database_url, 2).await?
    };
    let state = Arc::new(HostState::default());
    // Separate pool: a slow history insert cannot consume Ledger refresh connections.
    let history_repository = if config.history_accounts.is_empty() {
        None
    } else {
        Some(PgRepository::connect(&config.database_url, 1).await?)
    };
    state
        .ledger
        .refresh(&repository)
        .await
        .context("loading Ledger credit inputs from PostgreSQL")?;
    tracing::info!("loaded Ledger credit inputs from PostgreSQL");
    let client = tackt_nats::initialise(&config.nats_url).await?;
    let streams = RiskStreams::with_prefix(&config.stream_prefix);
    check_streams(&client, &streams, &cursors).await?;

    let (evaluation_tx, evaluation_rx) = mpsc::channel(config.evaluation_channel_capacity);
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    tracing::info!(address = %config.listen_addr, "risk-engine listening");
    let http_router = http::router(
        state.clone(),
        metrics.clone(),
        evaluation_tx.clone(),
        config.internal_token_hash,
    );
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel();
    let http = spawn_http(listener, http_router, http_shutdown_rx);

    // Histogram buckets grow unboundedly without periodic upkeep when
    // nothing scrapes /metrics.
    let upkeep = metrics.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tick.tick().await;
            upkeep.run_upkeep();
        }
    });

    let (ingest_tx, ingest_rx) = mpsc::channel(config.ingest_channel_capacity);
    let publisher = EgressPublisher::new(
        client.clone(),
        streams.recommendations.clone(),
        Handle::current(),
        state.clone(),
    );
    let (corrections_done_tx, corrections_done) = oneshot::channel();
    let (history_sampler, history_writer) = history_repository
        .map(|repository| history::start(repository, &config.history_accounts))
        .unzip();
    let task = engine::EngineTask {
        apply_loop,
        corrections,
        store,
        snapshot_interval: config.snapshot_interval,
        snapshot_event_count: config.snapshot_event_count,
        publisher,
        state: state.clone(),
        handle: Handle::current(),
        ingest: ingest_rx,
        evaluations: evaluation_rx,
        corrections_done: Some(corrections_done_tx),
        history: history_sampler,
        history_interval: config.history_interval,
    };
    let engine = std::thread::Builder::new()
        .name("risk-apply".to_owned())
        .spawn(move || task.run())
        .context("spawning engine thread")?;

    // Corrections must be on the egress stream before any live input applies.
    corrections_done
        .await
        .context("engine thread exited before publishing corrections")?;

    let mut workers = spawn_ingress(&client, &streams, &cursors, &ingest_tx);
    workers.push(tokio::spawn(ledger::refresh_loop(
        repository,
        state.clone(),
        config.ledger_refresh_interval,
    )));
    state.ready.store(true, Ordering::SeqCst);
    tracing::info!("risk-engine live");

    Running {
        state,
        workers,
        ingest_tx,
        evaluation_tx,
        engine,
        http_shutdown_tx,
        http,
        history_writer,
    }
    .drain()
    .await
}

/// Everything a live host owns until shutdown.
struct Running {
    state: Arc<HostState>,
    workers: Vec<tokio::task::JoinHandle<()>>,
    ingest_tx: mpsc::Sender<ingress::IngestMessage>,
    evaluation_tx: mpsc::Sender<engine::EvaluationMessage>,
    engine: std::thread::JoinHandle<()>,
    http_shutdown_tx: oneshot::Sender<()>,
    http: tokio::task::JoinHandle<()>,
    history_writer: Option<tokio::task::JoinHandle<()>>,
}

impl Running {
    /// Waits for SIGTERM/ctrl-c, then drains: readiness off, ingress closed, engine joined
    /// with its final snapshot, and bounded time for the history writer and HTTP server.
    async fn drain(self) -> anyhow::Result<()> {
        let Self {
            state,
            workers,
            ingest_tx,
            evaluation_tx,
            engine,
            http_shutdown_tx,
            mut http,
            mut history_writer,
        } = self;
        shutdown_signal().await;
        tracing::info!("shutting down: draining in-flight events");
        // Fail readiness first so orchestrators stop routing during the drain.
        state.ready.store(false, Ordering::SeqCst);
        state.draining.store(true, Ordering::SeqCst);
        let _ = http_shutdown_tx.send(());
        for worker in &workers {
            worker.abort();
        }
        drop(ingest_tx);
        drop(evaluation_tx);
        // The engine drains the channel, writes a final snapshot, and returns;
        // the timeout is a backstop against anything unforeseen wedging it.
        let join = tokio::task::spawn_blocking(move || engine.join());
        tokio::time::timeout(std::time::Duration::from_mins(1), join)
            .await
            .map_err(|_| {
                anyhow::anyhow!("engine thread did not drain in time; final snapshot not confirmed")
            })?
            .context("joining engine thread")?
            .map_err(|_panic| anyhow::anyhow!("engine thread panicked"))?;
        if let Some(writer) = &mut history_writer {
            match tokio::time::timeout(std::time::Duration::from_secs(3), &mut *writer).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "display history worker exited unexpectedly");
                }
                Err(_) => {
                    tracing::warn!("display history drain timed out; dropping remaining samples");
                    writer.abort();
                }
            }
        }
        if tokio::time::timeout(std::time::Duration::from_secs(5), &mut http)
            .await
            .is_err()
        {
            tracing::warn!("http connections did not drain in time; aborting server");
            http.abort();
        }
        Ok(())
    }
}

async fn check_streams(
    client: &tackt_nats::NatsClient,
    streams: &RiskStreams,
    cursors: &BTreeMap<String, u64>,
) -> anyhow::Result<()> {
    for spec in [
        &streams.prices,
        &streams.patches,
        &streams.sweeps,
        &streams.recommendations,
    ] {
        client.require_stream(spec).await?;
    }
    // This is the first production wire contract. A boot with no durable
    // cursors must start from empty pre-production ingress streams; otherwise
    // development payloads could deserialize differently and be terminally
    // discarded. Once a snapshot owns cursors, ordinary retained tail replay
    // is safe and required.
    if cursors.is_empty() {
        for stream in [&streams.prices, &streams.patches, &streams.sweeps] {
            let retained = client.stream_message_count(&stream.name).await?;
            anyhow::ensure!(
                retained == 0,
                "stream {} contains {retained} pre-production messages but the risk engine has no durable cursor; clear the risk ingress streams before first rollout",
                stream.name
            );
        }
    }

    Ok(())
}

fn spawn_http(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    shutdown: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.await;
            })
            .await
        {
            tracing::error!(%error, "http server failed");
        }
    })
}

/// One supervised consumer per ingress stream, positioned at the recovered
/// cursor (or the stream start without one).
fn spawn_ingress(
    client: &tackt_nats::NatsClient,
    streams: &RiskStreams,
    cursors: &BTreeMap<String, u64>,
    ingest: &mpsc::Sender<ingress::IngestMessage>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let start = |stream: &StreamSpec| {
        cursors
            .get(&stream.name)
            .map_or(StartPosition::DeliverAll, |cursor| {
                StartPosition::BySequence(cursor + 1)
            })
    };
    vec![
        tokio::spawn(ingress::supervise(
            client.clone(),
            streams.prices.clone(),
            StreamKind::Prices,
            start(&streams.prices),
            ingest.clone(),
            IngestEvent::Price,
        )),
        tokio::spawn(ingress::supervise(
            client.clone(),
            streams.patches.clone(),
            StreamKind::Patches,
            start(&streams.patches),
            ingest.clone(),
            IngestEvent::AccountPatch,
        )),
        tokio::spawn(ingress::supervise(
            client.clone(),
            streams.sweeps.clone(),
            StreamKind::Sweeps,
            start(&streams.sweeps),
            ingest.clone(),
            IngestEvent::Portfolio,
        )),
    ]
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
