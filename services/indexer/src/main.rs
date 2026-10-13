use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tackt_indexer::{
    Config, Indexer, NatsObservationSink, PgRepository, Repository, api,
    sink::ObservationSink,
    source::{
        Source,
        evm::{self, adapter::EthereumAdapter},
        hyperliquid::{self, adapter::HyperliquidAdapter},
        price_feed,
    },
    users,
};
use tackt_nats::{Consumer, UserEvent, UserEventKind};
use tokio::sync::broadcast;
use tokio::time::interval;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    Serve,
    PollOnce,
}

/// Restart-forever supervision with exponential backoff; the inner spawn
/// surfaces panics as JoinErrors so the loop survives them.
fn supervised<F, Fut>(name: &'static str, mut task: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut backoff = tackt_indexer::backoff::RestartBackoff::new(
            Duration::from_millis(500),
            Duration::from_secs(60),
        );
        loop {
            let started = std::time::Instant::now();
            match tokio::spawn(task()).await {
                Ok(()) => tracing::error!(worker = name, "worker returned; restarting"),
                Err(error) => tracing::error!(%error, worker = name, "worker panicked; restarting"),
            }
            tokio::time::sleep(backoff.next_delay(started.elapsed())).await;
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    tackt_telemetry::initialise();
    let metrics = tackt_telemetry::initialise_metrics();
    let cli = Cli::parse();
    let cfg = Config::from_env()?;
    // Non-fatal: a Hyperliquid outage must not block Ethereum ingestion; the
    // stream supervisor keeps retrying.
    if let Err(error) =
        hyperliquid::validate_connection(&cfg.hyperliquid_grpc_url, &cfg.hyperliquid_grpc_token)
            .await
    {
        tracing::error!(%error, "Hyperliquid gRPC validation failed; continuing");
    }
    let repo = Arc::new(PgRepository::initialise(&cfg.database_url, 10).await?);
    let events = Arc::new(tackt_nats::initialise(&cfg.nats_url).await?);
    // Requires the observation stream; the durable publish is the only record
    // of an observation, so startup fails without it.
    let sink = NatsObservationSink::initialise(events.clone()).await?;
    events
        .require_stream(&tackt_nats::subjects::users_stream())
        .await?;
    if !cfg.price_markets.is_empty() {
        for stream in [
            tackt_nats::subjects::price_evidence_stream(),
            tackt_nats::subjects::price_aggregate_stream(),
        ] {
            events.require_stream(&stream).await?;
        }
    }
    // Subscribe before seeding the registry so events committed during startup
    // are queued and idempotently applied after the PostgreSQL seed.
    let mut initial_user_consumer = match events.user_events().await {
        Ok(consumer) => Some(consumer),
        Err(error) => {
            tracing::warn!(%error, "initial user-event subscription failed; periodic registry reconciliation remains active");
            None
        }
    };
    let indexer = Indexer::new(
        repo.clone(),
        sink,
        cfg.evm_chains
            .iter()
            .map(|chain| EthereumAdapter::new(chain.rpc_url.clone(), chain.chain_id))
            .collect(),
        HyperliquidAdapter::new(&cfg.hyperliquid_grpc_url, &cfg.hyperliquid_grpc_token)?,
    );
    let registry_seed = indexer.users().reconcile(repo.as_ref()).await?;
    tracing::info!(
        users = registry_seed.users,
        ethereum_accounts = registry_seed.ethereum_accounts,
        "Indexer user registry seeded from PostgreSQL"
    );
    if matches!(cli.command, Some(Command::PollOnce)) {
        poll_once(&indexer).await;
        return Ok(());
    }
    for chain in &cfg.evm_chains {
        let worker = indexer.clone();
        let ws_url = chain.ws_url.clone();
        let chain_id = chain.chain_id;
        // One-time, bounded by the configured chain count.
        let name: &'static str = Box::leak(format!("evm-websocket-{chain_id}").into_boxed_str());
        supervised(name, move || {
            evm::supervise(ws_url.clone(), worker.clone(), chain_id)
        });
    }
    let user_registry = indexer.users().clone();
    let user_registry_repo = repo.clone();
    supervised("user-registry", move || {
        users::reconcile_forever(user_registry.clone(), user_registry_repo.clone())
    });
    let account_registry: hyperliquid::AccountRegistry = Arc::new(Default::default());
    let refresh = hyperliquid::refresh_accounts(repo.as_ref(), &account_registry).await?;
    tracing::info!(
        accounts = refresh.accounts,
        "Hyperliquid account registry loaded"
    );
    let (dirty_accounts, _) = broadcast::channel(4096);
    let stream_cursors = hyperliquid::StreamCursors::default();
    // Stream bootstrap replays the initial lookback, so the first worker
    // replay is redundant.
    account_registry.mark_replayed_current();
    // Non-fatal: the periodic reconciler retries.
    match indexer.reconcile_hyperliquid_accounts(None).await {
        Ok(published) => tracing::info!(
            published,
            "initial Hyperliquid portfolio reconciliation complete"
        ),
        Err(error) => {
            tracing::error!(%error, "initial Hyperliquid portfolio reconciliation failed")
        }
    }

    let grpc_worker = indexer.clone();
    let grpc_url = cfg.hyperliquid_grpc_url.clone();
    let grpc_token = cfg.hyperliquid_grpc_token.clone();
    let grpc_lookback = cfg.hyperliquid_initial_lookback_blocks;
    let grpc_accounts = account_registry.clone();
    let grpc_cursors = stream_cursors.clone();
    let grpc_dirty = dirty_accounts.clone();
    supervised("hyperliquid-grpc", move || {
        let worker = grpc_worker.clone();
        let url = grpc_url.clone();
        let token = grpc_token.clone();
        let accounts = grpc_accounts.clone();
        let cursors = grpc_cursors.clone();
        let dirty = grpc_dirty.clone();
        async move {
            if let Err(error) =
                hyperliquid::supervise(url, token, grpc_lookback, worker, accounts, cursors, dirty)
                    .await
            {
                tracing::error!(%error, "Hyperliquid gRPC supervisor failed");
            }
        }
    });

    let reconciliation_worker = indexer.clone();
    let reconciliation_dirty = dirty_accounts.clone();
    let reconciliation_interval = cfg.hyperliquid_reconcile_interval;
    supervised("hyperliquid-reconciliation", move || {
        reconcile_hyperliquid(
            reconciliation_worker.clone(),
            reconciliation_dirty.subscribe(),
            reconciliation_interval,
        )
    });

    let registry_worker = indexer.clone();
    let periodic_registry = account_registry.clone();
    let registry_cursors = stream_cursors.clone();
    let registry_dirty = dirty_accounts.clone();
    let registry_endpoint = cfg.hyperliquid_grpc_url.clone();
    supervised("hyperliquid-account-registry", move || {
        refresh_account_registry(
            registry_worker.clone(),
            periodic_registry.clone(),
            registry_cursors.clone(),
            registry_dirty.clone(),
            registry_endpoint.clone(),
            grpc_lookback,
        )
    });

    if !cfg.price_markets.is_empty() {
        let price_events = events.clone();
        let price_markets = cfg.price_markets.clone();
        supervised("price-feeds", move || {
            let events = price_events.clone();
            let markets = price_markets.clone();
            async move {
                if let Err(error) = price_feed::run(events, &markets).await {
                    tracing::error!(%error, "price pipeline failed");
                }
            }
        });
    }

    let mut required_streams = vec![tackt_nats::subjects::OBSERVATIONS_STREAM.to_string()];
    if !cfg.price_markets.is_empty() {
        required_streams.push(tackt_nats::subjects::PRICE_EVIDENCE_STREAM.to_string());
        required_streams.push(tackt_nats::subjects::PRICE_AGGREGATE_STREAM.to_string());
    }
    let api_state = api::ApiState {
        nats: events.clone(),
        required_streams,
        metrics,
    };
    let consumer = indexer.clone();
    let hyperliquid_refresh = HyperliquidEventRefresh {
        registry: account_registry.clone(),
        cursors: stream_cursors.clone(),
        dirty: dirty_accounts.clone(),
        endpoint: cfg.hyperliquid_grpc_url.clone(),
        lookback_blocks: grpc_lookback,
    };
    supervised("user-events", move || {
        let initial_consumer = initial_user_consumer.take();
        user_events(
            events.clone(),
            initial_consumer,
            consumer.clone(),
            hyperliquid_refresh.clone(),
        )
    });
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
    tracing::info!(address=%cfg.listen_addr,"indexer listening");
    axum::serve(listener, api::router(api_state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

async fn refresh_account_registry<R: Repository, S: ObservationSink>(
    indexer: Indexer<R, S>,
    registry: hyperliquid::AccountRegistry,
    cursors: hyperliquid::StreamCursors,
    dirty: broadcast::Sender<String>,
    endpoint: String,
    lookback_blocks: u64,
) {
    let mut ticker = interval(Duration::from_secs(60));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match hyperliquid::refresh_accounts(indexer.repo.as_ref(), &registry).await {
            Ok(refresh) => {
                tracing::debug!(
                    accounts = refresh.accounts,
                    "Hyperliquid account registry refreshed"
                );
                if registry.needs_replay()
                    && let Err(error) = hyperliquid::replay_registry_overlap(
                        lookback_blocks,
                        &indexer,
                        &registry,
                        &cursors,
                        &endpoint,
                        &dirty,
                    )
                    .await
                {
                    tracing::warn!(%error, "Hyperliquid account-registry overlap replay failed");
                }
            }
            Err(error) => tracing::warn!(%error, "Hyperliquid account registry refresh failed"),
        }
    }
}

async fn reconcile_hyperliquid<R: Repository, S: ObservationSink>(
    indexer: Indexer<R, S>,
    mut dirty: broadcast::Receiver<String>,
    period: Duration,
) {
    const DIRTY_MIN_INTERVAL: Duration = Duration::from_secs(5);
    let now = tokio::time::Instant::now();
    let mut full = tokio::time::interval_at(now + period, period);
    full.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut flush = tokio::time::interval(Duration::from_millis(200));
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending = HashSet::new();
    let mut last_account = HashMap::new();
    let mut last_full = now;
    loop {
        enum Wake {
            Full,
            Flush,
            Dirty(String),
            Lagged,
            Closed,
        }
        let wake = tokio::select! {
            _ = full.tick() => Wake::Full,
            _ = flush.tick() => Wake::Flush,
            received = dirty.recv() => match received {
                Ok(account) => Wake::Dirty(account),
                Err(broadcast::error::RecvError::Lagged(_)) => Wake::Lagged,
                Err(broadcast::error::RecvError::Closed) => Wake::Closed,
            }
        };
        match wake {
            Wake::Dirty(account) => {
                pending.insert(account);
            }
            Wake::Closed => return,
            Wake::Lagged | Wake::Full => match indexer.reconcile_hyperliquid_accounts(None).await {
                Ok(published) => {
                    tracing::info!(
                        published,
                        "full Hyperliquid portfolio reconciliation complete"
                    );
                    pending.clear();
                    last_account.clear();
                    last_full = tokio::time::Instant::now();
                }
                Err(error) => {
                    tracing::warn!(%error, "full Hyperliquid portfolio reconciliation failed")
                }
            },
            Wake::Flush => {
                let now = tokio::time::Instant::now();
                let due: HashSet<_> = pending
                    .iter()
                    .filter(|account| {
                        let last = last_account.get(*account).copied().unwrap_or(last_full);
                        now.duration_since(last) >= DIRTY_MIN_INTERVAL
                    })
                    .cloned()
                    .collect();
                if due.is_empty() {
                    continue;
                }
                match indexer.reconcile_hyperliquid_accounts(Some(&due)).await {
                    Ok(published) => {
                        tracing::debug!(
                            published,
                            accounts = due.len(),
                            "dirty Hyperliquid portfolio reconciliation complete"
                        );
                        for account in due {
                            pending.remove(&account);
                            last_account.insert(account, tokio::time::Instant::now());
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, accounts = due.len(), "dirty Hyperliquid portfolio reconciliation failed")
                    }
                }
            }
        }
    }
}

/// Core-api lifecycle events refresh the address registry and trigger a
/// reconciliation; the global gRPC subscriptions never need to reconnect.
#[derive(Clone)]
struct HyperliquidEventRefresh {
    registry: hyperliquid::AccountRegistry,
    cursors: hyperliquid::StreamCursors,
    dirty: broadcast::Sender<String>,
    endpoint: String,
    lookback_blocks: u64,
}

async fn user_events<R: Repository, S: ObservationSink>(
    client: Arc<tackt_nats::NatsClient>,
    initial_consumer: Option<Consumer<UserEvent>>,
    indexer: Indexer<R, S>,
    hyperliquid_refresh: HyperliquidEventRefresh,
) {
    let mut consumer = match initial_consumer {
        Some(consumer) => consumer,
        None => match client.user_events().await {
            Ok(consumer) => consumer,
            Err(error) => {
                tracing::error!(%error, "user-event consumer setup failed");
                return;
            }
        },
    };
    loop {
        let delivery = match consumer.next().await {
            Ok(delivery) => delivery,
            Err(error) => {
                tracing::error!(%error, "user-event consumer failed");
                return;
            }
        };
        let mut registry_users = HashSet::new();
        registry_users.insert(users::affected_user(delivery.message()));
        let mut refresh_hyperliquid = needs_hyperliquid_refresh(delivery.message());
        if let Err(error) = delivery.ack().await {
            tracing::warn!(%error, "user event ack failed");
        }
        // Bounded debounce: a steady stream cannot postpone the refresh forever.
        let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while let Ok(Ok(extra)) = tokio::time::timeout_at(
            drain_deadline.min(tokio::time::Instant::now() + Duration::from_millis(200)),
            consumer.next(),
        )
        .await
        {
            refresh_hyperliquid |= needs_hyperliquid_refresh(extra.message());
            registry_users.insert(users::affected_user(extra.message()));
            if let Err(error) = extra.ack().await {
                tracing::warn!(%error, "user event ack failed");
            }
        }
        indexer
            .users()
            .apply_events(indexer.repo.as_ref(), registry_users)
            .await;
        if !refresh_hyperliquid {
            continue;
        }
        match hyperliquid::refresh_accounts(indexer.repo.as_ref(), &hyperliquid_refresh.registry)
            .await
        {
            Ok(refresh) => {
                tracing::info!(
                    accounts = refresh.accounts,
                    "user events received; Hyperliquid accounts refreshed"
                );
                if hyperliquid_refresh.registry.needs_replay()
                    && let Err(error) = hyperliquid::replay_registry_overlap(
                        hyperliquid_refresh.lookback_blocks,
                        &indexer,
                        &hyperliquid_refresh.registry,
                        &hyperliquid_refresh.cursors,
                        &hyperliquid_refresh.endpoint,
                        &hyperliquid_refresh.dirty,
                    )
                    .await
                {
                    tracing::warn!(%error, "user-event account overlap replay failed");
                }
                if let Err(error) = indexer.reconcile_hyperliquid_accounts(None).await {
                    tracing::warn!(%error, "user-event-triggered reconciliation failed");
                }
            }
            Err(error) => tracing::warn!(%error, "user-event account refresh failed"),
        }
    }
}

/// Ethereum address changes only touch the user registry; everything else can
/// change which Hyperliquid accounts the gRPC streams must cover.
fn needs_hyperliquid_refresh(event: &UserEvent) -> bool {
    !matches!(
        &event.kind,
        UserEventKind::AccountAssociated {
            source: tackt_primitives::Source::Ethereum,
            ..
        }
    )
}

async fn poll_once<R: Repository, S: ObservationSink>(i: &Indexer<R, S>) {
    // Concurrent so one slow source cannot starve the other.
    let evm_chains = async {
        for chain_id in i.evm_chain_ids() {
            match i.poll_evm(chain_id).await {
                Ok(poll) => tracing::info!(
                    source = Source::Evm.as_str(),
                    chain_id,
                    published = poll.published,
                    caught_up = poll.caught_up,
                    "poll complete"
                ),
                Err(error) => {
                    tracing::error!(source = Source::Evm.as_str(), chain_id, %error, "poll failed")
                }
            }
        }
    };
    let ((), hyperliquid) = tokio::join!(evm_chains, i.reconcile_hyperliquid_accounts(None));
    match hyperliquid {
        Ok(published) => tracing::info!(
            source = Source::Hyperliquid.as_str(),
            published,
            "portfolio reconciliation complete"
        ),
        Err(error) => {
            tracing::error!(source = Source::Hyperliquid.as_str(), %error, "reconciliation failed")
        }
    }
}
