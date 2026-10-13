use anyhow::Result;
use std::sync::Arc;
use tackt_core_api::{Config, api, auth::UserAuth};
use tackt_postgres::PgRepository;

#[tokio::main]
async fn main() -> Result<()> {
    tackt_telemetry::initialise();
    let cfg = Config::from_env()?;
    if cfg.financial_services.is_some() {
        tracing::warn!(
            "financial services are configured but no action route is mounted; orchestration is inert"
        );
    }
    // Bearer tokens travel in cleartext on a non-loopback listener,
    // so that configuration must be a deliberate, auditable choice rather than a
    // log line nobody reads.
    if !cfg.listen_addr.ip().is_loopback() {
        if !cfg.allow_cleartext {
            anyhow::bail!(
                "refusing to bind {} without TLS in front: bearer tokens would travel in cleartext; set CORE_API_ALLOW_CLEARTEXT=true only when a TLS-terminating proxy or mesh fronts this service",
                cfg.listen_addr
            );
        }
        tracing::warn!(
            address=%cfg.listen_addr,
            "CORE_API_ALLOW_CLEARTEXT=true: serving bearer-token APIs on a non-loopback listener; a TLS-terminating proxy or mesh must front this service"
        );
    }
    let repo =
        Arc::new(PgRepository::initialise(&cfg.database_url, cfg.database_connections).await?);
    tackt_core_api::signing::backfill_wallet_addresses(&repo).await?;
    let user_auth = UserAuth::initialise(cfg.user_auth.clone()).await?;
    let events = match &cfg.nats_url {
        Some(url) => {
            let client = tackt_nats::initialise(url).await?;
            // Required infrastructure must already exist before accepting traffic.
            client
                .require_stream(&tackt_nats::subjects::users_stream())
                .await?;
            Some(Arc::new(client))
        }
        None => {
            tracing::warn!("NATS_URL not set; user lifecycle events disabled");
            None
        }
    };
    let (prices, price_feed) = match &events {
        Some(client) => {
            let (cache, feed) = tackt_core_api::prices::PriceCache::initialise(
                client.clone(),
                tackt_nats::subjects::price_aggregate_stream(),
            )
            .await?;
            (cache, Some(feed))
        }
        None => (tackt_core_api::prices::PriceCache::default(), None),
    };
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
    let price_task = price_feed.map(|feed| tokio::spawn(feed.run()));
    tracing::info!(address=%cfg.listen_addr,"core API listening");
    let result = axum::serve(
        listener,
        api::router(
            repo,
            cfg.operator_token,
            events,
            user_auth,
            cfg.user_signup_enabled,
            cfg.beta_invite_code,
            prices,
        )
        .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown())
    .await;
    if let Some(task) = price_task {
        task.abort();
        let _ = task.await;
    }
    result?;
    Ok(())
}
async fn shutdown() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
}
