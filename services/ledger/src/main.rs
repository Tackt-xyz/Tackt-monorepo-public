use anyhow::{Context, Result};
use clap::Parser;
use std::{net::SocketAddr, sync::Arc};
use tackt_ledger::Ledger;
use tackt_postgres::PgRepository;

#[derive(Parser)]
struct Config {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "LEDGER_MIGRATION_DATABASE_URL")]
    migration_database_url: Option<String>,
    #[arg(long, env = "NATS_URL")]
    nats_url: String,
    #[arg(long, env = "LEDGER_LISTEN_ADDR", default_value = "127.0.0.1:8082")]
    listen_addr: SocketAddr,
    #[arg(long, env = "LEDGER_ALLOW_CLEARTEXT", default_value_t = false)]
    allow_cleartext: bool,
    #[arg(long, env = "LEDGER_DATABASE_CONNECTIONS", default_value_t = 8)]
    database_connections: u32,
    #[arg(long, env = "LEDGER_HTTP_MAX_BODY_BYTES", default_value_t = 16 * 1024)]
    http_max_body_bytes: usize,
    #[arg(long, env = "LEDGER_HTTP_MAX_IN_FLIGHT", default_value_t = 64)]
    http_max_in_flight: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tackt_telemetry::initialise();
    let config = Config::parse();
    anyhow::ensure!(
        config.listen_addr.ip().is_loopback() || config.allow_cleartext,
        "non-loopback listener requires LEDGER_ALLOW_CLEARTEXT=true behind TLS termination"
    );
    anyhow::ensure!(
        config.database_connections >= 2,
        "observer and accounting need at least two database connections"
    );
    if !config.listen_addr.ip().is_loopback() {
        tracing::warn!(address = %config.listen_addr, "cleartext listener relies on external TLS termination");
    }
    let financial = tackt_ledger::FinancialHttpConfig::new(
        std::env::var("LEDGER_INTERNAL_TOKEN").context("LEDGER_INTERNAL_TOKEN is required")?,
        config.http_max_body_bytes,
        config.http_max_in_flight,
    )?;
    if let Some(url) = config.migration_database_url.as_deref() {
        PgRepository::initialise(url, 1).await?;
    }
    let repository =
        PgRepository::connect(&config.database_url, config.database_connections).await?;
    // An explicit migrator is optional for local development; runtime credentials stay narrow in production.
    if config.migration_database_url.is_none() {
        repository.migrate().await?;
    }
    let lease = repository.acquire_observer().await?;
    let client = Arc::new(tackt_nats::initialise(&config.nats_url).await?);
    let feed = tackt_nats::ObservationFeed::production();
    let ledger = Arc::new(Ledger::new(repository));
    let consumer = ledger.observation_consumer(&client, &feed).await?;
    let readiness = {
        let ledger = ledger.clone();
        move || {
            let ledger = ledger.clone();
            let client = client.clone();
            async move { client.is_connected() && ledger.health().await.is_ok() }
        }
    };
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    let app = tackt_ledger::router(ledger.clone(), readiness, financial);
    let server = axum::serve(listener, app).with_graceful_shutdown(async {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    });
    tracing::info!(address = %config.listen_addr, "Ledger ready");
    // A failed consumer stops the service; the supervisor restarts from the durable cursor.
    tokio::select! {
        result = tackt_ledger::observations::consume(ledger, feed, consumer, lease) => result,
        result = server => Ok(result?),
    }
}
