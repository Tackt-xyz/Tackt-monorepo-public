//! Explicit deployment step; never invoked by service startup.
use tackt_nats::{ObservationFeed, RiskStreams, subjects};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("NATS_URL").map_err(|_| anyhow::anyhow!("NATS_URL is required"))?;
    let client = tackt_nats::initialise(&url).await?;
    let prefix = std::env::var("RISK_ENGINE_STREAM_PREFIX")
        .unwrap_or_else(|_| subjects::RISK_STREAM_PREFIX.into());
    let risk = RiskStreams::with_prefix(&prefix);
    for stream in [
        subjects::observations_stream(),
        subjects::users_stream(),
        subjects::price_evidence_stream(),
        subjects::price_aggregate_stream(),
        risk.prices,
        risk.patches,
        risk.sweeps,
        risk.recommendations,
    ] {
        client.ensure_stream(&stream).await?;
        println!("provisioned {}", stream.name);
    }
    client
        .ensure_hyperliquid_resume(&ObservationFeed::production())
        .await?;
    println!("provisioned Hyperliquid resume KV");
    client
        .provision_ledger_consumer(ObservationFeed::production().stream())
        .await?;
    println!("provisioned Ledger durable (existing position preserved)");
    Ok(())
}
