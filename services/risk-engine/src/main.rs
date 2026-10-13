//! The risk-engine service: `JetStream` ingress → deterministic engine →
//! `JetStream` egress, with snapshot persistence and cursor-exact recovery.
//!
//! `main` is deliberately synchronous: recovery is CPU/disk work that runs
//! before the async runtime exists, so a long replay never occupies a
//! runtime worker and `/health` comes up as soon as the runtime does.

mod host;

use anyhow::Context;
use metrics_exporter_prometheus::PrometheusBuilder;
use risk_engine::persistence::{ApplyLoop, SnapshotStore, recover};
use risk_engine::processing::{AssetRegistry, PortfolioGeneration};

fn main() -> anyhow::Result<()> {
    tackt_telemetry::initialise();
    let config = host::config::Config::load()?;
    if !config.listen_addr.ip().is_loopback() {
        if !config.allow_cleartext {
            anyhow::bail!(
                "refusing to serve the bearer-token endpoint on cleartext {}; set RISK_ENGINE_ALLOW_CLEARTEXT=true only behind a TLS-terminating proxy or mesh",
                config.listen_addr
            );
        }
        tracing::warn!(
            address = %config.listen_addr,
            "RISK_ENGINE_ALLOW_CLEARTEXT=true: a TLS-terminating proxy or mesh must front this service"
        );
    }
    // Installed before any engine construction so the processing counters
    // register with the exporter from the first epoch.
    let metrics = PrometheusBuilder::new()
        .install_recorder()
        .context("installing Prometheus recorder")?;
    let registry = AssetRegistry::bundled()?;
    let policy = config.policy()?;
    let store = SnapshotStore::open(&config.snapshot_dir)?;

    let recovered = recover(
        &store,
        config.account_count,
        config.shard_count,
        policy.clone(),
        registry.clone(),
    )?;
    let (apply_loop, corrections) = if let Some(recovery) = recovered {
        tracing::info!(
            corrections = recovery.corrections.len(),
            "recovered from snapshot"
        );
        (recovery.apply_loop, recovery.corrections)
    } else {
        // First-ever boot: an empty generation at the configured version.
        // The first real reconciliation sweep must be stamped strictly
        // greater to supersede it.
        tracing::info!(
            version = config.initial_generation_version,
            "first boot: installing empty generation"
        );
        let generation = PortfolioGeneration::empty(config.initial_generation_version);
        (
            ApplyLoop::boot(
                config.account_count,
                config.shard_count,
                policy,
                registry,
                &generation,
            )?,
            Vec::new(),
        )
    };
    let cursors = apply_loop.fold().cursors().clone();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    runtime.block_on(host::run(
        config,
        metrics,
        store,
        apply_loop,
        corrections,
        cursors,
    ))
}
