//! Persists display samples captured on the engine thread. A dedicated pool and a bounded
//! timeout isolate history outages from Ledger input refresh; a failed sample is dropped and
//! the next cadence captures current state.

use std::time::Duration;

use risk_engine::egress::history::{CHANNEL_CAPACITY, CapturedHealth, HistorySampler};
use tackt_postgres::{PgRepository, RiskHealthSample};
use tackt_primitives::accounts::AccountSubject;
use tokio::sync::mpsc;
use uuid::Uuid;

const METRIC_SAMPLES_WRITTEN: &str = "risk_history_samples_written_total";
const METRIC_WRITE_FAILURES: &str = "risk_history_write_failures_total";

/// Sample identity is allocated here so capture stays allocation-free.
fn sample(captured: CapturedHealth) -> RiskHealthSample {
    RiskHealthSample {
        sample_id: Uuid::new_v4(),
        account: captured.subject,
        sampled_at: captured.sampled_at,
        classification: captured.classification,
        health: captured.health,
    }
}

pub async fn write_history(repository: PgRepository, mut receiver: mpsc::Receiver<CapturedHealth>) {
    while let Some(captured) = receiver.recv().await {
        let sample = sample(captured);
        if let Ok(Ok(())) = tokio::time::timeout(
            Duration::from_secs(2),
            repository.record_risk_health(&sample),
        )
        .await
        {
            metrics::counter!(METRIC_SAMPLES_WRITTEN).increment(1);
        } else {
            metrics::counter!(METRIC_WRITE_FAILURES).increment(1);
            tracing::warn!(account = %sample.account, "display history write failed or timed out; dropping sample");
        }
    }
}

/// Starts the bounded display handoff and its independent persistence worker.
pub fn start(
    repository: PgRepository,
    accounts: &[AccountSubject],
) -> (HistorySampler, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    (
        HistorySampler::new(accounts, sender),
        tokio::spawn(write_history(repository, receiver)),
    )
}
