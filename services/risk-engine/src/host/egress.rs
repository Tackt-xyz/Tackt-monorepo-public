//! Blocking bridge from the engine thread to acked `JetStream` publishes.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tackt_nats::{NatsClient, StreamSpec};
use tackt_primitives::risk::RiskRecommendation;
use tokio::runtime::Handle;

use super::HostState;

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// Publishes validated advisory recommendations from the dedicated engine thread.
///
/// Failures retry with capped backoff, blocking the engine thread — required:
/// the egress ledger records only broker-acked events, and input acks must
/// never precede the publishes they caused. Blocking propagates backpressure
/// through the bounded ingest channel; during a NATS outage the pipeline
/// stalls, which is correct because ingress is down anyway. The one escape is
/// shutdown: retrying then would wedge the drain unkillably, so the loop
/// aborts (input stays unacked and redelivers — safe).
pub struct EgressPublisher {
    client: NatsClient,
    stream: StreamSpec,
    handle: Handle,
    state: Arc<HostState>,
}

impl EgressPublisher {
    pub fn new(
        client: NatsClient,
        stream: StreamSpec,
        handle: Handle,
        state: Arc<HostState>,
    ) -> Self {
        Self {
            client,
            stream,
            handle,
            state,
        }
    }

    /// Publish one event, returning once it is durably stored. `false` means
    /// shutdown interrupted the retry loop and the event was NOT published.
    #[must_use]
    pub fn publish_blocking(&self, recommendation: &RiskRecommendation) -> bool {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            // The outer timeout bounds the one path the JetStream request
            // timeout does not: a wedged client send buffer. Without it a
            // half-up connection could block past the draining check forever.
            let attempt = self.handle.block_on(async {
                tokio::time::timeout(
                    ATTEMPT_TIMEOUT,
                    self.client
                        .publish_risk_recommendation(&self.stream, recommendation),
                )
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("publish attempt timed out")))
            });
            match attempt {
                Ok(()) => return true,
                Err(error) => {
                    if self.state.draining.load(Ordering::SeqCst) {
                        tracing::warn!(%error, recommendation_id = recommendation.recommendation_id(), "abandoning publish retry for shutdown");
                        return false;
                    }
                    tracing::warn!(%error, recommendation_id = recommendation.recommendation_id(), "risk recommendation publish failed; retrying");
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }
}
