//! Durable observation handoff.
//!
//! The acknowledged JetStream publish is the Indexer's only durable side effect for an
//! account observation — there is no PostgreSQL write to fall back to — so a sink must
//! not report success until the broker acknowledged durability, and callers must fail
//! closed on error.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tackt_nats::{
    HyperliquidObservationStream, NatsClient, ObservationEvent, ObservationFeed, PublishOutcome,
};

pub trait ObservationSink: Clone + Send + Sync + 'static {
    /// Latest durable observation's block, which must be replayed inclusively.
    fn hyperliquid_resume_block(
        &self,
        stream: HyperliquidObservationStream,
    ) -> impl Future<Output = Result<Option<u64>>> + Send;
    fn publish_batch(&self, event: &ObservationEvent) -> impl Future<Output = Result<u64>> + Send;
    fn last_evm_event(
        &self,
        chain_id: u64,
    ) -> impl Future<Output = Result<Option<(u64, ObservationEvent)>>> + Send;
    /// Durably publish one observation; returning `Ok` means the broker holds it.
    fn publish(
        &self,
        event: &ObservationEvent,
    ) -> impl Future<Output = Result<PublishOutcome>> + Send;
}

/// The production sink: the `TACKT_OBSERVATIONS` JetStream stream.
#[derive(Clone)]
pub struct NatsObservationSink {
    client: Arc<NatsClient>,
    feed: ObservationFeed,
}

impl NatsObservationSink {
    /// Require the provisioned observation stream and resume KV before accepting work.
    pub async fn initialise(client: Arc<NatsClient>) -> Result<Self> {
        Self::initialise_on(client, ObservationFeed::production()).await
    }

    pub async fn initialise_on(client: Arc<NatsClient>, feed: ObservationFeed) -> Result<Self> {
        client.require_hyperliquid_resume(&feed).await?;
        Ok(Self { client, feed })
    }
}

impl ObservationSink for NatsObservationSink {
    async fn hyperliquid_resume_block(
        &self,
        stream: HyperliquidObservationStream,
    ) -> Result<Option<u64>> {
        self.client
            .hyperliquid_resume_block(&self.feed, stream)
            .await
    }

    async fn publish_batch(&self, event: &ObservationEvent) -> Result<u64> {
        self.client.publish_evm_batch(&self.feed, event).await
    }
    async fn last_evm_event(&self, chain_id: u64) -> Result<Option<(u64, ObservationEvent)>> {
        self.client.last_evm_event(&self.feed, chain_id).await
    }
    async fn publish(&self, event: &ObservationEvent) -> Result<PublishOutcome> {
        self.client
            .publish_observation_to_feed(&self.feed, event)
            .await
    }
}

/// Stored events with the EVM chain each was published for (`None` = non-EVM).
type PublishedEvents = Vec<(ObservationEvent, Option<u64>)>;

/// In-memory sink for tests: deduplicates on `event_id` like the broker's duplicate
/// window (with no expiry) and records every stored event in publish order.
#[derive(Clone, Default)]
pub struct MemoryObservationSink {
    published: Arc<Mutex<PublishedEvents>>,
    seen: Arc<Mutex<HashSet<String>>>,
}

impl MemoryObservationSink {
    pub fn published(&self) -> Vec<ObservationEvent> {
        self.published
            .lock()
            .expect("sink lock")
            .iter()
            .map(|(event, _)| event.clone())
            .collect()
    }
}

/// Stream position of the newest event on one chain's subject: a 1-based index, like JetStream.
fn last_for_chain(published: &PublishedEvents, chain_id: u64) -> Option<(u64, &ObservationEvent)> {
    published
        .iter()
        .enumerate()
        .rev()
        .find(|(_, (_, chain))| *chain == Some(chain_id))
        .map(|(index, (event, _))| (index as u64 + 1, event))
}

impl ObservationSink for MemoryObservationSink {
    async fn hyperliquid_resume_block(
        &self,
        stream: HyperliquidObservationStream,
    ) -> Result<Option<u64>> {
        Ok(self
            .published
            .lock()
            .expect("sink lock")
            .iter()
            .rev()
            .find(|(event, _)| {
                HyperliquidObservationStream::from_observation(&event.observation) == Some(stream)
            })
            .and_then(|(event, _)| event.observation.block_number))
    }

    async fn publish_batch(&self, event: &ObservationEvent) -> Result<u64> {
        let batch = event.batch.as_ref().expect("batch");
        let mut published = self.published.lock().expect("sink lock");
        if let Some(index) = published.iter().position(|(stored, _)| stored == event) {
            return Ok(index as u64 + 1);
        }
        let previous =
            last_for_chain(&published, batch.chain_id).map_or(0, |(sequence, _)| sequence);
        anyhow::ensure!(previous == batch.previous_sequence, "stale block publisher");
        published.push((event.clone(), Some(batch.chain_id)));
        Ok(published.len() as u64)
    }
    async fn last_evm_event(&self, chain_id: u64) -> Result<Option<(u64, ObservationEvent)>> {
        let published = self.published.lock().expect("sink lock");
        Ok(last_for_chain(&published, chain_id).map(|(sequence, event)| (sequence, event.clone())))
    }
    async fn publish(&self, event: &ObservationEvent) -> Result<PublishOutcome> {
        if !self
            .seen
            .lock()
            .expect("sink lock")
            .insert(event.observation.event_id.clone())
        {
            return Ok(PublishOutcome::Duplicate);
        }
        self.published
            .lock()
            .expect("sink lock")
            .push((event.clone(), None));
        Ok(PublishOutcome::Stored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tackt_primitives::{EventKind, Observation, Provenance, Source};

    fn block(chain: u64, height: u64, previous_sequence: u64) -> ObservationEvent {
        let observation = Observation {
            event_id: format!("obs_{chain}_{height}"),
            source: Source::Ethereum,
            kind: EventKind::EthereumBlock,
            account: None,
            asset: None,
            external_asset: None,
            quantity: None,
            source_timestamp: Utc::now(),
            observed_at: Utc::now(),
            block_number: Some(height),
            block_hash: Some(format!("0x{height:x}")),
            confirmations: Some(1),
            canonical: true,
            finalized: false,
            raw: serde_json::json!({}),
            provenance: Provenance {
                endpoint: "fixture".into(),
                method: "eth_getBlockByNumber".into(),
                source_id: format!("{chain}:{height}"),
            },
        };
        let tip = observation.block_ref().unwrap();
        ObservationEvent::with_batch(
            observation,
            tackt_nats::EvmBatch {
                chain_id: chain,
                previous_sequence,
                finalized: tip.clone(),
                history: vec![tip],
                revert_from: None,
                observations: vec![],
            },
        )
    }

    /// Each chain resumes from its own newest observation: interleaved
    /// publishes from another chain must never move a chain's resume point.
    #[tokio::test]
    async fn resume_points_are_isolated_per_chain() {
        let sink = MemoryObservationSink::default();
        sink.publish_batch(&block(1, 100, 0)).await.unwrap();
        sink.publish_batch(&block(8453, 5_000, 0)).await.unwrap();
        sink.publish_batch(&block(8453, 5_001, 2)).await.unwrap();

        let mainnet = sink.last_evm_event(1).await.unwrap().unwrap();
        assert_eq!(mainnet.1.observation.block_number, Some(100));
        let base = sink.last_evm_event(8453).await.unwrap().unwrap();
        assert_eq!(base.1.observation.block_number, Some(5_001));
        assert!(sink.last_evm_event(10).await.unwrap().is_none());
    }
}
