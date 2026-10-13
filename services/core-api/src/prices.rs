//! Process-local latest aggregate prices, populated from live JetStream messages.
//!
//! Core preserves the received price and metadata. Freshness, decay, and tolerance policy
//! belong to downstream consumers; this cache has no price timeout or timestamp checks.

use std::{sync::Arc, time::Duration};

use moka::sync::Cache;
use tackt_nats::{
    Consumer, NatsClient, StreamSpec,
    aggregates::{AggregatePrice, AggregatePriceEnvelope},
};
use tackt_primitives::ledger::LedgerAsset;

// Transport reconnect backoff, not a price expiration policy.
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct PriceCache {
    entries: Cache<LedgerAsset, Arc<AggregatePrice>>,
}

impl Default for PriceCache {
    fn default() -> Self {
        Self {
            // The closed LedgerAsset enum bounds the key space. Keep the latest value for
            // each asset without TTL or capacity eviction.
            entries: Cache::builder().build(),
        }
    }
}

impl PriceCache {
    /// Attach an independent live consumer to a pre-provisioned aggregate stream. Run the
    /// returned feed for the lifetime of the API. A default cache starts empty.
    pub async fn initialise(
        client: Arc<NatsClient>,
        stream: StreamSpec,
    ) -> anyhow::Result<(Self, PriceFeed)> {
        let consumer = client.aggregate_prices(&stream).await?;
        let cache = Self::default();
        let feed = PriceFeed {
            cache: cache.clone(),
            client,
            stream,
            consumer,
        };
        Ok((cache, feed))
    }

    /// Latest received value, or `None` if this process has not received one for the asset.
    /// Reads are local and remain available when the feed disconnects or stops. Source
    /// timestamps and other metadata are preserved for consumers to interpret.
    pub fn get_latest(&self, asset: LedgerAsset) -> Option<Arc<AggregatePrice>> {
        self.entries.get(&asset)
    }

    fn insert(&self, price: AggregatePrice) {
        self.entries.insert(price.asset(), Arc::new(price));
    }
}

/// Owns the sole cache writer and its consumer. Core controls the task's lifecycle.
pub struct PriceFeed {
    cache: PriceCache,
    client: Arc<NatsClient>,
    stream: StreamSpec,
    consumer: Consumer<AggregatePriceEnvelope>,
}

impl PriceFeed {
    /// Follow live messages and reconnect live on errors, retaining the last received values.
    pub async fn run(mut self) {
        loop {
            let error = self
                .consume()
                .await
                .expect_err("price feed runs until an error");
            tracing::warn!(%error, "aggregate price feed interrupted; reconnecting live");
            loop {
                tokio::time::sleep(RETRY_DELAY).await;
                match self.client.aggregate_prices(&self.stream).await {
                    Ok(consumer) => {
                        self.consumer = consumer;
                        break;
                    }
                    Err(error) => tracing::warn!(%error, "aggregate price feed reconnect failed"),
                }
            }
        }
    }

    async fn consume(&mut self) -> anyhow::Result<()> {
        // One consumer reads the whole stream in order. Ignore older redeliveries, using
        // broker ordering rather than the producer's clock-derived counter. A new live
        // subscription starts a new cursor, including after the stream is recreated.
        let mut last_stream_sequence = 0;
        loop {
            let delivery = self.consumer.next().await?;
            let stream_sequence = delivery.stream_sequence();
            let (envelope, ack) = delivery.split();
            if stream_sequence <= last_stream_sequence {
                ack.ack().await?;
                continue;
            }
            self.cache.insert(envelope.payload);
            last_stream_sequence = stream_sequence;
            ack.ack().await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use tackt_nats::{PriceAuthority, PriceQuote, aggregates::AggregateSource};

    fn price(sequence: u64, at: DateTime<Utc>) -> AggregatePrice {
        AggregatePrice::new(
            LedgerAsset::Eth,
            PriceQuote::new(200_000_000_000, 8).unwrap(),
            sequence,
            at,
            vec![AggregateSource {
                authority: PriceAuthority::Binance,
                evidence_id: format!("priceev_{}", "a".repeat(64)),
                effective_weight_bps: 10_000,
            }],
        )
        .unwrap()
    }

    #[test]
    fn latest_received_value_preserves_metadata_regardless_of_clock_or_producer_counter() {
        let cache = PriceCache::default();
        let reader = cache.clone();
        assert!(reader.get_latest(LedgerAsset::Eth).is_none());
        let future = price(100, DateTime::from_timestamp(2_000_000_000, 0).unwrap());
        cache.insert(future.clone());
        assert_eq!(*reader.get_latest(LedgerAsset::Eth).unwrap(), future);
        let earlier = price(1, DateTime::from_timestamp(1_700_000_000, 0).unwrap());
        cache.insert(earlier.clone());
        assert_eq!(*reader.get_latest(LedgerAsset::Eth).unwrap(), earlier);
        assert!(reader.get_latest(LedgerAsset::Btc).is_none());
    }
}
