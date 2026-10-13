//! Requires the disposable NATS service from deploy/compose.yaml.

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tackt_core_api::prices::PriceCache;
use tackt_nats::{
    NatsClient, PriceAuthority, PriceQuote, StreamSpec,
    aggregates::{AggregatePrice, AggregateSource},
};
use tackt_primitives::ledger::LedgerAsset;

fn price(asset: LedgerAsset, sequence: u64, at: DateTime<Utc>) -> AggregatePrice {
    AggregatePrice::new(
        asset,
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

async fn expect_sequence(cache: &PriceCache, asset: LedgerAsset, sequence: u64) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if cache
                .get_latest(asset)
                .is_some_and(|price| price.sequence() == sequence)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cache receives the expected aggregate");
}

async fn start(
    client: Arc<NatsClient>,
    stream: StreamSpec,
) -> (PriceCache, tokio::task::JoinHandle<()>) {
    let (cache, feed) = PriceCache::initialise(client, stream).await.unwrap();
    let task = tokio::spawn(feed.run());
    (cache, task)
}

#[tokio::test]
async fn replicas_and_restarted_processes_receive_only_live_prices() {
    let url = std::env::var("NATS_URL")
        .expect("start deploy/compose.yaml and set NATS_URL for price integration tests");
    let client = Arc::new(tackt_nats::initialise(&url).await.unwrap());
    let id = uuid::Uuid::new_v4().simple().to_string();
    let stream = StreamSpec {
        name: format!("CORE_PRICES_{id}"),
        subjects: vec![format!("test.core.prices.{id}")],
        ..tackt_nats::subjects::price_aggregate_stream()
    };
    assert!(
        PriceCache::initialise(client.clone(), stream.clone())
            .await
            .is_err()
    );
    client.ensure_stream(&stream).await.unwrap();
    // Even fresh retained prices must not be consumed by a new Core subscription.
    client
        .publish_aggregate_price(&stream, &price(LedgerAsset::Eth, 900, Utc::now()))
        .await
        .unwrap();
    let (first, first_task) = start(client.clone(), stream.clone()).await;
    let (second, second_task) = start(client.clone(), stream.clone()).await;
    // A live BTC delivery is a barrier after all retained ETH messages.
    client
        .publish_aggregate_price(&stream, &price(LedgerAsset::Btc, 1, Utc::now()))
        .await
        .unwrap();
    for cache in [&first, &second] {
        expect_sequence(cache, LedgerAsset::Btc, 1).await;
        assert_eq!(cache.get_latest(LedgerAsset::Eth), None);
    }

    // Source timestamps are stored unchanged, even when far ahead of Core.
    client
        .publish_aggregate_price(
            &stream,
            &price(
                LedgerAsset::Eth,
                1_800_000_100_000,
                DateTime::from_timestamp(2_000_000_000, 0).unwrap(),
            ),
        )
        .await
        .unwrap();
    for cache in [&first, &second] {
        expect_sequence(cache, LedgerAsset::Eth, 1_800_000_100_000).await;
    }
    // A cold producer restart can lower its counter; the newer broker delivery wins.
    client
        .publish_aggregate_price(
            &stream,
            &price(LedgerAsset::Eth, 1_800_000_000_000, Utc::now()),
        )
        .await
        .unwrap();
    client
        .publish_aggregate_price(
            &stream,
            &price(
                LedgerAsset::Sol,
                1,
                DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            ),
        )
        .await
        .unwrap();
    client
        .publish_aggregate_price(&stream, &price(LedgerAsset::Btc, 2, Utc::now()))
        .await
        .unwrap();
    for cache in [&first, &second] {
        expect_sequence(cache, LedgerAsset::Btc, 2).await;
        assert_eq!(
            cache.get_latest(LedgerAsset::Eth).unwrap().sequence(),
            1_800_000_000_000
        );
        assert_eq!(
            cache.get_latest(LedgerAsset::Sol).unwrap().computed_at(),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap()
        );
    }

    first_task.abort();
    let _ = first_task.await;
    assert_eq!(
        first.get_latest(LedgerAsset::Eth).unwrap().sequence(),
        1_800_000_000_000
    );
    let (restarted, restarted_task) = start(client.clone(), stream.clone()).await;
    client
        .publish_aggregate_price(&stream, &price(LedgerAsset::Btc, 3, Utc::now()))
        .await
        .unwrap();
    expect_sequence(&restarted, LedgerAsset::Btc, 3).await;
    assert_eq!(restarted.get_latest(LedgerAsset::Eth), None);
    client
        .publish_aggregate_price(
            &stream,
            &price(LedgerAsset::Eth, 1_800_000_000_001, Utc::now()),
        )
        .await
        .unwrap();
    for cache in [&second, &restarted] {
        expect_sequence(cache, LedgerAsset::Eth, 1_800_000_000_001).await;
    }
    second_task.abort();
    restarted_task.abort();
    let _ = second_task.await;
    let _ = restarted_task.await;
    client.delete_stream(&stream.name).await.unwrap();
}
