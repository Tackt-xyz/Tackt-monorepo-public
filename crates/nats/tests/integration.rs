//! JetStream and core-NATS integration tests.
//!
//! These stay green on machines without a NATS server; CI supplies
//! `NATS_URL` (a JetStream-enabled server), so they actually run there.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tackt_nats::{
    ConsumerSpec, Discard, NatsClient, PriceEvidence, PriceEvidenceConsumer, PublishOutcome,
    Retention, StartPosition, StreamSpec,
};

/// Connects, or skips the test outside CI when `NATS_URL` is unset.
async fn client() -> Option<NatsClient> {
    let Ok(url) = std::env::var("NATS_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "NATS_URL is unset in CI; refusing to silently skip NATS integration tests"
        );
        eprintln!("skipping: set NATS_URL to run NATS integration tests");
        return None;
    };
    Some(tackt_nats::initialise(&url).await.expect("connect to NATS"))
}

fn unique(name: &str) -> String {
    format!("{name}_{}", uuid_like())
}

/// Unique suffix without depending on uuid in dev-deps: nanos since epoch.
fn uuid_like() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    )
}

fn test_stream(name: &str, subject: &str, retention: Retention) -> StreamSpec {
    StreamSpec {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        retention,
        max_age: Some(Duration::from_secs(300)),
        max_messages: None,
        max_bytes: Some(64 * 1024 * 1024),
        discard: Discard::Old,
        duplicate_window: Duration::from_secs(120),
    }
}

/// The production price-evidence stream under a test name and subject, so the
/// tests exercise the real retention, discard, and dedupe policy rather than a
/// convenient one. Capacity is the only field reduced: 7 days of headroom is
/// not something a test can fill.
fn price_stream(name: &str, subject: &str) -> StreamSpec {
    StreamSpec {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        max_bytes: Some(16 * 1024 * 1024),
        ..tackt_nats::subjects::price_evidence_stream()
    }
}

/// One piece of evidence, distinguished by its provider sequence: the sequence
/// is part of the identity, so a fresh one is a different update and the same
/// one is a retry of the same update.
fn evidence(source_sequence: u64) -> PriceEvidence {
    PriceEvidence::new(
        tackt_nats::PriceAuthority::Hyperliquid,
        tackt_nats::PriceSubject::Market(
            tackt_nats::MarketSymbol::new("BTC").expect("market symbol"),
        ),
        tackt_nats::PriceQuote::new(6_500_000_000_000, 8).expect("quote"),
        source_sequence,
        chrono::DateTime::from_timestamp(1_800_000_000, 0).expect("source timestamp"),
        chrono::DateTime::from_timestamp(1_800_000_001, 0).expect("observation timestamp"),
        tackt_primitives::Provenance {
            endpoint: "https://api.hyperliquid.xyz/info".to_string(),
            method: "allMids".to_string(),
            source_id: format!("mids:{source_sequence}"),
        },
        serde_json::json!({ "mid": "65000.0" }),
    )
    .expect("evidence")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    value: u64,
}

fn observation(event_id: &str, block_number: u64) -> tackt_nats::ObservationEvent {
    let observation: tackt_primitives::Observation = serde_json::from_value(serde_json::json!({
        "event_id": event_id,
        "source": "ethereum",
        "kind": "ethereum_block",
        "account": null,
        "asset": null,
        "external_asset": null,
        "quantity": null,
        "source_timestamp": "2026-01-01T00:00:00Z",
        "observed_at": "2026-01-01T00:00:00Z",
        "block_number": block_number,
        "confirmations": 0,
        "block_hash": null,
        "canonical": true,
        "finalized": false,
        "raw": {},
        "provenance": {
            "endpoint": "test",
            "method": "test",
            "source_id": "1",
        },
    }))
    .expect("observation fixture");
    tackt_nats::ObservationEvent::new(observation)
}

fn hyperliquid_observation(
    event_id: &str,
    block: u64,
    stream: tackt_nats::HyperliquidObservationStream,
) -> tackt_nats::ObservationEvent {
    let mut event = observation(event_id, block);
    event.observation.source = tackt_primitives::Source::Hyperliquid;
    event.observation.kind = tackt_primitives::EventKind::HyperliquidFee;
    event.observation.provenance.method = match stream {
        tackt_nats::HyperliquidObservationStream::Trades => "StreamDataBytes/TRADES",
        tackt_nats::HyperliquidObservationStream::Events => "StreamDataBytes/EVENTS",
    }
    .into();
    event
}

#[tokio::test]
async fn hyperliquid_kv_is_populated_by_the_broker_and_survives_source_expiry() {
    use tackt_nats::{
        HyperliquidObservationStream::{Events, Trades},
        ObservationFeed,
    };
    let Some(client) = client().await else { return };
    let feed = ObservationFeed::isolated(&unique("test_resume")).unwrap();
    client.ensure_stream(feed.stream()).await.unwrap();
    assert!(client.require_hyperliquid_resume(&feed).await.is_err());
    client.ensure_hyperliquid_resume(&feed).await.unwrap();
    client.ensure_hyperliquid_resume(&feed).await.unwrap();
    client.require_hyperliquid_resume(&feed).await.unwrap();
    assert_eq!(
        client
            .hyperliquid_resume_block(&feed, Trades)
            .await
            .unwrap(),
        None
    );
    let trades = hyperliquid_observation("obs_trade", 42, Trades);
    let events = hyperliquid_observation("obs_event", 90, Events);
    for event in [&trades, &events] {
        client
            .publish_observation_to_feed(&feed, event)
            .await
            .unwrap();
    }
    // The ordinary KV API returns the original envelopes: the server copied
    // both values without application puts.
    let raw = async_nats::connect(std::env::var("NATS_URL").unwrap())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(raw);
    let kv = js
        .get_key_value(feed.hyperliquid_resume_bucket())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if kv.get("trades").await.unwrap().is_some()
                && kv.get("events").await.unwrap().is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let stored: tackt_nats::ObservationEvent =
        serde_json::from_slice(&kv.get("trades").await.unwrap().unwrap()).unwrap();
    assert_eq!(stored, trades);
    let later = hyperliquid_observation("obs_trade_later", 43, Trades);
    client
        .publish_observation_to_feed(&feed, &later)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let latest: tackt_nats::ObservationEvent =
                serde_json::from_slice(&kv.get("trades").await.unwrap().unwrap()).unwrap();
            if latest == later {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut snapshot = hyperliquid_observation("obs_snapshot", 999, Trades);
    snapshot.observation.block_number = None;
    snapshot.observation.kind = tackt_primitives::EventKind::HyperliquidBalance;
    client
        .publish_observation_to_feed(&feed, &snapshot)
        .await
        .unwrap();
    assert_eq!(
        client
            .stream_message_count(&feed.hyperliquid_resume_stream())
            .await
            .unwrap(),
        2
    );
    // Removing only this test's source records simulates retention expiry. KV
    // must keep its last observations, independently of the evidence retention.
    js.get_stream(&feed.stream().name)
        .await
        .unwrap()
        .purge()
        .await
        .unwrap();
    let restarted = tackt_nats::initialise(&std::env::var("NATS_URL").unwrap())
        .await
        .unwrap();
    assert_eq!(
        restarted
            .hyperliquid_resume_block(&feed, Trades)
            .await
            .unwrap(),
        Some(43)
    );
    assert_eq!(
        restarted
            .hyperliquid_resume_block(&feed, Events)
            .await
            .unwrap(),
        Some(90)
    );
    client
        .delete_stream(&feed.hyperliquid_resume_stream())
        .await
        .unwrap();
    assert!(
        restarted
            .hyperliquid_resume_block(&feed, Trades)
            .await
            .is_err(),
        "missing bucket must not mean bootstrap"
    );
    client.delete_stream(&feed.stream().name).await.unwrap();
}

#[tokio::test]
async fn hyperliquid_resume_waits_for_kv_instead_of_bootstrapping_over_published_history() {
    use tackt_nats::{HyperliquidObservationStream::Trades, ObservationFeed};
    let Some(client) = client().await else { return };
    let feed = ObservationFeed::isolated(&unique("test_resume_wait")).unwrap();
    client.ensure_stream(feed.stream()).await.unwrap();
    client.ensure_hyperliquid_resume(&feed).await.unwrap();
    // Pause sourcing on this isolated bucket to make replication lag deterministic.
    let raw = async_nats::connect(std::env::var("NATS_URL").unwrap())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(raw);
    let bucket = js
        .get_stream(feed.hyperliquid_resume_stream())
        .await
        .unwrap();
    let mut config = bucket.cached_info().config.clone();
    for transform in &mut config.sources.as_mut().unwrap()[0].subject_transforms {
        transform.source.push_str(".paused");
    }
    js.update_stream(config).await.unwrap();
    let fresh = hyperliquid_observation("obs_fresh", 80, Trades);
    client
        .publish_observation_to_feed(&feed, &fresh)
        .await
        .unwrap();
    let error = client
        .hyperliquid_resume_block(&feed, Trades)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not caught up"));
    assert!(
        client.ensure_hyperliquid_resume(&feed).await.is_err(),
        "do not silently replace an incompatible bucket"
    );
    assert!(client.require_hyperliquid_resume(&feed).await.is_err());
    client
        .delete_stream(&feed.hyperliquid_resume_stream())
        .await
        .unwrap();
    client.delete_stream(&feed.stream().name).await.unwrap();
}

#[tokio::test]
async fn startup_checks_never_provision_or_change_streams_and_durables() {
    let Some(client) = client().await else { return };
    let feed = tackt_nats::ObservationFeed::isolated(&format!("test_manual_{}", uuid_like()))
        .expect("isolated feed");
    let spec = feed.stream();
    assert!(client.require_stream(spec).await.is_err());
    assert!(
        client
            .observations(spec, ConsumerSpec::durable("missing"))
            .await
            .is_err()
    );
    assert!(client.stream_message_count(&spec.name).await.is_err());

    client.ensure_stream(spec).await.expect("provision stream");
    client.require_stream(spec).await.expect("existing stream");
    let mut changed = spec.clone();
    changed.max_bytes = Some(32 * 1024 * 1024);
    assert!(client.require_stream(&changed).await.is_err());
    client
        .require_stream(spec)
        .await
        .expect("configuration unchanged");
    assert!(client.ledger_observations(spec).await.is_err());
    assert!(
        client.ledger_observations(spec).await.is_err(),
        "check must not create durable"
    );
    client
        .provision_ledger_consumer(spec)
        .await
        .expect("provision durable");
    let consumer = client
        .ledger_observations(spec)
        .await
        .expect("bind durable");
    let identity = consumer.stream_created_at().to_owned();
    drop(consumer);
    let rebound = client
        .ledger_observations(spec)
        .await
        .expect("rebind durable");
    assert_eq!(rebound.stream_created_at(), identity);
    drop(rebound);
    client.delete_stream(&spec.name).await.expect("cleanup");
}

#[tokio::test]
async fn binding_durable_resumes_acks_and_redelivers_unacked_messages() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_BIND_RESUME");
    let subject = format!("test.bind.{}", name.to_ascii_lowercase());
    let stream = price_stream(&name, &subject);
    client
        .ensure_stream(&stream)
        .await
        .expect("provision stream");
    let mut spec = ConsumerSpec::durable("resume");
    spec.ack_wait = Duration::from_millis(100);
    spec.max_ack_pending = Some(1);
    let mut consumer = client
        .consumer::<tackt_nats::PriceEvidenceEnvelope>(&stream, spec.clone())
        .await
        .expect("provision consumer");
    client
        .publish_price_evidence(&stream, &evidence(1))
        .await
        .expect("publish first");
    timeout(consumer.next())
        .await
        .ack()
        .await
        .expect("ack first");
    client
        .publish_price_evidence(&stream, &evidence(2))
        .await
        .expect("publish second");
    let unacked = timeout(consumer.next()).await;
    assert_eq!(unacked.stream_sequence(), 2);
    drop(unacked);
    drop(consumer);

    let mut resumed = client
        .bind_consumer::<tackt_nats::PriceEvidenceEnvelope>(&stream, spec)
        .await
        .expect("bind without reset");
    assert_eq!(resumed.acknowledged_sequence(), 1);
    let redelivered = timeout(resumed.next()).await;
    assert_eq!(redelivered.stream_sequence(), 2);
    redelivered.ack().await.expect("ack redelivery");
    drop(resumed);
    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn non_evm_observations_deduplicate_and_deliver_durably() {
    let Some(client) = client().await else { return };
    // Observation subjects are fixed per source, so this test provisions the
    // production stream spec itself; recreate it empty so lookups are
    // deterministic, and remove it afterwards. As with the price stream above,
    // capacity is the only field reduced: the production headroom can exceed a
    // CI server's JetStream store limit, and a test cannot fill it anyway.
    let feed = tackt_nats::ObservationFeed::production();
    let spec = StreamSpec {
        max_bytes: Some(16 * 1024 * 1024),
        ..feed.stream().clone()
    };
    let _ = client.delete_stream(&spec.name).await;
    client.ensure_stream(&spec).await.expect("create stream");

    assert_eq!(
        client.last_evm_event(&feed, 1).await.expect("empty lookup"),
        None,
        "an empty stream has no resume point"
    );

    let run = uuid_like();
    let mut first = observation(&format!("obs_{run}_1"), 1);
    first.observation.source = tackt_primitives::Source::Hyperliquid;
    first.observation.kind = tackt_primitives::EventKind::HyperliquidBalance;
    assert_eq!(
        client
            .publish_observation_to_feed(&feed, &first)
            .await
            .expect("publish"),
        PublishOutcome::Stored
    );
    // Replaying the same fact resolves to the stored identity.
    assert_eq!(
        client
            .publish_observation_to_feed(&feed, &first)
            .await
            .expect("republish"),
        PublishOutcome::Duplicate
    );
    let mut second = first.clone();
    second.observation.event_id = format!("obs_{run}_2");
    assert_eq!(
        client
            .publish_observation_to_feed(&feed, &second)
            .await
            .expect("publish"),
        PublishOutcome::Stored
    );
    assert_eq!(
        client
            .stream_message_count(&spec.name)
            .await
            .expect("count"),
        2,
        "one observation identity must not become two stored records"
    );

    assert!(
        client.last_evm_event(&feed, 1).await.unwrap().is_none(),
        "non-EVM publications must not move the EVM checkpoint"
    );

    let mut consumer = client
        .observations(&spec, ConsumerSpec::durable("observations-test"))
        .await
        .expect("consumer");
    for expected in [&first, &second] {
        let delivery = timeout(consumer.next()).await;
        assert_eq!(
            delivery.message().observation.event_id,
            expected.observation.event_id
        );
        delivery.ack().await.expect("ack");
    }

    client.delete_stream(&spec.name).await.expect("cleanup");
}

#[tokio::test]
async fn ensure_stream_is_idempotent_and_rejects_retention_change() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_ENSURE");
    let subject = format!("test.ensure.{}", name.to_ascii_lowercase());
    let spec = test_stream(&name, &subject, Retention::Limits);
    client.ensure_stream(&spec).await.expect("create");
    client
        .ensure_stream(&spec)
        .await
        .expect("idempotent re-ensure");
    let mut changed = spec.clone();
    changed.retention = Retention::WorkQueue;
    let error = client
        .ensure_stream(&changed)
        .await
        .expect_err("retention change must fail");
    assert!(
        error.to_string().contains("retention"),
        "unexpected error: {error:#}"
    );
    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn block_batches_deduplicate_retries_fence_stale_publishers_and_revisit_old_hashes() {
    use tackt_nats::{EvmBatch, ObservationEvent, ObservationFeed};
    use tackt_primitives::chain::BlockRef;
    let Some(client) = client().await else {
        return;
    };
    let feed = ObservationFeed::isolated(&format!("test_batches_{}", uuid_like())).unwrap();
    client.ensure_stream(feed.stream()).await.unwrap();
    let anchor = BlockRef {
        number: 1,
        hash: "anchor".into(),
    };
    let mut header = observation("obs_batch_a", 2).observation;
    header.block_hash = Some("a".into());
    let first = ObservationEvent::with_batch(
        header,
        EvmBatch {
            chain_id: 1,
            previous_sequence: 0,
            finalized: anchor.clone(),
            history: vec![
                anchor,
                BlockRef {
                    number: 2,
                    hash: "a".into(),
                },
            ],
            revert_from: None,
            observations: vec![],
        },
    );
    assert_eq!(client.publish_evm_batch(&feed, &first).await.unwrap(), 1);
    assert_eq!(client.publish_evm_batch(&feed, &first).await.unwrap(), 1);
    let mut second = first.clone();
    second.observation.block_hash = Some("b".into());
    second.batch.as_mut().unwrap().history[1].hash = "b".into();
    assert!(
        client.publish_evm_batch(&feed, &second).await.is_err(),
        "stale producer must not append"
    );
    second.batch.as_mut().unwrap().previous_sequence = 1;
    second.batch.as_mut().unwrap().revert_from = Some(2);
    assert_eq!(client.publish_evm_batch(&feed, &second).await.unwrap(), 2);
    let mut back = first.clone();
    back.batch.as_mut().unwrap().previous_sequence = 2;
    back.batch.as_mut().unwrap().revert_from = Some(2);
    assert_eq!(client.publish_evm_batch(&feed, &back).await.unwrap(), 3);
    assert_eq!(
        client.last_evm_event(&feed, 1).await.unwrap(),
        Some((3, back.clone()))
    );
    assert!(client.last_evm_event(&feed, 8453).await.unwrap().is_none());
    client
        .provision_ledger_consumer(feed.stream())
        .await
        .unwrap();
    let mut consumer = client.ledger_observations(feed.stream()).await.unwrap();
    for expected in [&first, &second, &back] {
        let delivery = timeout(consumer.next_strict()).await;
        assert_eq!(delivery.message(), expected);
        delivery.ack().await.unwrap();
    }
    let mut oversized = back;
    oversized.batch.as_mut().unwrap().previous_sequence = 3;
    oversized.observation.raw = serde_json::json!({"body":"x".repeat(10 * 1024 * 1024)});
    assert!(
        client
            .publish_evm_batch(&feed, &oversized)
            .await
            .unwrap_err()
            .to_string()
            .contains("payload limit")
    );
    assert_eq!(
        client
            .stream_message_count(&feed.stream().name)
            .await
            .unwrap(),
        3
    );
    client.delete_stream(&feed.stream().name).await.unwrap();
}

#[tokio::test]
async fn typed_consume_ack_nak_term_and_dedupe() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_FLOW");
    let subject = format!("test.flow.{}", name.to_ascii_lowercase());
    let spec = test_stream(&name, &subject, Retention::Limits);
    client.ensure_stream(&spec).await.expect("create stream");

    // Broker-side Nats-Msg-Id dedupe: two publishes, one stored message.
    // Keep the stream at a single message during the nak phase so redelivery
    // order is unambiguous (a naked message re-queues behind available ones).
    let event = TestEvent { value: 7 };
    client
        .publish_durable(&subject, Some("dedupe-1"), &event)
        .await
        .expect("publish");
    client
        .publish_durable(&subject, Some("dedupe-1"), &event)
        .await
        .expect("dup publish");

    let mut consumer = client
        .consumer::<TestEvent>(&spec, ConsumerSpec::durable("flow"))
        .await
        .expect("consumer");

    // First delivery: nak it, expect redelivery with a higher delivery count.
    let first = timeout(consumer.next()).await;
    assert_eq!(first.message().value, 7);
    assert_eq!(first.delivery_count(), 1);
    first.nak(Duration::from_millis(100)).await.expect("nak");
    let redelivered = timeout(consumer.next()).await;
    assert_eq!(
        redelivered.message().value,
        7,
        "dedupe stored one copy; nak redelivers it"
    );
    assert!(redelivered.delivery_count() >= 2, "nak must redeliver");
    redelivered.ack().await.expect("ack");

    // Second message: term must prevent redelivery, so the next thing the
    // consumer would see after terming is nothing (checked via timeout).
    let second = TestEvent { value: 8 };
    client
        .publish_durable(&subject, Some("dedupe-2"), &second)
        .await
        .expect("publish second");
    let second_delivery = timeout(consumer.next()).await;
    assert_eq!(second_delivery.message().value, 8);
    second_delivery.term().await.expect("term");
    let nothing = tokio::time::timeout(Duration::from_millis(500), consumer.next()).await;
    assert!(nothing.is_err(), "termed message must not redeliver");

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn undecodable_messages_are_termed_and_skipped() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_MALFORMED");
    let subject = format!("test.malformed.{}", name.to_ascii_lowercase());
    let spec = test_stream(&name, &subject, Retention::Limits);
    client.ensure_stream(&spec).await.expect("create stream");

    let malformed = serde_json::json!({"value": "not-a-number"});
    client
        .publish_durable(&subject, None, &malformed)
        .await
        .expect("publish malformed");
    let supported = TestEvent { value: 2 };
    client
        .publish_durable(&subject, None, &supported)
        .await
        .expect("publish supported");

    let mut consumer = client
        .consumer::<TestEvent>(&spec, ConsumerSpec::durable("malformed"))
        .await
        .expect("consumer");
    // next() must transparently term+skip the malformed message.
    let delivered = timeout(consumer.next()).await;
    assert_eq!(delivered.message().value, 2);
    delivered.ack().await.expect("ack");

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn by_sequence_start_overrides_durable_state() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_CURSOR");
    let subject = format!("test.cursor.{}", name.to_ascii_lowercase());
    let spec = test_stream(&name, &subject, Retention::Limits);
    client.ensure_stream(&spec).await.expect("create stream");

    for value in 1..=3u64 {
        let event = TestEvent { value };
        client
            .publish_durable(&subject, None, &event)
            .await
            .expect("publish");
    }

    // Consume and ack everything under a durable.
    let mut consumer = client
        .consumer::<TestEvent>(&spec, ConsumerSpec::durable("cursor"))
        .await
        .expect("consumer");
    for _ in 0..3 {
        timeout(consumer.next()).await.ack().await.expect("ack");
    }
    drop(consumer);

    // Recreate the same durable at sequence 2: the server-side position (done
    // with everything) must be overridden and messages 2 and 3 redelivered.
    let mut spec_consumer = ConsumerSpec::durable("cursor");
    spec_consumer.start = StartPosition::BySequence(2);
    let mut rewound = client
        .consumer::<TestEvent>(&spec, spec_consumer)
        .await
        .expect("rewound consumer");
    let replayed = timeout(rewound.next()).await;
    assert_eq!(
        replayed.message().value,
        2,
        "BySequence must rewind past durable state"
    );
    replayed.ack().await.expect("ack");

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn price_evidence_provisioning_is_idempotent_and_refuses_to_narrow() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_PRICE_ENSURE");
    let subject = format!("test.prices.{}", name.to_ascii_lowercase());
    let spec = price_stream(&name, &subject);
    client.ensure_stream(&spec).await.expect("create");
    // Every service calls this on every boot; a matching stream must be a no-op.
    client.ensure_stream(&spec).await.expect("re-ensure");

    for (label, narrowed) in [
        ("max_bytes", {
            let mut narrowed = spec.clone();
            narrowed.max_bytes = Some(1024 * 1024);
            narrowed
        }),
        ("max_age", {
            let mut narrowed = spec.clone();
            narrowed.max_age = Some(Duration::from_secs(60));
            narrowed
        }),
        ("discard policy", {
            let mut narrowed = spec.clone();
            narrowed.discard = Discard::Old;
            narrowed
        }),
        ("retention", {
            let mut narrowed = spec.clone();
            narrowed.retention = Retention::WorkQueue;
            narrowed
        }),
    ] {
        let error = client
            .ensure_stream(&narrowed)
            .await
            .expect_err(&format!("narrowing {label} must be refused"));
        let error = format!("{error:#}");
        assert!(
            error.contains(label),
            "refusal does not name {label}: {error}"
        );
    }

    // Widening still converges without an operator.
    let mut wider = spec.clone();
    wider.max_bytes = Some(32 * 1024 * 1024);
    client.ensure_stream(&wider).await.expect("widen");

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn a_price_evidence_publish_is_acked_and_deduplicated_by_identity() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_PRICE_PUBLISH");
    let subject = format!("test.prices.{}", name.to_ascii_lowercase());
    let spec = price_stream(&name, &subject);
    client.ensure_stream(&spec).await.expect("create stream");

    let update = evidence(2181);
    // Returning at all means the server acknowledged durable storage.
    assert_eq!(
        client
            .publish_price_evidence(&spec, &update)
            .await
            .expect("publish"),
        PublishOutcome::Stored
    );
    // The same provider update published again — a retry after an ambiguous
    // failure, or a feed re-serving one sequence — is the same identity.
    assert_eq!(
        client
            .publish_price_evidence(&spec, &update)
            .await
            .expect("republish"),
        PublishOutcome::Duplicate
    );
    assert_eq!(
        client.stream_message_count(&name).await.expect("count"),
        1,
        "one provider update must not become two stored records"
    );

    // A different provider sequence is a different update, not a duplicate.
    assert_eq!(
        client
            .publish_price_evidence(&spec, &evidence(2182))
            .await
            .expect("publish next"),
        PublishOutcome::Stored
    );
    assert_eq!(client.stream_message_count(&name).await.expect("count"), 2);

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn every_durable_consumer_receives_every_piece_of_evidence() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_PRICE_FANOUT");
    let subject = format!("test.prices.{}", name.to_ascii_lowercase());
    let spec = price_stream(&name, &subject);
    client.ensure_stream(&spec).await.expect("create stream");

    let update = evidence(4242);
    client
        .publish_price_evidence(&spec, &update)
        .await
        .expect("publish");

    // Limits retention, not a work queue: acking in one role must leave the
    // message for the other two.
    for role in PriceEvidenceConsumer::ALL {
        let mut consumer = client
            .price_evidence(&spec, role)
            .await
            .unwrap_or_else(|error| panic!("{role:?} consumer: {error:#}"));
        let delivery = timeout(consumer.next()).await;
        assert_eq!(
            delivery.message().payload.evidence_id(),
            update.evidence_id(),
            "{role:?} received different evidence"
        );
        delivery.ack().await.expect("ack");
    }
    assert_eq!(
        client.stream_message_count(&name).await.expect("count"),
        1,
        "acking must not consume the message the other consumers still need"
    );

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn a_naked_price_evidence_delivery_is_redelivered() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_PRICE_RETRY");
    let subject = format!("test.prices.{}", name.to_ascii_lowercase());
    let spec = price_stream(&name, &subject);
    client.ensure_stream(&spec).await.expect("create stream");
    client
        .publish_price_evidence(&spec, &evidence(7))
        .await
        .expect("publish");

    let mut consumer = client
        .price_evidence(&spec, PriceEvidenceConsumer::Archive)
        .await
        .expect("consumer");
    let first = timeout(consumer.next()).await;
    assert_eq!(first.delivery_count(), 1);
    // What a consumer does when its own durable effect failed transiently.
    first
        .nak(PriceEvidenceConsumer::retry_delay(1))
        .await
        .expect("nak");
    let redelivered = timeout(consumer.next()).await;
    assert!(
        redelivered.delivery_count() >= 2,
        "a naked delivery must come back"
    );
    redelivered.ack().await.expect("ack");

    client.delete_stream(&name).await.expect("cleanup");
}

#[tokio::test]
async fn a_full_fail_closed_stream_rejects_the_publish_and_keeps_its_backlog() {
    let Some(client) = client().await else { return };
    let name = unique("TEST_DISCARD_NEW");
    let subject = format!("test.discard.{}", name.to_ascii_lowercase());
    let mut spec = test_stream(&name, &subject, Retention::Limits);
    spec.discard = Discard::New;
    spec.max_bytes = Some(4 * 1024);
    client.ensure_stream(&spec).await.expect("create stream");

    assert!(
        client.stream_available(&name).await.is_ok(),
        "a provisioned stream is an available publish path"
    );
    assert!(
        client
            .stream_available(&unique("TEST_MISSING"))
            .await
            .is_err(),
        "a missing stream is not an available publish path"
    );

    let mut published = 0u64;
    let mut rejected = false;
    for value in 1..=500u64 {
        let event = TestEvent { value };
        match client
            .publish_durable(&subject, Some(&format!("fill-{value}")), &event)
            .await
        {
            Ok(_) => published += 1,
            Err(_) => {
                rejected = true;
                break;
            }
        }
    }
    assert!(
        rejected,
        "a full discard-new stream must reject the publish"
    );
    assert!(published > 0, "the stream never accepted anything");
    assert_eq!(
        client.stream_message_count(&name).await.expect("count"),
        published,
        "a rejected publish must not have cost the stream a stored message"
    );

    // The oldest message is still there: capacity exhaustion failed the
    // producer instead of silently discarding what nobody has consumed.
    let mut consumer = client
        .consumer::<TestEvent>(&spec, ConsumerSpec::durable("discard"))
        .await
        .expect("consumer");
    let oldest = timeout(consumer.next()).await;
    assert_eq!(oldest.message().value, 1);
    oldest.ack().await.expect("ack");

    client.delete_stream(&name).await.expect("cleanup");
}

async fn timeout<T>(
    future: impl Future<Output = anyhow::Result<tackt_nats::Delivery<T>>>,
) -> tackt_nats::Delivery<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("timely delivery")
        .expect("consumer alive")
}
