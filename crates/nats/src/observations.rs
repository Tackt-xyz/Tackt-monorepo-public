//! Durable account-observation feed (JetStream, at-least-once).
//!
//! The acknowledged publish to [`subjects::OBSERVATIONS_STREAM`] is the Indexer's only
//! durable side effect for an account observation — there is no PostgreSQL fallback to
//! reconcile from. Contract:
//!
//! EVM uses block batches with an explicit reorg range, finality and a
//! durable canonical suffix. Batch identity hashes the complete payload (including
//! the prior subject sequence), distinguishing a retry from a return to an old hash.
//! Non-EVM facts use individual observations, deduplicated by event ID.
//! Ledger commits facts, accounting and its cursor before acknowledging delivery.

use async_nats::jetstream::stream::LastRawMessageErrorKind;
use serde::{Deserialize, Deserializer, Serialize, de};
use sha2::{Digest, Sha256};
use tackt_primitives::chain::BlockRef;
use tackt_primitives::{Observation, Source};

use crate::NatsClient;
use crate::jetstream::{Consumer, ConsumerSpec, PublishOutcome, StreamSpec};
use crate::subjects;

pub const LEDGER_OBSERVATION_CONSUMER: &str = "ledger-deposit-evidence";

/// Typed routing for the production feed or a disjoint integration-test feed.
/// Tests use the same publisher/consumer path without injecting fake money into
/// live subjects on the shared NATS server.
#[derive(Clone)]
pub struct ObservationFeed {
    stream: StreamSpec,
    prefix: String,
}

impl ObservationFeed {
    pub fn production() -> Self {
        Self {
            stream: subjects::observations_stream(),
            prefix: "tackt.observations".into(),
        }
    }

    pub fn isolated(test_name: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            test_name.starts_with("test_")
                && test_name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "isolated observation feed requires a test_ identifier"
        );
        let prefix = format!("{test_name}.observations");
        Ok(Self {
            stream: StreamSpec {
                name: test_name.to_uppercase(),
                subjects: vec![format!("{prefix}.>")],
                // Tiny fixtures do not need the production stream's 8 GiB reservation.
                max_bytes: Some(16 * 1024 * 1024),
                ..subjects::observations_stream()
            },
            prefix,
        })
    }

    pub fn stream(&self) -> &StreamSpec {
        &self.stream
    }
    pub(crate) fn subject(&self, source: Source) -> String {
        format!("{}.{}", self.prefix, source.as_str())
    }
    pub fn evm_subject(&self, chain: u64) -> String {
        let base = self.subject(Source::Ethereum);
        if chain == 1 {
            base
        } else {
            format!("{base}.{chain}")
        }
    }
}

/// Envelope published for an observation.
///
/// Delivery is durable JetStream at-least-once; the broker deduplicates on the
/// observation's `event_id` or complete chain-update identity inside
/// [`subjects::OBSERVATIONS_DUPLICATE_WINDOW`].
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ObservationEvent {
    pub observation: Observation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<EvmBatch>,
}

/// One complete block, or a finality-only update (empty `observations`).
/// History is the connected canonical suffix from the finalized anchor to the tip.
/// A changed branch explicitly invalidates heights starting at `revert_from`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EvmBatch {
    pub chain_id: u64,
    pub previous_sequence: u64,
    pub finalized: BlockRef,
    pub history: Vec<BlockRef>,
    pub revert_from: Option<u64>,
    pub observations: Vec<Observation>,
}

impl<'de> Deserialize<'de> for ObservationEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            observation: Observation,
            #[serde(default)]
            batch: Option<EvmBatch>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.batch.is_none() && wire.observation.source == Source::Ethereum {
            return Err(de::Error::custom("EVM observations require a block batch"));
        }
        Ok(Self {
            observation: wire.observation,
            batch: wire.batch,
        })
    }
}

impl ObservationEvent {
    pub fn new(observation: Observation) -> Self {
        Self {
            observation,
            batch: None,
        }
    }

    /// Build an EVM envelope from the block header observation and its batch.
    pub fn with_batch(observation: Observation, batch: EvmBatch) -> Self {
        Self {
            observation,
            batch: Some(batch),
        }
    }
}

impl NatsClient {
    /// Publish one complete EVM update. The subject sequence fences stale publishers;
    /// it also distinguishes a new adoption of an old hash from retrying one update.
    pub async fn publish_evm_batch(
        &self,
        feed: &ObservationFeed,
        event: &ObservationEvent,
    ) -> anyhow::Result<u64> {
        let batch = event
            .batch
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing EVM batch"))?;
        let payload = serde_json::to_vec(event)?;
        let max = self.client.max_payload();
        anyhow::ensure!(
            payload.len() + 512 <= max,
            "EVM block batch is {} bytes, exceeding broker payload limit {max}; provision sufficient max_payload before retrying",
            payload.len()
        );
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            async_nats::header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            batch.previous_sequence.to_string(),
        );
        // Include content, not just the prior sequence: an ambiguous publish must never
        // acknowledge a different branch's payload as a duplicate.
        headers.insert(
            async_nats::header::NATS_MESSAGE_ID,
            hex::encode(Sha256::digest(&payload)),
        );
        let ack = self
            .jetstream
            .publish_with_headers(feed.evm_subject(batch.chain_id), headers, payload.into())
            .await?
            .await?;
        Ok(ack.sequence)
    }

    pub async fn last_evm_event(
        &self,
        feed: &ObservationFeed,
        chain_id: u64,
    ) -> anyhow::Result<Option<(u64, ObservationEvent)>> {
        let stream = self.jetstream.get_stream_no_info(&feed.stream.name).await?;
        match stream
            .get_last_raw_message_by_subject(&feed.evm_subject(chain_id))
            .await
        {
            Ok(message) => Ok(Some((
                message.sequence,
                serde_json::from_slice(&message.payload)?,
            ))),
            Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Bind Ledger's existing ordered durable; never provision or reset it at startup.
    pub async fn ledger_observations(
        &self,
        stream: &StreamSpec,
    ) -> anyhow::Result<Consumer<ObservationEvent>> {
        let mut spec = ConsumerSpec::durable(LEDGER_OBSERVATION_CONSUMER);
        spec.max_ack_pending = Some(1);
        self.bind_consumer(stream, spec).await
    }

    /// Explicit deployment/test setup. Existing durable state is never reset.
    pub async fn provision_ledger_consumer(&self, stream: &StreamSpec) -> anyhow::Result<()> {
        self.require_stream(stream).await?;
        self.jetstream
            .get_stream(&stream.name)
            .await?
            .get_or_create_consumer(
                LEDGER_OBSERVATION_CONSUMER,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(LEDGER_OBSERVATION_CONSUMER.into()),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(30),
                    max_deliver: -1,
                    max_ack_pending: 1,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Publish one non-EVM fact and wait for its durable acknowledgement.
    pub async fn publish_observation_to_feed(
        &self,
        feed: &ObservationFeed,
        event: &ObservationEvent,
    ) -> anyhow::Result<PublishOutcome> {
        anyhow::ensure!(
            event.batch.is_none() && event.observation.source != Source::Ethereum,
            "individual observations require a non-EVM source and no block batch"
        );
        let subject =
            match crate::HyperliquidObservationStream::from_observation(&event.observation) {
                Some(stream) => feed.hyperliquid_subject(stream),
                None => feed.subject(event.observation.source),
            };
        self.publish_durable(&subject, Some(&event.observation.event_id), event)
            .await
    }

    /// Bind a durable observation consumer on an already provisioned stream.
    ///
    /// Deliveries are at-least-once. A consumer acknowledges only after its own durable
    /// effect is complete. Fact identity and chain-update delivery identity are distinct.
    ///
    /// `stream` is [`subjects::observations_stream`] in production; tests pass uniquely
    /// named streams for isolation.
    pub async fn observations(
        &self,
        stream: &StreamSpec,
        spec: ConsumerSpec,
    ) -> anyhow::Result<Consumer<ObservationEvent>> {
        self.consumer(stream, spec).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tackt_primitives::{EventKind, Provenance, Source};

    use super::*;

    #[test]
    fn isolated_feed_is_small_and_keeps_production_delivery_rules() {
        let feed = ObservationFeed::isolated("test_ledger").unwrap();
        let production = subjects::observations_stream();
        assert_eq!(feed.stream().max_bytes, Some(16 * 1024 * 1024));
        assert_eq!(
            ObservationFeed::production().stream().max_bytes,
            production.max_bytes
        );
        assert_eq!(feed.stream().retention, production.retention);
        assert_eq!(feed.stream().discard, production.discard);
        assert_eq!(feed.stream().max_age, production.max_age);
        assert_eq!(feed.stream().duplicate_window, production.duplicate_window);
        assert_eq!(feed.evm_subject(1), "test_ledger.observations.ethereum");
        assert!(ObservationFeed::isolated("tackt").is_err());
    }

    #[test]
    fn observation_event_wire_shape_is_stable() {
        let observation = Observation {
            event_id: "obs_test".to_string(),
            source: Source::Hyperliquid,
            kind: EventKind::HyperliquidBalance,
            account: None,
            asset: None,
            external_asset: None,
            quantity: None,
            source_timestamp: Utc::now(),
            observed_at: Utc::now(),
            block_number: Some(1),
            confirmations: Some(0),
            block_hash: None,
            canonical: true,
            finalized: false,
            raw: serde_json::json!({}),
            provenance: Provenance {
                endpoint: "test".to_string(),
                method: "test".to_string(),
                source_id: "1".to_string(),
            },
        };
        let event = ObservationEvent::new(observation);
        let value = serde_json::to_value(&event).expect("serialize");
        assert_eq!(value["observation"]["event_id"], "obs_test");
        assert_eq!(value["observation"]["source"], "hyperliquid");
        let round_trip: ObservationEvent = serde_json::from_value(value).expect("deserialize");
        assert_eq!(round_trip, event);
    }

    #[test]
    fn observation_wire_checks_shape_not_business_rules() {
        let observation = Observation {
            event_id: "obs_invalid".to_string(),
            source: Source::Hyperliquid,
            kind: EventKind::HyperliquidBalance,
            account: None,
            asset: None,
            external_asset: None,
            quantity: None,
            source_timestamp: Utc::now(),
            observed_at: Utc::now(),
            block_number: Some(1),
            confirmations: Some(0),
            block_hash: None,
            canonical: true,
            finalized: false,
            raw: serde_json::json!({}),
            provenance: Provenance {
                endpoint: "test".to_string(),
                method: "test".to_string(),
                source_id: "1".to_string(),
            },
        };
        let mut wire = serde_json::to_value(ObservationEvent::new(observation)).expect("serialize");
        wire["observation"]["source"] = serde_json::json!("hyperliquid");
        assert!(serde_json::from_value::<ObservationEvent>(wire.clone()).is_ok());
        wire["observation"]["block_number"] = serde_json::json!("not-a-number");
        assert!(serde_json::from_value::<ObservationEvent>(wire.clone()).is_err());
        wire["observation"]["block_number"] = serde_json::json!(1);
        wire["observation"]["source"] = serde_json::json!("ethereum");
        assert!(
            serde_json::from_value::<ObservationEvent>(wire.clone()).is_err(),
            "EVM requires a block batch"
        );
    }
}
