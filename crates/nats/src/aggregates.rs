//! Typed aggregate prices: the Indexer's per-asset composite of its own price evidence.
//!
//! This is the executable output of the price pipeline. Where [`crate::prices::PriceEvidence`]
//! records what one source said, an [`AggregatePrice`] states the deviation-guarded weighted
//! composite the Indexer computed across every fresh source of one registry asset — and names
//! the evidence it was computed from, so any consumer can re-derive it from the archived
//! evidence stream. The subject is [`crate::subjects::PRICE_AGGREGATE`].
//!
//! # Derived, not evidence
//!
//! An aggregate is superseded by the next one and recomputable forever from evidence plus the
//! registry, which is why its stream retains briefly and discards old messages at capacity —
//! the opposite posture from the evidence stream, whose backlog is irreplaceable until
//! archived. Publishes carry no message id: the Indexer deliberately republishes an unchanged
//! aggregate as a heartbeat, and consumers separate "price is stable" from "publisher is
//! stalled" by the aggregate's freshness, never by deduplication.
//!
//! The stream covers only the registry assets the Indexer configures price sources for. An
//! asset with no configured source — USDC, the base asset valued at par by design — never
//! appears here, so its permanent absence is policy, not a staleness or outage signal.
//!
//! # Ordering
//!
//! [`AggregatePrice::sequence`] is the Indexer's strictly increasing per-asset counter,
//! derived from the computation clock and clamped monotonic within a running publisher. A
//! cold restart after a clock rollback can lower it. Core orders live deliveries by JetStream
//! stream sequence instead; each new live subscription starts a new broker cursor.
//! The single-active-indexer rule remains necessary to prevent competing publishers.
//!
//! # Contract changes
//!
//! The current typed payload rejects unknown fields. Deploy producers and consumers together
//! when changing its shape; preproduction contracts carry no compatibility versions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tackt_primitives::ledger::LedgerAsset;

use crate::NatsClient;
use crate::jetstream::{Envelope, PublishOutcome, StreamSpec};
use crate::prices::{
    MAX_EVIDENCE_UNIX_SECONDS, MIN_EVIDENCE_UNIX_SECONDS, PRICE_EVIDENCE_ID_PREFIX, PriceAuthority,
    PriceQuote,
};

/// The one fixed-point scale every aggregate is expressed in, matching the risk engine's
/// `1e8` price unit. The scale is contract, not data: consumers must never rescale.
pub const AGGREGATE_PRICE_SCALE: u8 = 8;

/// Most sources one aggregate may cite; also bounds the Indexer's per-asset configuration.
pub const MAX_AGGREGATE_SOURCES: usize = 16;

/// What one asset's source weights sum to (basis points); cited weights never exceed it.
pub const AGGREGATE_WEIGHT_TOTAL_BPS: u16 = 10_000;

/// Typed envelope carrying one aggregate price.
pub type AggregatePriceEnvelope = Envelope<AggregatePrice>;

/// One cited contribution to an aggregate: which authority's evidence, at what decayed weight.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AggregateSource {
    pub authority: PriceAuthority,
    /// Identity of the contributing evidence in the evidence stream and archive.
    pub evidence_id: String,
    /// The staleness-decayed weight this evidence carried in the mean.
    pub effective_weight_bps: u16,
}

/// One computed aggregate price and its complete derivation.
///
/// Fields are private and deserialization re-runs every check, so a value of this type is a
/// well-formed aggregate whichever direction it came from.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "AggregatePriceWire")]
pub struct AggregatePrice {
    asset: LedgerAsset,
    price: PriceQuote,
    sequence: u64,
    computed_at: DateTime<Utc>,
    inputs: Vec<AggregateSource>,
}

impl AggregatePrice {
    /// Builds one aggregate price.
    ///
    /// # Errors
    ///
    /// Fails closed on a price at any scale but [`AGGREGATE_PRICE_SCALE`], a zero sequence, a
    /// computation instant outside the representable window, and an input list that is empty,
    /// oversized, repeats evidence, carries a zero weight, or claims more total weight than
    /// [`AGGREGATE_WEIGHT_TOTAL_BPS`].
    pub fn new(
        asset: LedgerAsset,
        price: PriceQuote,
        sequence: u64,
        computed_at: DateTime<Utc>,
        inputs: Vec<AggregateSource>,
    ) -> Result<Self, AggregatePriceError> {
        if price.scale() != AGGREGATE_PRICE_SCALE {
            return Err(AggregatePriceError::WrongScale {
                scale: price.scale(),
            });
        }
        if sequence == 0 {
            return Err(AggregatePriceError::ZeroSequence);
        }
        let seconds = computed_at.timestamp();
        if !(MIN_EVIDENCE_UNIX_SECONDS..=MAX_EVIDENCE_UNIX_SECONDS).contains(&seconds) {
            return Err(AggregatePriceError::TimestampOutOfRange);
        }
        if inputs.is_empty() || inputs.len() > MAX_AGGREGATE_SOURCES {
            return Err(AggregatePriceError::InvalidInputCount {
                count: inputs.len(),
            });
        }
        let mut total_weight: u32 = 0;
        for (index, input) in inputs.iter().enumerate() {
            if !valid_evidence_id(&input.evidence_id) {
                return Err(AggregatePriceError::InvalidEvidenceId);
            }
            if inputs[..index]
                .iter()
                .any(|earlier| earlier.evidence_id == input.evidence_id)
            {
                return Err(AggregatePriceError::DuplicateEvidence);
            }
            if input.effective_weight_bps == 0 {
                return Err(AggregatePriceError::ZeroInputWeight);
            }
            total_weight += u32::from(input.effective_weight_bps);
        }
        if total_weight > u32::from(AGGREGATE_WEIGHT_TOTAL_BPS) {
            return Err(AggregatePriceError::WeightAboveTotal { total_weight });
        }
        Ok(Self {
            asset,
            price,
            sequence,
            computed_at,
            inputs,
        })
    }

    /// Registry asset this aggregate prices. Always an accounting asset — venue markets are
    /// inputs, never outputs.
    #[must_use]
    pub const fn asset(&self) -> LedgerAsset {
        self.asset
    }

    /// The composite price, always at [`AGGREGATE_PRICE_SCALE`].
    #[must_use]
    pub const fn price(&self) -> PriceQuote {
        self.price
    }

    /// The Indexer's per-asset counter, monotonic within one running publisher.
    /// Cold restarts after a clock rollback may lower it; see the module's ordering notes.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// The instant the aggregation was evaluated at; its freshness signal.
    #[must_use]
    pub const fn computed_at(&self) -> DateTime<Utc> {
        self.computed_at
    }

    /// The cited evidence, in the Indexer's source-configuration order.
    #[must_use]
    pub fn inputs(&self) -> &[AggregateSource] {
        &self.inputs
    }
}

fn valid_evidence_id(id: &str) -> bool {
    id.strip_prefix(PRICE_EVIDENCE_ID_PREFIX)
        .is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregatePriceWire {
    asset: LedgerAsset,
    price: PriceQuote,
    sequence: u64,
    computed_at: DateTime<Utc>,
    inputs: Vec<AggregateSource>,
}

impl TryFrom<AggregatePriceWire> for AggregatePrice {
    type Error = AggregatePriceError;

    fn try_from(wire: AggregatePriceWire) -> Result<Self, Self::Error> {
        Self::new(
            wire.asset,
            wire.price,
            wire.sequence,
            wire.computed_at,
            wire.inputs,
        )
    }
}

/// An aggregate that must not cross the durable boundary.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AggregatePriceError {
    #[error("an aggregate price must be at scale {AGGREGATE_PRICE_SCALE}, got {scale}")]
    WrongScale { scale: u8 },
    #[error("an aggregate sequence must be greater than zero")]
    ZeroSequence,
    #[error("the computation instant is outside the representable window")]
    TimestampOutOfRange,
    #[error("an aggregate must cite 1 to {MAX_AGGREGATE_SOURCES} inputs, got {count}")]
    InvalidInputCount { count: usize },
    #[error("a cited evidence id is not a well-formed price-evidence identity")]
    InvalidEvidenceId,
    #[error("an aggregate cites the same evidence twice")]
    DuplicateEvidence,
    #[error("a cited input carries zero weight")]
    ZeroInputWeight,
    #[error("cited weights sum to {total_weight}, above {AGGREGATE_WEIGHT_TOTAL_BPS}")]
    WeightAboveTotal { total_weight: u32 },
}

impl NatsClient {
    /// Independent per-process feed of aggregates published after subscription. Starts live
    /// on every attach; retained stream history is never delivered to a new consumer.
    pub async fn aggregate_prices(
        &self,
        stream: &StreamSpec,
    ) -> anyhow::Result<crate::Consumer<AggregatePriceEnvelope>> {
        self.consumer(stream, crate::ConsumerSpec::ephemeral())
            .await
    }

    /// Publish one aggregate and wait for JetStream to acknowledge it as durable.
    ///
    /// Deliberately without a message id: an unchanged aggregate republished on the heartbeat
    /// is a fresh statement that the price still holds, and must not be deduplicated away.
    /// An error means nothing was stored; the aggregate is derived data, so the caller logs
    /// and lets the next computation supersede it rather than failing closed.
    ///
    /// `stream` is [`crate::subjects::price_aggregate_stream`] in production; tests pass
    /// uniquely named streams for isolation.
    pub async fn publish_aggregate_price(
        &self,
        stream: &StreamSpec,
        aggregate: &AggregatePrice,
    ) -> anyhow::Result<PublishOutcome> {
        // A borrowed envelope serializes byte-identically to the owned alias without cloning
        // the aggregate and its cited inputs on every publish.
        let envelope = Envelope::<&AggregatePrice>::new(aggregate);
        self.publish_durable(stream.subject(), None, &envelope)
            .await
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn evidence_id(fill: char) -> String {
        format!("{PRICE_EVIDENCE_ID_PREFIX}{}", fill.to_string().repeat(64))
    }

    fn source(authority: PriceAuthority, fill: char, weight: u16) -> AggregateSource {
        AggregateSource {
            authority,
            evidence_id: evidence_id(fill),
            effective_weight_bps: weight,
        }
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }

    fn aggregate() -> AggregatePrice {
        AggregatePrice::new(
            LedgerAsset::Eth,
            PriceQuote::new(244_100_000_000, AGGREGATE_PRICE_SCALE).expect("quote"),
            1_800_000_000_000,
            at(1_800_000_000),
            vec![
                source(PriceAuthority::Binance, 'a', 5_000),
                source(PriceAuthority::Kraken, 'b', 5_000),
            ],
        )
        .expect("aggregate")
    }

    #[test]
    fn aggregate_wire_shape_is_stable() {
        let aggregate = aggregate();
        let envelope = AggregatePriceEnvelope::new(aggregate.clone());
        let wire = serde_json::to_value(&envelope).expect("serialize");

        let payload = &wire["payload"];
        assert_eq!(payload["asset"], "ETH");
        assert_eq!(
            payload["price"],
            json!({ "value": 244_100_000_000_u64, "scale": 8 })
        );
        assert_eq!(payload["sequence"], 1_800_000_000_000_u64);
        assert_eq!(payload["computed_at"], "2027-01-15T08:00:00Z");
        assert_eq!(payload["inputs"][0]["authority"], "binance");
        assert_eq!(payload["inputs"][0]["evidence_id"], evidence_id('a'));
        assert_eq!(payload["inputs"][1]["effective_weight_bps"], 5_000);

        let decoded: AggregatePriceEnvelope = serde_json::from_value(wire).expect("deserialize");
        assert_eq!(decoded.payload, aggregate);
    }

    #[test]
    fn malformed_aggregates_fail_closed() {
        let build = |price: PriceQuote, sequence, computed_at, inputs| {
            AggregatePrice::new(LedgerAsset::Eth, price, sequence, computed_at, inputs)
        };
        let scale_8 = PriceQuote::new(1, AGGREGATE_PRICE_SCALE).expect("quote");
        let one_input = || vec![source(PriceAuthority::Binance, 'a', 10_000)];

        assert_eq!(
            build(
                PriceQuote::new(1, 2).expect("quote"),
                1,
                at(1_800_000_000),
                one_input()
            ),
            Err(AggregatePriceError::WrongScale { scale: 2 })
        );
        assert_eq!(
            build(scale_8, 0, at(1_800_000_000), one_input()),
            Err(AggregatePriceError::ZeroSequence)
        );
        assert_eq!(
            build(scale_8, 1, at(MIN_EVIDENCE_UNIX_SECONDS - 1), one_input()),
            Err(AggregatePriceError::TimestampOutOfRange)
        );
        assert_eq!(
            build(scale_8, 1, at(1_800_000_000), vec![]),
            Err(AggregatePriceError::InvalidInputCount { count: 0 })
        );
        assert_eq!(
            build(
                scale_8,
                1,
                at(1_800_000_000),
                vec![source(PriceAuthority::Binance, 'a', 1); MAX_AGGREGATE_SOURCES + 1]
            ),
            Err(AggregatePriceError::InvalidInputCount {
                count: MAX_AGGREGATE_SOURCES + 1
            })
        );
        assert_eq!(
            build(
                scale_8,
                1,
                at(1_800_000_000),
                vec![AggregateSource {
                    evidence_id: "obs_forged".to_string(),
                    ..source(PriceAuthority::Binance, 'a', 1)
                }]
            ),
            Err(AggregatePriceError::InvalidEvidenceId)
        );
        assert_eq!(
            build(
                scale_8,
                1,
                at(1_800_000_000),
                vec![
                    source(PriceAuthority::Binance, 'a', 1),
                    source(PriceAuthority::Kraken, 'a', 1),
                ]
            ),
            Err(AggregatePriceError::DuplicateEvidence)
        );
        assert_eq!(
            build(
                scale_8,
                1,
                at(1_800_000_000),
                vec![source(PriceAuthority::Binance, 'a', 0)]
            ),
            Err(AggregatePriceError::ZeroInputWeight)
        );
        assert_eq!(
            build(
                scale_8,
                1,
                at(1_800_000_000),
                vec![
                    source(PriceAuthority::Binance, 'a', 6_000),
                    source(PriceAuthority::Kraken, 'b', 6_000),
                ]
            ),
            Err(AggregatePriceError::WeightAboveTotal {
                total_weight: 12_000
            })
        );
    }

    #[test]
    fn unknown_fields_and_foreign_shapes_are_refused() {
        let mut wire = serde_json::to_value(aggregate()).expect("serialize");
        wire["unexpected_field"] = json!(1);
        assert!(serde_json::from_value::<AggregatePrice>(wire).is_err());

        // Price evidence must never decode as an aggregate: it has no sequence-and-inputs
        // shape, and the two authorities' payloads must stay disjoint.
        let evidence = serde_json::to_value(
            crate::prices::PriceEvidence::new(
                PriceAuthority::Binance,
                crate::prices::PriceSubject::Asset(LedgerAsset::Eth),
                PriceQuote::new(1, 8).expect("quote"),
                1,
                at(1_800_000_000),
                at(1_800_000_000),
                tackt_primitives::Provenance {
                    endpoint: "https://example.com".to_string(),
                    method: "test".to_string(),
                    source_id: "seq:1".to_string(),
                },
                json!({}),
            )
            .expect("evidence"),
        )
        .expect("serialize");
        assert!(serde_json::from_value::<AggregatePrice>(evidence).is_err());
    }
}
