//! Typed price evidence: what an approved price source told the Indexer.
//!
//! This is the durable handoff contract. For each accepted price update the Indexer's only
//! durable side effect is one acknowledged JetStream publish of a [`PriceEvidenceEnvelope`];
//! independent downstream consumers feed risk, project latest state into NATS KV, and archive
//! immutable history. The subject is [`crate::subjects::PRICE_EVIDENCE`].
//!
//! # Evidence, not price authority
//!
//! A [`PriceEvidence`] message states that a *named authority* reported a *fixed-point quote*
//! for a *canonical subject* at a *provider sequence and timestamp*, and carries the provenance
//! and bounded raw payload that back the claim. It is deliberately not an executable price:
//! it has no registry price index and no engine ordering. Whether evidence
//! may move risk state — freshness, conflicts, quorum, conservative pricing — stays with the
//! Risk Engine and is decided after this contract has been satisfied. The Risk Engine's
//! executable `PriceUpdate` travels on its own stream, carries a disjoint set of required
//! fields, and neither payload decodes as the other.
//!
//! # Identity
//!
//! [`PriceEvidence::evidence_id`] is derived from exactly the facts that make two messages the
//! same provider update: authority, subject, provider sequence, quote, and
//! source timestamp. Retrying a publish — or fetching the same update through a second endpoint
//! — therefore produces the same identity, which is what makes broker deduplication and every
//! downstream consumer idempotent. Observation time, provenance, and raw evidence are carried
//! but excluded from identity, because they vary between attempts at the same update.
//!
//! # Bounds
//!
//! Every variable-length field is bounded ([`MAX_MARKET_SYMBOL_BYTES`],
//! [`MAX_PROVENANCE_FIELD_BYTES`], [`MAX_RAW_EVIDENCE_BYTES`]), so one encoded envelope stays
//! under [`MAX_ENCODED_EVIDENCE_BYTES`] whatever a provider returns.
//!
//! # Contract changes
//!
//! The current typed payload rejects unknown fields. Deploy producers and consumers together
//! when changing its shape; preproduction contracts carry no compatibility versions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tackt_primitives::ledger::LedgerAsset;
use tackt_primitives::{Provenance, ledger::valid_display_text};

use crate::NatsClient;
use crate::jetstream::{
    Consumer, ConsumerSpec, Envelope, PublishOutcome, StartPosition, StreamSpec,
};

/// Namespace that [`PriceEvidence::evidence_id`] owns. The literal is itself the invariant:
/// identities are published, deduplicated, and archived under it.
pub const PRICE_EVIDENCE_ID_PREFIX: &str = "priceev_";

/// Longest market symbol a venue may name.
pub const MAX_MARKET_SYMBOL_BYTES: usize = 32;

/// Longest text any one [`Provenance`] field may carry.
pub const MAX_PROVENANCE_FIELD_BYTES: usize = 256;

/// Largest encoded raw provider payload one message may carry. Raw evidence explains a quote; it
/// is not a place to tunnel a provider response body.
pub const MAX_RAW_EVIDENCE_BYTES: usize = 8 * 1024;

/// Largest fixed-point scale a quote may declare: the locked registry's own decimals ceiling, so
/// raising that ceiling cannot leave quotes rejected at the old scale.
pub const MAX_PRICE_SCALE: u8 = tackt_primitives::assets::MAX_DECIMALS;

/// How far a provider clock may run ahead of the Indexer before its timestamp is impossible
/// rather than merely skewed.
pub const MAX_SOURCE_CLOCK_SKEW_SECONDS: i64 = 60;

/// Earliest instant evidence may claim (`2020-01-01T00:00:00Z`). Anything older predates the
/// platform and means a misread provider field, not old data.
pub const MIN_EVIDENCE_UNIX_SECONDS: i64 = 1_577_836_800;

/// Latest instant evidence may claim (`2100-01-01T00:00:00Z`).
///
/// The window is deliberately absolute rather than relative to now: archived evidence must decode
/// to the same value forever, so validation may not consult a clock.
pub const MAX_EVIDENCE_UNIX_SECONDS: i64 = 4_102_444_800;

/// Size every encoded [`PriceEvidenceEnvelope`] stays under, given the field bounds above.
pub const MAX_ENCODED_EVIDENCE_BYTES: usize = 16 * 1024;

/// Typed envelope carrying one piece of price evidence.
pub type PriceEvidenceEnvelope = Envelope<PriceEvidence>;

/// A price source authority approved for this build.
///
/// Membership is closed and compiled in, not configured: an authority this build does not know is
/// not "unrecognized evidence", it is evidence from a source nobody approved, and it fails to
/// decode. Widening the set requires coordinated producer and consumer changes,
/// and the Indexer's own configured-authority check still decides which of these it may publish.
/// The concrete endpoint, method, and upstream identifier behind an authority live in
/// [`Provenance`].
///
/// Open, to settle when the approved price sources are implemented: this set is byte-identical on
/// the wire to [`tackt_primitives::Source`], which names account-observation provenance. Reusing
/// that type would widen observation provenance the moment a price-only authority is approved —
/// an oracle network quoting an asset it does not settle — while a free-form identifier would move
/// the approved list out of the contract, which is the one place it currently fails closed. The
/// duplication is deliberate until a real adapter forces the choice.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PriceAuthority {
    /// Ethereum-mainnet on-chain pricing.
    Ethereum,
    /// Hyperliquid venue pricing: `HyperCore` spot and perp mark prices.
    Hyperliquid,
    /// Binance spot-market pricing from the venue's public trade stream.
    ///
    /// Added (like [`Self::Kraken`]) under the module docs' recorded pre-consumer exception.
    Binance,
    /// Kraken spot-market pricing from the venue's public trade stream.
    Kraken,
    /// Bybit spot-market pricing from the venue's public trade stream.
    ///
    /// Added (like [`Self::Coinbase`]) under the module docs' recorded pre-consumer exception.
    Bybit,
    /// Coinbase Exchange spot-market pricing from the venue's public match stream.
    Coinbase,
}

impl PriceAuthority {
    /// Every configured authority.
    pub const ALL: [Self; 6] = [
        Self::Ethereum,
        Self::Hyperliquid,
        Self::Binance,
        Self::Kraken,
        Self::Bybit,
        Self::Coinbase,
    ];

    /// Name as it appears on the wire.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Hyperliquid => "hyperliquid",
            Self::Binance => "binance",
            Self::Kraken => "kraken",
            Self::Bybit => "bybit",
            Self::Coinbase => "coinbase",
        }
    }
}

/// A venue's own name for a market it quotes.
///
/// Case is preserved rather than normalized: venues ship symbols such as `kPEPE`, and folding
/// case would silently merge two markets into one identity.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(try_from = "String")]
pub struct MarketSymbol(String);

impl MarketSymbol {
    /// Validates one venue market symbol.
    ///
    /// # Errors
    ///
    /// Returns [`PriceEvidenceError::InvalidMarketSymbol`] unless the symbol is 1 to
    /// [`MAX_MARKET_SYMBOL_BYTES`] bytes, starts with an ASCII alphanumeric, and contains only
    /// ASCII alphanumerics and `-`, `_`, `.`, `:`.
    pub fn new(symbol: impl Into<String>) -> Result<Self, PriceEvidenceError> {
        let symbol = symbol.into();
        // Bound before scanning: the symbol arrives from the wire, so an oversized string must be
        // refused rather than walked end to end first.
        if symbol.len() > MAX_MARKET_SYMBOL_BYTES {
            return Err(PriceEvidenceError::InvalidMarketSymbol);
        }
        let starts_with_alphanumeric = symbol
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric());
        let every_byte_allowed = symbol
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'));
        if !starts_with_alphanumeric || !every_byte_allowed {
            return Err(PriceEvidenceError::InvalidMarketSymbol);
        }
        Ok(Self(symbol))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MarketSymbol {
    type Error = PriceEvidenceError;

    fn try_from(symbol: String) -> Result<Self, Self::Error> {
        Self::new(symbol)
    }
}

/// What a quote is a price *of*.
///
/// Registry assets are named by their accounting code, never by a restated chain, contract
/// address, or decimal count: those facts belong to the locked registry, and
/// A market symbol covers
/// what the registry does not describe — a venue's derivatives markets, whose mapping onto risk
/// inputs is the consumer's decision, not the Indexer's.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PriceSubject {
    /// A supported Release 1A registry asset.
    Asset(LedgerAsset),
    /// A market the quoting venue names itself.
    ///
    /// Open, to settle before the first publish: the venue is inferred from
    /// [`PriceEvidence::authority`], which holds only while every authority quotes its own venue.
    /// An aggregator or a relayed feed reporting another venue's market would give one market two
    /// identities, and no consumer could tell they are the same. A venue here is identity-bearing,
    /// so adding one later requires coordinated projection and archive changes; it is
    /// cheap only while no evidence exists.
    Market(MarketSymbol),
}

/// One fixed-point quote: `value * 10^-scale`.
///
/// The scale travels with the value because providers quote at different precisions and rounding
/// evidence to a single platform scale would destroy what the source actually said.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "PriceQuoteWire")]
pub struct PriceQuote {
    value: u128,
    scale: u8,
}

impl PriceQuote {
    /// Validates one quote.
    ///
    /// # Errors
    ///
    /// Returns [`PriceEvidenceError::NonPositivePrice`] for a zero value — no approved source
    /// quotes a free asset, so a zero is a parse failure wearing a number — and
    /// [`PriceEvidenceError::PriceScaleTooLarge`] above [`MAX_PRICE_SCALE`].
    pub fn new(value: u128, scale: u8) -> Result<Self, PriceEvidenceError> {
        if value == 0 {
            return Err(PriceEvidenceError::NonPositivePrice);
        }
        if scale > MAX_PRICE_SCALE {
            return Err(PriceEvidenceError::PriceScaleTooLarge { scale });
        }
        Ok(Self { value, scale })
    }

    /// Unscaled quote value.
    #[must_use]
    pub const fn value(self) -> u128 {
        self.value
    }

    /// Decimal places [`Self::value`] is expressed in.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Parses a venue decimal string (`"65000.10"`) into a quote with the exact value and scale
    /// the venue stated. No float ever touches the number, which is why every adapter must come
    /// through here rather than through a numeric type.
    ///
    /// # Errors
    ///
    /// Returns [`PriceEvidenceError::InvalidDecimalPrice`] for anything but a bounded, unsigned,
    /// plain decimal — including the exponent forms JSON permits, which no approved venue emits
    /// for a real price — and the [`Self::new`] errors for a zero value or an excessive scale.
    pub fn parse_decimal(text: &str) -> Result<Self, PriceEvidenceError> {
        if text.is_empty() || text.len() > 64 {
            return Err(PriceEvidenceError::InvalidDecimalPrice);
        }
        let (integral, fractional) = match text.split_once('.') {
            Some((integral, fractional)) => (integral, fractional),
            None => (text, ""),
        };
        if integral.is_empty() && fractional.is_empty() {
            return Err(PriceEvidenceError::InvalidDecimalPrice);
        }
        // The length bound above keeps the fractional digit count inside `u8`.
        let scale = fractional.len() as u8;
        let mut value: u128 = 0;
        for digit in integral.bytes().chain(fractional.bytes()) {
            if !digit.is_ascii_digit() {
                return Err(PriceEvidenceError::InvalidDecimalPrice);
            }
            value = value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u128::from(digit - b'0')))
                .ok_or(PriceEvidenceError::InvalidDecimalPrice)?;
        }
        Self::new(value, scale)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriceQuoteWire {
    value: u128,
    scale: u8,
}

impl TryFrom<PriceQuoteWire> for PriceQuote {
    type Error = PriceEvidenceError;

    fn try_from(wire: PriceQuoteWire) -> Result<Self, Self::Error> {
        Self::new(wire.value, wire.scale)
    }
}

/// One accepted price update, as evidence.
///
/// Fields are private and deserialization re-runs every check and re-derives the identity, so a
/// value of this type is well-formed evidence whichever direction it came from.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "PriceEvidenceWire")]
pub struct PriceEvidence {
    evidence_id: String,
    authority: PriceAuthority,
    subject: PriceSubject,
    price: PriceQuote,
    source_sequence: u64,
    source_timestamp: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    provenance: Provenance,
    raw: Value,
}

impl PriceEvidence {
    /// Builds and deterministically identifies one piece of price evidence.
    ///
    /// The typed arguments carry their own guarantees — [`PriceQuote`] is positive and within
    /// scale, [`MarketSymbol`] is
    /// bounded — so this checks what only the whole message can decide.
    ///
    /// # Errors
    ///
    /// Fails closed on a zero provider sequence, unbounded or blank provenance, raw evidence that
    /// is not a bounded JSON object, and timestamps outside the representable window or ordered
    /// impossibly against each other.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authority: PriceAuthority,
        subject: PriceSubject,
        price: PriceQuote,
        source_sequence: u64,
        source_timestamp: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        provenance: Provenance,
        raw: Value,
    ) -> Result<Self, PriceEvidenceError> {
        // A source with no sequence has no ordering, and a consumer cannot tell a stale
        // redelivery from a fresh quote. Adapters derive one (block height, round id, feed
        // sequence); zero means they did not.
        if source_sequence == 0 {
            return Err(PriceEvidenceError::ZeroSourceSequence);
        }
        validate_provenance(&provenance)?;
        validate_raw_evidence(&raw)?;
        validate_timestamps(source_timestamp, observed_at)?;
        let evidence_id = evidence_id(
            authority,
            &subject,
            price,
            source_sequence,
            source_timestamp,
        );
        Ok(Self {
            evidence_id,
            authority,
            subject,
            price,
            source_sequence,
            source_timestamp,
            observed_at,
            provenance,
            raw,
        })
    }

    /// Deterministic identity of the provider update behind this evidence.
    #[must_use]
    pub fn evidence_id(&self) -> &str {
        &self.evidence_id
    }

    /// Configured authority that reported the quote.
    #[must_use]
    pub const fn authority(&self) -> PriceAuthority {
        self.authority
    }

    /// What the quote prices.
    #[must_use]
    pub const fn subject(&self) -> &PriceSubject {
        &self.subject
    }

    /// The reported fixed-point quote.
    #[must_use]
    pub const fn price(&self) -> PriceQuote {
        self.price
    }

    /// Provider-assigned ordering of this update within its authority and subject.
    #[must_use]
    pub const fn source_sequence(&self) -> u64 {
        self.source_sequence
    }

    /// When the source says the quote was produced.
    #[must_use]
    pub const fn source_timestamp(&self) -> DateTime<Utc> {
        self.source_timestamp
    }

    /// When the Indexer accepted the quote.
    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    /// Endpoint, method, upstream identifier, and upstream schema behind the quote.
    #[must_use]
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// Bounded raw provider payload the quote was normalized from.
    #[must_use]
    pub const fn raw(&self) -> &Value {
        &self.raw
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriceEvidenceWire {
    evidence_id: String,
    authority: PriceAuthority,
    subject: PriceSubject,
    price: PriceQuote,
    source_sequence: u64,
    source_timestamp: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    provenance: Provenance,
    raw: Value,
}

impl TryFrom<PriceEvidenceWire> for PriceEvidence {
    type Error = PriceEvidenceError;

    fn try_from(wire: PriceEvidenceWire) -> Result<Self, Self::Error> {
        let evidence = Self::new(
            wire.authority,
            wire.subject,
            wire.price,
            wire.source_sequence,
            wire.source_timestamp,
            wire.observed_at,
            wire.provenance,
            wire.raw,
        )?;
        // The identity is derived, never accepted: a message whose id does not follow from its
        // own fields would give one update two identities and break every consumer's dedupe.
        if evidence.evidence_id != wire.evidence_id {
            return Err(PriceEvidenceError::EvidenceIdMismatch);
        }
        Ok(evidence)
    }
}

/// Price evidence that must not cross the durable boundary.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PriceEvidenceError {
    #[error("a quote value must be greater than zero")]
    NonPositivePrice,
    #[error("quote scale {scale} exceeds the maximum supported scale")]
    PriceScaleTooLarge { scale: u8 },
    #[error("a decimal price must be a bounded, unsigned, plain decimal")]
    InvalidDecimalPrice,
    #[error("a provider sequence must be greater than zero")]
    ZeroSourceSequence,
    #[error(
        "a market symbol must be bounded ASCII alphanumerics and -_.: starting with an \
         alphanumeric"
    )]
    InvalidMarketSymbol,
    #[error("provenance {field} must be bounded, non-blank, single-line text")]
    InvalidProvenance { field: &'static str },
    #[error("raw provider evidence must be a JSON object")]
    RawEvidenceNotAnObject,
    #[error("raw provider evidence is at least {at_least} bytes, above the supported maximum")]
    RawEvidenceTooLarge { at_least: usize },
    #[error("{field} is outside the representable evidence window")]
    TimestampOutOfRange { field: &'static str },
    #[error("the source timestamp runs further ahead of the observation than clock skew allows")]
    SourceTimestampAfterObservation,
    #[error("evidence_id is inconsistent with the evidence fields")]
    EvidenceIdMismatch,
}

/// A durable consumer of the price-evidence stream.
///
/// Every consumer reads the whole stream independently: the stream retains to its limits rather
/// than draining on the first ack, so risk, the projection, and the archive never compete for a
/// message and one stalled consumer never starves the others. The variants exist here, in the
/// crate that owns the contract, because a durable name is part of the wire: two deployments that
/// disagree about it silently fork into two consumers, each seeing half the evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PriceEvidenceConsumer {
    /// The Risk Engine, deciding whether evidence may move risk state.
    Risk,
    /// The projector maintaining the latest-state NATS KV bucket.
    KvProjector,
    /// The sink archiving immutable evidence into ClickHouse.
    Archive,
}

impl PriceEvidenceConsumer {
    /// Every durable consumer of the stream.
    pub const ALL: [Self; 3] = [Self::Risk, Self::KvProjector, Self::Archive];

    /// The durable name. Stable for the life of the deployment: renaming one resets its position
    /// to the start of the retained stream and replays everything.
    #[must_use]
    pub const fn durable_name(self) -> &'static str {
        match self {
            Self::Risk => "price-evidence-risk",
            Self::KvProjector => "price-evidence-kv",
            Self::Archive => "price-evidence-archive",
        }
    }

    /// How long the server waits for this role's acknowledgement before redelivering.
    ///
    /// Sized to the slowest legitimate unit of work, because expiring early does not recover a
    /// stuck consumer — it redelivers a message that is still being processed. The projector
    /// performs one compare-and-swap; the archive acknowledges only once ClickHouse has confirmed
    /// a batch; risk hands the message to a single-threaded engine that may be mid-snapshot.
    #[must_use]
    pub const fn ack_wait(self) -> Duration {
        match self {
            Self::Risk | Self::Archive => Duration::from_secs(5 * 60),
            Self::KvProjector => Duration::from_secs(30),
        }
    }

    /// The consumer specification: durable, replaying everything the stream still retains, with
    /// unlimited redelivery.
    ///
    /// `DeliverAll` is right for all three roles. The archive must never skip evidence; the
    /// projector converges on the latest sequence per key whatever order it replays in; risk
    /// re-derives its own state and rejects stale evidence on policy grounds, not by never having
    /// seen it.
    #[must_use]
    pub fn spec(self) -> ConsumerSpec {
        ConsumerSpec {
            max_ack_pending: None,
            durable_name: Some(self.durable_name().to_string()),
            ack_wait: self.ack_wait(),
            // No delivery ceiling: this stream is the only durable copy of the evidence until the
            // archive holds it, so a message must leave a consumer's view by an explicit decision
            // — ack or term — and never by exhausting a retry count nobody observed.
            max_deliveries: None,
            start: StartPosition::DeliverAll,
        }
    }

    /// How long to wait before redelivery when a consumer NAKs a transient failure, given the
    /// delivery count of the attempt that just failed ([`crate::Delivery::delivery_count`], 1 on
    /// first delivery).
    ///
    /// The ladder retries quickly at first, because most failures are a dependency that is already
    /// coming back, then flattens at five minutes and stays there. A downstream outage lasting the
    /// stream's whole retention window therefore costs one redelivery per message per five minutes
    /// instead of a hot loop, and the backlog is bounded by retention rather than by a retry
    /// budget: nothing is dropped for having been retried too often.
    #[must_use]
    pub const fn retry_delay(delivery_count: i64) -> Duration {
        match delivery_count {
            ..=1 => Duration::from_secs(1),
            2 => Duration::from_secs(5),
            3 => Duration::from_secs(30),
            4 => Duration::from_secs(2 * 60),
            _ => Duration::from_secs(5 * 60),
        }
    }
}

impl NatsClient {
    /// Publish one piece of evidence and wait for JetStream to acknowledge it as durable.
    ///
    /// [`PriceEvidence::evidence_id`] is the `Nats-Msg-Id`, so a publish retried after an
    /// ambiguous failure — and a provider that re-serves one update — resolves to the message
    /// already in the stream rather than a second copy of it. Returning
    /// [`PublishOutcome::Duplicate`] means exactly that: the evidence is durable and this call did
    /// not add anything.
    ///
    /// An error means nothing was stored. There is no core-NATS fallback and no database write to
    /// fall back to: the caller still owns the update and must fail closed.
    ///
    /// `stream` is [`crate::subjects::price_evidence_stream`] in production; tests pass uniquely
    /// named streams for isolation.
    pub async fn publish_price_evidence(
        &self,
        stream: &StreamSpec,
        evidence: &PriceEvidence,
    ) -> anyhow::Result<PublishOutcome> {
        // A borrowed envelope serializes byte-identically to the owned alias without cloning
        // the evidence on the per-trade publish path.
        let envelope = Envelope::<&PriceEvidence>::new(evidence);
        self.publish_durable(stream.subject(), Some(evidence.evidence_id()), &envelope)
            .await
    }

    /// Bind one role's durable consumer, requiring the provisioned stream first.
    ///
    /// Deliveries are at-least-once. A consumer acknowledges only after its own durable effect is
    /// complete, NAKs a transient failure with [`PriceEvidenceConsumer::retry_delay`], and
    /// terminates a message only when this build can never process it — the payload does not
    /// decode, which [`crate::Consumer::next`] handles before the message is returned.
    pub async fn price_evidence(
        &self,
        stream: &StreamSpec,
        consumer: PriceEvidenceConsumer,
    ) -> anyhow::Result<Consumer<PriceEvidenceEnvelope>> {
        self.consumer(stream, consumer.spec()).await
    }
}

/// Open, to settle when adapters populate this: [`Provenance::endpoint`] is copied from a
/// configured source URL, and such a URL can carry its credential in userinfo, a query parameter,
/// or a path segment. Price evidence is durable, replayed, and archived, so a credential that
/// reaches this field becomes a credential in a permanent domain record. The contract-level fix is
/// to accept only scheme, host, and port here — [`Provenance::method`] already carries the "what" —
/// which these checks do not yet enforce.
fn validate_provenance(provenance: &Provenance) -> Result<(), PriceEvidenceError> {
    for (field, value) in [
        ("endpoint", &provenance.endpoint),
        ("method", &provenance.method),
        ("source_id", &provenance.source_id),
    ] {
        if !valid_display_text(value, MAX_PROVENANCE_FIELD_BYTES) {
            return Err(PriceEvidenceError::InvalidProvenance { field });
        }
    }
    Ok(())
}

fn validate_raw_evidence(raw: &Value) -> Result<(), PriceEvidenceError> {
    if !raw.is_object() {
        return Err(PriceEvidenceError::RawEvidenceNotAnObject);
    }
    let mut encoded = EncodedLength {
        count: 0,
        limit: MAX_RAW_EVIDENCE_BYTES,
    };
    // A `Value` cannot hold a non-serializable number, so the only way encoding fails here is the
    // counter refusing the byte that crossed the bound. Either way the payload does not travel.
    if serde_json::to_writer(&mut encoded, raw).is_err() {
        return Err(PriceEvidenceError::RawEvidenceTooLarge {
            at_least: encoded.count,
        });
    }
    Ok(())
}

/// Measures a value's encoded size without materializing it, and fails the encoder as soon as the
/// bound is crossed.
///
/// Raw evidence is provider-supplied and only bounded by the broker's payload limit before it
/// reaches this check, so an oversized payload must be refused without first allocating a second
/// copy of it.
struct EncodedLength {
    count: usize,
    limit: usize,
}

impl std::io::Write for EncodedLength {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.count += buffer.len();
        if self.count > self.limit {
            return Err(std::io::Error::other(
                "encoded value crossed its size bound",
            ));
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_timestamps(
    source_timestamp: DateTime<Utc>,
    observed_at: DateTime<Utc>,
) -> Result<(), PriceEvidenceError> {
    for (field, value) in [
        ("source_timestamp", source_timestamp),
        ("observed_at", observed_at),
    ] {
        let seconds = value.timestamp();
        if !(MIN_EVIDENCE_UNIX_SECONDS..=MAX_EVIDENCE_UNIX_SECONDS).contains(&seconds) {
            return Err(PriceEvidenceError::TimestampOutOfRange { field });
        }
    }
    // Evidence cannot be observed before it exists. Provider clocks do drift ahead of ours, so
    // the rule tolerates bounded skew and rejects only the impossible.
    if source_timestamp.timestamp() - observed_at.timestamp() > MAX_SOURCE_CLOCK_SKEW_SECONDS {
        return Err(PriceEvidenceError::SourceTimestampAfterObservation);
    }
    Ok(())
}

/// Derives the identity of the provider update this evidence reports.
///
/// Each variable-length part is length-prefixed, so no two distinct part vectors can hash alike
/// regardless of the characters inside them.
fn evidence_id(
    authority: PriceAuthority,
    subject: &PriceSubject,
    price: PriceQuote,
    source_sequence: u64,
    source_timestamp: DateTime<Utc>,
) -> String {
    fn update_text(digest: &mut Sha256, value: &str) {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }

    let mut digest = Sha256::new();
    digest.update(b"tackt-price-evidence");
    update_text(&mut digest, authority.as_str());
    match subject {
        PriceSubject::Asset(asset) => {
            digest.update([0]);
            update_text(&mut digest, asset.code());
        }
        PriceSubject::Market(symbol) => {
            digest.update([1]);
            update_text(&mut digest, symbol.as_str());
        }
    }
    digest.update(price.value().to_be_bytes());
    digest.update([price.scale()]);
    digest.update(source_sequence.to_be_bytes());
    digest.update(source_timestamp.timestamp().to_be_bytes());
    digest.update(source_timestamp.timestamp_subsec_nanos().to_be_bytes());
    format!(
        "{PRICE_EVIDENCE_ID_PREFIX}{}",
        hex::encode(digest.finalize())
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }

    fn provenance() -> Provenance {
        Provenance {
            endpoint: "https://api.hyperliquid.xyz/info".to_string(),
            method: "allMids".to_string(),
            source_id: "mids:2181".to_string(),
        }
    }

    /// Valid evidence varying only the axis a test is about; every other fact is pinned.
    fn minimal(
        source_timestamp: i64,
        observed_at: i64,
        provenance: Provenance,
        raw: Value,
    ) -> Result<PriceEvidence, PriceEvidenceError> {
        PriceEvidence::new(
            PriceAuthority::Ethereum,
            PriceSubject::Asset(LedgerAsset::Usdc),
            PriceQuote::new(1, 8).expect("quote"),
            1,
            at(source_timestamp),
            at(observed_at),
            provenance,
            raw,
        )
    }

    fn evidence() -> PriceEvidence {
        PriceEvidence::new(
            PriceAuthority::Hyperliquid,
            PriceSubject::Market(MarketSymbol::new("BTC").expect("symbol")),
            PriceQuote::new(6_500_000_000_000, 8).expect("quote"),
            2181,
            at(1_800_000_000),
            at(1_800_000_001),
            provenance(),
            json!({ "mid": "65000.0" }),
        )
        .expect("evidence")
    }

    #[test]
    fn evidence_wire_shape_is_stable() {
        let evidence = evidence();
        let envelope = PriceEvidenceEnvelope::new(evidence.clone());
        let wire = serde_json::to_value(&envelope).expect("serialize");

        let payload = &wire["payload"];
        assert_eq!(payload["authority"], "hyperliquid");
        assert_eq!(payload["subject"], json!({ "kind": "market", "id": "BTC" }));
        assert_eq!(
            payload["price"],
            json!({ "value": 6_500_000_000_000_u64, "scale": 8 })
        );
        assert_eq!(payload["source_sequence"], 2181);
        assert_eq!(payload["source_timestamp"], "2027-01-15T08:00:00Z");
        assert_eq!(payload["observed_at"], "2027-01-15T08:00:01Z");
        assert_eq!(payload["provenance"]["method"], "allMids");
        assert_eq!(payload["raw"], json!({ "mid": "65000.0" }));
        assert!(payload["evidence_id"].as_str().is_some_and(|id| {
            id.strip_prefix(PRICE_EVIDENCE_ID_PREFIX)
                .is_some_and(|hex| hex.len() == 64)
        }));

        let decoded: PriceEvidenceEnvelope = serde_json::from_value(wire).expect("deserialize");
        assert_eq!(decoded.payload, evidence);
    }

    #[test]
    fn identity_is_stable_across_retries_of_one_provider_update() {
        let first = evidence();
        // A retry: observed later, through a different endpoint, with an incidental difference
        // in the raw payload. Same provider update, so the same identity.
        let retry = PriceEvidence::new(
            first.authority(),
            first.subject().clone(),
            first.price(),
            first.source_sequence(),
            first.source_timestamp(),
            at(1_800_000_045),
            Provenance {
                endpoint: "https://api-backup.hyperliquid.xyz/info".to_string(),
                ..provenance()
            },
            json!({ "mid": "65000.0", "served_at": 1_800_000_045_u64 }),
        )
        .expect("retry");

        assert_eq!(retry.evidence_id(), first.evidence_id());
        assert_ne!(retry, first);
    }

    #[test]
    fn identity_separates_every_fact_that_defines_an_update() {
        let base = evidence();
        let identified = |authority, subject, price, source_sequence, source_timestamp| {
            PriceEvidence::new(
                authority,
                subject,
                price,
                source_sequence,
                source_timestamp,
                base.observed_at(),
                base.provenance().clone(),
                base.raw().clone(),
            )
            .expect("variant")
        };
        let variants = [
            identified(
                PriceAuthority::Ethereum,
                base.subject().clone(),
                base.price(),
                base.source_sequence(),
                base.source_timestamp(),
            ),
            identified(
                base.authority(),
                PriceSubject::Asset(LedgerAsset::Btc),
                base.price(),
                base.source_sequence(),
                base.source_timestamp(),
            ),
            identified(
                base.authority(),
                base.subject().clone(),
                PriceQuote::new(base.price().value() + 1, base.price().scale()).expect("quote"),
                base.source_sequence(),
                base.source_timestamp(),
            ),
            identified(
                base.authority(),
                base.subject().clone(),
                PriceQuote::new(base.price().value(), base.price().scale() + 1).expect("quote"),
                base.source_sequence(),
                base.source_timestamp(),
            ),
            identified(
                base.authority(),
                base.subject().clone(),
                base.price(),
                base.source_sequence() + 1,
                base.source_timestamp(),
            ),
            identified(
                base.authority(),
                base.subject().clone(),
                base.price(),
                base.source_sequence(),
                base.source_timestamp() + chrono::TimeDelta::seconds(1),
            ),
        ];

        for variant in variants {
            assert_ne!(variant.evidence_id(), base.evidence_id());
        }
    }

    #[test]
    fn a_market_symbol_that_only_differs_in_case_is_its_own_market() {
        let lower = MarketSymbol::new("kPEPE").expect("symbol");
        let upper = MarketSymbol::new("KPEPE").expect("symbol");
        assert_ne!(lower, upper);
        assert_eq!(lower.as_str(), "kPEPE");
    }

    #[test]
    fn market_symbols_are_bounded_and_charset_checked() {
        for valid in ["BTC", "kPEPE", "BTC-PERP", "spot:USDC", "eth.usd_1"] {
            MarketSymbol::new(valid).unwrap_or_else(|_| panic!("{valid} is valid"));
        }
        for invalid in [
            "",
            "-BTC",
            ":BTC",
            "BTC PERP",
            "BTC\n",
            "BTC/USD",
            &"B".repeat(MAX_MARKET_SYMBOL_BYTES + 1),
        ] {
            assert_eq!(
                MarketSymbol::new(invalid),
                Err(PriceEvidenceError::InvalidMarketSymbol),
                "{invalid:?}"
            );
        }
        MarketSymbol::new("B".repeat(MAX_MARKET_SYMBOL_BYTES)).expect("at the bound");
    }

    #[test]
    fn decimal_strings_parse_at_the_venue_scale() {
        let quote = |text: &str| PriceQuote::parse_decimal(text).expect(text);
        assert_eq!(
            (quote("65000.10").value(), quote("65000.10").scale()),
            (6_500_010, 2)
        );
        assert_eq!(
            (quote("65000").value(), quote("65000").scale()),
            (65_000, 0)
        );
        assert_eq!((quote(".5").value(), quote(".5").scale()), (5, 1));
        assert_eq!((quote("5.").value(), quote("5.").scale()), (5, 0));
        for invalid in ["", ".", "1.2.3", " 1", "+1", "-1", "1e3", "NaN"] {
            assert_eq!(
                PriceQuote::parse_decimal(invalid),
                Err(PriceEvidenceError::InvalidDecimalPrice),
                "{invalid:?}"
            );
        }
        assert_eq!(
            PriceQuote::parse_decimal("0.000"),
            Err(PriceEvidenceError::NonPositivePrice)
        );
        assert!(matches!(
            PriceQuote::parse_decimal(&format!("0.{}1", "0".repeat(38))),
            Err(PriceEvidenceError::PriceScaleTooLarge { .. })
        ));
        assert_eq!(
            PriceQuote::parse_decimal(&"9".repeat(40)),
            Err(PriceEvidenceError::InvalidDecimalPrice),
            "u128 overflow fails closed"
        );
    }

    #[test]
    fn malformed_quotes_fail_closed() {
        assert_eq!(
            PriceQuote::new(0, 8),
            Err(PriceEvidenceError::NonPositivePrice)
        );
        assert_eq!(
            PriceQuote::new(1, MAX_PRICE_SCALE + 1),
            Err(PriceEvidenceError::PriceScaleTooLarge {
                scale: MAX_PRICE_SCALE + 1
            })
        );
        PriceQuote::new(1, MAX_PRICE_SCALE).expect("at the bound");
    }

    #[test]
    fn an_unknown_authority_or_asset_fails_closed() {
        for (field, value) in [
            ("authority", json!("chainlink")),
            ("subject", json!({ "kind": "asset", "id": "DOGE" })),
            ("subject", json!({ "kind": "venue", "id": "BTC" })),
        ] {
            let mut wire = serde_json::to_value(evidence()).expect("serialize");
            wire[field] = value.clone();
            assert!(
                serde_json::from_value::<PriceEvidence>(wire).is_err(),
                "{field} accepted {value}"
            );
        }
    }

    #[test]
    fn a_zero_provider_sequence_fails_closed() {
        assert_eq!(
            PriceEvidence::new(
                PriceAuthority::Ethereum,
                PriceSubject::Asset(LedgerAsset::Usdc),
                PriceQuote::new(1, 8).expect("quote"),
                0,
                at(1_800_000_000),
                at(1_800_000_000),
                provenance(),
                json!({}),
            ),
            Err(PriceEvidenceError::ZeroSourceSequence)
        );
    }

    #[test]
    fn impossible_timestamps_fail_closed() {
        let window = |source, observed| minimal(source, observed, provenance(), json!({}));

        assert_eq!(
            window(MIN_EVIDENCE_UNIX_SECONDS - 1, 1_800_000_000),
            Err(PriceEvidenceError::TimestampOutOfRange {
                field: "source_timestamp"
            })
        );
        assert_eq!(
            window(1_800_000_000, MAX_EVIDENCE_UNIX_SECONDS + 1),
            Err(PriceEvidenceError::TimestampOutOfRange {
                field: "observed_at"
            })
        );
        assert_eq!(
            window(
                1_800_000_000 + MAX_SOURCE_CLOCK_SKEW_SECONDS + 1,
                1_800_000_000
            ),
            Err(PriceEvidenceError::SourceTimestampAfterObservation)
        );
        // Bounded skew is tolerated: provider clocks genuinely run ahead.
        window(1_800_000_000 + MAX_SOURCE_CLOCK_SKEW_SECONDS, 1_800_000_000)
            .expect("skew inside the tolerance");
        // Both documented instants are inside the window.
        window(MIN_EVIDENCE_UNIX_SECONDS, MIN_EVIDENCE_UNIX_SECONDS).expect("at the floor");
        window(MAX_EVIDENCE_UNIX_SECONDS, MAX_EVIDENCE_UNIX_SECONDS).expect("at the ceiling");
    }

    #[test]
    fn the_evidence_window_names_the_documented_instants() {
        assert_eq!(
            at(MIN_EVIDENCE_UNIX_SECONDS).to_rfc3339(),
            "2020-01-01T00:00:00+00:00"
        );
        assert_eq!(
            at(MAX_EVIDENCE_UNIX_SECONDS).to_rfc3339(),
            "2100-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn unbounded_or_blank_provenance_fails_closed() {
        let attributed = |provenance| minimal(1_800_000_000, 1_800_000_000, provenance, json!({}));

        for (field, invalid) in [
            (
                "endpoint",
                Provenance {
                    endpoint: "   ".to_string(),
                    ..provenance()
                },
            ),
            (
                "method",
                Provenance {
                    method: "all\nMids".to_string(),
                    ..provenance()
                },
            ),
            (
                "source_id",
                Provenance {
                    source_id: "s".repeat(MAX_PROVENANCE_FIELD_BYTES + 1),
                    ..provenance()
                },
            ),
        ] {
            assert_eq!(
                attributed(invalid),
                Err(PriceEvidenceError::InvalidProvenance { field })
            );
        }
    }

    #[test]
    fn raw_evidence_is_a_bounded_object() {
        let carrying = |raw| minimal(1_800_000_000, 1_800_000_000, provenance(), raw);

        assert_eq!(
            carrying(json!("65000.0")),
            Err(PriceEvidenceError::RawEvidenceNotAnObject)
        );
        assert_eq!(
            carrying(Value::Null),
            Err(PriceEvidenceError::RawEvidenceNotAnObject)
        );
        assert!(matches!(
            carrying(json!({ "body": "x".repeat(MAX_RAW_EVIDENCE_BYTES) })),
            Err(PriceEvidenceError::RawEvidenceTooLarge { .. })
        ));
        carrying(json!({ "body": "x".repeat(MAX_RAW_EVIDENCE_BYTES - 32) }))
            .expect("inside the bound");
    }

    #[test]
    fn the_raw_evidence_bound_is_exact() {
        // The counter stops the encoder mid-stream, so pin the cutoff against a full encode:
        // measuring differently must not move where evidence starts being refused.
        let encoded_len = |raw: &Value| serde_json::to_vec(raw).expect("encode").len();
        let carrying = |raw| minimal(1_800_000_000, 1_800_000_000, provenance(), raw);

        // `{"body":"` + n + `"}` is n + 11 bytes.
        let at_bound = json!({ "body": "x".repeat(MAX_RAW_EVIDENCE_BYTES - 11) });
        assert_eq!(encoded_len(&at_bound), MAX_RAW_EVIDENCE_BYTES);
        carrying(at_bound).expect("exactly at the bound");

        let over_bound = json!({ "body": "x".repeat(MAX_RAW_EVIDENCE_BYTES - 10) });
        assert_eq!(encoded_len(&over_bound), MAX_RAW_EVIDENCE_BYTES + 1);
        assert!(matches!(
            carrying(over_bound),
            Err(PriceEvidenceError::RawEvidenceTooLarge { at_least })
                if at_least > MAX_RAW_EVIDENCE_BYTES
        ));
    }

    #[test]
    fn a_forged_identity_is_refused() {
        let mut wire = serde_json::to_value(evidence()).expect("serialize");
        wire["evidence_id"] = json!(format!("{PRICE_EVIDENCE_ID_PREFIX}{}", "0".repeat(64)));
        assert_eq!(
            serde_json::from_value::<PriceEvidence>(wire).map_err(|error| error.to_string()),
            Err(PriceEvidenceError::EvidenceIdMismatch.to_string())
        );
    }

    #[test]
    fn unknown_fields_are_refused() {
        let mut wire = serde_json::to_value(evidence()).expect("serialize");
        wire["unexpected_field"] = json!(1);
        assert!(serde_json::from_value::<PriceEvidence>(wire).is_err());
    }

    #[test]
    fn the_executable_risk_price_shape_is_not_price_evidence() {
        // The Risk Engine's `PriceUpdate` carries registry indexes and source ordering, with
        // no evidence identity. Its payload must never decode here.
        let executable = json!({
            "price_index": 1,
            "source": "ethereum",
            "source_sequence": 7,
            "price": 6_500_000_000_000_u64,
        });
        assert!(serde_json::from_value::<PriceEvidence>(executable).is_err());

        // And nothing in price evidence pretends to be executable.
        let wire = serde_json::to_value(evidence()).expect("serialize");
        for executable_only in ["price_index", "source"] {
            assert!(wire.get(executable_only).is_none(), "{executable_only}");
        }
    }

    #[test]
    fn a_maximal_envelope_stays_inside_the_documented_size_bound() {
        let evidence = PriceEvidence::new(
            PriceAuthority::Hyperliquid,
            PriceSubject::Market(
                MarketSymbol::new("M".repeat(MAX_MARKET_SYMBOL_BYTES)).expect("symbol"),
            ),
            PriceQuote::new(u128::MAX, MAX_PRICE_SCALE).expect("quote"),
            u64::MAX,
            at(MAX_EVIDENCE_UNIX_SECONDS - 1),
            at(MAX_EVIDENCE_UNIX_SECONDS - 1),
            Provenance {
                endpoint: "e".repeat(MAX_PROVENANCE_FIELD_BYTES),
                method: "m".repeat(MAX_PROVENANCE_FIELD_BYTES),
                source_id: "s".repeat(MAX_PROVENANCE_FIELD_BYTES),
            },
            json!({ "body": "x".repeat(MAX_RAW_EVIDENCE_BYTES - 16) }),
        )
        .expect("maximal evidence");

        let encoded = serde_json::to_vec(&PriceEvidenceEnvelope::new(evidence)).expect("serialize");
        assert!(
            encoded.len() <= MAX_ENCODED_EVIDENCE_BYTES,
            "{} bytes",
            encoded.len()
        );
    }

    #[test]
    fn every_authority_names_itself_on_the_wire() {
        for authority in PriceAuthority::ALL {
            let wire = serde_json::to_value(authority).expect("serialize");
            assert_eq!(wire, json!(authority.as_str()));
            assert_eq!(
                serde_json::from_value::<PriceAuthority>(wire).expect("deserialize"),
                authority
            );
        }
    }

    #[test]
    fn every_durable_consumer_has_its_own_name() {
        let names: BTreeSet<&str> = PriceEvidenceConsumer::ALL
            .iter()
            .map(|consumer| consumer.durable_name())
            .collect();
        assert_eq!(names.len(), PriceEvidenceConsumer::ALL.len());
        // Names are wire, not labels: a rename replays the whole retained
        // stream into a consumer that believes it has seen nothing.
        assert_eq!(
            PriceEvidenceConsumer::Risk.durable_name(),
            "price-evidence-risk"
        );
        assert_eq!(
            PriceEvidenceConsumer::KvProjector.durable_name(),
            "price-evidence-kv"
        );
        assert_eq!(
            PriceEvidenceConsumer::Archive.durable_name(),
            "price-evidence-archive"
        );
        for name in names {
            // NATS rejects these in a durable name; a panic at bind time would
            // be a deployment failure rather than a test failure.
            assert!(
                !name.contains(['.', '*', '>', ' ']),
                "{name} is not a usable durable name"
            );
        }
    }

    #[test]
    fn no_consumer_may_drop_a_message_it_never_decided_about() {
        for consumer in PriceEvidenceConsumer::ALL {
            let spec = consumer.spec();
            assert_eq!(spec.durable_name.as_deref(), Some(consumer.durable_name()));
            // Unlimited redelivery and a full replay: until ClickHouse holds
            // the evidence, this stream is the only copy of it.
            assert_eq!(spec.max_deliveries, None, "{consumer:?}");
            assert_eq!(spec.start, StartPosition::DeliverAll, "{consumer:?}");
            assert!(!spec.ack_wait.is_zero(), "{consumer:?}");
        }
    }

    #[test]
    fn retry_backoff_widens_and_then_holds() {
        let delays: Vec<Duration> = (1..=6).map(PriceEvidenceConsumer::retry_delay).collect();
        for pair in delays.windows(2) {
            assert!(pair[1] >= pair[0], "backoff must never shorten: {delays:?}");
        }
        assert_eq!(
            PriceEvidenceConsumer::retry_delay(1),
            Duration::from_secs(1)
        );
        // Flat tail: an outage lasting the stream's retention costs one
        // redelivery per message per five minutes, not a hot loop.
        assert_eq!(
            PriceEvidenceConsumer::retry_delay(9_999),
            Duration::from_secs(5 * 60)
        );
        // A delivery count below the documented floor is still a real
        // redelivery request, never a zero-delay spin.
        assert_eq!(
            PriceEvidenceConsumer::retry_delay(0),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn no_retry_outlasts_the_stream_that_holds_the_message() {
        // A backoff approaching retention would schedule an attempt against a
        // message the stream has already expired: a consumer would wait out an
        // outage only to find the evidence gone. The ladder stays two orders of
        // magnitude inside it, so a message is retried many times over before
        // its age is even a question.
        let longest = PriceEvidenceConsumer::retry_delay(i64::MAX);
        assert!(longest * 100 < crate::subjects::PRICE_EVIDENCE_RETENTION);
    }

    #[test]
    fn the_evidence_id_is_a_usable_message_id() {
        let evidence = evidence();
        // publish_price_evidence stamps this as Nats-Msg-Id, and publish_durable
        // rejects an id the header map would panic on.
        assert!(tackt_primitives::ledger::valid_idempotency_key(
            evidence.evidence_id()
        ));
        assert!(evidence.evidence_id().starts_with(PRICE_EVIDENCE_ID_PREFIX));
    }
}
