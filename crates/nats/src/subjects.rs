//! Every subject and stream this workspace publishes or consumes.
//!
//! Per the repository instructions, this module is the complete answer to
//! "what goes over NATS": subject constants and builders plus the
//! [`StreamSpec`] factory for every JetStream stream.

use std::time::Duration;

use tackt_primitives::Source;

use crate::jetstream::{Discard, Retention, StreamSpec};

const MIB: i64 = 1024 * 1024;
const GIB: i64 = 1024 * MIB;
const DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

/// Account-observation subject filter covering every source (durable JetStream,
/// [`OBSERVATIONS_STREAM`]).
pub const OBSERVATIONS_WILDCARD: &str = "tackt.observations.>";

/// Subject for one source's observation feed.
pub fn observations(source: Source) -> String {
    format!("tackt.observations.{}", source.as_str())
}

/// Subject for one EVM chain's observation feed.
///
/// Ethereum mainnet keeps the unsuffixed [`observations`] subject: durable
/// history and the indexer's restart resume point already live there, and
/// moving them would orphan the resume lookup at deploy. Every other EVM
/// chain publishes under a `.{chain_id}` suffix, still inside
/// [`OBSERVATIONS_WILDCARD`], so stream-wide consumers see all chains
/// without reconfiguration while per-chain last-message lookups stay exact.
pub fn evm_observations(chain_id: u64) -> String {
    if chain_id == 1 {
        observations(Source::Ethereum)
    } else {
        format!(
            "tackt.observations.{}.{chain_id}",
            Source::Ethereum.as_str()
        )
    }
}

/// Durable stream carrying every `tackt.observations.{source}` subject.
pub const OBSERVATIONS_STREAM: &str = "TACKT_OBSERVATIONS";

/// How long one observation identity is deduplicated by the broker.
///
/// Wide enough to absorb the Indexer's own bounded replays: a restart re-reads and
/// republishes its most recent Ethereum block, and a reconnect replays the recovery
/// overlap, all under the same deterministic event ids. Consumers still deduplicate on
/// `Observation::event_id`, which covers a repeat arriving after the window has passed.
pub const OBSERVATIONS_DUPLICATE_WINDOW: Duration = Duration::from_secs(10 * 60);

/// How long observations stay replayable, and the outage a stalled downstream consumer
/// may take before its backlog is unrecoverable from the stream alone.
pub const OBSERVATIONS_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Account-observation stream: the Indexer's durable handoff for external-account
/// evidence, mirroring the fail-closed posture of [`price_evidence_stream`].
///
/// The Indexer does not write observations to PostgreSQL: the acknowledged publish is
/// its only durable side effect for an observation, so nothing behind this stream can be
/// reconciled from a database and both limits fail closed rather than save space.
///
/// - **Retention `Limits`**: multiple independent consumers each read every message, so
///   a work queue, which deletes on the first ack, would be wrong.
/// - **[`OBSERVATIONS_RETENTION`] age**: covers a long-weekend outage of any one
///   consumer plus a working day to notice and drain it.
/// - **8 GiB capacity with [`Discard::New`]**: sized so age, not capacity, expires
///   observations under expected load; reaching it anyway means volume nobody planned
///   for, and the stream then rejects the publish — the Indexer fails closed instead of
///   the broker discarding evidence no consumer has persisted.
pub fn observations_stream() -> StreamSpec {
    StreamSpec {
        name: OBSERVATIONS_STREAM.to_string(),
        subjects: vec![OBSERVATIONS_WILDCARD.to_string()],
        retention: Retention::Limits,
        max_age: Some(OBSERVATIONS_RETENTION),
        max_messages: None,
        max_bytes: Some(8 * GIB),
        discard: Discard::New,
        duplicate_window: OBSERVATIONS_DUPLICATE_WINDOW,
    }
}

/// User created (JetStream, TACKT_USERS).
pub const USERS_CREATED: &str = "tackt.users.created";
/// User status transition (JetStream, TACKT_USERS).
pub const USERS_STATUS_CHANGED: &str = "tackt.users.status_changed";
/// User account association (JetStream, TACKT_USERS).
pub const USERS_ACCOUNT_ASSOCIATED: &str = "tackt.users.account_associated";
/// Subject filter covering every user-lifecycle event.
pub const USERS_WILDCARD: &str = "tackt.users.>";

/// Normalized price evidence from approved price sources (durable JetStream,
/// [`PRICE_EVIDENCE_STREAM`]).
///
/// Deliberately outside the `tackt.risk.*` family: this subject carries the Indexer's evidence
/// for many consumers — risk, the NATS KV latest-state projection, the ClickHouse archive — not
/// the Risk Engine's executable price ingress on [`RiskStreams::prices`].
///
/// Reached only through [`crate::NatsClient::publish_price_evidence`], which stamps
/// [`crate::prices::PriceEvidence::evidence_id`] as the message id. There is no core-NATS path
/// to this subject: core NATS neither acknowledges nor deduplicates, and price evidence has no
/// second durable copy to reconcile against.
pub const PRICE_EVIDENCE: &str = "tackt.prices.evidence";

/// Durable stream carrying [`PRICE_EVIDENCE`].
pub const PRICE_EVIDENCE_STREAM: &str = "TACKT_PRICE_EVIDENCE";

/// How long one price-evidence identity is deduplicated by the broker.
///
/// Wider than the platform default because it absorbs a stuck provider as well as a retried
/// publish: a feed that keeps re-serving one sequence produces one identity, and every repeat
/// inside this window costs the stream nothing. Consumers still deduplicate on
/// [`crate::prices::PriceEvidence::evidence_id`], which is what covers a repeat that arrives
/// after the window has passed.
pub const PRICE_EVIDENCE_DUPLICATE_WINDOW: Duration = Duration::from_secs(10 * 60);

/// How long price evidence stays replayable, and the outage a stalled downstream consumer may
/// take before its backlog is unrecoverable from the stream alone.
pub const PRICE_EVIDENCE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Price-evidence stream: the Indexer's durable handoff and the replay boundary for every
/// downstream consumer.
///
/// Unlike the user and observation feeds, nothing behind this stream can be reconciled from a
/// database — the acknowledged publish *is* the durable record until the ClickHouse archive has
/// it — so both limits are chosen to fail closed rather than to save space.
///
/// - **Retention `Limits`**: three independent consumers (risk, KV projector, archive) each read
///   every message, so a work queue, which deletes on the first ack, would be wrong.
/// - **[`PRICE_EVIDENCE_RETENTION`] age**: covers a long-weekend outage of any one consumer plus
///   a working day to notice and drain it. Age expiry is the normal way evidence leaves the
///   stream and is backed by consumer-lag alerting, not by this limit.
/// - **4 GiB capacity with [`Discard::New`]**: sized so age, not capacity, is what expires
///   evidence under expected load — roughly 3 messages a second at about 1 KiB each is 1.7 GiB a
///   week, so the cap carries better than twice the anticipated peak. Reaching it anyway means
///   volume nobody planned for, and the stream then rejects the publish: the Indexer fails closed
///   and stops accepting price updates instead of the broker discarding evidence no consumer has
///   archived.
pub fn price_evidence_stream() -> StreamSpec {
    StreamSpec {
        name: PRICE_EVIDENCE_STREAM.to_string(),
        subjects: vec![PRICE_EVIDENCE.to_string()],
        retention: Retention::Limits,
        max_age: Some(PRICE_EVIDENCE_RETENTION),
        max_messages: None,
        max_bytes: Some(4 * GIB),
        discard: Discard::New,
        duplicate_window: PRICE_EVIDENCE_DUPLICATE_WINDOW,
    }
}

/// The Indexer's per-asset aggregate prices (durable JetStream, [`PRICE_AGGREGATE_STREAM`]).
///
/// Derived output, disjoint from [`PRICE_EVIDENCE`]: the aggregate is recomputable forever
/// from the evidence stream plus the registry, so this stream is a recency buffer, not a
/// record. Reached only through [`crate::NatsClient::publish_aggregate_price`].
pub const PRICE_AGGREGATE: &str = "tackt.prices.aggregate";

/// Durable stream carrying [`PRICE_AGGREGATE`].
pub const PRICE_AGGREGATE_STREAM: &str = "TACKT_PRICE_AGGREGATE";

/// How long aggregates stay replayable. Only recency matters — a consumer starting cold wants
/// the latest aggregate per asset, and an hour bounds a `DeliverAll` replay the same way the
/// risk price streams do.
pub const PRICE_AGGREGATE_RETENTION: Duration = Duration::from_secs(60 * 60);

/// Aggregate-price stream: the opposite retention posture from the evidence stream, because
/// its content is derived and superseded rather than irreplaceable. The newest aggregate is
/// always the one worth keeping, so capacity discards the oldest instead of failing the
/// publisher, and the heartbeat republish of an unchanged price relies on the absence of
/// producer deduplication.
pub fn price_aggregate_stream() -> StreamSpec {
    StreamSpec {
        name: PRICE_AGGREGATE_STREAM.to_string(),
        subjects: vec![PRICE_AGGREGATE.to_string()],
        retention: Retention::Limits,
        max_age: Some(PRICE_AGGREGATE_RETENTION),
        max_messages: None,
        max_bytes: Some(256 * MIB),
        discard: Discard::Old,
        duplicate_window: DUPLICATE_WINDOW,
    }
}

/// User-lifecycle event stream: a bounded replay buffer, not a source of
/// truth — the users table in PostgreSQL remains authoritative.
pub fn users_stream() -> StreamSpec {
    StreamSpec {
        name: "TACKT_USERS".to_string(),
        subjects: vec![USERS_WILDCARD.to_string()],
        retention: Retention::Limits,
        max_age: Some(Duration::from_secs(7 * 24 * 60 * 60)),
        max_messages: None,
        max_bytes: Some(256 * MIB),
        // An acceleration channel: PostgreSQL is authoritative, so dropping the
        // oldest replay buffer entries costs a reconcile, not evidence.
        discard: Discard::Old,
        duplicate_window: DUPLICATE_WINDOW,
    }
}

/// The four risk streams (three ingress, one advisory egress), parameterized by a name
/// prefix so the risk host and its tests can run against isolated streams.
///
/// The subject prefix is derived from the stream prefix by lowercasing and
/// replacing `_` with `.`: the production prefix `TACKT_RISK` yields
/// `tackt.risk.prices` on stream `TACKT_RISK_PRICES`, and a test prefix like
/// `TEST_RISK_AB12` yields disjoint subjects, which JetStream requires across
/// streams.
#[derive(Clone, Debug)]
pub struct RiskStreams {
    pub prices: StreamSpec,
    pub patches: StreamSpec,
    pub sweeps: StreamSpec,
    pub recommendations: StreamSpec,
}

/// Production risk stream prefix.
pub const RISK_STREAM_PREFIX: &str = "TACKT_RISK";

impl RiskStreams {
    /// The production streams (`TACKT_RISK_*` / `tackt.risk.*`). These are the
    /// first production contracts; pre-production streams must be cleared as
    /// part of rollout rather than carrying development payloads forward.
    pub fn production() -> Self {
        Self::with_prefix(RISK_STREAM_PREFIX)
    }

    pub fn with_prefix(prefix: &str) -> Self {
        let subject_base = prefix.to_ascii_lowercase().replace('_', ".");
        let spec = |suffix: &str, subject: &str, max_age: Duration, max_bytes: i64| StreamSpec {
            name: format!("{prefix}_{suffix}"),
            subjects: vec![format!("{subject_base}.{subject}")],
            retention: Retention::Limits,
            max_age: Some(max_age),
            max_messages: None,
            max_bytes: Some(max_bytes),
            // Risk ingress and egress are recency-driven and the engine folds
            // forward from a snapshot; the newest message is always the one
            // worth keeping.
            discard: Discard::Old,
            duplicate_window: DUPLICATE_WINDOW,
        };
        Self {
            // Only recency matters for prices; age-bound tightly so a
            // cursor-less start (DeliverAll) replays a bounded history.
            prices: spec("PRICES", "prices", Duration::from_secs(60 * 60), 512 * MIB),
            patches: spec(
                "PATCHES",
                "patches",
                Duration::from_secs(24 * 60 * 60),
                512 * MIB,
            ),
            sweeps: spec(
                "SWEEPS",
                "sweeps",
                Duration::from_secs(24 * 60 * 60),
                512 * MIB,
            ),
            recommendations: spec(
                "RECOMMENDATIONS",
                "recommendations",
                Duration::from_secs(7 * 24 * 60 * 60),
                1024 * MIB,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_subjects_follow_source_names() {
        assert_eq!(
            observations(Source::Ethereum),
            "tackt.observations.ethereum"
        );
        assert_eq!(
            observations(Source::Hyperliquid),
            "tackt.observations.hyperliquid"
        );
    }

    #[test]
    fn observations_stream_captures_every_source_and_fails_closed() {
        let stream = observations_stream();
        assert_eq!(stream.name, "TACKT_OBSERVATIONS");
        assert_eq!(stream.subject(), OBSERVATIONS_WILDCARD);
        for source in [Source::Ethereum, Source::Hyperliquid] {
            let subject = observations(source);
            assert!(
                subject.starts_with("tackt.observations."),
                "source subject {subject} escapes the stream's filter"
            );
        }
        // The durable publish is the Indexer's only record of an observation, so the
        // stream takes the same fail-closed posture as price evidence.
        assert_eq!(stream.retention, Retention::Limits);
        assert_eq!(stream.discard, Discard::New);
        assert_eq!(stream.max_age, Some(OBSERVATIONS_RETENTION));
        assert_eq!(stream.duplicate_window, OBSERVATIONS_DUPLICATE_WINDOW);
        assert!(OBSERVATIONS_DUPLICATE_WINDOW < OBSERVATIONS_RETENTION);
        // No other stream's filter may capture an observation subject.
        for other in [
            users_stream(),
            price_evidence_stream(),
            price_aggregate_stream(),
        ]
        .into_iter()
        .chain(production_risk_specs())
        {
            assert!(
                !other
                    .subjects
                    .iter()
                    .any(|subject| subject.starts_with("tackt.observations")),
                "stream {} also claims an observation subject",
                other.name
            );
        }
    }

    #[test]
    fn production_risk_streams_use_the_documented_names() {
        let streams = RiskStreams::production();
        assert_eq!(streams.prices.name, "TACKT_RISK_PRICES");
        assert_eq!(streams.prices.subject(), "tackt.risk.prices");
        assert_eq!(streams.patches.name, "TACKT_RISK_PATCHES");
        assert_eq!(streams.patches.subject(), "tackt.risk.patches");
        assert_eq!(streams.sweeps.name, "TACKT_RISK_SWEEPS");
        assert_eq!(streams.sweeps.subject(), "tackt.risk.sweeps");
        assert_eq!(streams.recommendations.name, "TACKT_RISK_RECOMMENDATIONS");
        assert_eq!(
            streams.recommendations.subject(),
            "tackt.risk.recommendations"
        );
    }

    #[test]
    fn prefixed_risk_streams_stay_disjoint_from_production() {
        let streams = RiskStreams::with_prefix("TEST_RISK_AB12");
        assert_eq!(streams.prices.name, "TEST_RISK_AB12_PRICES");
        assert_eq!(streams.prices.subject(), "test.risk.ab12.prices");
    }

    #[test]
    fn price_evidence_is_disjoint_from_the_executable_risk_price_subject() {
        assert_eq!(PRICE_EVIDENCE, "tackt.prices.evidence");
        let risk_prices = RiskStreams::production().prices;
        assert_ne!(PRICE_EVIDENCE, risk_prices.subject());
        // Disjoint prefixes: no risk stream's subject filter can capture price evidence.
        assert!(!PRICE_EVIDENCE.starts_with("tackt.risk."));
    }

    fn production_risk_specs() -> [StreamSpec; 4] {
        let risk = RiskStreams::production();
        [risk.prices, risk.patches, risk.sweeps, risk.recommendations]
    }

    #[test]
    fn price_evidence_stream_carries_only_its_own_subject() {
        let stream = price_evidence_stream();
        assert_eq!(stream.name, "TACKT_PRICE_EVIDENCE");
        assert_eq!(stream.subject(), PRICE_EVIDENCE);
        // No other stream may capture the subject, and this stream may capture
        // nothing else: JetStream requires disjoint filters, and a shared one
        // would put executable prices and evidence in one archive.
        for other in [
            users_stream(),
            price_aggregate_stream(),
            observations_stream(),
        ]
        .into_iter()
        .chain(production_risk_specs())
        {
            assert!(
                !other
                    .subjects
                    .iter()
                    .any(|subject| subject == PRICE_EVIDENCE),
                "stream {} also claims the price-evidence subject",
                other.name
            );
        }
    }

    #[test]
    fn aggregate_prices_are_derived_and_discard_old() {
        let stream = price_aggregate_stream();
        assert_eq!(stream.name, "TACKT_PRICE_AGGREGATE");
        assert_eq!(stream.subject(), PRICE_AGGREGATE);
        assert_ne!(PRICE_AGGREGATE, PRICE_EVIDENCE);
        // Derived and superseded: the newest aggregate is the one worth keeping,
        // the opposite posture from the fail-closed evidence stream.
        assert_eq!(stream.retention, Retention::Limits);
        assert_eq!(stream.discard, Discard::Old);
        assert_eq!(stream.max_age, Some(PRICE_AGGREGATE_RETENTION));
        assert!(PRICE_AGGREGATE_RETENTION < PRICE_EVIDENCE_RETENTION);
        // Aggregates are not risk ingress: the risk price stream keeps its own subject.
        assert_ne!(PRICE_AGGREGATE, RiskStreams::production().prices.subject());
    }

    #[test]
    fn price_evidence_stream_fails_closed_at_capacity() {
        let stream = price_evidence_stream();
        // Every consumer reads every message, so the stream may not drain on ack.
        assert_eq!(stream.retention, Retention::Limits);
        // Capacity exhaustion rejects the publish rather than deleting evidence
        // that no downstream consumer has archived yet.
        assert_eq!(stream.discard, Discard::New);
        assert_eq!(stream.max_bytes, Some(4 * GIB));
        assert_eq!(stream.max_age, Some(PRICE_EVIDENCE_RETENTION));
        assert_eq!(stream.duplicate_window, PRICE_EVIDENCE_DUPLICATE_WINDOW);
    }

    #[test]
    fn price_evidence_retention_outlasts_its_duplicate_window() {
        // The window only has to cover retried and repeated publishes of one
        // update; retention has to cover a downstream outage. Inverting them
        // would make the broker deduplicate against messages the stream has
        // already dropped.
        assert!(PRICE_EVIDENCE_DUPLICATE_WINDOW < PRICE_EVIDENCE_RETENTION);
    }

    #[test]
    fn every_stream_states_what_its_backlog_is_worth() {
        // Read off the specs rather than left to the server default, so adding
        // a stream forces the decision instead of inheriting one.
        assert_eq!(users_stream().discard, Discard::Old);
        for stream in production_risk_specs() {
            assert_eq!(stream.discard, Discard::Old, "stream {}", stream.name);
        }
        assert_eq!(price_aggregate_stream().discard, Discard::Old);
        assert_eq!(price_evidence_stream().discard, Discard::New);
        assert_eq!(observations_stream().discard, Discard::New);
    }
}
