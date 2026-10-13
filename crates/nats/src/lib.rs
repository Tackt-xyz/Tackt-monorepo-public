//! Shared NATS transport boundary.
//!
//! This crate is the sole authority for NATS behavior and wire contracts:
//! connection management, subjects and stream names, envelopes, publisher and
//! consumer helpers, delivery and acknowledgement conventions, and typed serialization. Services depend on this crate and never on `async-nats`
//! directly (enforced by `scripts/check-boundaries.sh`).
//!
//! Provision streams explicitly with `cargo run --locked -p tackt-nats --example provision`.
//! Every service checks required streams with [`NatsClient::require_stream`]; runtime consumer
//! helpers never create streams. Ledger also binds a pre-provisioned durable without resetting it.
//!
//! # What goes over NATS
//!
//! | Subject | Stream | Delivery | Envelope | Producer → Consumer |
//! |---|---|---|---|---|
//! | `tackt.observations.{source}` | `TACKT_OBSERVATIONS` | at-least-once | [`ObservationEvent`] | indexer → anyone |
//! | `tackt.observations.ethereum.{chain_id}` | `TACKT_OBSERVATIONS` | at-least-once | [`ObservationEvent`] | indexer → anyone (non-mainnet EVM chains; mainnet stays unsuffixed) |
//! | `tackt.observations.hyperliquid.{trades,events}` | `TACKT_OBSERVATIONS` | at-least-once | [`ObservationEvent`] | indexer → Ledger, resume KV |
//! | `$KV.TACKT_OBSERVATIONS_HYPERLIQUID_RESUME.{trades,events}` | `KV_TACKT_OBSERVATIONS_HYPERLIQUID_RESUME` | latest value | [`ObservationEvent`] | JetStream source → Indexer recovery |
//! | `tackt.prices.evidence` | `TACKT_PRICE_EVIDENCE` | at-least-once | [`PriceEvidenceEnvelope`] | indexer → risk, KV projector, archive |
//! | `tackt.prices.aggregate` | `TACKT_PRICE_AGGREGATE` | at-least-once | [`aggregates::AggregatePriceEnvelope`] | indexer → Core API, Risk |
//! | `tackt.users.created` | `TACKT_USERS` | at-least-once | [`UserEvent`] | core-api → indexer |
//! | `tackt.users.status_changed` | `TACKT_USERS` | at-least-once | [`UserEvent`] | core-api → indexer |
//! | `tackt.users.account_associated` | `TACKT_USERS` | at-least-once | [`UserEvent`] | core-api → indexer |
//! | `tackt.risk.prices` | `TACKT_RISK_PRICES` | at-least-once | risk-engine host `wire` module | upstream → risk-engine |
//! | `tackt.risk.patches` | `TACKT_RISK_PATCHES` | at-least-once | risk-engine host `wire` module | upstream → risk-engine |
//! | `tackt.risk.sweeps` | `TACKT_RISK_SWEEPS` | at-least-once | risk-engine host `wire` module | upstream → risk-engine |
//! | `tackt.risk.recommendations` | `TACKT_RISK_RECOMMENDATIONS` | at-least-once | [`RiskRecommendationEnvelope`] | risk-engine → downstream |
//!
//! Risk ingress payloads live in `services/risk-engine/src/host/wire.rs` because the dependency
//! direction (`services -> nats -> primitives`) forbids this crate from referencing Risk Engine
//! internals. Public risk egress is the shared [`RiskRecommendationEnvelope`] owned here.
//!
//! # Price evidence is not an executable price
//!
//! `tackt.risk.prices` carries the Risk Engine's executable `PriceUpdate`: a registry price index,
//! a source sequence, and a price the engine may act on. `tackt.prices.evidence` carries
//! [`PriceEvidence`]: what a named [`PriceAuthority`] reported for a [`PriceSubject`], under a
//! deterministic [`PriceEvidence::evidence_id`] that keeps retries of one provider update on one
//! identity. The two payloads share no required field and neither decodes as the other, and the
//! subject sits outside the `tackt.risk.*` family because the stream is the Indexer's handoff to
//! several independent consumers rather than risk ingress. Turning evidence into an executable
//! price stays with the Risk Engine.
//!
//! [`prices`] states the complete contract: fields, validation, size bounds, and identity.
//! `architecture/indexer.md` owns the surrounding persistence and failure decision, including why
//! an acknowledged publish is the Indexer's only durable side effect for an accepted price update.
//!
//! # The evidence streams are the durable record
//!
//! The user stream accelerates something a database holds, so losing a message there costs a
//! reconcile. `TACKT_OBSERVATIONS` and `TACKT_PRICE_EVIDENCE` are different: each holds the
//! only durable copy of what the Indexer observed until a downstream consumer persists it.
//! The Indexer writes neither observations nor prices to PostgreSQL — the acknowledged publish
//! is its completion boundary for both, so both streams are configured to fail closed. For
//! `TACKT_PRICE_EVIDENCE`:
//! [`subjects::price_evidence_stream`] retains to its limits so three independent durable
//! consumers ([`PriceEvidenceConsumer`]) each read everything, discards new rather than old at
//! capacity so an unplanned backlog stops the producer instead of deleting unarchived evidence,
//! and gives its consumers no delivery ceiling so a message leaves only by an explicit ack or
//! term. [`NatsClient::publish_price_evidence`] is the one way in, and there is no core-NATS
//! path to the subject.

pub mod aggregates;
pub mod jetstream;
mod observation_resume;
pub mod observations;
pub mod prices;
pub mod recommendations;
pub mod subjects;
pub mod users;

pub use jetstream::{
    AckHandle, Consumer, ConsumerSpec, Delivery, Discard, Disposition, Envelope, PublishOutcome,
    Retention, StartPosition, StreamSpec,
};
pub use observation_resume::HyperliquidObservationStream;
pub use observations::{EvmBatch, ObservationEvent, ObservationFeed};
pub use prices::{
    MarketSymbol, PriceAuthority, PriceEvidence, PriceEvidenceConsumer, PriceEvidenceEnvelope,
    PriceEvidenceError, PriceQuote, PriceSubject,
};
pub use recommendations::RiskRecommendationEnvelope;
pub use subjects::RiskStreams;
pub use users::{UserEvent, UserEventKind};

/// Shared NATS client: one managed core connection plus a JetStream context.
#[derive(Clone)]
pub struct NatsClient {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
}

impl NatsClient {
    /// Whether the managed NATS connection is currently established.
    pub fn is_connected(&self) -> bool {
        self.client.connection_state() == async_nats::connection::State::Connected
    }
}

/// Connect and fully initialize the shared NATS client.
///
/// The connection retries on initial connect, so this returns successfully even
/// while the server is still unreachable; publishes buffer until the managed
/// connection is established. Connection lifecycle events are logged through
/// `tracing`. Calling this more than once is safe: each call creates an
/// independent client backed by async-nats' managed connection and reconnect
/// loop.
pub async fn initialise(server_url: &str) -> anyhow::Result<NatsClient> {
    // async-nats accepts comma-separated server lists; warn if ANY member is
    // a cleartext scheme pointing beyond loopback.
    for member in server_url.split(',') {
        let parsed = url::Url::parse(member.trim());
        let secure = parsed
            .as_ref()
            .is_ok_and(|u| matches!(u.scheme(), "tls" | "wss"));
        let loopback = parsed.as_ref().is_ok_and(|u| match u.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            Some(url::Host::Domain(host)) => host == "localhost",
            None => false,
        });
        if !secure && !loopback {
            tracing::warn!(
                server = member.trim(),
                "non-TLS NATS URL beyond loopback: traffic travels in cleartext"
            );
        }
    }
    let client = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .event_callback(|event| async move {
            match event {
                async_nats::Event::Disconnected => tracing::warn!("nats disconnected"),
                async_nats::Event::Connected => tracing::info!("nats connected"),
                other => tracing::warn!(event = %other, "nats connection event"),
            }
        })
        .connect(server_url)
        .await?;
    let jetstream = async_nats::jetstream::new(client.clone());
    Ok(NatsClient { client, jetstream })
}
