//! Deterministic multi-source price aggregation.
//!
//! The engine keeps the latest accepted quote per configured source and derives one composite
//! price per asset from them. It is a pure state machine: quotes enter through
//! [`AggregationEngine::accept`] only after their evidence is durable, and
//! [`AggregationEngine::aggregate`] is a function of the table, the configuration, and an
//! explicit `now` — the clock is an argument, never read inside, so every published aggregate is
//! recomputable from archived evidence and its `computed_at`.
//!
//! # Policy
//!
//! For one asset, at one instant:
//!
//! 1. Each source's registry weight decays linearly with the age of its latest quote, from full
//!    weight at [`PriceSourceConfig::new`]'s `full_weight_within` to zero at its `max_staleness`.
//!    A slow feed sized to its own cadence keeps full weight between ticks; a dead feed fades
//!    out instead of pinning the price.
//! 2. The weighted median of the surviving quotes is the reference point, and any quote deviating
//!    from it by more than the asset's `max_deviation_bps` is evicted. A single wedged or hostile
//!    feed can therefore move the aggregate by at most its weight within the deviation band,
//!    never proportionally to how wrong it is.
//! 3. If the survivors' effective weight falls below the asset's `min_quorum_weight_bps`, there
//!    is no aggregate: the asset's price goes unavailable rather than degrading silently.
//! 4. Otherwise the aggregate is the effective-weight-weighted mean of the survivors, in
//!    [`AGGREGATE_PRICE_SCALE`] fixed point with truncating division.
//!
//! Every computation reports which sources contributed at what effective weight and which were
//! excluded and why, so the eventual aggregate wire message can carry its complete derivation.
//!
//! Configuration is constructed by the caller for now; a later change sources it from the locked
//! asset registry so weights and windows share the registry's version and digest.

mod config;
mod engine;
mod math;

pub use config::{
    AggregationConfig, AggregationConfigError, AssetAggregationConfig, PriceSourceConfig,
};
pub use engine::{
    Accepted, Aggregate, AggregateInput, AggregationEngine, AggregationError, ExcludedSource,
    ExclusionReason, UpdateRejection,
};

use std::time::Duration;

// The scale, weight denominator, and source bound are the aggregate-price wire contract's:
// what the engine computes is exactly what `tackt-nats` publishes, and the two must not drift.
pub use tackt_nats::aggregates::AGGREGATE_PRICE_SCALE;
pub use tackt_nats::aggregates::AGGREGATE_WEIGHT_TOTAL_BPS as WEIGHT_TOTAL_BPS;
pub use tackt_nats::aggregates::MAX_AGGREGATE_SOURCES as MAX_SOURCES_PER_ASSET;

/// Longest staleness window a source may declare. Far beyond any real feed cadence — even a
/// heartbeat-style oracle — and keeps every age computation inside `u64` milliseconds.
pub const MAX_STALENESS_CEILING: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Shared fixtures for the submodule tests.
#[cfg(test)]
mod test_support {
    use chrono::{DateTime, TimeZone, Utc};
    use serde_json::json;
    use tackt_nats::prices::{
        MarketSymbol, PriceAuthority, PriceEvidence, PriceQuote, PriceSubject,
    };
    use tackt_primitives::Provenance;

    use super::{AGGREGATE_PRICE_SCALE, PriceSourceConfig};
    use std::time::Duration;

    const BASE_SECONDS: i64 = 1_800_000_000;

    pub fn at(seconds_after_base: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(BASE_SECONDS + seconds_after_base, 0)
            .single()
            .expect("timestamp")
    }

    pub fn market(symbol: &str) -> PriceSubject {
        PriceSubject::Market(MarketSymbol::new(symbol).expect("symbol"))
    }

    pub fn source(
        authority: PriceAuthority,
        subject: PriceSubject,
        weight_bps: u16,
        full_secs: u64,
        max_secs: u64,
    ) -> PriceSourceConfig {
        PriceSourceConfig::new(
            authority,
            subject,
            weight_bps,
            Duration::from_secs(full_secs),
            Duration::from_secs(max_secs),
        )
        .expect("source config")
    }

    pub fn evidence(
        authority: PriceAuthority,
        subject: PriceSubject,
        value: u128,
        scale: u8,
        source_sequence: u64,
        seconds_after_base: i64,
    ) -> PriceEvidence {
        PriceEvidence::new(
            authority,
            subject,
            PriceQuote::new(value, scale).expect("quote"),
            source_sequence,
            at(seconds_after_base),
            at(seconds_after_base),
            Provenance {
                endpoint: "https://example.com".to_string(),
                method: "test".to_string(),
                source_id: format!("seq:{source_sequence}"),
            },
            json!({ "test": true }),
        )
        .expect("evidence")
    }

    /// A quote at the aggregate scale, stamped at the base instant.
    pub fn quote(
        authority: PriceAuthority,
        subject: PriceSubject,
        value: u128,
        sequence: u64,
    ) -> PriceEvidence {
        evidence(
            authority,
            subject,
            value,
            AGGREGATE_PRICE_SCALE,
            sequence,
            0,
        )
    }
}
