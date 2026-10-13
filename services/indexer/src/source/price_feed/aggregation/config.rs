//! Validated aggregation policy: which sources feed which asset, at what weight, and the
//! freshness, quorum, and deviation rules the engine enforces. Every rule fails closed at
//! construction, so a bad policy is a startup failure, never a running-state one.

use std::collections::HashSet;
use std::time::Duration;

use tackt_nats::prices::{PriceAuthority, PriceSubject};
use tackt_primitives::ledger::LedgerAsset;

use super::{MAX_SOURCES_PER_ASSET, MAX_STALENESS_CEILING, WEIGHT_TOTAL_BPS};

/// One configured price source feeding one asset.
///
/// Windows are stored as milliseconds because [`MAX_STALENESS_CEILING`] guarantees they fit;
/// the conversion happens once here rather than on every aggregate computation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PriceSourceConfig {
    pub(super) authority: PriceAuthority,
    pub(super) subject: PriceSubject,
    pub(super) weight_bps: u16,
    pub(super) full_weight_within_ms: u64,
    pub(super) max_staleness_ms: u64,
}

impl PriceSourceConfig {
    /// Validates one source definition.
    ///
    /// `full_weight_within` is the age up to which a quote carries full weight — size it to the
    /// source's native cadence (a deviation-plus-heartbeat oracle's heartbeat counts as its
    /// cadence, because its silence means "unchanged"). `max_staleness` is the hard cutoff.
    ///
    /// # Errors
    ///
    /// Fails closed on a zero or over-total weight and on a freshness window that is empty,
    /// inverted, or beyond [`MAX_STALENESS_CEILING`].
    pub fn new(
        authority: PriceAuthority,
        subject: PriceSubject,
        weight_bps: u16,
        full_weight_within: Duration,
        max_staleness: Duration,
    ) -> Result<Self, AggregationConfigError> {
        if weight_bps == 0 || weight_bps > WEIGHT_TOTAL_BPS {
            return Err(AggregationConfigError::InvalidSourceWeight { weight_bps });
        }
        if max_staleness <= full_weight_within || max_staleness > MAX_STALENESS_CEILING {
            return Err(AggregationConfigError::InvalidFreshnessWindow { authority, subject });
        }
        let window_ms = |window: Duration| {
            u64::try_from(window.as_millis()).expect("windows are bounded by MAX_STALENESS_CEILING")
        };
        Ok(Self {
            authority,
            subject,
            weight_bps,
            full_weight_within_ms: window_ms(full_weight_within),
            max_staleness_ms: window_ms(max_staleness),
        })
    }
}

/// How one asset's sources combine into its aggregate price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetAggregationConfig {
    pub(super) asset: LedgerAsset,
    pub(super) sources: Vec<PriceSourceConfig>,
    pub(super) min_quorum_weight_bps: u16,
    pub(super) max_deviation_bps: u32,
}

impl AssetAggregationConfig {
    /// Validates one asset's aggregation policy.
    ///
    /// # Errors
    ///
    /// Fails closed on an empty or oversized source list, a repeated source, weights that do not
    /// sum to exactly [`WEIGHT_TOTAL_BPS`], an unusable quorum, or a zero deviation bound (which
    /// would evict every source but the median itself).
    pub fn new(
        asset: LedgerAsset,
        sources: Vec<PriceSourceConfig>,
        min_quorum_weight_bps: u16,
        max_deviation_bps: u32,
    ) -> Result<Self, AggregationConfigError> {
        if sources.is_empty() || sources.len() > MAX_SOURCES_PER_ASSET {
            return Err(AggregationConfigError::InvalidSourceCount {
                asset,
                count: sources.len(),
            });
        }
        ensure_distinct_sources(sources.iter(), &mut HashSet::new())?;
        let total_bps: u32 = sources
            .iter()
            .map(|source| u32::from(source.weight_bps))
            .sum();
        if total_bps != u32::from(WEIGHT_TOTAL_BPS) {
            return Err(AggregationConfigError::WeightsDoNotSumToTotal { asset, total_bps });
        }
        if min_quorum_weight_bps == 0 || min_quorum_weight_bps > WEIGHT_TOTAL_BPS {
            return Err(AggregationConfigError::InvalidQuorum {
                asset,
                min_quorum_weight_bps,
            });
        }
        if max_deviation_bps == 0 {
            return Err(AggregationConfigError::ZeroDeviationBound { asset });
        }
        Ok(Self {
            asset,
            sources,
            min_quorum_weight_bps,
            max_deviation_bps,
        })
    }
}

/// The complete aggregation configuration: which assets are priced and from what.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregationConfig {
    pub(super) assets: Vec<AssetAggregationConfig>,
}

impl AggregationConfig {
    /// Validates the cross-asset rules a single asset cannot see.
    ///
    /// # Errors
    ///
    /// Fails closed when an asset is configured twice or one source feeds two assets — a shared
    /// source would make quote routing ambiguous.
    pub fn new(assets: Vec<AssetAggregationConfig>) -> Result<Self, AggregationConfigError> {
        let mut seen_assets = HashSet::new();
        let mut seen_sources = HashSet::new();
        for asset in &assets {
            if !seen_assets.insert(asset.asset) {
                return Err(AggregationConfigError::DuplicateAsset { asset: asset.asset });
            }
            ensure_distinct_sources(asset.sources.iter(), &mut seen_sources)?;
        }
        Ok(Self { assets })
    }
}

/// Extends `seen` with each source's identity, failing on the first repeat.
fn ensure_distinct_sources<'a>(
    sources: impl Iterator<Item = &'a PriceSourceConfig>,
    seen: &mut HashSet<(PriceAuthority, PriceSubject)>,
) -> Result<(), AggregationConfigError> {
    for source in sources {
        if !seen.insert((source.authority, source.subject.clone())) {
            return Err(AggregationConfigError::DuplicateSource {
                authority: source.authority,
                subject: source.subject.clone(),
            });
        }
    }
    Ok(())
}

/// A configuration that must not drive the executable price.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AggregationConfigError {
    #[error("a source weight must be 1 to {WEIGHT_TOTAL_BPS} basis points, got {weight_bps}")]
    InvalidSourceWeight { weight_bps: u16 },
    #[error(
        "source {authority:?}/{subject:?} needs full_weight_within < max_staleness <= the ceiling"
    )]
    InvalidFreshnessWindow {
        authority: PriceAuthority,
        subject: PriceSubject,
    },
    #[error("asset {asset:?} must configure 1 to {MAX_SOURCES_PER_ASSET} sources, got {count}")]
    InvalidSourceCount { asset: LedgerAsset, count: usize },
    #[error("source {authority:?}/{subject:?} is configured more than once")]
    DuplicateSource {
        authority: PriceAuthority,
        subject: PriceSubject,
    },
    #[error("asset {asset:?} weights sum to {total_bps} basis points, not {WEIGHT_TOTAL_BPS}")]
    WeightsDoNotSumToTotal { asset: LedgerAsset, total_bps: u32 },
    #[error(
        "asset {asset:?} quorum must be 1 to {WEIGHT_TOTAL_BPS} basis points, \
         got {min_quorum_weight_bps}"
    )]
    InvalidQuorum {
        asset: LedgerAsset,
        min_quorum_weight_bps: u16,
    },
    #[error("asset {asset:?} declares a zero deviation bound")]
    ZeroDeviationBound { asset: LedgerAsset },
    #[error("asset {asset:?} is configured more than once")]
    DuplicateAsset { asset: LedgerAsset },
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{market, source};
    use super::*;

    #[test]
    fn invalid_configurations_fail_closed() {
        let src = |weight_bps, full, max| {
            PriceSourceConfig::new(
                PriceAuthority::Ethereum,
                market("BTC"),
                weight_bps,
                full,
                max,
            )
        };
        assert_eq!(
            src(0, Duration::from_secs(1), Duration::from_secs(2)),
            Err(AggregationConfigError::InvalidSourceWeight { weight_bps: 0 })
        );
        assert!(matches!(
            src(1, Duration::from_secs(2), Duration::from_secs(2)),
            Err(AggregationConfigError::InvalidFreshnessWindow { .. })
        ));
        assert!(matches!(
            src(
                1,
                Duration::ZERO,
                MAX_STALENESS_CEILING + Duration::from_secs(1)
            ),
            Err(AggregationConfigError::InvalidFreshnessWindow { .. })
        ));

        let full_weight = |subject| {
            source(
                PriceAuthority::Hyperliquid,
                subject,
                WEIGHT_TOTAL_BPS,
                5,
                30,
            )
        };
        assert!(matches!(
            AssetAggregationConfig::new(LedgerAsset::Btc, vec![], 1, 1),
            Err(AggregationConfigError::InvalidSourceCount { count: 0, .. })
        ));
        assert!(matches!(
            AssetAggregationConfig::new(
                LedgerAsset::Btc,
                vec![full_weight(market("BTC")), full_weight(market("BTC"))],
                1,
                1,
            ),
            Err(AggregationConfigError::DuplicateSource { .. })
        ));
        assert_eq!(
            AssetAggregationConfig::new(
                LedgerAsset::Btc,
                vec![
                    full_weight(market("BTC")),
                    source(PriceAuthority::Hyperliquid, market("BTC2"), 1, 5, 30),
                ],
                1,
                1,
            ),
            Err(AggregationConfigError::WeightsDoNotSumToTotal {
                asset: LedgerAsset::Btc,
                total_bps: u32::from(WEIGHT_TOTAL_BPS) + 1,
            })
        );
        assert!(matches!(
            AssetAggregationConfig::new(LedgerAsset::Btc, vec![full_weight(market("BTC"))], 0, 1),
            Err(AggregationConfigError::InvalidQuorum { .. })
        ));
        assert_eq!(
            AssetAggregationConfig::new(LedgerAsset::Btc, vec![full_weight(market("BTC"))], 1, 0),
            Err(AggregationConfigError::ZeroDeviationBound {
                asset: LedgerAsset::Btc
            })
        );

        let asset = |asset, subject| {
            AssetAggregationConfig::new(asset, vec![full_weight(subject)], 1, 1).expect("asset")
        };
        assert_eq!(
            AggregationConfig::new(vec![
                asset(LedgerAsset::Btc, market("BTC")),
                asset(LedgerAsset::Btc, market("XBT")),
            ]),
            Err(AggregationConfigError::DuplicateAsset {
                asset: LedgerAsset::Btc
            })
        );
        assert!(matches!(
            AggregationConfig::new(vec![
                asset(LedgerAsset::Btc, market("BTC")),
                asset(LedgerAsset::Eth, market("BTC")),
            ]),
            Err(AggregationConfigError::DuplicateSource { .. })
        ));
    }
}
