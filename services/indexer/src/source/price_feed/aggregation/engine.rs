//! The aggregation state machine: latest-per-source tables fed by accepted evidence, and the
//! per-asset composite computation over them.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tackt_nats::prices::{PriceAuthority, PriceEvidence, PriceQuote, PriceSubject};
use tackt_primitives::ledger::LedgerAsset;

use super::config::{AggregationConfig, AssetAggregationConfig};
use super::{AGGREGATE_PRICE_SCALE, math};

/// Latest accepted quote from one source, already at [`AGGREGATE_PRICE_SCALE`].
#[derive(Clone, Debug)]
struct LatestQuote {
    price: u128,
    source_sequence: u64,
    source_timestamp: DateTime<Utc>,
    evidence_id: String,
    /// Consecutive sequence rejections since this quote was stored. A venue that resets its
    /// sequence numbering wedges the slot for the process lifetime (restart is the reset), and
    /// that must be visible above debug level.
    rejections: u64,
}

/// One asset's aggregation policy plus the current table of latest quotes, index-aligned with
/// the configured sources.
struct AssetTable {
    config: AssetAggregationConfig,
    slots: Vec<Option<LatestQuote>>,
}

impl AssetTable {
    /// Policy step 1: the sources whose latest quote still carries weight at `now`, and the
    /// sources that dropped out (never reported, or decayed to zero).
    fn fresh_candidates(&self, now: DateTime<Utc>) -> (Vec<AggregateInput>, Vec<ExcludedSource>) {
        let mut candidates = Vec::new();
        let mut excluded = Vec::new();
        for (source, slot) in self.config.sources.iter().zip(&self.slots) {
            let exclude = |reason| ExcludedSource {
                authority: source.authority,
                subject: source.subject.clone(),
                effective_weight_bps: 0,
                reason,
            };
            let Some(quote) = slot else {
                excluded.push(exclude(ExclusionReason::NeverReported));
                continue;
            };
            let age_ms = math::quote_age_ms(quote.source_timestamp, now);
            let effective_weight_bps = math::effective_weight(
                source.weight_bps,
                age_ms,
                source.full_weight_within_ms,
                source.max_staleness_ms,
            );
            if effective_weight_bps == 0 {
                excluded.push(exclude(ExclusionReason::Stale { age_ms }));
                continue;
            }
            candidates.push(AggregateInput {
                authority: source.authority,
                subject: source.subject.clone(),
                evidence_id: quote.evidence_id.clone(),
                price: quote.price,
                effective_weight_bps,
            });
        }
        (candidates, excluded)
    }
}

/// Policy step 2: splits the candidates into those within the deviation band around their
/// weighted median and those evicted for deviating, mirroring [`AssetTable::fresh_candidates`].
fn within_deviation(
    candidates: Vec<AggregateInput>,
    max_deviation_bps: u32,
) -> (Vec<AggregateInput>, Vec<ExcludedSource>) {
    let median = math::weighted_median(
        candidates
            .iter()
            .map(|candidate| (candidate.price, candidate.effective_weight_bps))
            .collect(),
    );
    let mut inputs = Vec::new();
    let mut evicted = Vec::new();
    for candidate in candidates {
        let deviation = math::deviation_bps(candidate.price, median);
        if deviation > u64::from(max_deviation_bps) {
            evicted.push(ExcludedSource {
                authority: candidate.authority,
                subject: candidate.subject,
                effective_weight_bps: candidate.effective_weight_bps,
                reason: ExclusionReason::Deviated {
                    deviation_bps: deviation,
                },
            });
        } else {
            inputs.push(candidate);
        }
    }
    (inputs, evicted)
}

/// Policy step 4: the effective-weight-weighted mean at [`AGGREGATE_PRICE_SCALE`], or `None` on
/// 128-bit overflow.
///
/// Truncating division; still positive because the mean of positive prices is at least their
/// minimum, and every accepted price is at least 1.
fn weighted_mean(inputs: &[AggregateInput], total_weight_bps: u16) -> Option<u128> {
    let mut numerator: u128 = 0;
    for input in inputs {
        let weighted = input
            .price
            .checked_mul(u128::from(input.effective_weight_bps))?;
        numerator = numerator.checked_add(weighted)?;
    }
    Some(numerator / u128::from(total_weight_bps))
}

/// The per-asset latest-quote tables and the routing from source identity to its slot.
///
/// Not internally synchronized: the owner serializes access, matching the single-writer model of
/// the wider indexer.
pub struct AggregationEngine {
    assets: Vec<AssetTable>,
    /// `authority -> subject -> (asset index, slot index)`; unique by config validation.
    /// Nested so an accept looks a source up by borrowed subject, with no per-trade key clone.
    routes: HashMap<PriceAuthority, HashMap<PriceSubject, (usize, usize)>>,
}

impl AggregationEngine {
    #[must_use]
    pub fn new(config: AggregationConfig) -> Self {
        let mut routes: HashMap<PriceAuthority, HashMap<PriceSubject, (usize, usize)>> =
            HashMap::new();
        let assets: Vec<AssetTable> = config
            .assets
            .into_iter()
            .map(|config| {
                let slots = vec![None; config.sources.len()];
                AssetTable { config, slots }
            })
            .collect();
        for (asset_index, table) in assets.iter().enumerate() {
            for (slot_index, source) in table.config.sources.iter().enumerate() {
                routes
                    .entry(source.authority)
                    .or_default()
                    .insert(source.subject.clone(), (asset_index, slot_index));
            }
        }
        Self { assets, routes }
    }

    /// Every configured asset, in configuration order.
    #[must_use]
    pub fn assets(&self) -> Vec<LedgerAsset> {
        self.assets.iter().map(|table| table.config.asset).collect()
    }

    /// Records one durably published piece of evidence as its source's latest opinion.
    ///
    /// Callers feed evidence here only after its JetStream publish is acknowledged, so nothing
    /// can influence the executable price without an audit record. Per-source sequences are
    /// strictly increasing: a redelivered or reordered quote can never overwrite a newer one.
    ///
    /// # Errors
    ///
    /// Rejects evidence from a source no asset configures, evidence whose sequence does not
    /// advance the slot, and a price that truncates to zero at [`AGGREGATE_PRICE_SCALE`].
    pub fn accept(&mut self, evidence: &PriceEvidence) -> Result<Accepted, UpdateRejection> {
        let route = self
            .routes
            .get(&evidence.authority())
            .and_then(|by_subject| by_subject.get(evidence.subject()));
        let Some(&(asset_index, slot_index)) = route else {
            return Err(UpdateRejection::UnconfiguredSource {
                authority: evidence.authority(),
                subject: evidence.subject().clone(),
            });
        };
        let table = &mut self.assets[asset_index];
        let asset = table.config.asset;
        let Some(price) = math::rescale_to_aggregate(evidence.price()) else {
            return Err(UpdateRejection::UnrepresentablePrice { asset });
        };
        if let Some(held) = &mut table.slots[slot_index] {
            if evidence.source_sequence() < held.source_sequence {
                held.rejections += 1;
                if held.rejections == 1 || held.rejections.is_multiple_of(1_000) {
                    tracing::warn!(
                        asset = ?asset,
                        held = held.source_sequence,
                        received = evidence.source_sequence(),
                        rejections = held.rejections,
                        "source sequence is not advancing; a venue sequence reset needs a restart"
                    );
                }
                return Err(UpdateRejection::StaleSequence {
                    asset,
                    held: held.source_sequence,
                    received: evidence.source_sequence(),
                });
            }
            if evidence.source_sequence() == held.source_sequence {
                if evidence.evidence_id() == held.evidence_id {
                    return Ok(Accepted::Duplicate { asset });
                }
                // Two different updates under one sequence: the source's ordering is broken and
                // neither quote can be trusted as "the" opinion at that sequence.
                return Err(UpdateRejection::ConflictingSequence {
                    asset,
                    source_sequence: evidence.source_sequence(),
                });
            }
        }
        table.slots[slot_index] = Some(LatestQuote {
            price,
            source_sequence: evidence.source_sequence(),
            source_timestamp: evidence.source_timestamp(),
            evidence_id: evidence.evidence_id().to_owned(),
            rejections: 0,
        });
        Ok(Accepted::Updated { asset })
    }

    /// Computes one asset's aggregate at an explicit instant, running the module's four policy
    /// steps in order.
    ///
    /// Deterministic: the same table, configuration, and `now` always produce the same result,
    /// which is what makes a published aggregate recomputable from archived evidence.
    ///
    /// # Errors
    ///
    /// Fails closed for an unconfigured asset, for surviving weight below the quorum (including
    /// the empty table at startup), and on arithmetic overflow.
    pub fn aggregate(
        &self,
        asset: LedgerAsset,
        now: DateTime<Utc>,
    ) -> Result<Aggregate, AggregationError> {
        let table = self
            .assets
            .iter()
            .find(|table| table.config.asset == asset)
            .ok_or(AggregationError::UnconfiguredAsset { asset })?;
        let below_quorum = |surviving_weight_bps| AggregationError::BelowQuorum {
            asset,
            surviving_weight_bps,
            required_bps: table.config.min_quorum_weight_bps,
        };
        let (candidates, mut excluded) = table.fresh_candidates(now);
        if candidates.is_empty() {
            return Err(below_quorum(0));
        }
        let (inputs, evicted) = within_deviation(candidates, table.config.max_deviation_bps);
        // Fits u16: effective weights never exceed registry weights, which sum to the total.
        let surviving_weight_bps: u16 = inputs.iter().map(|input| input.effective_weight_bps).sum();
        if surviving_weight_bps < table.config.min_quorum_weight_bps {
            return Err(below_quorum(surviving_weight_bps));
        }
        // Survivors must strictly outweigh what the deviation guard evicted. Without this, two
        // equal-weight sources that disagree resolve to whichever the weighted median favors —
        // deterministically the lower — and a single wedged venue sets the price alone. When
        // the split has no majority, nobody can tell which side is wrong: no price.
        let evicted_weight_bps: u16 = evicted
            .iter()
            .map(|source| source.effective_weight_bps)
            .sum();
        if surviving_weight_bps <= evicted_weight_bps {
            return Err(AggregationError::NoDeviationMajority {
                asset,
                surviving_weight_bps,
                evicted_weight_bps,
            });
        }
        excluded.extend(evicted);
        let value = weighted_mean(&inputs, surviving_weight_bps)
            .ok_or(AggregationError::Overflow { asset })?;
        let price = PriceQuote::new(value, AGGREGATE_PRICE_SCALE)
            .expect("a weighted mean of positive prices is positive");
        Ok(Aggregate {
            asset,
            price,
            computed_at: now,
            inputs,
            excluded,
        })
    }
}

/// What [`AggregationEngine::accept`] did with a piece of evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Accepted {
    /// The source's slot now holds this quote.
    Updated { asset: LedgerAsset },
    /// The slot already holds exactly this update; nothing changed.
    Duplicate { asset: LedgerAsset },
}

/// Evidence that must not become a source's current opinion.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum UpdateRejection {
    #[error("no configured asset aggregates {authority:?}/{subject:?}")]
    UnconfiguredSource {
        authority: PriceAuthority,
        subject: PriceSubject,
    },
    #[error("sequence {received} does not advance the held sequence {held} for {asset:?}")]
    StaleSequence {
        asset: LedgerAsset,
        held: u64,
        received: u64,
    },
    #[error("two distinct updates for {asset:?} claim source sequence {source_sequence}")]
    ConflictingSequence {
        asset: LedgerAsset,
        source_sequence: u64,
    },
    #[error("price for {asset:?} truncates to zero at the aggregate scale")]
    UnrepresentablePrice { asset: LedgerAsset },
}

/// One computed aggregate and its complete derivation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Aggregate {
    pub asset: LedgerAsset,
    /// The composite price, always at [`AGGREGATE_PRICE_SCALE`].
    pub price: PriceQuote,
    /// The instant the computation was evaluated at; with the inputs, sufficient to recompute it.
    pub computed_at: DateTime<Utc>,
    /// Contributing sources in configuration order, with the weights actually applied.
    pub inputs: Vec<AggregateInput>,
    /// Configured sources that did not contribute, and why.
    pub excluded: Vec<ExcludedSource>,
}

/// One source's contribution to an aggregate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateInput {
    pub authority: PriceAuthority,
    pub subject: PriceSubject,
    /// Identity of the evidence behind the quote, linking the aggregate to its audit records.
    pub evidence_id: String,
    /// The quote at [`AGGREGATE_PRICE_SCALE`].
    pub price: u128,
    /// The decayed weight this quote carried in the mean.
    pub effective_weight_bps: u16,
}

/// A configured source that did not contribute to an aggregate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExcludedSource {
    pub authority: PriceAuthority,
    pub subject: PriceSubject,
    /// The decayed weight the source would have carried; zero when it was stale or silent.
    pub effective_weight_bps: u16,
    pub reason: ExclusionReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExclusionReason {
    /// The source has not reported since the engine started.
    NeverReported,
    /// The latest quote's effective weight decayed to zero.
    Stale { age_ms: u64 },
    /// The quote sits further from the weighted median than the asset's deviation bound.
    Deviated { deviation_bps: u64 },
}

/// No executable aggregate exists for the asset right now.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AggregationError {
    #[error("asset {asset:?} has no aggregation configuration")]
    UnconfiguredAsset { asset: LedgerAsset },
    #[error(
        "asset {asset:?} has {surviving_weight_bps} of the required {required_bps} basis points \
         of fresh agreeing sources"
    )]
    BelowQuorum {
        asset: LedgerAsset,
        surviving_weight_bps: u16,
        required_bps: u16,
    },
    #[error(
        "asset {asset:?} split with no majority: {surviving_weight_bps} basis points agree, \
         {evicted_weight_bps} were evicted for deviating"
    )]
    NoDeviationMajority {
        asset: LedgerAsset,
        surviving_weight_bps: u16,
        evicted_weight_bps: u16,
    },
    #[error("aggregating {asset:?} overflowed 128-bit arithmetic")]
    Overflow { asset: LedgerAsset },
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{at, evidence, market, quote, source};
    use super::super::{AggregationConfig, AssetAggregationConfig};
    use super::*;

    /// One asset over two markets: a fast venue feed (60%) and a slower one (40%).
    fn two_source_engine() -> AggregationEngine {
        let config = AggregationConfig::new(vec![
            AssetAggregationConfig::new(
                LedgerAsset::Btc,
                vec![
                    source(PriceAuthority::Hyperliquid, market("BTC"), 6_000, 5, 30),
                    source(PriceAuthority::Ethereum, market("BTC-ETH"), 4_000, 15, 60),
                ],
                5_000,
                500,
            )
            .expect("asset config"),
        ])
        .expect("config");
        AggregationEngine::new(config)
    }

    #[test]
    fn accept_routes_by_source_and_rejects_the_unconfigured() {
        let mut engine = two_source_engine();
        let accepted = engine
            .accept(&quote(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                6_500_000_000_000,
                1,
            ))
            .expect("accepted");
        assert_eq!(
            accepted,
            Accepted::Updated {
                asset: LedgerAsset::Btc
            }
        );
        assert!(matches!(
            engine.accept(&quote(PriceAuthority::Hyperliquid, market("DOGE"), 1, 1)),
            Err(UpdateRejection::UnconfiguredSource { .. })
        ));
    }

    #[test]
    fn sequences_only_advance() {
        let mut engine = two_source_engine();
        let fill =
            |value, sequence| quote(PriceAuthority::Hyperliquid, market("BTC"), value, sequence);
        engine.accept(&fill(100_000_000, 5)).expect("first");
        assert_eq!(
            engine.accept(&fill(100_000_000, 4)),
            Err(UpdateRejection::StaleSequence {
                asset: LedgerAsset::Btc,
                held: 5,
                received: 4,
            })
        );
        // The identical update again is a harmless redelivery.
        assert_eq!(
            engine.accept(&fill(100_000_000, 5)),
            Ok(Accepted::Duplicate {
                asset: LedgerAsset::Btc
            })
        );
        // A different update under the same sequence means the source's ordering is broken.
        assert_eq!(
            engine.accept(&fill(200_000_000, 5)),
            Err(UpdateRejection::ConflictingSequence {
                asset: LedgerAsset::Btc,
                source_sequence: 5,
            })
        );
        engine.accept(&fill(200_000_000, 6)).expect("advances");
    }

    #[test]
    fn prices_that_truncate_to_zero_are_rejected() {
        let mut engine = two_source_engine();
        // 1e-38 is positive to the contract but zero at the aggregate scale.
        assert_eq!(
            engine.accept(&evidence(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                1,
                38,
                1,
                0,
            )),
            Err(UpdateRejection::UnrepresentablePrice {
                asset: LedgerAsset::Btc
            })
        );
    }

    #[test]
    fn mixed_scales_agree_after_rescaling() {
        let mut engine = two_source_engine();
        // 65000.00 quoted at scale 2 and scale 12: identical after rescaling.
        engine
            .accept(&evidence(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                6_500_000,
                2,
                1,
                0,
            ))
            .expect("scale 2");
        engine
            .accept(&evidence(
                PriceAuthority::Ethereum,
                market("BTC-ETH"),
                65_000_000_000_000_009, // sub-scale precision truncates
                12,
                1,
                0,
            ))
            .expect("scale 12");
        let aggregate = engine.aggregate(LedgerAsset::Btc, at(1)).expect("fresh");
        assert_eq!(aggregate.price.value(), 6_500_000_000_000);
        assert_eq!(aggregate.price.scale(), AGGREGATE_PRICE_SCALE);
        assert_eq!(aggregate.inputs.len(), 2);
        assert!(aggregate.excluded.is_empty());
    }

    #[test]
    fn effective_weight_decays_linearly_and_shifts_the_mean() {
        let mut engine = two_source_engine();
        // Fresh at the evaluation instant below.
        engine
            .accept(&evidence(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                100_000_000_000,
                8,
                1,
                40,
            ))
            .expect("fast source");
        // Aged 40s at evaluation: 5/9 of the way from full weight (15s) to cutoff (60s).
        engine
            .accept(&quote(
                PriceAuthority::Ethereum,
                market("BTC-ETH"),
                104_000_000_000,
                1,
            ))
            .expect("slow source");
        let aggregate = engine.aggregate(LedgerAsset::Btc, at(40)).expect("quorum");
        let weights: Vec<u16> = aggregate
            .inputs
            .iter()
            .map(|input| input.effective_weight_bps)
            .collect();
        // 4000 * (60 - 40) / (60 - 15) = 1777, truncating.
        assert_eq!(weights, vec![6_000, 1_777]);
        // (100e9 * 6000 + 104e9 * 1777) / 7777, truncating.
        assert_eq!(aggregate.price.value(), 100_913_977_111);
        assert_eq!(aggregate.computed_at, at(40));
    }

    #[test]
    fn stale_sources_drop_out_and_quorum_fails_closed() {
        let mut engine = two_source_engine();
        engine
            .accept(&quote(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                100_000_000_000,
                1,
            ))
            .expect("quote");
        // At 4s the fast source is inside its full-weight window and alone
        // holds the 5000 bps quorum.
        let aggregate = engine.aggregate(LedgerAsset::Btc, at(4)).expect("held");
        assert_eq!(aggregate.price.value(), 100_000_000_000);
        assert_eq!(
            aggregate.excluded,
            vec![ExcludedSource {
                authority: PriceAuthority::Ethereum,
                subject: market("BTC-ETH"),
                effective_weight_bps: 0,
                reason: ExclusionReason::NeverReported,
            }]
        );
        // Past its 30s cutoff the quote is stale and nothing survives.
        assert_eq!(
            engine.aggregate(LedgerAsset::Btc, at(31)),
            Err(AggregationError::BelowQuorum {
                asset: LedgerAsset::Btc,
                surviving_weight_bps: 0,
                required_bps: 5_000,
            })
        );
        // An empty table is the same failure: no aggregate exists before sources report.
        assert!(matches!(
            two_source_engine().aggregate(LedgerAsset::Btc, at(0)),
            Err(AggregationError::BelowQuorum {
                surviving_weight_bps: 0,
                ..
            })
        ));
    }

    #[test]
    fn unconfigured_assets_fail_closed() {
        assert_eq!(
            two_source_engine().aggregate(LedgerAsset::Usdc, at(0)),
            Err(AggregationError::UnconfiguredAsset {
                asset: LedgerAsset::Usdc
            })
        );
    }

    fn three_source_engine() -> AggregationEngine {
        let config = AggregationConfig::new(vec![
            AssetAggregationConfig::new(
                LedgerAsset::Btc,
                vec![
                    source(PriceAuthority::Hyperliquid, market("BTC"), 4_000, 30, 60),
                    source(PriceAuthority::Ethereum, market("BTC-ETH"), 3_000, 30, 60),
                    source(PriceAuthority::Ethereum, market("BTC-DEX"), 3_000, 30, 60),
                ],
                4_000,
                500, // 5%
            )
            .expect("asset config"),
        ])
        .expect("config");
        AggregationEngine::new(config)
    }

    #[test]
    fn a_deviating_source_is_evicted_not_averaged() {
        let mut engine = three_source_engine();
        let mut fill = |authority, subject, value| {
            engine
                .accept(&quote(authority, subject, value, 1))
                .expect("quote");
        };
        fill(PriceAuthority::Hyperliquid, market("BTC"), 100_000_000_000);
        fill(PriceAuthority::Ethereum, market("BTC-ETH"), 101_000_000_000);
        // A wedged feed 50% below the market; a plain weighted mean would move 15%.
        fill(PriceAuthority::Ethereum, market("BTC-DEX"), 50_000_000_000);
        let aggregate = engine.aggregate(LedgerAsset::Btc, at(1)).expect("quorum");
        assert_eq!(
            aggregate.excluded,
            vec![ExcludedSource {
                authority: PriceAuthority::Ethereum,
                subject: market("BTC-DEX"),
                effective_weight_bps: 3_000,
                reason: ExclusionReason::Deviated {
                    deviation_bps: 5_000
                },
            }]
        );
        // (100e9 * 4000 + 101e9 * 3000) / 7000, truncating.
        assert_eq!(aggregate.price.value(), 100_428_571_428);
        let ids: Vec<&str> = aggregate
            .inputs
            .iter()
            .map(|input| input.evidence_id.as_str())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.iter().all(|id| id.starts_with("priceev_")));
    }

    #[test]
    fn a_split_without_a_majority_fails_closed() {
        // The heaviest source holds the weighted median and evicts both outliers — but the
        // evicted weight (6000) outweighs the survivor (4000), so nobody is a majority and no
        // price may exist rather than one source setting it alone.
        let mut engine = three_source_engine();
        let mut fill = |authority, subject, value| {
            engine
                .accept(&quote(authority, subject, value, 1))
                .expect("quote");
        };
        fill(PriceAuthority::Hyperliquid, market("BTC"), 100_000_000_000);
        fill(PriceAuthority::Ethereum, market("BTC-ETH"), 120_000_000_000);
        fill(PriceAuthority::Ethereum, market("BTC-DEX"), 80_000_000_000);
        assert_eq!(
            engine.aggregate(LedgerAsset::Btc, at(1)),
            Err(AggregationError::NoDeviationMajority {
                asset: LedgerAsset::Btc,
                surviving_weight_bps: 4_000,
                evicted_weight_bps: 6_000,
            })
        );
    }

    #[test]
    fn two_equal_sources_that_disagree_produce_no_price() {
        // The two-source degenerate case: the weighted median deterministically favors the
        // lower quote, so without the majority rule a wedged low venue would evict the healthy
        // one and set the price alone.
        let config = AggregationConfig::new(vec![
            AssetAggregationConfig::new(
                LedgerAsset::Btc,
                vec![
                    source(PriceAuthority::Hyperliquid, market("BTC"), 5_000, 30, 60),
                    source(PriceAuthority::Ethereum, market("BTC-ETH"), 5_000, 30, 60),
                ],
                2_500,
                500,
            )
            .expect("asset config"),
        ])
        .expect("config");
        let mut engine = AggregationEngine::new(config);
        engine
            .accept(&quote(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                100_000_000_000,
                1,
            ))
            .expect("healthy venue");
        engine
            .accept(&quote(
                PriceAuthority::Ethereum,
                market("BTC-ETH"),
                50_000_000_000,
                1,
            ))
            .expect("wedged venue");
        assert_eq!(
            engine.aggregate(LedgerAsset::Btc, at(1)),
            Err(AggregationError::NoDeviationMajority {
                asset: LedgerAsset::Btc,
                surviving_weight_bps: 5_000,
                evicted_weight_bps: 5_000,
            })
        );
        // Agreement within the band prices normally.
        engine
            .accept(&quote(
                PriceAuthority::Ethereum,
                market("BTC-ETH"),
                100_200_000_000,
                2,
            ))
            .expect("recovered venue");
        let aggregate = engine.aggregate(LedgerAsset::Btc, at(1)).expect("majority");
        assert_eq!(aggregate.price.value(), 100_100_000_000);
    }

    #[test]
    fn overflow_fails_closed_instead_of_wrapping() {
        let mut engine = two_source_engine();
        engine
            .accept(&quote(
                PriceAuthority::Hyperliquid,
                market("BTC"),
                u128::MAX,
                1,
            ))
            .expect("extreme quote");
        assert_eq!(
            engine.aggregate(LedgerAsset::Btc, at(1)),
            Err(AggregationError::Overflow {
                asset: LedgerAsset::Btc
            })
        );
    }
}
