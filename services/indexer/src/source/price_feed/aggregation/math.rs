//! The engine's fixed-point arithmetic: rescaling, age, decay, median, and deviation. All pure,
//! all deterministic, and every overflow path fails closed or saturates toward rejection.

use chrono::{DateTime, Utc};
use tackt_nats::prices::PriceQuote;

use super::{AGGREGATE_PRICE_SCALE, WEIGHT_TOTAL_BPS};

/// Rescales a source quote to [`AGGREGATE_PRICE_SCALE`], or `None` when the value cannot be
/// represented there (overflow scaling up, or truncation to zero scaling down). Precision beyond
/// the aggregate scale is deliberately truncated: the aggregate scale is the platform resolution.
pub(super) fn rescale_to_aggregate(quote: PriceQuote) -> Option<u128> {
    let scale = quote.scale();
    let value = if scale == AGGREGATE_PRICE_SCALE {
        quote.value()
    } else if scale < AGGREGATE_PRICE_SCALE {
        let factor = 10_u128.pow(u32::from(AGGREGATE_PRICE_SCALE - scale));
        quote.value().checked_mul(factor)?
    } else {
        let factor = 10_u128.pow(u32::from(scale - AGGREGATE_PRICE_SCALE));
        quote.value() / factor
    };
    (value > 0).then_some(value)
}

/// Age of a quote at `now`. A source clock running ahead of ours (bounded by the evidence
/// contract's skew allowance) reads as age zero rather than wrapping.
pub(super) fn quote_age_ms(source_timestamp: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    u64::try_from((now - source_timestamp).num_milliseconds()).unwrap_or(0)
}

/// Linear decay from full weight at `full_ms` to zero at `max_ms` (`max_ms > full_ms` by config
/// validation).
pub(super) fn effective_weight(weight_bps: u16, age_ms: u64, full_ms: u64, max_ms: u64) -> u16 {
    if age_ms >= max_ms {
        return 0;
    }
    if age_ms <= full_ms {
        return weight_bps;
    }
    let remaining = u64::from(weight_bps) * (max_ms - age_ms) / (max_ms - full_ms);
    u16::try_from(remaining).expect("decay never exceeds the configured weight")
}

/// Weighted median of `(price, weight)` pairs: the lowest price at which the cumulative weight
/// reaches half the total. Deterministic for tied prices because the sort is stable over the
/// callers' configuration order.
pub(super) fn weighted_median(mut weighted_prices: Vec<(u128, u16)>) -> u128 {
    weighted_prices.sort_by_key(|(price, _)| *price);
    let total: u64 = weighted_prices
        .iter()
        .map(|(_, weight)| u64::from(*weight))
        .sum();
    let mut cumulative = 0_u64;
    for (price, weight) in &weighted_prices {
        cumulative += u64::from(*weight);
        if cumulative * 2 >= total {
            return *price;
        }
    }
    unreachable!("candidates are non-empty and weights are positive")
}

/// Relative distance from the median in basis points, truncating. An astronomically distant
/// price whose distance overflows the multiply is, by definition, deviated.
pub(super) fn deviation_bps(price: u128, median: u128) -> u64 {
    let difference = price.abs_diff(median);
    match difference.checked_mul(u128::from(WEIGHT_TOTAL_BPS)) {
        Some(scaled) => u64::try_from(scaled / median).unwrap_or(u64::MAX),
        None => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::at;
    use super::*;

    #[test]
    fn weighted_median_walks_cumulative_weight() {
        // Cumulative weight reaches half the total (5000 of 10000) at price 100.
        assert_eq!(
            weighted_median(vec![(300, 1_000), (100, 5_000), (200, 4_000)]),
            100
        );
        // With only light weight below, the middle price holds the median.
        assert_eq!(
            weighted_median(vec![(300, 4_000), (100, 1_000), (200, 4_000)]),
            200
        );
        assert_eq!(weighted_median(vec![(42, 10_000)]), 42);
    }

    #[test]
    fn deviation_is_relative_and_saturates() {
        assert_eq!(deviation_bps(100, 100), 0);
        assert_eq!(deviation_bps(105, 100), 500);
        assert_eq!(deviation_bps(95, 100), 500);
        assert_eq!(deviation_bps(u128::MAX, 1), u64::MAX);
    }

    #[test]
    fn rescaling_is_exact_upward_and_truncates_downward() {
        let quote = |value, scale| PriceQuote::new(value, scale).expect("quote");
        assert_eq!(rescale_to_aggregate(quote(1, 8)), Some(1));
        assert_eq!(
            rescale_to_aggregate(quote(65_000, 0)),
            Some(6_500_000_000_000)
        );
        assert_eq!(rescale_to_aggregate(quote(1_999, 11)), Some(1));
        assert_eq!(rescale_to_aggregate(quote(999, 11)), None);
        assert_eq!(rescale_to_aggregate(quote(u128::MAX, 0)), None);
    }

    #[test]
    fn a_source_clock_ahead_of_ours_reads_as_fresh() {
        assert_eq!(quote_age_ms(at(10), at(0)), 0);
        assert_eq!(effective_weight(4_000, 0, 15_000, 60_000), 4_000);
    }
}
