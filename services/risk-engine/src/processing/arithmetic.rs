use ethnum::I256;
use tackt_primitives::assets::MAX_DECIMALS;

use crate::error::{Error, Result};

use super::{Amount, Factor, MarginTier, Price, SignedAmount, Value};

/// Amounts use nine decimal places.
pub const AMOUNT_SCALE: i128 = 1_000_000_000;
/// Prices and values use eight decimal places.
pub const PRICE_SCALE: i128 = 100_000_000;
/// Risk factors and health ratios use six decimal places.
pub const FACTOR_SCALE: i128 = 1_000_000;
/// Values have the same scale as prices.
pub const VALUE_SCALE: i128 = PRICE_SCALE;

/// Bounds keep the full three-term product within a signed 256-bit intermediate.
pub const MAX_AMOUNT_RAW: i128 = 1_000_000_000_000_000_000_000_000_000_000;
pub const MAX_PRICE_RAW: i128 = 1_000_000_000_000_000_000_000_000;
pub const MAX_FACTOR_RAW: i128 = 10 * FACTOR_SCALE;
pub const MAX_VALUE_RAW: i128 = 1_000_000_000_000_000_000_000_000_000_000;

/// Exact shared precision for pooling registry representations before Amount quantization.
#[derive(Default)]
pub(crate) struct AtomicAmountAccumulator(I256);

impl AtomicAmountAccumulator {
    pub(crate) fn add(&mut self, quantity: i128, decimals: u8) -> Result<()> {
        validate_non_negative(quantity, "validating collateral atomic quantity")?;
        let shift = MAX_DECIMALS
            .checked_sub(decimals)
            .ok_or(Error::ArithmeticOverflow {
                operation: "normalizing collateral decimals",
            })?;
        let scaled = checked_mul(
            I256::from(quantity),
            I256::from(10_i128.pow(u32::from(shift))),
            "normalizing collateral quantity",
        )?;
        self.0 = self
            .0
            .checked_add(scaled)
            .ok_or(Error::ArithmeticOverflow {
                operation: "pooling collateral quantity",
            })?;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Amount> {
        let divisor = I256::from(10_i128.pow(u32::from(MAX_DECIMALS)) / AMOUNT_SCALE);
        let raw = checked_i128(self.0 / divisor, "converting pooled collateral quantity")?;
        validate_component(
            raw,
            MAX_AMOUNT_RAW,
            "enforcing pooled collateral quantity bounds",
        )?;
        Ok(Amount::from_raw(raw))
    }
}

/// Calculates a collateral value and rounds down.
///
/// # Errors
///
/// Returns an arithmetic error for negative, out-of-range, or overflowing inputs.
pub fn collateral_contribution(amount: Amount, price: Price, factor: Factor) -> Result<Value> {
    contribution(amount, price, factor, Rounding::Down)
}

/// Calculates a debt value and rounds up.
///
/// # Errors
///
/// Returns an arithmetic error for negative, out-of-range, or overflowing inputs.
pub fn debt_contribution(amount: Amount, price: Price, factor: Factor) -> Result<Value> {
    contribution(amount, price, factor, Rounding::Up)
}

pub(crate) fn weighted_amount(amount: Amount, factor: Factor) -> Result<i128> {
    validate_component(amount.raw(), MAX_AMOUNT_RAW, "validating amount")?;
    validate_component(factor.raw(), MAX_FACTOR_RAW, "validating factor")?;
    amount
        .raw()
        .checked_mul(factor.raw())
        .ok_or(Error::ArithmeticOverflow {
            operation: "precomputing weighted exposure amount",
        })
}

pub(crate) fn collateral_contribution_weighted(
    weighted_amount: i128,
    price: Price,
) -> Result<Value> {
    weighted_contribution(weighted_amount, price, Rounding::Down)
}

#[cfg(test)]
pub(crate) fn debt_contribution_weighted(weighted_amount: i128, price: Price) -> Result<Value> {
    weighted_contribution(weighted_amount, price, Rounding::Up)
}

/// Calculates the notional value of a signed position size at a price and rounds up.
///
/// # Errors
///
/// Returns an arithmetic error for out-of-range inputs or an out-of-range result.
pub fn notional_value(size: SignedAmount, price: Price) -> Result<Value> {
    validate_signed_amount(size)?;
    validate_component(price.raw(), MAX_PRICE_RAW, "validating price")?;
    let numerator = checked_mul(
        I256::from(size.raw().abs()),
        I256::from(price.raw()),
        "calculating notional value",
    )?;
    let raw = checked_i128(
        divide(numerator, I256::from(AMOUNT_SCALE), Rounding::Up),
        "converting notional value",
    )?;
    if raw > MAX_VALUE_RAW {
        return Err(Error::ArithmeticOverflow {
            operation: "enforcing maximum notional value",
        });
    }
    Ok(Value::from_raw(raw))
}

/// Calculates the exact tiered maintenance requirement at a notional value.
///
/// Marginal notional at each tier is charged at that tier's rate; the tier's continuity
/// deduction folds the lower tiers in. The requirement rounds up.
///
/// # Errors
///
/// Returns an error for an invalid notional, empty tiers, or overflowing arithmetic.
pub fn maintenance_margin(notional: Value, tiers: &[MarginTier]) -> Result<Value> {
    validate_component(notional.raw(), MAX_VALUE_RAW, "validating notional value")?;
    let tier = margin_tier(notional, tiers)?;
    let charged = checked_i128(
        divide(
            checked_mul(
                I256::from(notional.raw()),
                I256::from(tier.maintenance_factor.raw()),
                "calculating tiered maintenance",
            )?,
            I256::from(FACTOR_SCALE),
            Rounding::Up,
        ),
        "converting tiered maintenance",
    )?;
    let raw = charged
        .checked_sub(tier.maintenance_deduction.raw())
        .ok_or(Error::ArithmeticOverflow {
            operation: "applying maintenance deduction",
        })?;
    if raw > MAX_VALUE_RAW {
        return Err(Error::ArithmeticOverflow {
            operation: "enforcing maximum maintenance margin",
        });
    }
    // Continuity of validated tiers keeps the requirement non-negative; clamp for safety.
    Ok(Value::from_raw(raw.max(0)))
}

/// Hyperliquid maintenance using the exact `1 / (2 * max_leverage)` rate in each band.
/// Each band's charge rounds up to the engine's monetary precision, avoiding a rounded
/// parts-per-million rate (notably for 15x leverage). Registry tiers are already validated.
pub(crate) fn native_maintenance_margin(notional: Value, tiers: &[MarginTier]) -> Result<Value> {
    validate_component(notional.raw(), MAX_VALUE_RAW, "validating native notional")?;
    if tiers.is_empty() {
        return Err(Error::InvalidAssetRegistry {
            reason: "perp market has no margin tiers".to_owned(),
        });
    }
    let mut total = 0_i128;
    for (index, tier) in tiers.iter().enumerate() {
        let upper = tiers.get(index + 1).map_or(notional.raw(), |next| {
            next.lower_bound_notional.raw().min(notional.raw())
        });
        let band = upper - tier.lower_bound_notional.raw();
        if band <= 0 {
            break;
        }
        let divisor = 2 * i128::from(tier.max_leverage);
        if divisor == 0 {
            return Err(Error::InvalidAssetRegistry {
                reason: "zero tier leverage".to_owned(),
            });
        }
        total =
            total
                .checked_add((band + divisor - 1) / divisor)
                .ok_or(Error::ArithmeticOverflow {
                    operation: "summing native maintenance",
                })?;
    }
    Ok(Value::from_raw(total))
}

/// Required value divided by signed equity, rounded up. `i128::MAX` denotes unbounded
/// utilization when equity is negative, or zero with a positive requirement.
pub(crate) fn utilization_ratio(required: Value, equity: Value) -> Result<Factor> {
    validate_component(
        required.raw(),
        MAX_VALUE_RAW,
        "validating utilization requirement",
    )?;
    validate_signed_value(equity.raw(), "validating utilization equity")?;
    if equity.raw() < 0 || (equity.raw() == 0 && required.raw() > 0) {
        return Ok(Factor::from_raw(i128::MAX));
    }
    if required.raw() == 0 {
        return Ok(Factor::zero());
    }
    let numerator = checked_mul(
        I256::from(required.raw()),
        I256::from(FACTOR_SCALE),
        "calculating utilization",
    )?;
    Ok(Factor::from_raw(checked_i128(
        divide(numerator, I256::from(equity.raw()), Rounding::Up),
        "converting utilization",
    )?))
}

/// Calculates a position's average maintenance rate at its anchor price and rounds down.
///
/// The average rate makes a linear extrapolation exact at the anchor and pass
/// through zero maintenance at zero price, so it can never extrapolate a negative
/// requirement. A zero notional returns the base-tier rate. Native venue valuation does
/// not use this approximation; it recalculates tiered requirements at each mark.
///
/// # Errors
///
/// Returns an error for out-of-range inputs, empty tiers, or overflowing arithmetic.
pub fn average_maintenance_factor(
    size: SignedAmount,
    anchor: Price,
    tiers: &[MarginTier],
) -> Result<Factor> {
    let notional = notional_value(size, anchor)?;
    if notional.raw() == 0 {
        return Ok(margin_tier(notional, tiers)?.maintenance_factor);
    }
    let maintenance = maintenance_margin(notional, tiers)?;
    let numerator = checked_mul(
        I256::from(maintenance.raw()),
        I256::from(FACTOR_SCALE),
        "calculating average maintenance factor",
    )?;
    checked_i128(
        numerator / I256::from(notional.raw()),
        "converting average maintenance factor",
    )
    .map(Factor::from_raw)
}

fn margin_tier(notional: Value, tiers: &[MarginTier]) -> Result<&MarginTier> {
    tiers
        .iter()
        .rev()
        .find(|tier| notional.raw() >= tier.lower_bound_notional.raw())
        .or_else(|| tiers.first())
        .ok_or(Error::InvalidAssetRegistry {
            reason: "perp market has no margin tiers".to_owned(),
        })
}

pub(crate) fn checked_uncertain_add(current: u32, delta: i64) -> Result<u32> {
    let value = i64::from(current)
        .checked_add(delta)
        .ok_or(Error::ArithmeticOverflow {
            operation: "updating uncertain input count",
        })?;
    u32::try_from(value).map_err(|_| Error::ArithmeticOverflow {
        operation: "updating uncertain input count",
    })
}

/// Calculates a health ratio and rounds down.
///
/// # Errors
///
/// Returns an arithmetic error for negative or overflowing inputs.
pub fn health_ratio(collateral: Value, debt: Value) -> Result<Factor> {
    validate_non_negative(collateral.raw(), "calculating health ratio")?;
    validate_non_negative(debt.raw(), "calculating health ratio")?;

    if debt.raw() == 0 {
        return Ok(Factor::from_raw(i128::MAX));
    }

    let raw = if let Some(numerator) = collateral.raw().checked_mul(FACTOR_SCALE) {
        numerator / debt.raw()
    } else {
        let numerator = checked_mul(
            I256::from(collateral.raw()),
            I256::from(FACTOR_SCALE),
            "calculating health ratio",
        )?;
        checked_i128(
            numerator / I256::from(debt.raw()),
            "converting health ratio",
        )?
    };
    Ok(Factor::from_raw(raw))
}

/// Calculates overall account health from pooled collateral, liabilities, and maintenance.
///
/// Ethereum and Hyperliquid value are pooled in the numerator. Hyperliquid maintenance remains a
/// liquidation boundary in the denominator.
///
/// # Errors
///
/// Returns an error if the net account value or resulting ratio overflows.
pub fn account_health_ratio(collateral: Value, debt: Value, maintenance: Value) -> Result<Factor> {
    validate_non_negative(collateral.raw(), "calculating account health")?;
    validate_non_negative(debt.raw(), "calculating account health")?;
    let account_value =
        collateral
            .raw()
            .checked_sub(debt.raw())
            .ok_or(Error::ArithmeticOverflow {
                operation: "calculating net account value",
            })?;
    margin_health_ratio(Value::from_raw(account_value), maintenance)
}

/// Calculates signed Hyperliquid `PnL` for a mark-price movement.
///
/// Positive `PnL` rounds down and negative `PnL` rounds away from zero so missing precision cannot
/// make an account healthier.
///
/// # Errors
///
/// Returns an error for an out-of-range size, price delta, or result.
pub fn perp_pnl(signed_size: SignedAmount, price_delta: i128) -> Result<Value> {
    validate_signed_amount(signed_size)?;
    if price_delta.unsigned_abs() > MAX_PRICE_RAW as u128 {
        return Err(Error::ArithmeticOverflow {
            operation: "validating perp price delta",
        });
    }
    let numerator = checked_mul(
        I256::from(signed_size.raw()),
        I256::from(price_delta),
        "calculating perp PnL",
    )?;
    let raw = checked_i128(
        divide_signed_conservative(numerator, I256::from(AMOUNT_SCALE)),
        "converting perp PnL",
    )?;
    validate_signed_value(raw, "validating perp PnL")?;
    Ok(Value::from_raw(raw))
}

/// Calculates the signed change in maintenance caused by a mark-price movement.
///
/// Maintenance increases round up and decreases round toward zero.
///
/// # Errors
///
/// Returns an error for an out-of-range size, factor, price delta, or result.
pub fn maintenance_delta(
    signed_size: SignedAmount,
    price_delta: i128,
    factor: Factor,
) -> Result<Value> {
    validate_signed_amount(signed_size)?;
    validate_component(
        factor.raw(),
        MAX_FACTOR_RAW,
        "validating maintenance factor",
    )?;
    if price_delta.unsigned_abs() > MAX_PRICE_RAW as u128 {
        return Err(Error::ArithmeticOverflow {
            operation: "validating maintenance price delta",
        });
    }
    let numerator = checked_mul(
        checked_mul(
            I256::from(signed_size.raw().unsigned_abs()),
            I256::from(price_delta),
            "calculating maintenance sensitivity",
        )?,
        I256::from(factor.raw()),
        "applying maintenance factor",
    )?;
    let denominator = I256::from(AMOUNT_SCALE) * I256::from(FACTOR_SCALE);
    let quotient = numerator / denominator;
    let raw = if numerator > I256::ZERO && numerator % denominator != I256::ZERO {
        checked_i128(quotient + I256::ONE, "rounding maintenance increase")?
    } else {
        checked_i128(quotient, "converting maintenance delta")?
    };
    validate_signed_value(raw, "validating maintenance delta")?;
    Ok(Value::from_raw(raw))
}

/// Calculates signed account-value to maintenance health.
///
/// # Errors
///
/// Returns an error for out-of-range inputs or an overflowing ratio.
pub fn margin_health_ratio(account_value: Value, maintenance: Value) -> Result<Factor> {
    validate_component(
        maintenance.raw(),
        MAX_VALUE_RAW,
        "validating maintenance value",
    )?;
    validate_signed_value(account_value.raw(), "validating account value")?;
    if maintenance.raw() == 0 {
        return Ok(if account_value.raw() < 0 {
            Factor::from_raw(i128::MIN)
        } else {
            Factor::from_raw(i128::MAX)
        });
    }
    let numerator = checked_mul(
        I256::from(account_value.raw()),
        I256::from(FACTOR_SCALE),
        "calculating margin health",
    )?;
    Ok(Factor::from_raw(checked_i128(
        divide_signed_conservative(numerator, I256::from(maintenance.raw())),
        "converting margin health",
    )?))
}

fn contribution(amount: Amount, price: Price, factor: Factor, rounding: Rounding) -> Result<Value> {
    let weighted_amount = weighted_amount(amount, factor)?;
    weighted_contribution(weighted_amount, price, rounding)
}

fn weighted_contribution(weighted_amount: i128, price: Price, rounding: Rounding) -> Result<Value> {
    validate_component(price.raw(), MAX_PRICE_RAW, "validating price")?;
    let numerator = checked_mul(
        I256::from(weighted_amount),
        I256::from(price.raw()),
        "multiplying weighted amount by price",
    )?;
    let denominator = I256::from(AMOUNT_SCALE) * I256::from(FACTOR_SCALE);
    let value = divide(numerator, denominator, rounding);
    let raw = checked_i128(value, "converting contribution")?;
    if raw > MAX_VALUE_RAW {
        return Err(Error::ArithmeticOverflow {
            operation: "enforcing maximum supported value",
        });
    }
    Ok(Value::from_raw(raw))
}

pub(crate) fn validate_component(raw: i128, maximum: i128, operation: &'static str) -> Result<()> {
    validate_non_negative(raw, operation)?;
    if raw > maximum {
        return Err(Error::ArithmeticOverflow { operation });
    }
    Ok(())
}

pub(crate) fn add_credit_values(left: Value, right: Value) -> Result<Value> {
    validate_component(left.raw(), MAX_VALUE_RAW, "summing credit values")?;
    validate_component(right.raw(), MAX_VALUE_RAW, "summing credit values")?;
    let raw = left
        .raw()
        .checked_add(right.raw())
        .ok_or(Error::ArithmeticOverflow {
            operation: "summing credit values",
        })?;
    validate_component(raw, MAX_VALUE_RAW, "summing credit values")?;
    Ok(Value::from_raw(raw))
}

pub(crate) fn credit_headroom(capacity: Value, exposure: Value) -> Result<(Value, Value)> {
    validate_signed_value(capacity.raw(), "calculating credit headroom")?;
    validate_component(exposure.raw(), MAX_VALUE_RAW, "calculating credit headroom")?;
    let difference =
        capacity
            .raw()
            .checked_sub(exposure.raw())
            .ok_or(Error::ArithmeticOverflow {
                operation: "calculating credit headroom",
            })?;
    validate_signed_value(difference, "enforcing credit headroom bounds")?;
    Ok((
        Value::from_raw(difference.max(0)),
        Value::from_raw((-difference).max(0)),
    ))
}

fn validate_non_negative(raw: i128, operation: &'static str) -> Result<()> {
    if raw < 0 {
        return Err(Error::ArithmeticOverflow { operation });
    }
    Ok(())
}

fn validate_signed_amount(amount: SignedAmount) -> Result<()> {
    if amount.raw().unsigned_abs() > MAX_AMOUNT_RAW as u128 {
        return Err(Error::ArithmeticOverflow {
            operation: "validating signed amount",
        });
    }
    Ok(())
}

pub(crate) fn validate_signed_value(raw: i128, operation: &'static str) -> Result<()> {
    if raw.unsigned_abs() > MAX_VALUE_RAW as u128 {
        return Err(Error::ArithmeticOverflow { operation });
    }
    Ok(())
}

/// Wide totals allow signed collateral to offset without order-dependent intermediate limits.
#[derive(Default)]
pub(crate) struct CreditCapacity {
    borrowing: I256,
    liquidation: I256,
}

impl CreditCapacity {
    pub(crate) fn add(&mut self, borrowing: Value, liquidation: Value) -> Result<()> {
        self.borrowing = self
            .borrowing
            .checked_add(I256::from(borrowing.raw()))
            .ok_or(Error::ArithmeticOverflow {
                operation: "summing borrowing capacity",
            })?;
        self.liquidation = self
            .liquidation
            .checked_add(I256::from(liquidation.raw()))
            .ok_or(Error::ArithmeticOverflow {
                operation: "summing liquidation capacity",
            })?;
        Ok(())
    }

    pub(crate) fn reserve(
        &mut self,
        notional: Value,
        opening: Factor,
        maintenance: Factor,
    ) -> Result<()> {
        let opening = perp_reserve(notional, opening)?;
        let maintenance = perp_reserve(notional, maintenance)?;
        // Reserves are non-negative and bounded well below i128::MAX.
        self.add(
            Value::from_raw(-opening.raw()),
            Value::from_raw(-maintenance.raw()),
        )
    }

    pub(crate) fn finish(self) -> Result<(Value, Value)> {
        let borrowing = checked_i128(self.borrowing, "converting borrowing capacity")?;
        let liquidation = checked_i128(self.liquidation, "converting liquidation capacity")?;
        validate_signed_value(borrowing, "enforcing borrowing capacity bounds")?;
        validate_signed_value(liquidation, "enforcing liquidation capacity bounds")?;
        Ok((Value::from_raw(borrowing), Value::from_raw(liquidation)))
    }
}

fn perp_reserve(notional: Value, rate: Factor) -> Result<Value> {
    validate_signed_value(notional.raw(), "validating perp notional")?;
    validate_component(rate.raw(), FACTOR_SCALE, "validating perp reserve rate")?;
    let numerator = checked_mul(
        I256::from(notional.raw()).abs(),
        I256::from(rate.raw()),
        "calculating perp reserve",
    )?;
    let raw = checked_i128(
        divide(numerator, I256::from(FACTOR_SCALE), Rounding::Up),
        "converting perp reserve",
    )?;
    Ok(Value::from_raw(raw))
}

pub(crate) fn credit_health_ratio(capacity: Value, debt: Value) -> Result<Factor> {
    validate_component(debt.raw(), MAX_VALUE_RAW, "validating account credit debt")?;
    validate_signed_value(capacity.raw(), "validating credit liquidation capacity")?;
    if debt == Value::zero() {
        return Ok(Factor::from_raw(i128::MAX));
    }
    margin_health_ratio(capacity, debt)
}

fn checked_mul(left: I256, right: I256, operation: &'static str) -> Result<I256> {
    left.checked_mul(right)
        .ok_or(Error::ArithmeticOverflow { operation })
}

fn checked_i128(value: I256, operation: &'static str) -> Result<i128> {
    i128::try_from(value).map_err(|_| Error::ArithmeticOverflow { operation })
}

fn divide(numerator: I256, denominator: I256, rounding: Rounding) -> I256 {
    let quotient = numerator / denominator;
    if rounding == Rounding::Up && numerator % denominator != I256::ZERO {
        quotient + I256::ONE
    } else {
        quotient
    }
}

fn divide_signed_conservative(numerator: I256, denominator: I256) -> I256 {
    let quotient = numerator / denominator;
    if numerator < I256::ZERO && numerator % denominator != I256::ZERO {
        quotient - I256::ONE
    } else {
        quotient
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rounding {
    Down,
    Up,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_pool_supports_registry_precision_bounds_and_rejects_invalid_input() {
        let mut pooled = AtomicAmountAccumulator::default();
        pooled.add(1, 0).expect("whole unit");
        pooled
            .add(10_i128.pow(u32::from(MAX_DECIMALS)), MAX_DECIMALS)
            .expect("highest supported precision");
        assert_eq!(pooled.finish(), Ok(Amount::from_raw(2 * AMOUNT_SCALE)));

        let mut invalid = AtomicAmountAccumulator::default();
        assert!(invalid.add(1, MAX_DECIMALS + 1).is_err());
        assert!(invalid.add(-1, 0).is_err());
        assert_eq!(invalid.finish(), Ok(Amount::zero()));
    }

    #[test]
    fn collateral_rounds_down_and_debt_rounds_up() {
        let amount = Amount::from_raw(1);
        let price = Price::from_raw(1);
        let factor = Factor::from_raw(1);

        assert_eq!(
            collateral_contribution(amount, price, factor),
            Ok(Value::zero())
        );
        assert_eq!(
            debt_contribution(amount, price, factor),
            Ok(Value::from_raw(1))
        );
    }

    #[test]
    fn one_unit_has_expected_value() {
        let amount = Amount::from_raw(AMOUNT_SCALE);
        let price = Price::from_raw(25 * PRICE_SCALE);
        let factor = Factor::from_raw(800_000);

        assert_eq!(
            collateral_contribution(amount, price, factor),
            Ok(Value::from_raw(20 * VALUE_SCALE))
        );
    }

    #[test]
    fn zero_debt_has_maximum_health() {
        assert_eq!(
            health_ratio(Value::from_raw(VALUE_SCALE), Value::zero()),
            Ok(Factor::from_raw(i128::MAX))
        );
    }

    #[test]
    fn overall_health_pools_collateral_before_comparing_with_maintenance() {
        assert_eq!(
            account_health_ratio(
                Value::from_raw(110 * VALUE_SCALE),
                Value::from_raw(10 * VALUE_SCALE),
                Value::from_raw(50 * VALUE_SCALE),
            ),
            Ok(Factor::from_raw(2 * FACTOR_SCALE))
        );
    }

    #[test]
    fn negative_margin_health_rounds_against_the_account() {
        assert_eq!(
            margin_health_ratio(Value::from_raw(-1), Value::from_raw(3)),
            Ok(Factor::from_raw(-(FACTOR_SCALE / 3) - 1))
        );
    }

    #[test]
    fn rejects_values_above_documented_bounds() {
        let result = collateral_contribution(
            Amount::from_raw(MAX_AMOUNT_RAW + 1),
            Price::from_raw(PRICE_SCALE),
            Factor::from_raw(FACTOR_SCALE),
        );

        assert!(matches!(result, Err(Error::ArithmeticOverflow { .. })));
    }

    #[test]
    fn cached_weight_produces_the_same_conservative_contributions() {
        let amount = Amount::from_raw(1_234_567_891);
        let price = Price::from_raw(9_876_543_219);
        let factor = Factor::from_raw(812_345);
        let weighted = weighted_amount(amount, factor).expect("valid weight");

        assert_eq!(
            collateral_contribution_weighted(weighted, price),
            collateral_contribution(amount, price, factor)
        );
        assert_eq!(
            debt_contribution_weighted(weighted, price),
            debt_contribution(amount, price, factor)
        );
    }

    fn btc_tiers() -> Vec<MarginTier> {
        vec![
            MarginTier {
                lower_bound_notional: Value::zero(),
                max_leverage: 40,
                maintenance_factor: Factor::from_raw(12_500),
                maintenance_deduction: Value::zero(),
            },
            MarginTier {
                lower_bound_notional: Value::from_raw(150_000_000 * VALUE_SCALE),
                max_leverage: 20,
                maintenance_factor: Factor::from_raw(25_000),
                maintenance_deduction: Value::from_raw(1_875_000 * VALUE_SCALE),
            },
        ]
    }

    #[test]
    fn native_maintenance_uses_exact_leverage_divisors() {
        let registry = super::super::AssetRegistry::bundled().expect("registry");
        let tiers = &registry.perp_market(2).expect("ETH market").margin_tiers;
        // $100M / 50 + $100M / 30, rounded upward at 1e-8 monetary precision.
        assert_eq!(
            native_maintenance_margin(Value::from_raw(200_000_000 * VALUE_SCALE), tiers),
            Ok(Value::from_raw(533_333_333_333_334)),
        );
        assert_eq!(
            native_maintenance_margin(Value::from_raw(100_000_000 * VALUE_SCALE), tiers),
            Ok(Value::from_raw(2_000_000 * VALUE_SCALE)),
        );
        assert_eq!(
            native_maintenance_margin(Value::zero(), tiers),
            Ok(Value::zero())
        );
    }

    #[test]
    fn utilization_handles_empty_and_insolvent_pools() {
        for (required, equity, expected) in [
            (0, 0, 0),
            (0, 10, 0),
            (10, 0, i128::MAX),
            (10, -1, i128::MAX),
            (0, -1, i128::MAX),
            (1, 3, 333_334),
        ] {
            assert_eq!(
                utilization_ratio(Value::from_raw(required), Value::from_raw(equity)),
                Ok(Factor::from_raw(expected))
            );
        }
    }

    #[test]
    fn tiered_maintenance_is_marginal_and_continuous() {
        let tiers = btc_tiers();

        // Base tier: 1.25% of a small notional, no deduction.
        assert_eq!(
            maintenance_margin(Value::from_raw(100 * VALUE_SCALE), &tiers),
            Ok(Value::from_raw(125_000_000))
        );
        // Both tiers agree exactly at the boundary.
        let boundary = Value::from_raw(150_000_000 * VALUE_SCALE);
        assert_eq!(
            maintenance_margin(boundary, &tiers),
            Ok(Value::from_raw(1_875_000 * VALUE_SCALE))
        );
        // Above the boundary only the marginal notional is charged at 2.5%:
        // 200M × 2.5% − 1.875M = 3.125M.
        assert_eq!(
            maintenance_margin(Value::from_raw(200_000_000 * VALUE_SCALE), &tiers),
            Ok(Value::from_raw(3_125_000 * VALUE_SCALE))
        );
    }

    #[test]
    fn average_maintenance_factor_is_anchored_and_never_extrapolates_negative() {
        let tiers = btc_tiers();
        let size = SignedAmount::from_raw(2_000 * AMOUNT_SCALE);
        let anchor = Price::from_raw(100_000 * PRICE_SCALE);

        // 3.125M maintenance over 200M notional is a 1.5625% average rate.
        let factor = average_maintenance_factor(size, anchor, &tiers).expect("factor");
        assert_eq!(factor, Factor::from_raw(15_625));

        // The linear model through the anchor reaches zero maintenance only at price zero.
        let maintenance_at_anchor =
            maintenance_margin(notional_value(size, anchor).expect("notional"), &tiers)
                .expect("maintenance");
        let full_drop = maintenance_delta(size, -anchor.raw(), factor).expect("delta");
        assert_eq!(maintenance_at_anchor.raw() + full_drop.raw(), 0);

        // A zero notional falls back to the base-tier rate.
        assert_eq!(
            average_maintenance_factor(SignedAmount::zero(), anchor, &tiers),
            Ok(Factor::from_raw(12_500))
        );
    }

    #[test]
    fn long_and_short_pnl_and_maintenance_have_expected_signs() {
        let size = SignedAmount::from_raw(2 * AMOUNT_SCALE);
        let delta = 5 * PRICE_SCALE;

        assert_eq!(perp_pnl(size, delta), Ok(Value::from_raw(10 * VALUE_SCALE)));
        assert_eq!(
            perp_pnl(SignedAmount::from_raw(-size.raw()), delta),
            Ok(Value::from_raw(-10 * VALUE_SCALE))
        );
        assert_eq!(
            maintenance_delta(size, delta, Factor::from_raw(50_000)),
            Ok(Value::from_raw(VALUE_SCALE / 2))
        );
    }
}
