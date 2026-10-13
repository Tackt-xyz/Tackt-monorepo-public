//! Stateless portfolio credit: spot collateral and pledged perp equity support one account debt.
//!
//! Callers supply coherent, versioned snapshots, retain their identities with results, and own
//! stale-update rejection. Risk selects collateral from Ledger buckets and custody facts, pools
//! approved representations, and excludes margin balances already counted through pool equity.
//! Pool equity includes P&L, funding, and fees, but never deducts the Tackt loan. Proving control,
//! venue constraints, authorization, and liquidation execution remain outside this calculation.
//! All capacities are derived on demand; nothing is cached or mutated.

mod eligibility;
mod policy;
mod types;

pub use policy::{
    CreditAssetPolicy, CreditPolicy, PendingDepositPolicy, PerpReservePolicy, ReservedSpotPolicy,
};
pub use types::{
    CollateralBalance, CollateralControl, CollateralEligibility, CreditAccount, CreditEvaluation,
    CreditHealth, CreditMarketSnapshot, CreditPerpPosition, CreditPrice, PerpMarginSnapshot,
};

use crate::processing::{
    Amount, MAX_PRICE_RAW, MAX_VALUE_RAW, RiskState, Value,
    arithmetic::{
        CreditCapacity, add_credit_values, credit_headroom, credit_health_ratio,
        validate_component, validate_signed_value,
    },
    collateral_contribution,
};
use crate::{Error, Result};
use eligibility::{eligible_spot, validate_balances};

/// Calculates one account from borrowed inputs without allocating or changing them.
///
/// Spot values and pool equity share one signed capacity calculation. Reserves are rounded up
/// per absolute position notional. Negative contributions are never floored before pooling.
/// Undrawn commitments consume borrowing capacity but never enter liquidation debt.
///
/// # Errors
/// Rejects zero snapshot revisions, orphan positions, invalid amounts, and overflow.
pub fn evaluate_credit(
    account: &CreditAccount,
    market: &CreditMarketSnapshot,
    policy: &CreditPolicy,
) -> Result<CreditEvaluation> {
    if account.version == 0 || market.version == 0 {
        return Err(Error::InvalidCreditInput {
            reason: "snapshot version must be positive",
        });
    }
    validate_component(
        account.debt.raw(),
        MAX_VALUE_RAW,
        "validating account credit debt",
    )?;
    validate_component(
        account.pending_draws.raw(),
        MAX_VALUE_RAW,
        "validating pending draws",
    )?;
    let exposure = add_credit_values(account.debt, account.pending_draws)?;
    let mut capacity = CreditCapacity::default();
    let spot_complete = value_spot(account, market, policy, &mut capacity)?;
    let perps_complete = value_perps(account, market, policy, &mut capacity)?;
    if !spot_complete || !perps_complete {
        return Ok(CreditEvaluation::DataIncomplete);
    }
    let (borrowing_capacity, liquidation_capacity) = capacity.finish()?;
    let (available_credit, capacity_shortfall) = credit_headroom(borrowing_capacity, exposure)?;
    let state = if account.debt > Value::zero() && account.debt >= liquidation_capacity {
        RiskState::Liquidatable
    } else if exposure > borrowing_capacity {
        RiskState::Watch
    } else {
        RiskState::Healthy
    };
    Ok(CreditEvaluation::Complete(CreditHealth {
        borrowing_capacity,
        liquidation_capacity,
        available_credit,
        capacity_shortfall,
        health_factor: credit_health_ratio(liquidation_capacity, account.debt)?,
        state,
    }))
}

fn value_spot(
    account: &CreditAccount,
    market: &CreditMarketSnapshot,
    policy: &CreditPolicy,
    capacity: &mut CreditCapacity,
) -> Result<bool> {
    let mut complete = validate_balances(account, market, policy)?;
    for (&asset, rates) in &policy.assets {
        let (borrowing, liquidation) = eligible_spot(account, asset, policy)?;
        if borrowing == Amount::zero() && liquidation == Amount::zero() {
            continue;
        }
        let Some(quote) = market.prices.get(&asset) else {
            complete = false;
            continue;
        };
        validate_component(quote.price.raw(), MAX_PRICE_RAW, "validating credit price")?;
        if quote.price.raw() == 0 || !is_fresh(quote.observed_at_ms, market) {
            complete = false;
            continue;
        }
        capacity.add(
            collateral_contribution(borrowing, quote.price, rates.max_ltv)?,
            collateral_contribution(liquidation, quote.price, rates.liquidation_threshold)?,
        )?;
    }
    Ok(complete)
}

fn value_perps(
    account: &CreditAccount,
    market: &CreditMarketSnapshot,
    policy: &CreditPolicy,
    capacity: &mut CreditCapacity,
) -> Result<bool> {
    let mut complete = true;
    for snapshot in account.margin_pools.values() {
        let Some(snapshot) = snapshot else {
            complete = false;
            continue;
        };
        if snapshot.source_version == 0 {
            return Err(Error::InvalidCreditInput {
                reason: "perp pool source version is zero",
            });
        }
        validate_signed_value(snapshot.equity.raw(), "validating perp pool equity")?;
        if snapshot.control != CollateralControl::Controlled
            || !is_fresh(snapshot.observed_at_ms, market)
        {
            complete = false;
            continue;
        }
        capacity.add(snapshot.equity, snapshot.equity)?;
    }
    for position in account.perp_positions.values() {
        let Some(pool) = account.margin_pools.get(&position.margin_pool) else {
            return Err(Error::InvalidCreditInput {
                reason: "perp position has no margin pool",
            });
        };
        if position.source_version == 0
            || pool.is_some_and(|pool| pool.source_version != position.source_version)
        {
            return Err(Error::InvalidCreditInput {
                reason: "perp position and pool versions differ",
            });
        }
        validate_signed_value(position.notional.raw(), "validating perp notional")?;
        if let Some(rates) = policy.perps {
            capacity.reserve(position.notional, rates.opening, rates.maintenance)?;
        }
    }
    // Ignoring unconfigured perp exposure could hide losses, unlike disabling a free spot asset.
    if policy.perps.is_none() && !account.margin_pools.is_empty() {
        complete = false;
    }
    Ok(complete)
}

fn is_fresh(observed_at_ms: u64, market: &CreditMarketSnapshot) -> bool {
    market
        .as_of_ms
        .checked_sub(observed_at_ms)
        .is_some_and(|age| age <= market.max_age_ms)
}

#[cfg(test)]
mod eligibility_tests;
#[cfg(test)]
mod perp_tests;
#[cfg(test)]
mod tests;
