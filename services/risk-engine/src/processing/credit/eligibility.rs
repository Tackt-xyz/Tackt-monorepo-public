//! Select collateral from authoritative Ledger buckets; never replay their lifecycle in Risk.

use tackt_primitives::{
    assets::{self, EconomicAsset},
    ledger::{BalanceBucket, LedgerAsset},
};

use crate::processing::{Amount, arithmetic::AtomicAmountAccumulator};
use crate::{Error, Result};

use super::{
    CollateralBalance, CollateralControl, CollateralEligibility, CreditAccount,
    CreditMarketSnapshot, CreditPolicy, PendingDepositPolicy, ReservedSpotPolicy, is_fresh,
};

impl CollateralBalance {
    /// Explains this balance's eligibility under the pinned policy without storing a projection.
    /// Snapshot freshness and quantity validation occur in `evaluate_credit`.
    #[must_use]
    pub fn eligibility(
        &self,
        asset: LedgerAsset,
        bucket: BalanceBucket,
        policy: &CreditPolicy,
    ) -> CollateralEligibility {
        use CollateralEligibility as Eligibility;
        let treatment = match bucket {
            BalanceBucket::Available => Eligibility::Included,
            BalanceBucket::Reserved
            | BalanceBucket::OrderReserved
            | BalanceBucket::WithdrawalReserved => match policy.reserved_spot {
                ReservedSpotPolicy::Excluded => Eligibility::Reserved,
                ReservedSpotPolicy::LiquidationOnly => Eligibility::LiquidationOnly,
            },
            BalanceBucket::Pending => match policy.pending_deposits {
                PendingDepositPolicy::Excluded => return Eligibility::Pending,
                PendingDepositPolicy::Included => Eligibility::Included,
            },
            BalanceBucket::HyperliquidMargin => return Eligibility::MarginEquity,
            BalanceBucket::PrincipalLiability
            | BalanceBucket::LiquidationProceeds
            | BalanceBucket::CustomerSurplus
            | BalanceBucket::ProviderShortfall => return Eligibility::NotCollateral,
        };
        if !policy.supports_representation(asset) {
            return Eligibility::Unconfigured;
        }
        if treatment == Eligibility::Reserved {
            return treatment;
        }
        match self.control {
            CollateralControl::Controlled => treatment,
            CollateralControl::Uncontrolled => Eligibility::Uncontrolled,
            CollateralControl::Unknown => Eligibility::DataIncomplete,
        }
    }
}

pub(super) fn validate_balances(
    account: &CreditAccount,
    market: &CreditMarketSnapshot,
    policy: &CreditPolicy,
) -> Result<bool> {
    let mut complete = account.balances.is_empty() || is_fresh(account.balances_as_of_ms, market);
    for (&(asset, bucket), balance) in &account.balances {
        if matches!(
            bucket,
            BalanceBucket::Pending
                | BalanceBucket::Available
                | BalanceBucket::Reserved
                | BalanceBucket::OrderReserved
                | BalanceBucket::WithdrawalReserved
        ) && balance.quantity < 0
        {
            return Err(Error::InvalidCreditInput {
                reason: "negative collateral balance",
            });
        }
        if balance.quantity == 0 {
            continue;
        }
        match balance.eligibility(asset, bucket, policy) {
            CollateralEligibility::DataIncomplete => complete = false,
            CollateralEligibility::MarginEquity if account.margin_pools.is_empty() => {
                complete = false;
            }
            _ => {}
        }
    }
    Ok(complete)
}

/// Pool exact atomic quantities before rounding, so splitting representations cannot change dust.
pub(super) fn eligible_spot(
    account: &CreditAccount,
    asset: EconomicAsset,
    policy: &CreditPolicy,
) -> Result<(Amount, Amount)> {
    let mut borrowing = AtomicAmountAccumulator::default();
    let mut liquidation = AtomicAmountAccumulator::default();
    for (&(representation, bucket), balance) in &account.balances {
        let info = assets::info(representation);
        if info.classification.economic_asset != asset {
            continue;
        }
        match balance.eligibility(representation, bucket, policy) {
            CollateralEligibility::Included => {
                borrowing.add(balance.quantity, info.decimals)?;
                liquidation.add(balance.quantity, info.decimals)?;
            }
            CollateralEligibility::LiquidationOnly => {
                liquidation.add(balance.quantity, info.decimals)?;
            }
            _ => {}
        }
    }
    Ok((borrowing.finish()?, liquidation.finish()?))
}
