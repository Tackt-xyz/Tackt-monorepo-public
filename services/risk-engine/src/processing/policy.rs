use ethnum::I256;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{FACTOR_SCALE, Factor, MAX_FACTOR_RAW, RiskReason, RiskState, Value};

/// Health thresholds classifying accounts and margin domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskPolicy {
    warning_health: Factor,
    liquidation_health: Factor,
}

impl RiskPolicy {
    /// Constructs policy thresholds.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid threshold ordering.
    pub fn new(warning_health: Factor, liquidation_health: Factor) -> Result<Self> {
        if liquidation_health.raw() < 0
            || warning_health.raw() <= liquidation_health.raw()
            || warning_health.raw() > MAX_FACTOR_RAW
        {
            return Err(Error::InvalidPolicy);
        }
        Ok(Self {
            warning_health,
            liquidation_health,
        })
    }

    #[must_use]
    pub const fn warning_health(&self) -> Factor {
        self.warning_health
    }

    #[must_use]
    pub const fn liquidation_health(&self) -> Factor {
        self.liquidation_health
    }

    #[must_use]
    pub(crate) fn evaluate_account(
        &self,
        collateral: Value,
        debt: Value,
        maintenance: Value,
        has_uncertain_data: bool,
    ) -> RiskState {
        // Aggregates are bounded by MAX_VALUE_RAW, so the pooled difference cannot overflow.
        self.evaluate_margin(
            Value::from_raw(collateral.raw() - debt.raw()),
            maintenance,
            has_uncertain_data,
        )
    }

    #[must_use]
    pub(crate) fn evaluate_margin(
        &self,
        account_value: Value,
        maintenance: Value,
        has_uncertain_data: bool,
    ) -> RiskState {
        if has_uncertain_data {
            RiskState::DataIncomplete
        } else if maintenance.raw() == 0 {
            if account_value.raw() < 0 {
                RiskState::Liquidatable
            } else {
                RiskState::Healthy
            }
        } else if below_threshold(
            account_value.raw(),
            maintenance.raw(),
            self.liquidation_health,
        ) {
            RiskState::Liquidatable
        } else if below_threshold(account_value.raw(), maintenance.raw(), self.warning_health) {
            RiskState::Watch
        } else {
            RiskState::Healthy
        }
    }

    /// Standard Hyperliquid liquidation uses a strict equity/maintenance comparison.
    /// The optional Watch band is Tackt monitoring policy, independent of liquidation policy.
    pub(crate) fn evaluate_venue_margin(
        &self,
        equity: Value,
        maintenance: Value,
        uncertain: bool,
    ) -> RiskState {
        if uncertain {
            RiskState::DataIncomplete
        } else if equity.raw() < maintenance.raw() {
            RiskState::Liquidatable
        } else if maintenance.raw() > 0
            && below_threshold(equity.raw(), maintenance.raw(), self.warning_health)
        {
            RiskState::Watch
        } else {
            RiskState::Healthy
        }
    }

    #[must_use]
    pub(crate) const fn reason(current: RiskState) -> RiskReason {
        match current {
            RiskState::Healthy => RiskReason::Recovered,
            RiskState::DataIncomplete => RiskReason::MissingInput,
            RiskState::Liquidatable => RiskReason::HealthBelowLiquidation,
            RiskState::Watch => RiskReason::HealthBelowWarning,
        }
    }
}

fn below_threshold(numerator: i128, denominator: i128, threshold: Factor) -> bool {
    let left = I256::from(numerator) * I256::from(FACTOR_SCALE);
    let right = I256::from(denominator) * I256::from(threshold.raw());
    left <= right
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RiskPolicy {
        RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(FACTOR_SCALE))
            .expect("valid policy")
    }

    #[test]
    fn account_health_pools_all_value_against_total_maintenance() {
        let policy = policy();
        assert_eq!(
            policy.evaluate_account(
                Value::from_raw(200),
                Value::zero(),
                Value::from_raw(100),
                false
            ),
            RiskState::Healthy
        );
        assert_eq!(
            policy.evaluate_account(
                Value::from_raw(100),
                Value::zero(),
                Value::from_raw(100),
                false
            ),
            RiskState::Liquidatable
        );
    }

    #[test]
    fn missing_data_takes_precedence() {
        assert_eq!(
            policy().evaluate_account(Value::zero(), Value::zero(), Value::zero(), true),
            RiskState::DataIncomplete
        );
    }
}
