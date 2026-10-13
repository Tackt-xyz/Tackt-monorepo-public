use std::collections::BTreeMap;

use tackt_primitives::assets::EconomicAsset;

use super::tests::{spot_balance, spot_key};
use super::*;
use crate::processing::{AMOUNT_SCALE, FACTOR_SCALE, Factor, PRICE_SCALE, Price, VALUE_SCALE};

fn usd(value: i128) -> Value {
    Value::from_raw(value * VALUE_SCALE)
}

pub(super) fn fixture() -> (CreditAccount, CreditMarketSnapshot) {
    let account = CreditAccount {
        version: 1,
        balances: BTreeMap::new(),
        balances_as_of_ms: 9_500,
        margin_pools: BTreeMap::from([(
            0,
            Some(PerpMarginSnapshot {
                control: CollateralControl::Controlled,
                source_version: 1,
                equity: usd(5_000),
                observed_at_ms: 9_500,
            }),
        )]),
        perp_positions: BTreeMap::from([(
            0,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: usd(25_000),
            },
        )]),
        debt: usd(2_500),
        pending_draws: Value::zero(),
    };
    let market = CreditMarketSnapshot {
        version: 1,
        as_of_ms: 10_000,
        max_age_ms: 1_000,
        prices: BTreeMap::new(),
    };
    (account, market)
}

fn complete(account: &CreditAccount, market: &CreditMarketSnapshot) -> CreditHealth {
    let CreditEvaluation::Complete(health) =
        evaluate_credit(account, market, &CreditPolicy::current()).expect("valid portfolio")
    else {
        panic!("expected complete health")
    };
    health
}

#[test]
fn five_x_exposure_has_fifty_percent_borrowing_and_seventy_five_percent_liquidation() {
    let (mut account, market) = fixture();
    let before = account.clone();
    let health = complete(&account, &market);
    assert_eq!(health.borrowing_capacity, usd(2_500));
    assert_eq!(health.liquidation_capacity, usd(3_750));
    assert_eq!(health.available_credit, Value::zero());
    assert_eq!(health.health_factor, Factor::from_raw(1_500_000));
    assert_eq!(health.state, RiskState::Healthy);
    assert_eq!(account, before);
    for (debt, state) in [
        (2_500 * VALUE_SCALE + 1, RiskState::Watch),
        (3_750 * VALUE_SCALE - 1, RiskState::Watch),
        (3_750 * VALUE_SCALE, RiskState::Liquidatable),
        (3_750 * VALUE_SCALE + 1, RiskState::Liquidatable),
    ] {
        account.debt = Value::from_raw(debt);
        assert_eq!(complete(&account, &market).state, state);
    }
}

#[test]
fn spot_and_perps_support_one_debt_including_interest_and_pending_draws() {
    let (mut account, mut market) = fixture();
    account.balances.insert(
        spot_key(EconomicAsset::Sol),
        spot_balance(EconomicAsset::Sol, Amount::from_raw(10 * AMOUNT_SCALE)),
    );
    market.prices.insert(
        EconomicAsset::Sol,
        CreditPrice {
            price: Price::from_raw(100 * PRICE_SCALE),
            observed_at_ms: 9_500,
        },
    );
    // The debt authority has already included $100 accrued interest in this account total.
    account.debt = usd(3_100);
    account.pending_draws = usd(100);
    let health = complete(&account, &market);
    assert_eq!(health.borrowing_capacity, usd(3_350));
    assert_eq!(health.liquidation_capacity, usd(4_650));
    assert_eq!(health.available_credit, usd(150));
    assert_eq!(health.health_factor, Factor::from_raw(1_500_000));
    account.pending_draws = usd(2_000);
    let pending = complete(&account, &market);
    assert_eq!(pending.liquidation_capacity, health.liquidation_capacity);
    assert_eq!(pending.health_factor, health.health_factor);
    assert_eq!(pending.capacity_shortfall, usd(1_750));
    assert_eq!(pending.state, RiskState::Watch);
}

#[test]
fn shared_margin_counts_once_and_opposing_notionals_do_not_net() {
    let (mut account, market) = fixture();
    let expected = complete(&account, &market);
    account.perp_positions = BTreeMap::from([
        (
            1,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: usd(12_500),
            },
        ),
        (
            2,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: usd(-12_500),
            },
        ),
    ]);
    assert_eq!(complete(&account, &market), expected);
}

#[test]
fn negative_pool_contributions_reduce_the_whole_portfolio() {
    let (mut account, market) = fixture();
    account.margin_pools.insert(
        1,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 1,
            equity: usd(-1_000),
            observed_at_ms: 9_500,
        }),
    );
    account.perp_positions.insert(
        1,
        CreditPerpPosition {
            source_version: 1,
            margin_pool: 1,
            notional: usd(-10_000),
        },
    );
    let health = complete(&account, &market);
    assert_eq!(health.borrowing_capacity, usd(500));
    assert_eq!(health.liquidation_capacity, usd(2_250));
    assert_eq!(health.capacity_shortfall, usd(2_000));
    assert_eq!(health.state, RiskState::Liquidatable);
}

#[test]
fn current_equity_and_exposure_replace_the_five_x_illustration() {
    let (mut account, market) = fixture();
    account.margin_pools.insert(
        0,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 2,
            equity: usd(900),
            observed_at_ms: 10_000,
        }),
    );
    account.perp_positions.insert(
        0,
        CreditPerpPosition {
            source_version: 2,
            margin_pool: 0,
            notional: usd(20_000),
        },
    );
    account.debt = usd(100);
    let health = complete(&account, &market);
    assert_eq!(health.borrowing_capacity, usd(-1_100));
    assert_eq!(health.liquidation_capacity, usd(-100));
    assert_eq!(health.health_factor, Factor::from_raw(-FACTOR_SCALE));
    assert_eq!(health.capacity_shortfall, usd(1_200));
    assert_eq!(health.state, RiskState::Liquidatable);
    account.debt = Value::zero();
    let unborrowed = complete(&account, &market);
    assert_eq!(unborrowed.state, RiskState::Watch);
    assert_eq!(unborrowed.health_factor, Factor::from_raw(i128::MAX));
}

#[test]
fn closing_exposure_and_repaying_from_released_equity_recomputes_health() {
    let (mut account, market) = fixture();
    account.debt = usd(3_750);
    assert_eq!(complete(&account, &market).state, RiskState::Liquidatable);
    // Half the notional closes; $2,000 equity is released and applied to debt.
    account.version += 1;
    account.margin_pools.insert(
        0,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 2,
            equity: usd(3_000),
            observed_at_ms: 10_000,
        }),
    );
    account.perp_positions.insert(
        0,
        CreditPerpPosition {
            source_version: 2,
            margin_pool: 0,
            notional: usd(12_500),
        },
    );
    account.debt = usd(1_750);
    let health = complete(&account, &market);
    assert_eq!(health.borrowing_capacity, usd(1_750));
    assert_eq!(health.liquidation_capacity, usd(2_375));
    assert_eq!(health.state, RiskState::Healthy);
}

#[test]
fn missing_stale_and_future_pool_snapshots_return_no_measurements() {
    let (account, market) = fixture();
    for snapshot in [
        None,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 1,
            equity: usd(5_000),
            observed_at_ms: 8_999,
        }),
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 1,
            equity: usd(5_000),
            observed_at_ms: 10_001,
        }),
    ] {
        let mut changed = account.clone();
        changed.margin_pools.insert(0, snapshot);
        assert_eq!(
            evaluate_credit(&changed, &market, &CreditPolicy::current()),
            Ok(CreditEvaluation::DataIncomplete)
        );
    }
    let mut boundary = market.clone();
    boundary.as_of_ms = 10_500;
    assert_eq!(complete(&account, &boundary).borrowing_capacity, usd(2_500));
    boundary.as_of_ms += 1;
    assert_eq!(
        evaluate_credit(&account, &boundary, &CreditPolicy::current()),
        Ok(CreditEvaluation::DataIncomplete)
    );
}

#[test]
fn pool_membership_and_snapshot_versions_fail_closed() {
    let (account, market) = fixture();
    let policy = CreditPolicy::current();
    let mut changed = account.clone();
    changed.margin_pools.clear();
    assert!(evaluate_credit(&changed, &market, &policy).is_err());
    for version in [0, 2] {
        let mut changed = account.clone();
        changed
            .perp_positions
            .get_mut(&0)
            .expect("position")
            .source_version = version;
        assert!(evaluate_credit(&changed, &market, &policy).is_err());
    }
    let mut changed = account;
    changed.margin_pools.insert(
        0,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 0,
            equity: usd(5_000),
            observed_at_ms: 9_500,
        }),
    );
    assert!(evaluate_credit(&changed, &market, &policy).is_err());
}

#[test]
fn current_policy_sets_perp_reserves_and_spot_only_policy_requires_complete_pools() {
    let (mut account, market) = fixture();
    let current = CreditPolicy::current();
    assert_eq!(
        current.perps,
        Some(PerpReservePolicy {
            opening: Factor::from_raw(100_000),
            maintenance: Factor::from_raw(50_000),
        })
    );
    let spot = super::tests::spot_policy();
    assert_eq!(
        evaluate_credit(&account, &market, &spot),
        Ok(CreditEvaluation::DataIncomplete)
    );
    account.margin_pools.clear();
    account.perp_positions.clear();
    account.debt = Value::zero();
    assert!(matches!(
        evaluate_credit(&account, &market, &spot),
        Ok(CreditEvaluation::Complete(_))
    ));
}

#[test]
fn reserve_policy_rejects_invalid_rates() {
    for (opening, maintenance) in [
        (0, 0),
        (100_000, -1),
        (100_000, 100_000),
        (50_000, 100_000),
        (FACTOR_SCALE + 1, 50_000),
    ] {
        assert_eq!(
            CreditPolicy::new(
                BTreeMap::new(),
                Some(PerpReservePolicy {
                    opening: Factor::from_raw(opening),
                    maintenance: Factor::from_raw(maintenance),
                }),
                ReservedSpotPolicy::Excluded,
                PendingDepositPolicy::Excluded
            ),
            Err(Error::InvalidPolicy)
        );
    }
}

#[test]
fn each_positions_reserves_round_up_and_signed_health_rounds_down() {
    let (mut account, market) = fixture();
    account.margin_pools.insert(
        0,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 1,
            equity: Value::from_raw(1),
            observed_at_ms: 9_500,
        }),
    );
    account.perp_positions = BTreeMap::from([
        (
            1,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: Value::from_raw(1),
            },
        ),
        (
            2,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: Value::from_raw(-1),
            },
        ),
    ]);
    account.debt = Value::from_raw(3);
    let health = complete(&account, &market);
    assert_eq!(health.borrowing_capacity, Value::from_raw(-1));
    assert_eq!(health.liquidation_capacity, Value::from_raw(-1));
    assert_eq!(health.health_factor, Factor::from_raw(-333_334));
    assert_eq!(health.capacity_shortfall, Value::from_raw(4));
}

#[test]
fn invalid_signed_inputs_and_output_overflow_are_rejected() {
    let (account, market) = fixture();
    for raw in [i128::MIN, i128::MAX, MAX_VALUE_RAW + 1, -MAX_VALUE_RAW - 1] {
        let mut changed = account.clone();
        changed
            .perp_positions
            .get_mut(&0)
            .expect("position")
            .notional = Value::from_raw(raw);
        assert!(evaluate_credit(&changed, &market, &CreditPolicy::current()).is_err());
        let mut changed = account.clone();
        changed.margin_pools.insert(
            0,
            Some(PerpMarginSnapshot {
                control: CollateralControl::Controlled,
                source_version: 1,
                equity: Value::from_raw(raw),
                observed_at_ms: 9_500,
            }),
        );
        assert!(evaluate_credit(&changed, &market, &CreditPolicy::current()).is_err());
    }
    let mut changed = account;
    changed.perp_positions.clear();
    changed.margin_pools.insert(
        0,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 1,
            equity: Value::from_raw(-MAX_VALUE_RAW),
            observed_at_ms: 9_500,
        }),
    );
    // Even though signed capacity and debt are individually bounded, their shortfall is not.
    assert!(evaluate_credit(&changed, &market, &CreditPolicy::current()).is_err());
}

#[test]
fn signed_pooling_does_not_depend_on_the_order_of_positive_and_negative_equity() {
    let (mut account, market) = fixture();
    account.debt = Value::zero();
    account.perp_positions.clear();
    for equities in [
        [MAX_VALUE_RAW, MAX_VALUE_RAW, -MAX_VALUE_RAW],
        [-MAX_VALUE_RAW, MAX_VALUE_RAW, MAX_VALUE_RAW],
    ] {
        account.margin_pools = [0, 1, 2]
            .into_iter()
            .zip(equities)
            .map(|(id, equity)| {
                (
                    id,
                    Some(PerpMarginSnapshot {
                        control: CollateralControl::Controlled,
                        source_version: 1,
                        equity: Value::from_raw(equity),
                        observed_at_ms: 9_500,
                    }),
                )
            })
            .collect();
        let health = complete(&account, &market);
        assert_eq!(health.borrowing_capacity, Value::from_raw(MAX_VALUE_RAW));
        assert_eq!(health.liquidation_capacity, Value::from_raw(MAX_VALUE_RAW));
    }
}
