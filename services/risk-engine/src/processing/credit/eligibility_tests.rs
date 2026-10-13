use std::collections::BTreeMap;

use tackt_primitives::{
    assets::{self, EconomicAsset},
    ledger::{BalanceBucket, LedgerAsset},
};

use super::tests::{complete, fixture, spot_policy};
use super::*;
use crate::processing::{AMOUNT_SCALE, MAX_AMOUNT_RAW, VALUE_SCALE};

fn balance(asset: LedgerAsset, units: i128) -> CollateralBalance {
    CollateralBalance {
        quantity: units * 10_i128.pow(u32::from(assets::info(asset).decimals)),
        control: CollateralControl::Controlled,
    }
}

#[test]
fn registered_representations_pool_using_registry_decimals() {
    let (mut account, _) = fixture();
    for (group, first, second) in [
        (EconomicAsset::Sol, LedgerAsset::Sol, LedgerAsset::SolSolana),
        (EconomicAsset::Eth, LedgerAsset::Eth, LedgerAsset::EthBase),
    ] {
        account.balances = BTreeMap::from([
            ((first, BalanceBucket::Available), balance(first, 5)),
            ((second, BalanceBucket::Available), balance(second, 5)),
        ]);
        assert_eq!(
            eligible_spot(&account, group, &CreditPolicy::current()),
            Ok((
                Amount::from_raw(10 * AMOUNT_SCALE),
                Amount::from_raw(10 * AMOUNT_SCALE)
            ))
        );
    }
}

#[test]
fn splitting_dust_between_representations_does_not_change_eligible_quantity() {
    let (mut account, _) = fixture();
    account.balances.clear();
    for asset in [LedgerAsset::Eth, LedgerAsset::EthBase] {
        let atomic_per_risk_unit =
            10_i128.pow(u32::from(assets::info(asset).decimals)) / AMOUNT_SCALE;
        account.balances.insert(
            (asset, BalanceBucket::Available),
            CollateralBalance {
                quantity: atomic_per_risk_unit - 1,
                control: CollateralControl::Controlled,
            },
        );
    }
    assert_eq!(
        eligible_spot(&account, EconomicAsset::Eth, &CreditPolicy::current()),
        Ok((Amount::from_raw(1), Amount::from_raw(1)))
    );
}

#[test]
fn bucket_explanations_are_explicit_and_margin_is_never_free_spot() {
    let available = balance(LedgerAsset::SolSolana, 10);
    for (bucket, expected) in [
        (BalanceBucket::Pending, CollateralEligibility::Included),
        (BalanceBucket::Available, CollateralEligibility::Included),
        (
            BalanceBucket::Reserved,
            CollateralEligibility::LiquidationOnly,
        ),
        (
            BalanceBucket::OrderReserved,
            CollateralEligibility::LiquidationOnly,
        ),
        (
            BalanceBucket::WithdrawalReserved,
            CollateralEligibility::LiquidationOnly,
        ),
        (
            BalanceBucket::HyperliquidMargin,
            CollateralEligibility::MarginEquity,
        ),
        (
            BalanceBucket::PrincipalLiability,
            CollateralEligibility::NotCollateral,
        ),
        (
            BalanceBucket::LiquidationProceeds,
            CollateralEligibility::NotCollateral,
        ),
        (
            BalanceBucket::CustomerSurplus,
            CollateralEligibility::NotCollateral,
        ),
        (
            BalanceBucket::ProviderShortfall,
            CollateralEligibility::NotCollateral,
        ),
    ] {
        assert_eq!(
            available.eligibility(LedgerAsset::SolSolana, bucket, &CreditPolicy::current()),
            expected
        );
    }
    assert_eq!(
        balance(LedgerAsset::UsdcSolana, 10).eligibility(
            LedgerAsset::UsdcSolana,
            BalanceBucket::Available,
            &CreditPolicy::current()
        ),
        CollateralEligibility::Unconfigured
    );
}

#[test]
fn initial_policy_waits_for_ledger_to_finalize_pending_deposits() {
    let (mut account, market) = fixture();
    account.balances = BTreeMap::from([(
        (LedgerAsset::SolSolana, BalanceBucket::Pending),
        balance(LedgerAsset::SolSolana, 10),
    )]);
    assert_eq!(
        complete(&account, &market, &spot_policy()).borrowing_capacity,
        Value::zero()
    );
    let deposited = account
        .balances
        .remove(&(LedgerAsset::SolSolana, BalanceBucket::Pending))
        .expect("pending");
    account.balances.insert(
        (LedgerAsset::SolSolana, BalanceBucket::Available),
        deposited,
    );
    assert_eq!(
        complete(&account, &market, &spot_policy()).borrowing_capacity,
        Value::from_raw(850 * VALUE_SCALE)
    );
}

#[test]
fn reserve_release_and_settlement_change_capacity_once() {
    let (mut account, market) = fixture();
    account.debt = Value::from_raw(500 * VALUE_SCALE);
    let policy = CreditPolicy::current();
    account.balances = BTreeMap::from([
        (
            (LedgerAsset::SolSolana, BalanceBucket::Available),
            balance(LedgerAsset::SolSolana, 8),
        ),
        (
            (LedgerAsset::SolSolana, BalanceBucket::WithdrawalReserved),
            balance(LedgerAsset::SolSolana, 2),
        ),
    ]);
    let reserved = complete(&account, &market, &policy);
    assert_eq!(
        reserved.borrowing_capacity,
        Value::from_raw(680 * VALUE_SCALE)
    );
    assert_eq!(
        reserved.liquidation_capacity,
        Value::from_raw(900 * VALUE_SCALE)
    );
    assert_eq!(
        reserved.available_credit,
        Value::from_raw(180 * VALUE_SCALE)
    );
    // Cancellation: Ledger returns the funds to Available.
    account
        .balances
        .remove(&(LedgerAsset::SolSolana, BalanceBucket::WithdrawalReserved));
    account.balances.insert(
        (LedgerAsset::SolSolana, BalanceBucket::Available),
        balance(LedgerAsset::SolSolana, 10),
    );
    assert_eq!(
        complete(&account, &market, &policy).available_credit,
        Value::from_raw(350 * VALUE_SCALE)
    );
    // Settlement: the two SOL have left custody; only the remaining eight are supplied.
    account.balances.insert(
        (LedgerAsset::SolSolana, BalanceBucket::Available),
        balance(LedgerAsset::SolSolana, 8),
    );
    let settled = complete(&account, &market, &policy);
    assert_eq!(settled.borrowing_capacity, reserved.borrowing_capacity);
    assert_eq!(
        settled.liquidation_capacity,
        Value::from_raw(720 * VALUE_SCALE)
    );
}

#[test]
fn reservations_of_drawn_funds_do_not_create_a_second_debt() {
    let (mut account, market) = fixture();
    account.debt = Value::from_raw(500 * VALUE_SCALE);
    account.balances = BTreeMap::from([
        (
            (LedgerAsset::SolSolana, BalanceBucket::Available),
            balance(LedgerAsset::SolSolana, 8),
        ),
        (
            (LedgerAsset::SolSolana, BalanceBucket::OrderReserved),
            balance(LedgerAsset::SolSolana, 2),
        ),
        (
            (LedgerAsset::Usdc, BalanceBucket::PrincipalLiability),
            balance(LedgerAsset::Usdc, -500),
        ),
    ]);
    let health = complete(&account, &market, &CreditPolicy::current());
    assert_eq!(health.available_credit, Value::from_raw(180 * VALUE_SCALE));
    assert_eq!(
        health.liquidation_capacity,
        Value::from_raw(900 * VALUE_SCALE)
    );
}

#[test]
fn missing_control_and_stale_spot_snapshots_cannot_produce_usable_health() {
    let (mut account, mut market) = fixture();
    let policy = spot_policy();
    for timestamp in [8_999, 10_001] {
        account.balances_as_of_ms = timestamp;
        assert_eq!(
            evaluate_credit(&account, &market, &policy),
            Ok(CreditEvaluation::DataIncomplete)
        );
    }
    account.balances_as_of_ms = 9_000;
    assert!(matches!(
        evaluate_credit(&account, &market, &policy),
        Ok(CreditEvaluation::Complete(_))
    ));
    account
        .balances
        .values_mut()
        .next()
        .expect("balance")
        .control = CollateralControl::Unknown;
    assert_eq!(
        evaluate_credit(&account, &market, &policy),
        Ok(CreditEvaluation::DataIncomplete)
    );
    account
        .balances
        .values_mut()
        .next()
        .expect("balance")
        .control = CollateralControl::Uncontrolled;
    market.prices.clear();
    assert_eq!(
        complete(&account, &market, &policy).borrowing_capacity,
        Value::zero()
    );
}

#[test]
fn margin_cash_is_covered_by_verified_pool_equity_once() {
    let (mut account, market) = super::perp_tests::fixture();
    let policy = CreditPolicy::current();
    account.balances.insert(
        (LedgerAsset::Usdc, BalanceBucket::HyperliquidMargin),
        balance(LedgerAsset::Usdc, 5_000),
    );
    assert_eq!(
        complete(&account, &market, &policy).borrowing_capacity,
        Value::from_raw(2_500 * VALUE_SCALE)
    );
    for control in [CollateralControl::Unknown, CollateralControl::Uncontrolled] {
        let snapshot = account
            .margin_pools
            .get_mut(&0)
            .expect("pool")
            .as_mut()
            .expect("snapshot");
        snapshot.control = control;
        snapshot.equity = Value::from_raw(-500 * VALUE_SCALE);
        assert_eq!(
            evaluate_credit(&account, &market, &policy),
            Ok(CreditEvaluation::DataIncomplete)
        );
    }
    account.margin_pools.clear();
    account.perp_positions.clear();
    assert_eq!(
        evaluate_credit(&account, &market, &policy),
        Ok(CreditEvaluation::DataIncomplete)
    );
}

#[test]
fn reserved_collateral_follows_configured_eligibility() {
    let (mut account, market) = fixture();
    account.balances = BTreeMap::from([(
        (LedgerAsset::SolSolana, BalanceBucket::Reserved),
        balance(LedgerAsset::SolSolana, 10),
    )]);
    let before = complete(&account, &market, &spot_policy());
    assert_eq!(before.borrowing_capacity, Value::zero());
    assert_eq!(before.liquidation_capacity, Value::zero());
    let after = complete(&account, &market, &CreditPolicy::current());
    assert_eq!(after.borrowing_capacity, Value::zero());
    assert_eq!(
        after.liquidation_capacity,
        Value::from_raw(900 * VALUE_SCALE)
    );
}

#[test]
fn invalid_atomic_quantities_and_pooled_overflow_fail_closed() {
    let (mut account, market) = fixture();
    account.balances.clear();
    for quantity in [-1, i128::MAX, MAX_AMOUNT_RAW + 1] {
        account.balances.insert(
            (LedgerAsset::SolSolana, BalanceBucket::Available),
            CollateralBalance {
                quantity,
                control: CollateralControl::Controlled,
            },
        );
        assert!(evaluate_credit(&account, &market, &spot_policy()).is_err());
    }
    for asset in [LedgerAsset::Sol, LedgerAsset::SolSolana] {
        account.balances.insert(
            (asset, BalanceBucket::Available),
            CollateralBalance {
                quantity: MAX_AMOUNT_RAW,
                control: CollateralControl::Controlled,
            },
        );
    }
    assert!(evaluate_credit(&account, &market, &spot_policy()).is_err());
}

#[test]
fn pending_deposits_count_once_through_finalization_and_reorg_removal() {
    let (mut account, market) = fixture();
    let policy = CreditPolicy::current();
    account.debt = Value::from_raw(800 * VALUE_SCALE);
    account.pending_draws = Value::from_raw(20 * VALUE_SCALE);
    account.balances = BTreeMap::from([
        (
            (LedgerAsset::SolSolana, BalanceBucket::Available),
            balance(LedgerAsset::SolSolana, 8),
        ),
        (
            (LedgerAsset::SolSolana, BalanceBucket::Pending),
            balance(LedgerAsset::SolSolana, 2),
        ),
    ]);
    let pending_account = account.clone();
    let pending = complete(&account, &market, &policy);
    assert_eq!(
        pending.borrowing_capacity,
        Value::from_raw(850 * VALUE_SCALE)
    );
    assert_eq!(
        pending.liquidation_capacity,
        Value::from_raw(900 * VALUE_SCALE)
    );
    assert_eq!(pending.available_credit, Value::from_raw(30 * VALUE_SCALE));
    assert_eq!(pending.state, RiskState::Healthy);

    // Ledger moves Pending to Available on finalization; total collateral stays the same.
    account.version += 1;
    account
        .balances
        .remove(&(LedgerAsset::SolSolana, BalanceBucket::Pending));
    account.balances.insert(
        (LedgerAsset::SolSolana, BalanceBucket::Available),
        balance(LedgerAsset::SolSolana, 10),
    );
    assert_eq!(complete(&account, &market, &policy), pending);

    // Alternative next snapshot: Ledger removes the reorged deposit, retaining drawn debt.
    let mut reorged = pending_account;
    reorged.version += 1;
    reorged
        .balances
        .remove(&(LedgerAsset::SolSolana, BalanceBucket::Pending));
    let health = complete(&reorged, &market, &policy);
    assert_eq!(
        health.borrowing_capacity,
        Value::from_raw(680 * VALUE_SCALE)
    );
    assert_eq!(
        health.liquidation_capacity,
        Value::from_raw(720 * VALUE_SCALE)
    );
    assert_eq!(health.available_credit, Value::zero());
    assert_eq!(
        health.capacity_shortfall,
        Value::from_raw(140 * VALUE_SCALE)
    );
    assert_eq!(health.state, RiskState::Liquidatable);
}

#[test]
fn pending_deposit_eligibility_follows_configured_policy() {
    let (mut account, market) = fixture();
    account.balances = BTreeMap::from([(
        (LedgerAsset::SolSolana, BalanceBucket::Pending),
        balance(LedgerAsset::SolSolana, 10),
    )]);
    let excluded = complete(&account, &market, &spot_policy());
    assert_eq!(excluded.borrowing_capacity, Value::zero());
    assert_eq!(excluded.liquidation_capacity, Value::zero());
    assert_eq!(
        complete(&account, &market, &CreditPolicy::current()).borrowing_capacity,
        Value::from_raw(850 * VALUE_SCALE)
    );
}

#[test]
fn pending_deposits_require_control_freshness_price_and_valid_quantity() {
    let (mut account, mut market) = fixture();
    let policy = CreditPolicy::current();
    let key = (LedgerAsset::SolSolana, BalanceBucket::Pending);
    account.balances = BTreeMap::from([(key, balance(key.0, 10))]);
    let valid = account.clone();
    account.balances.get_mut(&key).expect("pending").control = CollateralControl::Unknown;
    assert_eq!(
        evaluate_credit(&account, &market, &policy),
        Ok(CreditEvaluation::DataIncomplete)
    );
    account.balances.get_mut(&key).expect("pending").control = CollateralControl::Uncontrolled;
    let health = complete(&account, &market, &policy);
    assert_eq!(health.borrowing_capacity, Value::zero());
    assert_eq!(health.liquidation_capacity, Value::zero());
    account = valid;
    account.balances_as_of_ms = 8_999;
    assert_eq!(
        evaluate_credit(&account, &market, &policy),
        Ok(CreditEvaluation::DataIncomplete)
    );
    account.balances_as_of_ms = 9_500;
    market.prices.clear();
    assert_eq!(
        evaluate_credit(&account, &market, &policy),
        Ok(CreditEvaluation::DataIncomplete)
    );
    account.balances.get_mut(&key).expect("pending").quantity = -1;
    assert!(evaluate_credit(&account, &market, &policy).is_err());
}

#[test]
fn onboarding_does_not_silently_admit_new_instruments_to_collateral() {
    let new_assets = [
        LedgerAsset::Wsol,
        LedgerAsset::Jitosol,
        LedgerAsset::Msol,
        LedgerAsset::Jupsol,
        LedgerAsset::Weth,
        LedgerAsset::Steth,
        LedgerAsset::Wsteth,
        LedgerAsset::Cbeth,
        LedgerAsset::Cbbtc,
        LedgerAsset::Tbtc,
        LedgerAsset::Lbtc,
        LedgerAsset::Solvbtc,
    ];
    for policy in [spot_policy(), CreditPolicy::current()] {
        let (mut account, mut prices) = fixture();
        account.balances.clear();
        prices.prices.clear();
        for asset in new_assets {
            let balance = balance(asset, 100);
            assert_eq!(
                balance.eligibility(asset, BalanceBucket::Available, &policy),
                CollateralEligibility::Unconfigured
            );
            for bucket in [
                BalanceBucket::Available,
                BalanceBucket::Pending,
                BalanceBucket::Reserved,
                BalanceBucket::WithdrawalReserved,
            ] {
                account.balances.insert((asset, bucket), balance);
            }
        }
        // No underlying price is required: these balances have no configured conversion or
        // collateral treatment, even though their economic families have policy rates.
        let health = complete(&account, &prices, &policy);
        assert_eq!(health.borrowing_capacity, Value::zero());
        assert_eq!(health.liquidation_capacity, Value::zero());
    }
}
