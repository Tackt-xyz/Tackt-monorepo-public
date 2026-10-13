use std::collections::BTreeMap;

use tackt_primitives::assets::{self, EconomicAsset};
use tackt_primitives::ledger::{BalanceBucket, LedgerAsset};

use super::*;
use crate::processing::{
    AMOUNT_SCALE, FACTOR_SCALE, Factor, MAX_AMOUNT_RAW, PRICE_SCALE, Price, VALUE_SCALE,
};

pub(super) fn spot_policy() -> CreditPolicy {
    CreditPolicy::new(
        CreditPolicy::current().assets,
        None,
        ReservedSpotPolicy::Excluded,
        PendingDepositPolicy::Excluded,
    )
    .expect("test policy")
}
pub(super) fn fixture() -> (CreditAccount, CreditMarketSnapshot) {
    let account = CreditAccount {
        version: 1,
        margin_pools: BTreeMap::new(),
        perp_positions: BTreeMap::new(),
        balances: BTreeMap::from([(
            spot_key(EconomicAsset::Sol),
            spot_balance(EconomicAsset::Sol, Amount::from_raw(10 * AMOUNT_SCALE)),
        )]),
        balances_as_of_ms: 9_500,
        debt: Value::zero(),
        pending_draws: Value::zero(),
    };
    let prices = CreditMarketSnapshot {
        version: 1,
        as_of_ms: 10_000,
        max_age_ms: 1_000,
        prices: BTreeMap::from([(
            EconomicAsset::Sol,
            CreditPrice {
                price: Price::from_raw(100 * PRICE_SCALE),
                observed_at_ms: 9_500,
            },
        )]),
    };
    (account, prices)
}

pub(super) fn complete(
    account: &CreditAccount,
    prices: &CreditMarketSnapshot,
    policy: &CreditPolicy,
) -> CreditHealth {
    let CreditEvaluation::Complete(health) =
        evaluate_credit(account, prices, policy).expect("valid credit snapshot")
    else {
        panic!("expected complete health")
    };
    health
}

#[test]
fn mixed_collateral_supports_one_account_debt() {
    let (mut account, mut prices) = fixture();
    for (asset, price) in [(EconomicAsset::Eth, 2_000), (EconomicAsset::Btc, 50_000)] {
        account.balances.insert(
            spot_key(asset),
            spot_balance(asset, Amount::from_raw(AMOUNT_SCALE)),
        );
        prices.prices.insert(
            asset,
            CreditPrice {
                price: Price::from_raw(price * PRICE_SCALE),
                observed_at_ms: 9_500,
            },
        );
    }
    account.debt = Value::from_raw(40_000 * VALUE_SCALE);
    account.pending_draws = Value::from_raw(1_000 * VALUE_SCALE);
    let policy = spot_policy();
    let before = (account.clone(), prices.clone(), policy.clone());
    let health = complete(&account, &prices, &policy);
    assert_eq!(
        health.borrowing_capacity,
        Value::from_raw(45_050 * VALUE_SCALE)
    );
    assert_eq!(
        health.liquidation_capacity,
        Value::from_raw(47_700 * VALUE_SCALE)
    );
    assert_eq!(
        health.available_credit,
        Value::from_raw(4_050 * VALUE_SCALE)
    );
    assert_eq!(health.capacity_shortfall, Value::zero());
    assert_eq!(health.health_factor, Factor::from_raw(1_192_500));
    assert_eq!(health.state, RiskState::Healthy);
    assert_eq!(health, complete(&account, &prices, &policy));
    assert_eq!(before, (account, prices, policy));
}

#[test]
fn exact_borrowing_and_liquidation_boundaries_use_values_not_rounded_ratio() {
    let (mut account, prices) = fixture();
    let policy = spot_policy();
    for (debt, state) in [
        (850 * VALUE_SCALE - 1, RiskState::Healthy),
        (850 * VALUE_SCALE, RiskState::Healthy),
        (850 * VALUE_SCALE + 1, RiskState::Watch),
        (900 * VALUE_SCALE - 1, RiskState::Watch),
        (900 * VALUE_SCALE, RiskState::Liquidatable),
        (900 * VALUE_SCALE + 1, RiskState::Liquidatable),
    ] {
        account.debt = Value::from_raw(debt);
        let health = complete(&account, &prices, &policy);
        assert_eq!(health.state, state, "debt={debt}");
        if debt == 900 * VALUE_SCALE - 1 {
            assert_eq!(health.health_factor, Factor::from_raw(FACTOR_SCALE));
        }
    }
}

#[test]
fn undrawn_commitments_reduce_credit_without_creating_liquidation_debt() {
    let (mut account, prices) = fixture();
    account.debt = Value::from_raw(800 * VALUE_SCALE);
    account.pending_draws = Value::from_raw(200 * VALUE_SCALE);
    let health = complete(&account, &prices, &spot_policy());
    assert_eq!(health.available_credit, Value::zero());
    assert_eq!(
        health.capacity_shortfall,
        Value::from_raw(150 * VALUE_SCALE)
    );
    assert_eq!(health.health_factor, Factor::from_raw(1_125_000));
    assert_eq!(health.state, RiskState::Watch);
    account.debt = Value::zero();
    account.pending_draws = Value::from_raw(1_000 * VALUE_SCALE);
    let health = complete(&account, &prices, &spot_policy());
    assert_eq!(health.health_factor, Factor::from_raw(i128::MAX));
    assert_eq!(health.state, RiskState::Watch);
}

#[test]
fn empty_account_is_healthy_but_unbacked_debt_is_liquidatable() {
    let (mut account, prices) = fixture();
    account.balances.clear();
    let policy = spot_policy();
    let health = complete(&account, &prices, &policy);
    assert_eq!(health.state, RiskState::Healthy);
    assert_eq!(health.available_credit, Value::zero());
    assert_eq!(health.health_factor, Factor::from_raw(i128::MAX));
    account.debt = Value::from_raw(1);
    let health = complete(&account, &prices, &policy);
    assert_eq!(health.state, RiskState::Liquidatable);
    assert_eq!(health.health_factor, Factor::zero());
}

#[test]
fn missing_policy_and_zero_quantity_need_no_price() {
    let (mut account, mut prices) = fixture();
    account.balances.insert(
        spot_key(EconomicAsset::Sol),
        spot_balance(EconomicAsset::Sol, Amount::zero()),
    );
    account.balances.insert(
        spot_key(EconomicAsset::Usdc),
        spot_balance(EconomicAsset::Usdc, Amount::from_raw(1_000 * AMOUNT_SCALE)),
    );
    prices.prices.clear();
    let health = complete(&account, &prices, &spot_policy());
    assert_eq!(health.borrowing_capacity, Value::zero());
    assert_eq!(health.liquidation_capacity, Value::zero());
}

#[test]
fn missing_stale_future_and_zero_prices_return_no_partial_health() {
    let (mut account, prices) = fixture();
    account.balances.insert(
        spot_key(EconomicAsset::Eth),
        spot_balance(EconomicAsset::Eth, Amount::from_raw(AMOUNT_SCALE)),
    );
    for quote in [
        None,
        Some(CreditPrice {
            price: Price::from_raw(PRICE_SCALE),
            observed_at_ms: 8_999,
        }),
        Some(CreditPrice {
            price: Price::from_raw(PRICE_SCALE),
            observed_at_ms: 10_001,
        }),
        Some(CreditPrice {
            price: Price::zero(),
            observed_at_ms: 10_000,
        }),
    ] {
        let mut prices = prices.clone();
        if let Some(quote) = quote {
            prices.prices.insert(EconomicAsset::Eth, quote);
        }
        assert_eq!(
            evaluate_credit(&account, &prices, &spot_policy()),
            Ok(CreditEvaluation::DataIncomplete)
        );
    }
}

#[test]
fn freshness_boundary_and_price_changes_are_evaluated_just_in_time() {
    let (mut account, mut prices) = fixture();
    let policy = spot_policy();
    prices.as_of_ms = 10_500;
    assert_eq!(
        complete(&account, &prices, &policy).borrowing_capacity,
        Value::from_raw(850 * VALUE_SCALE)
    );
    prices.as_of_ms += 1;
    assert_eq!(
        evaluate_credit(&account, &prices, &policy),
        Ok(CreditEvaluation::DataIncomplete)
    );
    prices.version += 1;
    account.balances_as_of_ms = prices.as_of_ms;
    prices.prices.insert(
        EconomicAsset::Sol,
        CreditPrice {
            price: Price::from_raw(50 * PRICE_SCALE),
            observed_at_ms: prices.as_of_ms,
        },
    );
    assert_eq!(
        complete(&account, &prices, &policy).borrowing_capacity,
        Value::from_raw(425 * VALUE_SCALE)
    );
}

#[test]
fn per_asset_rates_can_be_added_without_changing_the_calculator() {
    let (mut account, mut prices) = fixture();
    account.balances.insert(
        spot_key(EconomicAsset::Usdc),
        spot_balance(EconomicAsset::Usdc, Amount::from_raw(1_000 * AMOUNT_SCALE)),
    );
    prices.prices.insert(
        EconomicAsset::Usdc,
        CreditPrice {
            price: Price::from_raw(PRICE_SCALE),
            observed_at_ms: 10_000,
        },
    );
    let mut rates = spot_policy().assets;
    rates.insert(
        EconomicAsset::Usdc,
        CreditAssetPolicy {
            max_ltv: Factor::from_raw(500_000),
            liquidation_threshold: Factor::from_raw(600_000),
        },
    );
    let policy = CreditPolicy::new(
        rates,
        None,
        ReservedSpotPolicy::Excluded,
        PendingDepositPolicy::Excluded,
    )
    .expect("test-only policy");
    let health = complete(&account, &prices, &policy);
    assert_eq!(
        health.borrowing_capacity,
        Value::from_raw(1_350 * VALUE_SCALE)
    );
    assert_eq!(
        health.liquidation_capacity,
        Value::from_raw(1_500 * VALUE_SCALE)
    );
}

#[test]
fn policy_rejects_invalid_rates() {
    for (ltv, threshold) in [
        (-1, 900_000),
        (0, 0),
        (900_000, 900_000),
        (900_001, 900_000),
        (850_000, 1_000_001),
    ] {
        assert_eq!(
            CreditPolicy::new(
                BTreeMap::from([(
                    EconomicAsset::Sol,
                    CreditAssetPolicy {
                        max_ltv: Factor::from_raw(ltv),
                        liquidation_threshold: Factor::from_raw(threshold),
                    }
                )]),
                None,
                ReservedSpotPolicy::Excluded,
                PendingDepositPolicy::Excluded
            ),
            Err(Error::InvalidPolicy)
        );
    }
    assert!(
        CreditPolicy::new(
            BTreeMap::from([(
                EconomicAsset::Sol,
                CreditAssetPolicy {
                    max_ltv: Factor::zero(),
                    liquidation_threshold: Factor::from_raw(FACTOR_SCALE),
                }
            )]),
            None,
            ReservedSpotPolicy::Excluded,
            PendingDepositPolicy::Excluded
        )
        .is_ok()
    );
}

#[test]
fn capacities_round_down_once_per_pooled_asset() {
    let (mut account, mut prices) = fixture();
    account.balances.insert(
        spot_key(EconomicAsset::Sol),
        spot_balance(EconomicAsset::Sol, Amount::from_raw(AMOUNT_SCALE + 1)),
    );
    prices.prices.insert(
        EconomicAsset::Sol,
        CreditPrice {
            price: Price::from_raw(101),
            observed_at_ms: 10_000,
        },
    );
    account.debt = Value::from_raw(1);
    let health = complete(&account, &prices, &spot_policy());
    assert_eq!(health.borrowing_capacity, Value::from_raw(85));
    assert_eq!(health.liquidation_capacity, Value::from_raw(90));
    assert_eq!(health.available_credit, Value::from_raw(84));
}

#[test]
fn invalid_amounts_prices_and_overflow_fail_closed() {
    let (account, prices) = fixture();
    let policy = spot_policy();
    for value in [-1, MAX_VALUE_RAW + 1] {
        let mut changed = account.clone();
        changed.debt = Value::from_raw(value);
        assert!(evaluate_credit(&changed, &prices, &policy).is_err());
        changed.debt = Value::zero();
        changed.pending_draws = Value::from_raw(value);
        assert!(evaluate_credit(&changed, &prices, &policy).is_err());
    }
    for amount in [-1, MAX_AMOUNT_RAW + 1] {
        let mut changed = account.clone();
        changed.balances.insert(
            spot_key(EconomicAsset::Sol),
            spot_balance(EconomicAsset::Sol, Amount::from_raw(amount)),
        );
        assert!(evaluate_credit(&changed, &prices, &policy).is_err());
    }
    for price in [-1, MAX_PRICE_RAW + 1] {
        let mut changed = prices.clone();
        changed
            .prices
            .get_mut(&EconomicAsset::Sol)
            .expect("quote")
            .price = Price::from_raw(price);
        assert!(evaluate_credit(&account, &changed, &policy).is_err());
    }
    let mut changed = account.clone();
    changed.debt = Value::from_raw(MAX_VALUE_RAW);
    changed.pending_draws = Value::from_raw(1);
    assert!(evaluate_credit(&changed, &prices, &policy).is_err());
    changed.debt = Value::zero();
    changed.pending_draws = Value::zero();
    changed.balances.insert(
        spot_key(EconomicAsset::Sol),
        spot_balance(EconomicAsset::Sol, Amount::from_raw(MAX_AMOUNT_RAW)),
    );
    assert!(evaluate_credit(&changed, &prices, &policy).is_err());
}

#[test]
fn zero_snapshot_versions_are_rejected() {
    let (account, prices) = fixture();
    let policy = spot_policy();
    let mut changed_account = account.clone();
    changed_account.version = 0;
    assert!(evaluate_credit(&changed_account, &prices, &policy).is_err());
    let mut changed_prices = prices.clone();
    changed_prices.version = 0;
    assert!(evaluate_credit(&account, &changed_prices, &policy).is_err());
}

#[test]
fn total_capacity_cannot_exceed_the_value_bound() {
    let (mut account, mut prices) = fixture();
    let policy = spot_policy();
    for asset in [EconomicAsset::Sol, EconomicAsset::Eth] {
        account.balances.insert(
            spot_key(asset),
            spot_balance(asset, Amount::from_raw(MAX_AMOUNT_RAW / 100)),
        );
        prices.prices.insert(
            asset,
            CreditPrice {
                price: Price::from_raw(100 * AMOUNT_SCALE),
                observed_at_ms: 10_000,
            },
        );
    }
    assert!(evaluate_credit(&account, &prices, &policy).is_err());
    account.balances.remove(&spot_key(EconomicAsset::Eth));
    let health = complete(&account, &prices, &policy);
    assert_eq!(
        health.borrowing_capacity,
        Value::from_raw(MAX_VALUE_RAW / 100 * 85)
    );
    assert_eq!(
        health.liquidation_capacity,
        Value::from_raw(MAX_VALUE_RAW / 100 * 90)
    );
}

pub(super) fn spot_key(asset: EconomicAsset) -> (LedgerAsset, BalanceBucket) {
    (
        assets::registry()
            .representations(asset)
            .next()
            .expect("registered representation")
            .asset,
        BalanceBucket::Available,
    )
}

pub(super) fn spot_balance(asset: EconomicAsset, amount: Amount) -> CollateralBalance {
    let info = assets::info(spot_key(asset).0);
    let quantity = if info.decimals >= 9 {
        amount
            .raw()
            .checked_mul(10_i128.pow(u32::from(info.decimals - 9)))
            .expect("test amount fits")
    } else {
        amount.raw() / 10_i128.pow(u32::from(9 - info.decimals))
    };
    CollateralBalance {
        quantity,
        control: CollateralControl::Controlled,
    }
}
