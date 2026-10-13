//! Run with `cargo run --locked --release -p risk-engine --example credit_health_benchmark`.

use std::alloc::System;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use risk_engine::processing::{
    PRICE_SCALE, Price, VALUE_SCALE, Value,
    credit::{
        CollateralBalance, CollateralControl, CreditAccount, CreditMarketSnapshot,
        CreditPerpPosition, CreditPolicy, CreditPrice, PerpMarginSnapshot, evaluate_credit,
    },
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use tackt_primitives::assets::{self, EconomicAsset};
use tackt_primitives::ledger::{BalanceBucket, LedgerAsset};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    println!("{} registered representations", assets::all().len());
    let representations = [LedgerAsset::Eth, LedgerAsset::Btc, LedgerAsset::SolSolana];
    let mut account = CreditAccount {
        version: 1,
        margin_pools: BTreeMap::new(),
        perp_positions: BTreeMap::new(),
        balances: representations
            .into_iter()
            .map(|asset| {
                (
                    (asset, BalanceBucket::Available),
                    CollateralBalance {
                        quantity: 10_i128.pow(u32::from(assets::info(asset).decimals)),
                        control: CollateralControl::Controlled,
                    },
                )
            })
            .collect(),
        balances_as_of_ms: 9_500,
        debt: Value::from_raw(40_000 * VALUE_SCALE),
        pending_draws: Value::from_raw(1_000 * VALUE_SCALE),
    };
    let prices = CreditMarketSnapshot {
        version: 1,
        as_of_ms: 10_000,
        max_age_ms: 1_000,
        prices: BTreeMap::from(
            [
                (EconomicAsset::Eth, 2_000),
                (EconomicAsset::Btc, 50_000),
                (EconomicAsset::Sol, 100),
            ]
            .map(|(asset, price)| {
                (
                    asset,
                    CreditPrice {
                        price: Price::from_raw(price * PRICE_SCALE),
                        observed_at_ms: 9_500,
                    },
                )
            }),
        ),
    };
    measure("spot only", &account, &prices, &CreditPolicy::current());
    account.margin_pools.insert(
        0,
        Some(PerpMarginSnapshot {
            control: CollateralControl::Controlled,
            source_version: 1,
            equity: Value::from_raw(5_000 * VALUE_SCALE),
            observed_at_ms: 9_500,
        }),
    );
    account.perp_positions.extend([
        (
            1,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: Value::from_raw(12_500 * VALUE_SCALE),
            },
        ),
        (
            2,
            CreditPerpPosition {
                source_version: 1,
                margin_pool: 0,
                notional: Value::from_raw(-12_500 * VALUE_SCALE),
            },
        ),
    ]);
    measure(
        "spot and shared-margin perps",
        &account,
        &prices,
        &CreditPolicy::current(),
    );
    let pending = account
        .balances
        .remove(&(LedgerAsset::SolSolana, BalanceBucket::Available))
        .expect("benchmark SOL balance");
    account
        .balances
        .insert((LedgerAsset::SolSolana, BalanceBucket::Pending), pending);
    measure(
        "spot including pending deposit and shared-margin perps",
        &account,
        &prices,
        &CreditPolicy::current(),
    );
}

fn measure(
    name: &str,
    account: &CreditAccount,
    prices: &CreditMarketSnapshot,
    policy: &CreditPolicy,
) {
    const BATCHES: usize = 100;
    const PER_BATCH: u32 = 10_000;
    let evaluate = || {
        black_box(
            evaluate_credit(black_box(account), black_box(prices), black_box(policy))
                .expect("valid benchmark snapshot"),
        )
    };
    for _ in 0..PER_BATCH {
        evaluate();
    }
    let mut batches = Vec::with_capacity(BATCHES);
    let allocations = Region::new(GLOBAL);
    for _ in 0..BATCHES {
        let start = Instant::now();
        for _ in 0..PER_BATCH {
            evaluate();
        }
        batches.push(start.elapsed());
    }
    let allocation_stats = allocations.change();
    batches.sort_unstable();
    println!("{name}: 3 assets; {BATCHES} batches of {PER_BATCH} evaluations");
    println!("batch p50: {:?}; batch p99: {:?}", batches[49], batches[98]);
    println!(
        "p50 batch / evaluation: {:?}; p99 batch / evaluation: {:?}",
        batches[49] / PER_BATCH,
        batches[98] / PER_BATCH
    );
    println!(
        "allocations: {}; reallocations: {}; allocated bytes: {}",
        allocation_stats.allocations,
        allocation_stats.reallocations,
        allocation_stats.bytes_allocated
    );
    assert_eq!(allocation_stats.allocations, 0);
    assert_eq!(allocation_stats.reallocations, 0);
}
