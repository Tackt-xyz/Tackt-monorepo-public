//! Account-page display arithmetic. These demo values never authorize financial actions.

mod balances;

use balances::AssetBalances;
use std::collections::BTreeMap;

use axum::{Json, Router, extract::State};
use chrono::{DateTime, Utc};
use ethnum::I256;
use serde::Serialize;
use tackt_nats::aggregates::AggregatePrice;
use tackt_postgres::UserLedgerBalanceRead;
use tackt_primitives::{
    assets,
    ledger::{BalanceBucket, LedgerAsset, LedgerBalanceTotals, UserLedgerBalanceSnapshot},
};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::ApiState,
    auth::{AuthError, AuthenticatedUser},
    openapi::routes,
};

pub(crate) fn router() -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    routes!(summary => __path_summary)
}

#[utoipa::path(get, path = "/api/v1/users/me/account-summary",
    responses((status = 200, body = AccountSummary)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Account")]
async fn summary(
    user: AuthenticatedUser,
    State(state): State<ApiState>,
) -> Result<Json<AccountSummary>, AuthError> {
    let read = state
        .repo
        .user_ledger_balance_snapshot(user.user_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, user_id = %user.user_id, "account summary read failed");
            AuthError::Internal
        })?;
    let snapshot = match read {
        UserLedgerBalanceRead::UserNotFound => return Err(AuthError::NotRegistered),
        UserLedgerBalanceRead::UserClosed => return Err(AuthError::Closed),
        UserLedgerBalanceRead::Available(snapshot) => snapshot,
    };
    AccountSummary::new(user.user_id, snapshot, |asset| {
        state.prices.get_latest(asset)
    })
    .map(Json)
    .map_err(|error| {
        tracing::error!(%error, user_id = %user.user_id, "account summary calculation failed");
        AuthError::Internal
    })
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct AccountSummary {
    user_id: Uuid,
    /// Database statement time, not an ingestion or price freshness guarantee.
    #[serde(with = "chrono::serde::ts_milliseconds")]
    #[schema(value_type = i64)]
    as_of: DateTime<Utc>,
    /// Always demo. The 85% LTV and 90% liquidation threshold do not grant authorization.
    calculation_mode: &'static str,
    /// False when a nonzero holding or liability has no price. Affected totals are null.
    complete: bool,
    missing_price_asset_ids: Vec<&'static str>,
    /// Gross booked portfolio value, before debt, as requested for the demo.
    /// Excludes pending deposits, principal liabilities and provider shortfalls.
    #[schema(required = true)]
    net_equity_usd: Option<String>,
    #[schema(required = true)]
    credit_limit_usd: Option<String>,
    #[schema(required = true)]
    outstanding_debt_usd: Option<String>,
    #[schema(required = true)]
    available_credit_usd: Option<String>,
    /// Ordered by assetId. Includes all recorded assets, even with zero holdings.
    assets: Vec<SummaryAsset>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct SummaryAsset {
    #[serde(flatten)]
    balances: AssetBalances,
    /// Net included holdings in token units. Bucket totals remain atomic integer strings.
    quantity: String,
    outstanding_debt_quantity: String,
    /// Registry fixed USD price or latest aggregate without a freshness cutoff; null if missing.
    #[schema(required = true)]
    price_usd: Option<String>,
    /// Aggregate computation time; null for a registry fixed price or missing aggregate.
    #[schema(required = true)]
    price_as_of: Option<i64>,
    /// USD values are decimal strings with eight fractional digits, truncated toward zero.
    #[schema(required = true)]
    value_usd: Option<String>,
    /// Demo rate for every asset, regardless of collateral configuration.
    ltv: &'static str,
    liquidation_threshold: &'static str,
    #[schema(required = true)]
    credit_contribution_usd: Option<String>,
    #[schema(required = true)]
    outstanding_debt_usd: Option<String>,
}

impl AccountSummary {
    fn new(
        user: Uuid,
        snapshot: UserLedgerBalanceSnapshot,
        price_for: impl Fn(LedgerAsset) -> Option<std::sync::Arc<AggregatePrice>>,
    ) -> anyhow::Result<Self> {
        let mut groups: BTreeMap<&str, (LedgerAsset, Vec<LedgerBalanceTotals>)> = BTreeMap::new();
        for balance in snapshot.balances {
            groups
                .entry(balance.asset.code())
                .or_insert_with(|| (balance.asset, Vec::new()))
                .1
                .push(balance);
        }
        let mut assets = Vec::with_capacity(groups.len());
        let mut missing = Vec::new();
        let mut equity = Some(I256::ZERO);
        let mut credit = Some(I256::ZERO);
        let mut debt = Some(I256::ZERO);
        for (_, (asset, balances)) in groups {
            let (quantity, liability) = quantities(&balances)?;
            let (price, price_as_of) = match assets::info(asset).pricing {
                assets::AssetPricing::FixedUsd { price_e8 } => (Some(u128::from(price_e8)), None),
                assets::AssetPricing::Aggregate {} => {
                    price_for(asset).map_or((None, None), |price| {
                        (
                            Some(price.price().value()),
                            Some(price.computed_at().timestamp_millis()),
                        )
                    })
                }
            };
            if price.is_none() && (quantity != I256::ZERO || liability != I256::ZERO) {
                missing.push(asset.code());
            }
            let value = value_usd(quantity, asset, price)?;
            let liability_value = value_usd(liability, asset, price)?;
            let contribution = value
                .map(|value| {
                    value
                        .max(I256::ZERO)
                        .checked_mul(I256::from(85))
                        .map(|value| value / I256::from(100))
                        .ok_or_else(|| anyhow::anyhow!("demo credit calculation overflow"))
                })
                .transpose()?;
            equity = sum(equity, value)?;
            credit = sum(credit, contribution)?;
            debt = sum(debt, liability_value)?;
            let decimals = assets::info(asset).decimals;
            assets.push(SummaryAsset {
                balances: AssetBalances::with_balances(asset, &balances),
                quantity: decimal(quantity, decimals),
                outstanding_debt_quantity: decimal(liability, decimals),
                price_usd: price.map(|price| usd(I256::from(price))),
                price_as_of,
                value_usd: value.map(usd),
                ltv: "0.85",
                liquidation_threshold: "0.90",
                credit_contribution_usd: contribution.map(usd),
                outstanding_debt_usd: liability_value.map(usd),
            });
        }
        let available = credit
            .zip(debt)
            .map(|(credit, debt)| {
                credit
                    .checked_sub(debt)
                    .map(|value| value.max(I256::ZERO))
                    .ok_or_else(|| anyhow::anyhow!("available credit overflow"))
            })
            .transpose()?;
        Ok(Self {
            user_id: user,
            as_of: snapshot.as_of,
            calculation_mode: "demo",
            complete: missing.is_empty(),
            missing_price_asset_ids: missing,
            net_equity_usd: equity.map(usd),
            credit_limit_usd: credit.map(usd),
            outstanding_debt_usd: debt.map(usd),
            available_credit_usd: available.map(usd),
            assets,
        })
    }
}

fn quantities(balances: &[LedgerBalanceTotals]) -> anyhow::Result<(I256, I256)> {
    let mut holdings = I256::ZERO;
    let mut liability = I256::ZERO;
    for balance in balances {
        let net = I256::from(balance.debit_quantity) - I256::from(balance.credit_quantity);
        match balance.bucket {
            BalanceBucket::Pending | BalanceBucket::ProviderShortfall => {}
            BalanceBucket::PrincipalLiability => {
                liability = liability
                    .checked_sub(net)
                    .ok_or_else(|| anyhow::anyhow!("liability overflow"))?
            }
            BalanceBucket::Available
            | BalanceBucket::Reserved
            | BalanceBucket::OrderReserved
            | BalanceBucket::WithdrawalReserved
            | BalanceBucket::HyperliquidMargin
            | BalanceBucket::LiquidationProceeds
            | BalanceBucket::CustomerSurplus => {
                holdings = holdings
                    .checked_add(net)
                    .ok_or_else(|| anyhow::anyhow!("holdings overflow"))?;
            }
        }
    }
    Ok((holdings, liability.max(I256::ZERO)))
}

fn value_usd(
    quantity: I256,
    asset: LedgerAsset,
    price: Option<u128>,
) -> anyhow::Result<Option<I256>> {
    if quantity == I256::ZERO {
        return Ok(Some(I256::ZERO));
    }
    let Some(price) = price else {
        return Ok(None);
    };
    // Registry and aggregate prices use eight decimals. Preserve atomic dust before division.
    let value = quantity
        .checked_mul(I256::from(price))
        .ok_or_else(|| anyhow::anyhow!("portfolio valuation overflow"))?
        / I256::from(10_u128.pow(u32::from(assets::info(asset).decimals)));
    Ok(Some(value))
}

fn sum(total: Option<I256>, value: Option<I256>) -> anyhow::Result<Option<I256>> {
    total
        .zip(value)
        .map(|(total, value)| {
            total
                .checked_add(value)
                .ok_or_else(|| anyhow::anyhow!("portfolio total overflow"))
        })
        .transpose()
}

pub(crate) fn decimal(value: I256, decimals: u8) -> String {
    let magnitude = value.unsigned_abs();
    if decimals == 0 {
        return value.to_string();
    }
    let scale = ethnum::U256::from(10_u128.pow(u32::from(decimals)));
    let sign = if value < I256::ZERO { "-" } else { "" };
    format!(
        "{sign}{}.{:0>width$}",
        magnitude / scale,
        (magnitude % scale).to_string(),
        width = usize::from(decimals)
    )
}

fn usd(value: I256) -> String {
    decimal(value, 8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tackt_nats::{PriceAuthority, PriceQuote, aggregates::AggregateSource};

    fn balance(
        asset: LedgerAsset,
        bucket: BalanceBucket,
        debit: u128,
        credit: u128,
    ) -> LedgerBalanceTotals {
        LedgerBalanceTotals {
            asset,
            bucket,
            debit_quantity: debit,
            credit_quantity: credit,
            updated_at: Utc::now(),
        }
    }

    fn units(asset: LedgerAsset, quantity: u128) -> u128 {
        quantity * 10_u128.pow(u32::from(assets::info(asset).decimals))
    }

    fn price(asset: LedgerAsset, dollars: u128) -> std::sync::Arc<AggregatePrice> {
        std::sync::Arc::new(
            AggregatePrice::new(
                asset,
                PriceQuote::new(dollars * 100_000_000, 8).unwrap(),
                1,
                Utc::now(),
                vec![AggregateSource {
                    authority: PriceAuthority::Binance,
                    evidence_id: format!("priceev_{}", "a".repeat(64)),
                    effective_weight_bps: 10_000,
                }],
            )
            .unwrap(),
        )
    }

    fn snapshot(balances: Vec<LedgerBalanceTotals>) -> UserLedgerBalanceSnapshot {
        UserLedgerBalanceSnapshot {
            as_of: Utc::now(),
            balances,
        }
    }

    #[test]
    fn values_settled_holdings_debt_and_uniform_demo_rates_from_one_snapshot() {
        use BalanceBucket::*;
        use LedgerAsset::*;
        let result = AccountSummary::new(
            Uuid::new_v4(),
            snapshot(vec![
                balance(Eth, Available, units(Eth, 4), 0),
                balance(Eth, Reserved, units(Eth, 1), 0),
                balance(Eth, Pending, units(Eth, 100), 0),
                balance(Usdc, Available, units(Usdc, 2500), units(Usdc, 500)),
                balance(
                    Usdc,
                    PrincipalLiability,
                    units(Usdc, 500),
                    units(Usdc, 2500),
                ),
                balance(Usdc, ProviderShortfall, units(Usdc, 100), 0),
            ]),
            |asset| Some(price(asset, if asset == Eth { 3000 } else { 1 })),
        )
        .unwrap();
        assert!(result.complete);
        assert_eq!(result.net_equity_usd.as_deref(), Some("17000.00000000"));
        assert_eq!(result.credit_limit_usd.as_deref(), Some("14450.00000000"));
        assert_eq!(
            result.outstanding_debt_usd.as_deref(),
            Some("2000.00000000")
        );
        assert_eq!(
            result.available_credit_usd.as_deref(),
            Some("12450.00000000")
        );
        let json = serde_json::to_value(result).unwrap();
        assert_eq!(json["assets"][0]["assetId"], "ETH");
        assert_eq!(
            json["assets"][0]["buckets"]["pending"]["debitQuantity"],
            units(Eth, 100).to_string()
        );
        for asset in json["assets"].as_array().unwrap() {
            assert_eq!(asset["ltv"], "0.85");
            assert_eq!(asset["liquidationThreshold"], "0.90");
            assert_eq!(asset["priceAsOf"].is_null(), asset["assetId"] == "USDC");
        }
    }

    #[test]
    fn cold_cache_invalidates_only_affected_totals_and_empty_accounts_are_zero() {
        let empty = AccountSummary::new(Uuid::new_v4(), snapshot(vec![]), |_| None).unwrap();
        assert!(empty.complete);
        assert_eq!(empty.net_equity_usd.as_deref(), Some("0.00000000"));
        let result = AccountSummary::new(
            Uuid::new_v4(),
            snapshot(vec![balance(
                LedgerAsset::Eth,
                BalanceBucket::Available,
                1,
                0,
            )]),
            |_| None,
        )
        .unwrap();
        assert!(!result.complete);
        assert_eq!(result.missing_price_asset_ids, vec!["ETH"]);
        assert!(result.net_equity_usd.is_none());
        assert!(result.credit_limit_usd.is_none());
        assert!(result.available_credit_usd.is_none());
        assert_eq!(result.outstanding_debt_usd.as_deref(), Some("0.00000000"));
        let debt_only = AccountSummary::new(
            Uuid::new_v4(),
            snapshot(vec![balance(
                LedgerAsset::Eth,
                BalanceBucket::PrincipalLiability,
                0,
                1,
            )]),
            |_| None,
        )
        .unwrap();
        assert_eq!(debt_only.net_equity_usd.as_deref(), Some("0.00000000"));
        assert!(debt_only.outstanding_debt_usd.is_none());
        assert!(debt_only.available_credit_usd.is_none());
        let pending = AccountSummary::new(
            Uuid::new_v4(),
            snapshot(vec![balance(
                LedgerAsset::Eth,
                BalanceBucket::Pending,
                100,
                0,
            )]),
            |_| None,
        )
        .unwrap();
        assert!(pending.complete);
        assert!(pending.assets[0].price_usd.is_none());
    }

    #[test]
    fn large_atomic_totals_are_netted_exactly_and_overdrawn_credit_is_zero() {
        let asset = LedgerAsset::Usdc;
        let one = units(asset, 1);
        let result = AccountSummary::new(
            Uuid::new_v4(),
            snapshot(vec![
                balance(asset, BalanceBucket::Available, u128::MAX, u128::MAX - one),
                balance(asset, BalanceBucket::PrincipalLiability, 0, units(asset, 2)),
            ]),
            |asset| Some(price(asset, 1)),
        )
        .unwrap();
        assert_eq!(result.net_equity_usd.as_deref(), Some("1.00000000"));
        assert_eq!(result.credit_limit_usd.as_deref(), Some("0.85000000"));
        assert_eq!(result.available_credit_usd.as_deref(), Some("0.00000000"));
        assert_eq!(decimal(I256::from(-1), 18), "-0.000000000000000001");
        let negative = AccountSummary::new(
            Uuid::new_v4(),
            snapshot(vec![balance(
                asset,
                BalanceBucket::HyperliquidMargin,
                0,
                one,
            )]),
            |asset| Some(price(asset, 1)),
        )
        .unwrap();
        assert_eq!(negative.net_equity_usd.as_deref(), Some("-1.00000000"));
        assert_eq!(negative.credit_limit_usd.as_deref(), Some("0.00000000"));
    }

    #[test]
    fn fixed_usdc_prices_value_holdings_and_debt_without_a_feed_on_every_chain() {
        for asset in [
            LedgerAsset::Usdc,
            LedgerAsset::UsdcBase,
            LedgerAsset::UsdcSolana,
        ] {
            let result = AccountSummary::new(
                Uuid::new_v4(),
                snapshot(vec![
                    balance(asset, BalanceBucket::Available, units(asset, 2) + 1, 0),
                    balance(asset, BalanceBucket::PrincipalLiability, 0, units(asset, 1)),
                ]),
                |_| panic!("fixed pricing must not consult or be overridden by the live cache"),
            )
            .unwrap();
            assert!(result.complete);
            assert!(result.missing_price_asset_ids.is_empty());
            assert_eq!(result.net_equity_usd.as_deref(), Some("2.00000100"));
            assert_eq!(result.outstanding_debt_usd.as_deref(), Some("1.00000000"));
            assert_eq!(result.available_credit_usd.as_deref(), Some("0.70000085"));
            assert_eq!(result.assets[0].price_usd.as_deref(), Some("1.00000000"));
            assert_eq!(result.assets[0].price_as_of, None);
        }
    }

    #[test]
    fn valuation_overflow_returns_error_instead_of_wrapping_or_panicking() {
        let asset = LedgerAsset::Eth;
        let quote = std::sync::Arc::new(
            AggregatePrice::new(
                asset,
                PriceQuote::new(u128::MAX, 8).unwrap(),
                1,
                Utc::now(),
                vec![AggregateSource {
                    authority: PriceAuthority::Binance,
                    evidence_id: format!("priceev_{}", "b".repeat(64)),
                    effective_weight_bps: 10_000,
                }],
            )
            .unwrap(),
        );
        assert!(
            AccountSummary::new(
                Uuid::new_v4(),
                snapshot(vec![balance(asset, BalanceBucket::Available, u128::MAX, 0),]),
                |_| Some(quote.clone())
            )
            .is_err()
        );
    }
}
