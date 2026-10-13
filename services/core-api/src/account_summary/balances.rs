//! Asset metadata and exact Ledger bucket totals embedded in the account summary.

use crate::catalog::{CollateralConfig, token_address};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use tackt_primitives::assets;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AssetBalances {
    asset_id: &'static str,
    symbol: String,
    /// CAIP-2 chain identity from the locked asset registry.
    chain: String,
    /// CAIP-19 token identity from the locked asset registry.
    caip19: String,
    /// ERC-20 contract or SPL mint address; null for native assets.
    #[schema(required = true)]
    token_address: Option<String>,
    decimals: u8,
    /// Streamed Risk configuration, not user eligibility; null when not configured.
    #[schema(required = true)]
    collateral: Option<CollateralConfig>,
    /// Recorded Ledger buckets: pending, available, reserved, order_reserved,
    /// withdrawal_reserved, hyperliquid_margin, principal_liability,
    /// liquidation_proceeds, customer_surplus, provider_shortfall.
    buckets: BTreeMap<String, BucketTotals>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct BucketTotals {
    /// Cumulative debit postings in atomic units, as an exact unsigned integer string.
    debit_quantity: String,
    /// Cumulative credit postings in atomic units, as an exact unsigned integer string.
    credit_quantity: String,
    /// Last stored balance update in Unix milliseconds, not external ingestion freshness.
    #[serde(with = "chrono::serde::ts_milliseconds")]
    #[schema(value_type = i64)]
    updated_at: DateTime<Utc>,
}

impl AssetBalances {
    pub(crate) fn new(asset: tackt_primitives::ledger::LedgerAsset) -> Self {
        let info = assets::info(asset);
        Self {
            asset_id: asset.code(),
            symbol: info.symbol.clone(),
            chain: info.caip2.clone(),
            caip19: info.caip19.clone(),
            token_address: token_address(&info.token),
            decimals: info.decimals,
            collateral: info.collateral.as_ref().map(CollateralConfig::from),
            buckets: BTreeMap::new(),
        }
    }

    pub(crate) fn with_balances(
        asset: tackt_primitives::ledger::LedgerAsset,
        balances: &[tackt_primitives::ledger::LedgerBalanceTotals],
    ) -> Self {
        let mut result = Self::new(asset);
        for balance in balances {
            result.buckets.insert(
                balance.bucket.as_ref().to_owned(),
                BucketTotals {
                    debit_quantity: balance.debit_quantity.to_string(),
                    credit_quantity: balance.credit_quantity.to_string(),
                    updated_at: balance.updated_at,
                },
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::AccountSummary;
    use super::*;
    use tackt_primitives::ledger::UserLedgerBalanceSnapshot;
    use tackt_primitives::ledger::{BalanceBucket, LedgerAsset, LedgerBalanceTotals};
    use uuid::Uuid;

    #[test]
    fn solana_assets_include_mint_addresses_only_for_tokens() {
        let at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        let response = AccountSummary::new(
            Uuid::from_u128(1),
            UserLedgerBalanceSnapshot {
                as_of: at,
                balances: [LedgerAsset::SolSolana, LedgerAsset::UsdcSolana]
                    .map(|asset| LedgerBalanceTotals {
                        asset,
                        bucket: BalanceBucket::Available,
                        debit_quantity: 10,
                        credit_quantity: 0,
                        updated_at: at,
                    })
                    .to_vec(),
            },
            |_| None,
        )
        .unwrap();
        let json = serde_json::to_value(response).unwrap();
        assert!(json["assets"][0]["tokenAddress"].is_null());
        assert!(json["assets"][0]["collateral"].is_null());
        let info = assets::info(LedgerAsset::UsdcSolana);
        let assets::TokenIdentity::Spl { mint } = &info.token else {
            panic!("USDC on Solana must have a mint");
        };
        assert_eq!(json["assets"][1]["tokenAddress"], *mint);
        assert_eq!(json["assets"][1]["caip19"], info.caip19);
    }

    #[test]
    fn response_groups_assets_without_netting_or_losing_integer_precision() {
        let at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        let balance = |asset, bucket, debit_quantity, credit_quantity| LedgerBalanceTotals {
            asset,
            bucket,
            debit_quantity,
            credit_quantity,
            updated_at: at,
        };
        let response = AccountSummary::new(
            Uuid::from_u128(1),
            UserLedgerBalanceSnapshot {
                as_of: at,
                balances: vec![
                    balance(
                        LedgerAsset::Usdc,
                        BalanceBucket::PrincipalLiability,
                        20,
                        100,
                    ),
                    balance(
                        LedgerAsset::Eth,
                        BalanceBucket::Available,
                        u128::MAX,
                        u128::MAX - 1,
                    ),
                    balance(LedgerAsset::Usdc, BalanceBucket::Available, 50, 50),
                ],
            },
            |_| None,
        )
        .unwrap();
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["asOf"], at.timestamp_millis());
        assert_eq!(json["assets"].as_array().unwrap().len(), 2);
        let eth = &json["assets"][0];
        assert_eq!(eth["assetId"], "ETH");
        assert!(eth["tokenAddress"].is_null());
        assert_eq!(eth["decimals"], assets::info(LedgerAsset::Eth).decimals);
        assert_eq!(
            eth["buckets"]["available"]["debitQuantity"],
            u128::MAX.to_string()
        );
        assert_eq!(
            eth["buckets"]["available"]["creditQuantity"],
            (u128::MAX - 1).to_string()
        );
        assert!(eth["buckets"]["available"].get("quantity").is_none());
        let usdc = &json["assets"][1];
        let info = assets::info(LedgerAsset::Usdc);
        assert_eq!(usdc["chain"], info.caip2);
        assert_eq!(usdc["caip19"], info.caip19);
        assert_eq!(usdc["tokenAddress"], info.erc20_address().unwrap());
        assert_eq!(
            usdc["collateral"],
            serde_json::to_value(CollateralConfig::from(info.collateral.as_ref().unwrap()))
                .unwrap()
        );
        assert_eq!(
            usdc["buckets"]["principal_liability"]["creditQuantity"],
            "100"
        );
        assert_eq!(
            usdc["buckets"]["principal_liability"]["debitQuantity"],
            "20"
        );
        assert_eq!(
            usdc["buckets"]["available"]["updatedAt"],
            at.timestamp_millis()
        );
        assert_eq!(usdc["buckets"].as_object().unwrap().len(), 2);
    }
}
