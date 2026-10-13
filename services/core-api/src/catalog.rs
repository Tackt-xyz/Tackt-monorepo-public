//! Read-only display of the embedded registry shared with Risk and Ledger.

use std::sync::LazyLock;

use axum::{Json, Router};
use serde::Serialize;
use tackt_primitives::assets::{self, risk};
use utoipa::ToSchema;

use crate::{api::ApiState, auth::AuthenticatedUser, openapi::routes};

pub(crate) fn router() -> (Router<ApiState>, utoipa::openapi::OpenApi) {
    routes!(asset_catalog => __path_asset_catalog, market_catalog => __path_market_catalog)
}

/// Full configured catalog, including assets without Ledger support or current user holdings.
#[utoipa::path(get, path = "/api/v1/assets",
    responses((status = 200, body = AssetCatalog)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Catalog")]
async fn asset_catalog(_user: AuthenticatedUser) -> Json<&'static AssetCatalog> {
    Json(&ASSET_CATALOG)
}

/// Configured perpetual markets and leverage tiers, not live prices or venue health.
#[utoipa::path(get, path = "/api/v1/markets",
    responses((status = 200, body = MarketCatalog)),
    security(("AccessCookie" = []), ("UserBearer" = [], "UserId" = [])), tag = "Catalog")]
async fn market_catalog(_user: AuthenticatedUser) -> Json<&'static MarketCatalog> {
    Json(&MARKET_CATALOG)
}

// Pure functions of the embedded registry, so each catalog is built once per process.
static ASSET_CATALOG: LazyLock<AssetCatalog> = LazyLock::new(AssetCatalog::current);
static MARKET_CATALOG: LazyLock<MarketCatalog> = LazyLock::new(MarketCatalog::current);

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct AssetCatalog {
    /// Complete catalog in registry order; CAIP-19 identifies each representation.
    assets: Vec<CatalogAsset>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct CatalogAsset {
    /// Ledger accounting code; null when this asset is supported only by Risk.
    #[schema(required = true)]
    asset_id: Option<&'static str>,
    symbol: String,
    /// Grouping across chains; does not make balances interchangeable.
    #[schema(value_type = String, examples("ETH", "BTC", "SOL", "USDC"))]
    economic_asset: assets::EconomicAsset,
    /// CAIP-2 chain identity.
    chain: String,
    /// Canonical or provisional CAIP-19 asset identity.
    caip19: String,
    /// ERC-20 contract or SPL mint; null for native and HIP-1 assets.
    #[schema(required = true)]
    token_address: Option<String>,
    decimals: u8,
    /// Streamed Risk settings; null when no collateral configuration exists.
    #[schema(required = true)]
    collateral: Option<CollateralConfig>,
}

/// Asset-level configuration, not user eligibility or a borrowing limit.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CollateralConfig {
    /// Whether the streamed Risk model enables standalone collateral for this asset.
    enabled: bool,
    /// Exact valuation multiplier, e.g. "0.950000"; not a credit LTV.
    factor: String,
    venue: CatalogVenue,
    price: CatalogPrice,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct CatalogVenue {
    #[schema(value_type = String, examples("ethereum", "hyper_core", "hyperliquid_perps"))]
    kind: risk::VenueKind,
    name: String,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct CatalogPrice {
    #[schema(value_type = String, examples("ethereum", "hyperliquid"))]
    source: risk::PriceSourceKind,
    symbol: String,
    /// Configured source-specific feed identifier; no live price is included.
    external_id: String,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct MarketCatalog {
    /// Complete configured perpetual-market list in registry order.
    markets: Vec<CatalogMarket>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct CatalogMarket {
    market_id: u32,
    name: String,
    symbol: String,
    venue: CatalogVenue,
    price: CatalogPrice,
    margin_tiers: Vec<CatalogMarginTier>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct CatalogMarginTier {
    /// Inclusive notional bound, converted exactly from the registry's 1e8 fixed point.
    lower_bound_notional: String,
    /// Configured tier leverage, not the leverage available to a particular user.
    max_leverage: u32,
}

impl AssetCatalog {
    fn current() -> Self {
        Self {
            assets: assets::registry()
                .catalog()
                .iter()
                .map(|asset| CatalogAsset {
                    asset_id: asset.ledger_asset.map(|asset| asset.code()),
                    symbol: asset.symbol.clone(),
                    economic_asset: asset.classification.economic_asset,
                    chain: asset.caip2.clone(),
                    caip19: asset.caip19.clone(),
                    token_address: token_address(&asset.token),
                    decimals: asset.decimals,
                    collateral: asset.collateral.as_ref().map(CollateralConfig::from),
                })
                .collect(),
        }
    }
}

impl From<&risk::CollateralSettings> for CollateralConfig {
    fn from(settings: &risk::CollateralSettings) -> Self {
        Self {
            enabled: settings.enabled,
            factor: fixed_decimal(settings.factor, 6),
            venue: CatalogVenue::lookup(settings.venue),
            price: CatalogPrice::lookup(settings.price_index),
        }
    }
}

impl CatalogVenue {
    fn lookup(index: u16) -> Self {
        let venue = assets::registry()
            .venues()
            .iter()
            .find(|venue| venue.index == index)
            .expect("registry validates venue references");
        Self {
            kind: venue.kind,
            name: venue.name.clone(),
        }
    }
}

impl CatalogPrice {
    fn lookup(index: u32) -> Self {
        let price = assets::registry()
            .price_factors()
            .iter()
            .find(|price| price.index == index)
            .expect("registry validates price references");
        Self {
            source: price.source,
            symbol: price.symbol.clone(),
            external_id: price.external_id.clone(),
        }
    }
}

impl MarketCatalog {
    fn current() -> Self {
        Self {
            markets: assets::registry()
                .perp_markets()
                .iter()
                .map(|market| CatalogMarket {
                    market_id: market.index,
                    name: market.name.clone(),
                    symbol: market.symbol.clone(),
                    venue: CatalogVenue::lookup(market.venue),
                    price: CatalogPrice::lookup(market.price_index),
                    margin_tiers: market
                        .margin_tiers
                        .iter()
                        .map(|tier| CatalogMarginTier {
                            lower_bound_notional: fixed_decimal(tier.lower_bound_notional, 8),
                            max_leverage: tier.max_leverage,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

pub(crate) fn token_address(token: &assets::TokenIdentity) -> Option<String> {
    match token {
        assets::TokenIdentity::Native | assets::TokenIdentity::Hip1 { .. } => None,
        assets::TokenIdentity::Erc20 { address } => Some(address.clone()),
        assets::TokenIdentity::Spl { mint } => Some(mint.clone()),
    }
}

/// Exact fixed-point to decimal text; never floats.
pub(crate) fn fixed_decimal(raw: i128, decimals: u32) -> String {
    let magnitude = raw.unsigned_abs();
    let scale = 10_u128.pow(decimals);
    let sign = if raw < 0 { "-" } else { "" };
    format!(
        "{sign}{}.{:0width$}",
        magnitude / scale,
        magnitude % scale,
        width = decimals as usize,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_includes_risk_only_assets_without_inventing_ledger_support() {
        let catalog = AssetCatalog::current();
        assert_eq!(catalog.assets.len(), assets::registry().catalog().len());
        let risk_only: Vec<_> = catalog
            .assets
            .iter()
            .filter(|asset| asset.asset_id.is_none())
            .collect();
        assert!(!risk_only.is_empty());
        for asset in risk_only {
            assert!(asset.collateral.is_some());
            assert!(asset.token_address.is_none());
            assert!(
                assets::registry()
                    .assets()
                    .iter()
                    .all(|ledger| ledger.caip19 != asset.caip19)
            );
            let json = serde_json::to_value(asset).unwrap();
            assert_eq!(json["assetId"], serde_json::Value::Null);
        }
        let unconfigured = catalog
            .assets
            .iter()
            .find(|asset| asset.collateral.is_none())
            .unwrap();
        assert!(unconfigured.asset_id.is_some());
        assert_eq!(
            serde_json::to_value(unconfigured).unwrap()["collateral"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn disabled_collateral_retains_its_configured_factor_and_feed() {
        let mut settings = assets::registry()
            .catalog()
            .iter()
            .find_map(|asset| asset.collateral.clone())
            .unwrap();
        settings.enabled = false;
        settings.factor = 950_001;
        let json = serde_json::to_value(CollateralConfig::from(&settings)).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["factor"], "0.950001");
        let price = assets::registry()
            .price_factors()
            .iter()
            .find(|price| price.index == settings.price_index)
            .unwrap();
        assert_eq!(json["price"]["externalId"], price.external_id);
    }

    #[test]
    fn market_catalog_retains_every_configured_tier_and_exact_notional() {
        let catalog = MarketCatalog::current();
        assert_eq!(
            catalog.markets.len(),
            assets::registry().perp_markets().len()
        );
        assert!(!catalog.markets.is_empty());
        for (market, definition) in catalog
            .markets
            .iter()
            .zip(assets::registry().perp_markets())
        {
            assert_eq!(market.market_id, definition.index);
            assert_eq!(market.margin_tiers.len(), definition.margin_tiers.len());
            for (tier, definition) in market.margin_tiers.iter().zip(&definition.margin_tiers) {
                assert_eq!(tier.max_leverage, definition.max_leverage);
                let (whole, fraction) = tier.lower_bound_notional.split_once('.').unwrap();
                assert_eq!(fraction.len(), 8);
                let raw = whole.parse::<i128>().unwrap() * 100_000_000
                    + fraction.parse::<i128>().unwrap();
                assert_eq!(raw, definition.lower_bound_notional);
            }
        }
        assert_eq!(
            fixed_decimal(900_719_925_474_099_301, 8),
            "9007199254.74099301"
        );
        assert_eq!(fixed_decimal(1, 6), "0.000001");
    }
}
