//! Shared, dependency-light configuration for the streamed Risk model.
//! Asset identity and risk settings are embedded together in the root registry.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use strum::{Display as EnumDisplay, EnumIter, EnumString};

use super::RegistryError;

/// Supported chain families.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumDisplay, EnumString, EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ChainKind {
    /// The `HyperCore` mainnet chain.
    HyperCore,
    /// Ethereum mainnet.
    Ethereum,
}

/// Supported trading and custody venues.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumDisplay, EnumString, EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum VenueKind {
    /// Ethereum mainnet ERC-20 balances.
    Ethereum,
    /// `HyperCore` HIP-1 spot balances.
    HyperCore,
    /// Hyperliquid perp markets.
    HyperliquidPerps,
}

/// Identification scheme used by an asset's canonical identifier.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumDisplay, EnumString, EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AssetIdentifierProfile {
    /// A standardized CAIP-19 namespace.
    Caip19,
    /// A provisional CAIP-19 namespace pending standardization.
    ProvisionalCaip19,
}

/// Source authority of a price index.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumDisplay,
    EnumString,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PriceSourceKind {
    /// Ethereum ERC-20 prices.
    Ethereum,
    /// `HyperCore` spot and Hyperliquid perp mark prices.
    Hyperliquid,
}

/// A trading or custody venue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueDefinition {
    /// Stable index used by streamed risk inputs.
    pub index: u16,
    /// Supported source or venue family.
    pub kind: VenueKind,
    /// Display name.
    pub name: String,
}

/// A source-specific valuation price.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceFactorDefinition {
    /// Stable index used by streamed risk inputs.
    pub index: u32,
    /// Price authority.
    pub source: PriceSourceKind,
    /// Display symbol.
    pub symbol: String,
    /// Source-specific feed identifier.
    pub external_id: String,
}

/// Streamed collateral valuation settings attached to the canonical asset entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollateralSettings {
    /// Stable asset index used by streamed risk inputs.
    pub index: u32,
    /// Index of the custody or trading venue.
    pub venue: u16,
    /// Index of the approved valuation price.
    pub price_index: u32,
    /// Collateral factor in 1e6 fixed point.
    pub factor: i128,
    /// Whether this asset contributes standalone collateral.
    pub enabled: bool,
}

/// A perpetual market and its configured margin tiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerpMarketDefinition {
    /// Stable index used by streamed risk inputs.
    pub index: u32,
    /// Index of the configured venue.
    pub venue: u16,
    /// Index of the configured valuation price.
    pub price_index: u32,
    /// Display name.
    pub name: String,
    /// Display symbol.
    pub symbol: String,
    /// Ascending notional bounds and leverage limits.
    pub margin_tiers: Vec<MarginTierDefinition>,
}

/// A venue margin tier before Risk derives its continuity deduction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarginTierDefinition {
    /// Inclusive notional bound in 1e8 fixed point.
    pub lower_bound_notional: i128,
    /// Maximum permitted leverage for this tier.
    pub max_leverage: u32,
}

/// Validate registry references once, before any service reads asset collateral settings.
pub(super) fn validate_links(
    assets: &[super::RegisteredAsset],
    venues: &[VenueDefinition],
    prices: &[PriceFactorDefinition],
    markets: &[PerpMarketDefinition],
) -> Result<(), RegistryError> {
    let venue_ids = unique_indices(venues.iter().map(|venue| u32::from(venue.index)))?;
    let price_ids = unique_indices(prices.iter().map(|price| price.index))?;
    unique_indices(markets.iter().map(|market| market.index))?;
    unique_indices(
        assets
            .iter()
            .filter_map(|asset| asset.collateral.as_ref().map(|settings| settings.index)),
    )?;
    for settings in assets.iter().filter_map(|asset| asset.collateral.as_ref()) {
        if !venue_ids.contains(&u32::from(settings.venue))
            || !price_ids.contains(&settings.price_index)
            || settings.factor < 0
        {
            return Err(RegistryError::invalid(
                "collateral has an undefined venue/price or negative factor",
            ));
        }
    }
    for market in markets {
        if !venue_ids.contains(&u32::from(market.venue)) || !price_ids.contains(&market.price_index)
        {
            return Err(RegistryError::invalid(
                "perp market references an undefined venue or price",
            ));
        }
    }
    Ok(())
}

/// Registry indices are dense identities: zero is reserved and duplicates are ambiguous.
pub(super) fn unique_indices(
    indices: impl Iterator<Item = u32>,
) -> Result<HashSet<u32>, RegistryError> {
    let mut seen = HashSet::new();
    for index in indices {
        if index == 0 || !seen.insert(index) {
            return Err(RegistryError::invalid(
                "registry indices must be nonzero and unique within each kind",
            ));
        }
    }
    Ok(seen)
}
