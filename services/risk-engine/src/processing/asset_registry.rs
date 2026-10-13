use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use serde::Serialize;
pub use shared_assets::risk::{AssetIdentifierProfile, ChainKind, VenueKind};
use shared_assets::risk::{
    MarginTierDefinition, PerpMarketDefinition, PriceFactorDefinition, VenueDefinition,
};
use tackt_primitives::assets as shared_assets;

use crate::error::{Error, Result};

use super::{
    AssetIndex, ChainIndex, FACTOR_SCALE, Factor, MAX_FACTOR_RAW, MAX_VALUE_RAW, PerpMarketIndex,
    PriceIndex, PriceSourceKind, Value, VenueIndex,
};

const MAX_INDEX: usize = 65_536;
const MAX_MARGIN_TIERS: usize = 16;
const MAX_TIER_LEVERAGE: u32 = 1_000;
type AssetsByIndex = Vec<Option<AssetMetadata>>;
type AssetLookup = HashMap<Box<str>, AssetIndex>;

/// Canonical CAIP-2 chain identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[repr(transparent)]
pub struct Caip2ChainId(Box<str>);

impl Caip2ChainId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Caip2ChainId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Canonical CAIP-19 asset identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[repr(transparent)]
pub struct Caip19AssetId(Box<str>);

impl Caip19AssetId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Caip19AssetId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Cold metadata of one registered chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainMetadata {
    /// Compact registry index.
    pub index: ChainIndex,
    /// Chain family.
    pub kind: ChainKind,
    /// Canonical CAIP-2 identifier.
    pub caip2: Caip2ChainId,
}

/// Cold metadata of one registered venue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VenueMetadata {
    /// Compact registry index.
    pub index: VenueIndex,
    /// Venue family.
    pub kind: VenueKind,
    /// Human-readable venue name.
    pub name: Box<str>,
}

/// Cold metadata of one registered price factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PriceFactorMetadata {
    /// Compact registry index.
    pub index: PriceIndex,
    /// Source authority allowed to publish this price.
    pub source: PriceSourceKind,
    /// Display symbol.
    pub symbol: Box<str>,
    /// Source-specific external identifier.
    pub external_id: Box<str>,
}

/// Cold metadata of one registered standalone asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AssetMetadata {
    /// Compact registry index.
    pub index: AssetIndex,
    /// Chain the asset lives on.
    pub chain: ChainIndex,
    /// Venue holding the balances.
    pub venue: VenueIndex,
    /// Price factor used to value the asset.
    pub price_index: PriceIndex,
    /// Identification scheme of the canonical identifier.
    pub identifier_profile: AssetIdentifierProfile,
    /// Canonical CAIP-19 identifier.
    pub caip19: Caip19AssetId,
    /// Display symbol.
    pub symbol: Box<str>,
    /// On-chain decimal places.
    pub decimals: u8,
    /// Collateral weight in `1e6` fixed point.
    pub collateral_factor: Factor,
    /// Whether the asset may contribute standalone collateral.
    pub collateral_enabled: bool,
}

/// One maintenance-margin tier of a perp market.
///
/// Marginal notional at each tier is charged at the tier's rate; the deduction keeps total
/// maintenance continuous across tier boundaries:
/// `maintenance = notional × maintenance_factor − maintenance_deduction`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarginTier {
    /// Inclusive lower bound of position notional for this tier.
    pub lower_bound_notional: Value,
    /// Maximum leverage the source allows at this tier.
    pub max_leverage: u32,
    /// Marginal maintenance rate: half the initial-margin rate at maximum leverage.
    pub maintenance_factor: Factor,
    /// Continuity deduction accumulated from lower tiers.
    pub maintenance_deduction: Value,
}

/// Cold metadata of one registered Hyperliquid perp market.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PerpMarketMetadata {
    /// Compact registry index.
    pub index: PerpMarketIndex,
    /// Venue hosting the market.
    pub venue: VenueIndex,
    /// Price factor carrying the mark price.
    pub price_index: PriceIndex,
    /// Human-readable market name.
    pub name: Box<str>,
    /// Display symbol.
    pub symbol: Box<str>,
    /// Maintenance-margin tiers ordered by ascending notional bound.
    pub margin_tiers: Box<[MarginTier]>,
}

/// Validated, index-addressed registry of chains, venues, prices, assets, and perp markets.
#[derive(Debug, Clone)]
pub struct AssetRegistry {
    chains: Vec<Option<ChainMetadata>>,
    venues: Vec<Option<VenueMetadata>>,
    prices: Vec<Option<PriceFactorMetadata>>,
    assets: Vec<Option<AssetMetadata>>,
    perp_markets: Vec<Option<PerpMarketMetadata>>,
    by_caip19: HashMap<Box<str>, AssetIndex>,
}

impl AssetRegistry {
    /// Builds the streamed model from the same embedded registry used by Core and Ledger.
    ///
    /// # Errors
    /// Returns an error if the locked risk mappings or margin tiers are inconsistent.
    pub fn bundled() -> Result<Self> {
        Self::build(shared_assets::registry())
    }

    #[cfg(test)]
    fn from_json(contents: &str) -> Result<Self> {
        let shared = shared_assets::AssetRegistry::parse(contents)
            .map_err(|error| invalid_registry(error.to_string()))?;
        Self::build(&shared)
    }

    #[must_use]
    pub fn chain_count(&self) -> usize {
        count_entries(&self.chains)
    }

    #[must_use]
    pub fn venue_count(&self) -> usize {
        count_entries(&self.venues)
    }

    #[must_use]
    pub fn price_count(&self) -> usize {
        count_entries(&self.prices)
    }

    #[must_use]
    pub fn asset_count(&self) -> usize {
        count_entries(&self.assets)
    }

    #[must_use]
    pub fn perp_market_count(&self) -> usize {
        count_entries(&self.perp_markets)
    }

    #[must_use]
    pub fn chain(&self, index: ChainIndex) -> Option<&ChainMetadata> {
        self.chains.get(usize::from(index)).and_then(Option::as_ref)
    }

    #[must_use]
    pub fn venue(&self, index: VenueIndex) -> Option<&VenueMetadata> {
        self.venues.get(usize::from(index)).and_then(Option::as_ref)
    }

    #[must_use]
    pub fn price(&self, index: PriceIndex) -> Option<&PriceFactorMetadata> {
        usize::try_from(index)
            .ok()
            .and_then(|offset| self.prices.get(offset))
            .and_then(Option::as_ref)
    }

    #[must_use]
    pub fn asset(&self, index: AssetIndex) -> Option<&AssetMetadata> {
        usize::try_from(index)
            .ok()
            .and_then(|offset| self.assets.get(offset))
            .and_then(Option::as_ref)
    }

    #[must_use]
    pub fn perp_market(&self, index: PerpMarketIndex) -> Option<&PerpMarketMetadata> {
        usize::try_from(index)
            .ok()
            .and_then(|offset| self.perp_markets.get(offset))
            .and_then(Option::as_ref)
    }

    /// Resolves a canonicalized external asset identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied identifier is malformed.
    pub fn resolve(&self, caip19: &str) -> Result<Option<AssetIndex>> {
        let canonical = canonicalize_asset_id(caip19)?;
        Ok(self.by_caip19.get(canonical.as_ref()).copied())
    }

    fn build(shared: &shared_assets::AssetRegistry) -> Result<Self> {
        let used_chains: HashSet<_> = shared
            .catalog()
            .iter()
            .filter(|asset| asset.collateral.is_some())
            .map(|asset| asset.caip2.as_str())
            .collect();
        let chains = build_chains(
            shared
                .chains()
                .iter()
                .filter(|chain| used_chains.contains(chain.caip2.as_str()))
                .cloned()
                .collect(),
        )?;
        let venues = build_venues(shared.venues().to_vec())?;
        let prices = build_prices(shared.price_factors().to_vec())?;
        let (assets, by_caip19) = build_assets(shared, &chains, &venues, &prices)?;
        let perp_markets = build_perp_markets(shared.perp_markets().to_vec(), &venues, &prices)?;

        Ok(Self {
            chains,
            venues,
            prices,
            assets,
            perp_markets,
            by_caip19,
        })
    }
}

fn build_chains(definitions: Vec<shared_assets::ChainInfo>) -> Result<Vec<Option<ChainMetadata>>> {
    build_indexed(definitions, |definition| {
        let caip2 = canonicalize_chain_id(&definition.caip2)?;
        let kind = match caip2.as_str() {
            "eip155:1" => ChainKind::Ethereum,
            shared_assets::HYPERCORE_CAIP2 => ChainKind::HyperCore,
            _ => {
                return Err(invalid_registry(format!(
                    "unsupported streamed chain: {caip2}"
                )));
            }
        };
        Ok((
            usize::from(definition.index),
            ChainMetadata {
                index: definition.index,
                kind,
                caip2,
            },
        ))
    })
}

fn build_venues(definitions: Vec<VenueDefinition>) -> Result<Vec<Option<VenueMetadata>>> {
    build_indexed(definitions, |definition| {
        validate_name(&definition.name, "venue")?;
        Ok((
            usize::from(definition.index),
            VenueMetadata {
                index: definition.index,
                kind: definition.kind,
                name: definition.name.into_boxed_str(),
            },
        ))
    })
}

fn build_prices(
    definitions: Vec<PriceFactorDefinition>,
) -> Result<Vec<Option<PriceFactorMetadata>>> {
    let mut external_ids = HashSet::new();
    build_indexed(definitions, |definition| {
        validate_name(&definition.symbol, "price symbol")?;
        validate_name(&definition.external_id, "price external_id")?;
        if !external_ids.insert((definition.source, definition.external_id.clone())) {
            return Err(invalid_registry("duplicate price source external_id"));
        }
        Ok((
            usize::try_from(definition.index).expect("u32 fits usize"),
            PriceFactorMetadata {
                index: definition.index,
                source: definition.source,
                symbol: definition.symbol.into_boxed_str(),
                external_id: definition.external_id.into_boxed_str(),
            },
        ))
    })
}

fn build_assets(
    shared: &shared_assets::AssetRegistry,
    chains: &[Option<ChainMetadata>],
    venues: &[Option<VenueMetadata>],
    prices: &[Option<PriceFactorMetadata>],
) -> Result<(AssetsByIndex, AssetLookup)> {
    let definitions: Vec<_> = shared
        .catalog()
        .iter()
        .filter_map(|asset| {
            asset
                .collateral
                .as_ref()
                .map(|collateral| (asset, collateral))
        })
        .collect();
    let mut by_caip19 = HashMap::with_capacity(definitions.len());
    let assets = build_indexed(definitions, |(asset, definition)| {
        let chain_index = shared
            .chains()
            .iter()
            .find(|chain| chain.caip2 == asset.caip2)
            .expect("shared registry validates every asset's chain")
            .index;
        let chain = get_indexed(chains, chain_index, "chain")?;
        let venue = get_indexed(venues, definition.venue, "venue")?;
        let price = get_indexed(prices, definition.price_index, "price")?;
        let identifier_profile = if matches!(asset.token, shared_assets::TokenIdentity::Hip1 { .. })
        {
            AssetIdentifierProfile::ProvisionalCaip19
        } else {
            AssetIdentifierProfile::Caip19
        };
        validate_name(&asset.symbol, "asset symbol")?;
        if asset.decimals > 38 || definition.factor < 0 || definition.factor > MAX_FACTOR_RAW {
            return Err(invalid_registry(format!(
                "asset {} has invalid decimals or factor",
                definition.index
            )));
        }
        let expected = match venue.kind {
            VenueKind::Ethereum => (ChainKind::Ethereum, PriceSourceKind::Ethereum),
            VenueKind::HyperCore => (ChainKind::HyperCore, PriceSourceKind::Hyperliquid),
            VenueKind::HyperliquidPerps => {
                return Err(invalid_registry("assets cannot use the perp venue"));
            }
        };
        if chain.kind != expected.0 || price.source != expected.1 {
            return Err(invalid_registry(format!(
                "asset {} has incompatible chain, venue, or price source",
                definition.index
            )));
        }
        let caip19 = canonicalize_asset_id(&asset.caip19)?;
        let (asset_chain, _, asset_namespace, _) = parse_asset_id(caip19.as_ref())?;
        if asset_chain != chain.caip2.as_str() {
            return Err(invalid_registry(format!(
                "asset {} identifier does not match its chain",
                definition.index
            )));
        }
        validate_identifier_profile(chain.kind, identifier_profile, asset_namespace)?;
        if by_caip19.insert(caip19.clone(), definition.index).is_some() {
            return Err(invalid_registry("duplicate asset identifier"));
        }
        Ok((
            usize::try_from(definition.index).expect("u32 fits usize"),
            AssetMetadata {
                index: definition.index,
                chain: chain_index,
                venue: definition.venue,
                price_index: definition.price_index,
                identifier_profile,
                caip19: Caip19AssetId(caip19),
                symbol: asset.symbol.clone().into_boxed_str(),
                decimals: asset.decimals,
                collateral_factor: Factor::from_raw(definition.factor),
                collateral_enabled: definition.enabled,
            },
        ))
    })?;
    Ok((assets, by_caip19))
}

fn build_perp_markets(
    definitions: Vec<PerpMarketDefinition>,
    venues: &[Option<VenueMetadata>],
    prices: &[Option<PriceFactorMetadata>],
) -> Result<Vec<Option<PerpMarketMetadata>>> {
    build_indexed(definitions, |definition| {
        let venue = get_indexed(venues, definition.venue, "venue")?;
        let price = get_indexed(prices, definition.price_index, "price")?;
        if venue.kind != VenueKind::HyperliquidPerps || price.source != PriceSourceKind::Hyperliquid
        {
            return Err(invalid_registry(format!(
                "perp market {} has incompatible metadata",
                definition.index
            )));
        }
        validate_name(&definition.name, "market name")?;
        validate_name(&definition.symbol, "market symbol")?;
        let margin_tiers = build_margin_tiers(definition.index, definition.margin_tiers)?;
        Ok((
            usize::try_from(definition.index).expect("u32 fits usize"),
            PerpMarketMetadata {
                index: definition.index,
                venue: definition.venue,
                price_index: definition.price_index,
                name: definition.name.into_boxed_str(),
                symbol: definition.symbol.into_boxed_str(),
                margin_tiers,
            },
        ))
    })
}

fn build_margin_tiers(
    market: PerpMarketIndex,
    definitions: Vec<MarginTierDefinition>,
) -> Result<Box<[MarginTier]>> {
    let invalid_tiers =
        |reason: &str| invalid_registry(format!("perp market {market} margin tiers: {reason}"));
    if definitions.is_empty() || definitions.len() > MAX_MARGIN_TIERS {
        return Err(invalid_tiers("tier count must be between 1 and 16"));
    }
    if definitions[0].lower_bound_notional != 0 {
        return Err(invalid_tiers("first tier must start at notional zero"));
    }

    let mut tiers = Vec::with_capacity(definitions.len());
    let mut deduction = 0_i128;
    let mut previous_factor = 0_i128;
    let mut previous_bound = 0_i128;
    for (offset, definition) in definitions.into_iter().enumerate() {
        if definition.max_leverage == 0 || definition.max_leverage > MAX_TIER_LEVERAGE {
            return Err(invalid_tiers("tier max leverage is out of range"));
        }
        if !(0..=MAX_VALUE_RAW).contains(&definition.lower_bound_notional) {
            return Err(invalid_tiers("tier notional bound is out of range"));
        }
        if offset > 0 && definition.lower_bound_notional <= previous_bound {
            return Err(invalid_tiers("tier notional bounds must strictly increase"));
        }
        // Maintenance is half of initial margin at maximum leverage; round up conservatively.
        let divisor = 2 * i128::from(definition.max_leverage);
        let factor = (FACTOR_SCALE + divisor - 1) / divisor;
        if factor <= previous_factor || factor > MAX_FACTOR_RAW {
            return Err(invalid_tiers(
                "tier maintenance rates must strictly increase",
            ));
        }
        // The deduction keeps maintenance continuous across the tier boundary; rounding it
        // down keeps the derived requirement conservatively high.
        let step = (factor - previous_factor)
            .checked_mul(definition.lower_bound_notional)
            .ok_or_else(|| invalid_tiers("tier deduction overflows"))?
            / FACTOR_SCALE;
        deduction = deduction
            .checked_add(step)
            .ok_or_else(|| invalid_tiers("tier deduction overflows"))?;
        tiers.push(MarginTier {
            lower_bound_notional: Value::from_raw(definition.lower_bound_notional),
            max_leverage: definition.max_leverage,
            maintenance_factor: Factor::from_raw(factor),
            maintenance_deduction: Value::from_raw(deduction),
        });
        previous_factor = factor;
        previous_bound = definition.lower_bound_notional;
    }
    Ok(tiers.into_boxed_slice())
}

fn build_indexed<D, T: Clone>(
    definitions: Vec<D>,
    mut convert: impl FnMut(D) -> Result<(usize, T)>,
) -> Result<Vec<Option<T>>> {
    if definitions.is_empty() {
        return Err(invalid_registry("registry sections must not be empty"));
    }
    let mut converted = Vec::with_capacity(definitions.len());
    let mut maximum = 0;
    for definition in definitions {
        let (index, metadata) = convert(definition)?;
        if index == 0 || index > MAX_INDEX {
            return Err(invalid_registry(format!("invalid registry index {index}")));
        }
        maximum = maximum.max(index);
        converted.push((index, metadata));
    }
    let mut entries = vec![None; maximum + 1];
    for (index, metadata) in converted {
        if entries[index].replace(metadata).is_some() {
            return Err(invalid_registry(format!(
                "duplicate registry index {index}"
            )));
        }
    }
    for (index, entry) in entries.iter().enumerate().skip(1) {
        if entry.is_none() {
            return Err(invalid_registry(format!(
                "registry index {index} is missing"
            )));
        }
    }
    Ok(entries)
}

fn get_indexed<'a, T, I>(entries: &'a [Option<T>], index: I, kind: &str) -> Result<&'a T>
where
    I: TryInto<usize> + Copy,
{
    let offset = index
        .try_into()
        .map_err(|_| invalid_registry(format!("invalid {kind} index")))?;
    entries
        .get(offset)
        .and_then(Option::as_ref)
        .ok_or_else(|| invalid_registry(format!("unknown {kind} index")))
}

fn count_entries<T>(entries: &[Option<T>]) -> usize {
    entries.iter().flatten().count()
}

fn validate_name(value: &str, kind: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        return Err(invalid_registry(format!("invalid {kind}")));
    }
    Ok(())
}

fn canonicalize_chain_id(value: &str) -> Result<Caip2ChainId> {
    let (namespace, reference) = parse_chain_id(value)?;
    if namespace == "eip155"
        && (!reference.bytes().all(|byte| byte.is_ascii_digit())
            || (reference.len() > 1 && reference.starts_with('0')))
    {
        return Err(invalid_registry(format!(
            "EIP-155 chain reference {reference:?} must be canonical decimal without leading zeroes"
        )));
    }
    Ok(Caip2ChainId(value.into()))
}

fn canonicalize_asset_id(value: &str) -> Result<Box<str>> {
    let (chain_id, chain_namespace, asset_namespace, reference) = parse_asset_id(value)?;
    let canonical_chain_id = canonicalize_chain_id(chain_id)?;
    let canonical_reference = match (chain_namespace, asset_namespace) {
        ("eip155", "erc20") => canonical_hex_reference(reference, 20, "ERC-20")?,
        ("hlcore", "hip1") => canonical_hex_reference(reference, 16, "HIP-1")?,
        _ => reference.to_owned(),
    };
    Ok(format!("{canonical_chain_id}/{asset_namespace}:{canonical_reference}").into_boxed_str())
}

fn validate_identifier_profile(
    kind: ChainKind,
    profile: AssetIdentifierProfile,
    asset_namespace: &str,
) -> Result<()> {
    let valid = match kind {
        ChainKind::HyperCore => {
            profile == AssetIdentifierProfile::ProvisionalCaip19 && asset_namespace == "hip1"
        }
        ChainKind::Ethereum => {
            profile == AssetIdentifierProfile::Caip19 && asset_namespace == "erc20"
        }
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_registry(format!(
            "chain kind {kind} is incompatible with identifier profile {profile} and asset namespace {asset_namespace}"
        )))
    }
}

fn canonical_hex_reference(reference: &str, byte_count: usize, kind: &str) -> Result<String> {
    let expected_length = 2 + byte_count * 2;
    if reference.len() != expected_length
        || !reference.starts_with("0x")
        || !reference[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_registry(format!(
            "{kind} reference must be a 0x-prefixed {byte_count}-byte hexadecimal value"
        )));
    }
    Ok(reference.to_ascii_lowercase())
}

fn parse_asset_id(value: &str) -> Result<(&str, &str, &str, &str)> {
    let (chain_id, asset) = value
        .split_once('/')
        .ok_or_else(|| invalid_registry(format!("asset identifier {value:?} is missing '/'")))?;
    if asset.contains('/') {
        return Err(invalid_registry(format!(
            "fungible asset identifier {value:?} must not contain a token-id suffix"
        )));
    }
    let (chain_namespace, _) = parse_chain_id(chain_id)?;
    let (asset_namespace, reference) = asset
        .split_once(':')
        .ok_or_else(|| invalid_registry(format!("asset identifier {value:?} is missing ':'")))?;
    if !(3..=8).contains(&asset_namespace.len())
        || !asset_namespace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(invalid_registry(format!(
            "asset identifier {value:?} has an invalid asset namespace"
        )));
    }
    if reference.is_empty()
        || reference.len() > 128
        || !reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'%'))
    {
        return Err(invalid_registry(format!(
            "asset identifier {value:?} has an invalid asset reference"
        )));
    }
    Ok((chain_id, chain_namespace, asset_namespace, reference))
}

fn parse_chain_id(value: &str) -> Result<(&str, &str)> {
    let (namespace, reference) = value
        .split_once(':')
        .ok_or_else(|| invalid_registry(format!("chain identifier {value:?} is missing ':'")))?;
    if !(3..=8).contains(&namespace.len())
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(invalid_registry(format!(
            "chain identifier {value:?} has an invalid namespace"
        )));
    }
    if reference.is_empty()
        || reference.len() > 32
        || !reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(invalid_registry(format!(
            "chain identifier {value:?} has an invalid reference"
        )));
    }
    Ok((namespace, reference))
}

fn invalid_registry(reason: impl Into<String>) -> Error {
    Error::InvalidAssetRegistry {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value as JsonValue, json};

    // Tests obtain their fixture through primitives; services never embed a second copy.
    fn registry_json() -> String {
        shared_assets::test_support::registry_json().to_owned()
    }

    fn registry_value() -> JsonValue {
        serde_json::from_str(&registry_json()).expect("bundled registry JSON")
    }

    fn assert_invalid_registry(value: &JsonValue) {
        let serialized = serde_json::to_string(value).expect("serialize registry fixture");
        assert!(matches!(
            AssetRegistry::from_json(&serialized),
            Err(Error::InvalidAssetRegistry { .. })
        ));
    }

    #[test]
    fn loads_all_registry_surfaces_and_keeps_same_symbol_assets_distinct() {
        let registry = AssetRegistry::bundled().expect("bundled registry");

        assert_eq!(registry.chain_count(), 2);
        assert_eq!(registry.venue_count(), 3);
        assert_eq!(registry.price_count(), 4);
        assert_eq!(registry.asset_count(), 3);
        assert_eq!(registry.perp_market_count(), 2);
        assert_ne!(
            registry.asset(1).expect("HyperCore USDC").price_index,
            registry.asset(2).expect("Ethereum USDC").price_index
        );
        assert_eq!(
            registry.price(1).expect("HyperCore price").source,
            PriceSourceKind::Hyperliquid
        );
        assert_eq!(
            registry.price(2).expect("Ethereum price").source,
            PriceSourceKind::Ethereum
        );
    }

    #[test]
    fn resolved_accounting_metadata_and_collateral_factors_are_preserved() {
        let registry = AssetRegistry::bundled().expect("bundled registry");
        let ethereum = registry.asset(2).expect("Ethereum USDC");
        let shared = shared_assets::info(tackt_primitives::ledger::LedgerAsset::Usdc);
        assert_eq!(ethereum.caip19.as_str(), shared.caip19);
        assert_eq!(ethereum.symbol.as_ref(), shared.symbol);
        assert_eq!(ethereum.decimals, shared.decimals);
        for (index, factor, decimals) in [(1, 1_000_000, 8), (2, 900_000, 6), (3, 800_000, 8)] {
            let asset = registry.asset(index).expect("asset");
            assert_eq!(asset.collateral_factor.raw(), factor);
            assert_eq!(asset.decimals, decimals);
            assert!(asset.collateral_enabled);
        }
    }

    #[test]
    fn derives_margin_tier_rates_and_continuity_deductions() {
        let registry = AssetRegistry::bundled().expect("bundled registry");

        let btc = &registry.perp_market(1).expect("BTC market").margin_tiers;
        assert_eq!(btc.len(), 2);
        assert_eq!(btc[0].maintenance_factor, Factor::from_raw(12_500));
        assert_eq!(btc[0].maintenance_deduction, Value::zero());
        assert_eq!(btc[1].maintenance_factor, Factor::from_raw(25_000));
        assert_eq!(
            btc[1].maintenance_deduction,
            Value::from_raw(187_500_000_000_000)
        );

        let ethereum = &registry.perp_market(2).expect("ETH market").margin_tiers;
        assert_eq!(ethereum[0].maintenance_factor, Factor::from_raw(20_000));
        assert_eq!(ethereum[1].maintenance_factor, Factor::from_raw(33_334));
    }

    #[test]
    fn rejects_disordered_margin_tiers() {
        let mut invalid = registry_value();
        invalid["perp_markets"][0]["margin_tiers"][1]["max_leverage"] = json!(40);
        assert_invalid_registry(&invalid);

        let mut missing_base = registry_value();
        missing_base["perp_markets"][0]["margin_tiers"][0]["lower_bound_notional"] = json!(1);
        assert_invalid_registry(&missing_base);
    }

    #[test]
    fn rejects_hyperevm_and_cross_source_mapping() {
        let hyperevm = registry_json().replace("\"kind\": \"ethereum\"", "\"kind\": \"hyper_evm\"");
        assert!(AssetRegistry::from_json(&hyperevm).is_err());

        let wrong_source =
            registry_json().replacen("\"source\": \"ethereum\"", "\"source\": \"hyperliquid\"", 1);
        assert!(matches!(
            AssetRegistry::from_json(&wrong_source),
            Err(Error::InvalidAssetRegistry { .. })
        ));
    }

    #[test]
    fn rejects_noncanonical_eip155_chain_references() {
        for invalid_reference in ["mainnet", "01"] {
            let mut invalid = registry_value();
            invalid["chains"][0]["caip2"] = json!(format!("eip155:{invalid_reference}"));
            assert_invalid_registry(&invalid);
        }
    }

    #[test]
    fn rejects_unsupported_asset_namespace_profiles() {
        let mut invalid = registry_value();
        invalid["assets"][8]["caip19"] = json!("hlcore:mainnet/erc20:notanaddress");
        assert_invalid_registry(&invalid);
    }
}
