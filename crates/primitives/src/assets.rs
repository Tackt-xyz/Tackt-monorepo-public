//! The locked Release 1A asset registry.
//!
//! [`asset-registry.json`](../../../asset-registry.json) at the workspace root is the single
//! definition of which assets Release 1A supports and what each one is: symbol, chain, canonical
//! CAIP-19 identity, token address, decimals, and classification. Every service resolves those
//! facts through this module, so two services built from the same commit cannot disagree about an
//! asset, and no deployment can widen the supported set.
//!
//! The file is embedded at compile time rather than read at runtime: the registry is *locked*, so
//! a process must not be able to start against a different one. It is still ordinary JSON at a
//! stable path, readable by any tool that needs the list.
//!
//! Everything absent from the registry is unsupported and every lookup here fails closed.
//! Unsupported assets may still be recorded as external evidence, but they receive no borrowing
//! value and are never credited, withdrawn, or signed automatically.
//!
//! Each canonical asset entry contains its accounting identity and optional collateral settings.
//! HyperCore entries explicitly omit Ledger support; all services share the same asset and chain lists.
//! Risk owns calculation policy and validates its compiled price/venue mappings and margin tiers.

use std::collections::HashSet;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::ledger::LedgerAsset;

/// Typed settings for the streamed risk model in the same canonical registry.
pub mod risk;

/// Workspace-relative path of the registry file, for diagnostics.
pub const REGISTRY_PATH: &str = "asset-registry.json";

/// Provisional chain identifier of HyperCore mainnet; its assets are Risk-only.
pub const HYPERCORE_CAIP2: &str = "hlcore:mainnet";

/// Largest number of decimal places an asset may declare. Atomic quantities are `u128`, so this
/// only rejects definitions that could never describe a real balance. Public because wire
/// contracts carrying a fixed-point scale bound themselves by the same ceiling.
pub const MAX_DECIMALS: u8 = 38;

const REGISTRY_JSON: &str = include_str!("../../../asset-registry.json");

/// The embedded registry, parsed and validated once.
///
/// # Panics
///
/// Panics if the embedded file is invalid. That is a build defect, not a runtime condition: the
/// file ships inside the binary and this crate's tests validate it. Refusing to run is the
/// fail-closed outcome — a service must never operate on an asset list it could not verify.
static REGISTRY: LazyLock<AssetRegistry> = LazyLock::new(|| {
    AssetRegistry::parse(REGISTRY_JSON)
        .unwrap_or_else(|error| panic!("embedded {REGISTRY_PATH} is invalid: {error}"))
});

/// The locked Release 1A asset registry.
#[must_use]
pub fn registry() -> &'static AssetRegistry {
    &REGISTRY
}

/// Every supported asset, in file order.
#[must_use]
pub fn all() -> &'static [AssetInfo] {
    REGISTRY.assets()
}

/// See [`AssetRegistry::asset`].
#[must_use]
pub fn info(asset: LedgerAsset) -> &'static AssetInfo {
    REGISTRY.asset(asset)
}

/// See [`AssetRegistry::by_symbol`].
///
/// # Errors
///
/// Returns [`AssetError::UnsupportedAsset`] for any code the registry does not define.
pub fn by_symbol(symbol: &str) -> Result<&'static AssetInfo, AssetError> {
    REGISTRY.by_symbol(symbol)
}

/// See [`AssetRegistry::by_caip19`].
///
/// # Errors
///
/// Returns [`AssetError::UnsupportedAsset`] for any identifier the registry does not define.
pub fn by_caip19(caip19: &str) -> Result<&'static AssetInfo, AssetError> {
    REGISTRY.by_caip19(caip19)
}

/// See [`AssetRegistry::by_erc20_contract`].
///
/// # Errors
///
/// Returns [`AssetError::UnsupportedChain`] or [`AssetError::UnsupportedAsset`].
pub fn by_erc20_contract(chain_id: u64, address: &str) -> Result<&'static AssetInfo, AssetError> {
    REGISTRY.by_erc20_contract(chain_id, address)
}

/// See [`AssetRegistry::native_asset`].
///
/// # Errors
///
/// Returns [`AssetError::UnsupportedChain`] or [`AssetError::MissingNativeAsset`].
pub fn native_asset(chain_id: u64) -> Result<&'static AssetInfo, AssetError> {
    REGISTRY.native_asset(chain_id)
}

/// See [`AssetRegistry::ensure_chain`].
///
/// # Errors
///
/// Returns [`AssetError::UnsupportedChain`] for a chain the registry does not cover.
pub fn ensure_chain(chain_id: u64) -> Result<(), AssetError> {
    REGISTRY.ensure_chain(chain_id)
}

/// Compact reference to one supported accounting asset.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct AssetReference {
    asset: LedgerAsset,
}

impl AssetReference {
    /// References an asset in the embedded registry.
    #[must_use]
    pub const fn new(asset: LedgerAsset) -> Self {
        Self { asset }
    }

    /// Accounting asset resolved by this reference.
    #[must_use]
    pub const fn asset(self) -> LedgerAsset {
        self.asset
    }

    /// Canonical registry metadata for this asset.
    #[must_use]
    pub fn info(self) -> &'static AssetInfo {
        info(self.asset)
    }
}

/// How an asset is held on its chain.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenIdentity {
    /// The chain's native currency, which has no contract address.
    Native,
    /// An ERC-20 contract, lowercase and `0x`-prefixed.
    Erc20 { address: String },
    /// An SPL token mint, encoded as a case-sensitive 32-byte base58 address.
    Spl { mint: String },
    /// A provisional HyperCore HIP-1 token identity; never accepted as a Ledger asset.
    Hip1 { token_id: String },
}

/// One chain the registry covers.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChainInfo {
    /// Stable chain index shared with Risk mappings.
    pub index: u16,
    /// Canonical chain identifier: EIP-155, Solana, or provisional `hlcore:mainnet`.
    pub caip2: String,
    pub name: String,
}

/// Chain-independent identity used to group approved representations for risk valuation.
///
/// Membership is explicit in the locked registry, never inferred from a symbol or address.
/// Sharing this identity does not make accounting balances interchangeable, grant collateral
/// eligibility, or define an LTV. Quantities still use each representation's own decimals.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum EconomicAsset {
    /// Ether.
    Eth,
    /// Bitcoin.
    Btc,
    /// SOL.
    Sol,
    /// USDC.
    Usdc,
}

/// Economic form of an instrument, independent of its chain's token standard.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentType {
    /// A chain's native currency.
    Native,
    /// A token representation of an underlying asset, including bridged representations.
    Wrapped,
    /// A liquid staking token.
    Lst,
    /// A wrapper around a liquid staking token.
    WrappedLst,
    /// A Bitcoin yield instrument whose strategy need not be staking.
    YieldBearingBtc,
    /// A token designed to track a stable reference value.
    Stablecoin,
}

/// An explicit characteristic that can accompany different instrument types.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AssetTrait {
    /// Holding the instrument participates in its yield mechanism; no return is guaranteed.
    YieldBearing,
}

/// Structured classification from the locked registry.
///
/// These labels do not define exchange rates, accounting equivalence, or collateral policy.
/// Traits are explicit and never inferred from the instrument type. An empty list means no
/// traits are assigned; native SOL, for example, is distinct from a staked position in SOL.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AssetClassification {
    /// Economic family of the instrument, independent of its chain and accounting code.
    pub economic_asset: EconomicAsset,
    /// One explicit instrument type.
    pub instrument_type: InstrumentType,
    /// Additional characteristics, with duplicate assignments refused at registry load.
    pub traits: Vec<AssetTrait>,
}

impl AssetClassification {
    /// Whether this classification explicitly carries a characteristic.
    #[must_use]
    pub fn has_trait(&self, asset_trait: AssetTrait) -> bool {
        self.traits.contains(&asset_trait)
    }

    fn validate(&self, symbol: &str, token: &TokenIdentity) -> Result<(), RegistryError> {
        if (self.instrument_type == InstrumentType::Native)
            != matches!(token, TokenIdentity::Native)
        {
            return Err(RegistryError::invalid(format!(
                "asset {symbol:?} has an instrument type inconsistent with its native token identity"
            )));
        }
        if self.instrument_type == InstrumentType::YieldBearingBtc
            && self.economic_asset != EconomicAsset::Btc
        {
            return Err(RegistryError::invalid(format!(
                "asset {symbol:?} classifies a non-BTC economic asset as yield-bearing BTC"
            )));
        }
        let mut seen = HashSet::new();
        for asset_trait in &self.traits {
            if !seen.insert(asset_trait) {
                return Err(RegistryError::invalid(format!(
                    "asset {symbol:?} repeats classification trait {asset_trait:?}"
                )));
            }
        }
        Ok(())
    }
}

/// Explicit USD valuation source for account summaries, independent of streamed Risk prices.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssetPricing {
    /// Requires a received aggregate market price.
    Aggregate {},
    /// Uses a fixed registry value without a market observation timestamp.
    FixedUsd {
        /// Positive USD price in units of 1e-8 dollars; 100000000 means $1.
        price_e8: u64,
    },
}

/// One supported asset representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetInfo {
    /// Accounting identity of this asset.
    pub asset: LedgerAsset,
    /// Explicit descriptive metadata; accounting identity remains representation-specific.
    pub classification: AssetClassification,
    /// Explicit USD valuation source from the locked registry.
    pub pricing: AssetPricing,
    /// Code as stored and sent on the wire.
    pub symbol: String,
    /// Canonical CAIP-2 identifier of the chain this representation lives on.
    pub caip2: String,
    /// Canonical CAIP-19 asset identifier.
    pub caip19: String,
    pub token: TokenIdentity,
    /// On-chain decimal places. Quantities are always carried in raw atomic units.
    pub decimals: u8,
    /// Streamed collateral settings from this same asset definition, when configured.
    pub collateral: Option<risk::CollateralSettings>,
}

impl AssetInfo {
    /// EIP-155 chain id, or `None` for a Solana representation.
    #[must_use]
    pub fn evm_chain_id(&self) -> Option<u64> {
        self.caip2.strip_prefix("eip155:")?.parse().ok()
    }

    /// The ERC-20 contract address, or `None` for native currencies and SPL tokens.
    #[must_use]
    pub fn erc20_address(&self) -> Option<&str> {
        match &self.token {
            TokenIdentity::Native | TokenIdentity::Spl { .. } | TokenIdentity::Hip1 { .. } => None,
            TokenIdentity::Erc20 { address } => Some(address),
        }
    }
}

/// One canonical asset entry, including its accounting support and collateral settings.
/// Accounting APIs expose only entries with a registered Ledger identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredAsset {
    /// Accounting code, or None for a Risk-only HyperCore asset.
    pub ledger_asset: Option<LedgerAsset>,
    /// Display symbol; accounting entries retain their existing wire code.
    pub symbol: String,
    /// Explicit descriptive metadata shared across representations.
    pub classification: AssetClassification,
    /// Explicit USD valuation source; never inferred from classification or symbol.
    pub pricing: AssetPricing,
    /// Canonical chain identity, referencing the common chain list.
    #[serde(rename = "chain")]
    pub caip2: String,
    /// Canonical or provisional CAIP-19 identity.
    pub caip19: String,
    /// Token or native-currency identity.
    pub token: TokenIdentity,
    /// Atomic decimal places.
    pub decimals: u8,
    /// Streamed collateral settings, when this representation is in the Risk model.
    pub collateral: Option<risk::CollateralSettings>,
}

/// A validated asset registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetRegistry {
    chains: Vec<ChainInfo>,
    assets: Vec<AssetInfo>,
    catalog: Vec<RegisteredAsset>,
    venues: Vec<risk::VenueDefinition>,
    price_factors: Vec<risk::PriceFactorDefinition>,
    perp_markets: Vec<risk::PerpMarketDefinition>,
}

impl AssetRegistry {
    /// Parses and validates registry JSON.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] for malformed JSON or any definition
    /// that is internally inconsistent, duplicated, or does not cover exactly the accounting
    /// assets this build knows.
    pub fn parse(contents: &str) -> Result<Self, RegistryError> {
        let file: RegistryFile = serde_json::from_str(contents)
            .map_err(|error| RegistryError::Parse(error.to_string()))?;
        let chains = build_chains(file.chains)?;
        let assets = build_assets(&file.assets, &chains)?;
        risk::validate_links(
            &file.assets,
            &file.venues,
            &file.price_factors,
            &file.perp_markets,
        )?;
        Ok(Self {
            chains,
            assets,
            catalog: file.assets,
            venues: file.venues,
            price_factors: file.price_factors,
            perp_markets: file.perp_markets,
        })
    }

    #[must_use]
    pub fn chains(&self) -> &[ChainInfo] {
        &self.chains
    }

    #[must_use]
    pub fn assets(&self) -> &[AssetInfo] {
        &self.assets
    }

    /// All canonical asset entries, including assets explicitly excluded from Ledger.
    #[must_use]
    pub fn catalog(&self) -> &[RegisteredAsset] {
        &self.catalog
    }

    /// Common trading and custody venues.
    #[must_use]
    pub fn venues(&self) -> &[risk::VenueDefinition] {
        &self.venues
    }

    /// Common source-specific valuation prices.
    #[must_use]
    pub fn price_factors(&self) -> &[risk::PriceFactorDefinition] {
        &self.price_factors
    }

    /// Perpetual markets and their margin tiers.
    #[must_use]
    pub fn perp_markets(&self) -> &[risk::PerpMarketDefinition] {
        &self.perp_markets
    }

    /// Approved representations of an economic asset, in registry order.
    ///
    /// This returns identities only; callers must apply collateral policy and normalize each
    /// representation's atomic quantities before combining them.
    pub fn representations(
        &self,
        economic_asset: EconomicAsset,
    ) -> impl Iterator<Item = &AssetInfo> {
        self.assets
            .iter()
            .filter(move |entry| entry.classification.economic_asset == economic_asset)
    }

    /// Representations with this instrument type, in registry order.
    pub fn with_instrument_type(
        &self,
        instrument_type: InstrumentType,
    ) -> impl Iterator<Item = &AssetInfo> {
        self.assets
            .iter()
            .filter(move |entry| entry.classification.instrument_type == instrument_type)
    }

    /// Representations explicitly carrying this trait, in registry order.
    pub fn with_trait(&self, asset_trait: AssetTrait) -> impl Iterator<Item = &AssetInfo> {
        self.assets
            .iter()
            .filter(move |entry| entry.classification.has_trait(asset_trait))
    }

    /// Registry entry for an accounting asset. Total by construction: a registry is rejected at
    /// load unless it defines exactly one entry per [`LedgerAsset`].
    #[must_use]
    pub fn asset(&self, asset: LedgerAsset) -> &AssetInfo {
        self.assets
            .iter()
            .find(|entry| entry.asset == asset)
            .expect("registry validation guarantees an entry per LedgerAsset")
    }

    /// Resolves an asset code exactly as stored and sent on the wire (`"USDC"`).
    ///
    /// # Errors
    ///
    /// Returns [`AssetError::UnsupportedAsset`] for any code the registry does not define,
    /// including one that differs only in case.
    pub fn by_symbol(&self, symbol: &str) -> Result<&AssetInfo, AssetError> {
        self.assets
            .iter()
            .find(|entry| entry.symbol == symbol)
            .ok_or_else(|| self.unsupported_asset(symbol))
    }

    /// Resolves a CAIP-19 asset identifier. EVM identifiers accept hex case variants;
    /// Solana identifiers are matched exactly, preserving base58 case sensitivity.
    ///
    /// # Errors
    ///
    /// Returns [`AssetError::UnsupportedAsset`] for any identifier the registry does not define.
    pub fn by_caip19(&self, caip19: &str) -> Result<&AssetInfo, AssetError> {
        self.assets
            .iter()
            .find(|entry| {
                if entry.caip2.starts_with("eip155:") {
                    entry.caip19.eq_ignore_ascii_case(caip19)
                } else {
                    entry.caip19 == caip19
                }
            })
            .ok_or_else(|| self.unsupported_asset(caip19))
    }

    /// Resolves an ERC-20 contract address on a chain, accepting any hex case.
    ///
    /// A native currency is never returned: it has no contract, and a wrapped token must not
    /// credit the native custody balance.
    ///
    /// # Errors
    ///
    /// Returns [`AssetError::UnsupportedChain`] when the chain is outside the registry, and
    /// [`AssetError::UnsupportedAsset`] when the address is malformed or names an unsupported
    /// token.
    pub fn by_erc20_contract(
        &self,
        chain_id: u64,
        address: &str,
    ) -> Result<&AssetInfo, AssetError> {
        self.ensure_chain(chain_id)?;
        let canonical =
            canonical_erc20_address(address).ok_or_else(|| self.unsupported_asset(address))?;
        self.assets
            .iter()
            .find(|entry| {
                entry.evm_chain_id() == Some(chain_id) && entry.erc20_address() == Some(&canonical)
            })
            .ok_or_else(|| self.unsupported_asset(address))
    }

    /// Resolves an EIP-155 chain's native currency. A chain may register tokens alone, so
    /// this is not total: a caller that credits native transfers must fail closed on the error
    /// rather than assume the currency of any other chain.
    ///
    /// # Errors
    ///
    /// Returns [`AssetError::UnsupportedChain`] when the chain is outside the registry, and
    /// [`AssetError::MissingNativeAsset`] when it registers no native currency.
    pub fn native_asset(&self, chain_id: u64) -> Result<&AssetInfo, AssetError> {
        self.ensure_chain(chain_id)?;
        self.assets
            .iter()
            .find(|entry| {
                entry.evm_chain_id() == Some(chain_id)
                    && matches!(entry.token, TokenIdentity::Native)
            })
            .ok_or(AssetError::MissingNativeAsset { chain_id })
    }

    /// Accepts an EIP-155 chain the registry covers.
    ///
    /// # Errors
    ///
    /// Returns [`AssetError::UnsupportedChain`] for every other chain.
    pub fn ensure_chain(&self, chain_id: u64) -> Result<(), AssetError> {
        let caip2 = format!("eip155:{chain_id}");
        if self.chains.iter().any(|chain| chain.caip2 == caip2) {
            Ok(())
        } else {
            Err(AssetError::UnsupportedChain { chain_id })
        }
    }

    fn unsupported_asset(&self, identifier: &str) -> AssetError {
        AssetError::UnsupportedAsset {
            identifier: identifier.to_owned(),
        }
    }
}

/// An asset or chain outside the supported registry.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AssetError {
    #[error("asset {identifier:?} is not in the asset registry")]
    UnsupportedAsset { identifier: String },
    #[error("chain {chain_id} is not in the asset registry")]
    UnsupportedChain { chain_id: u64 },
    #[error("chain {chain_id} registers no native currency in the asset registry")]
    MissingNativeAsset { chain_id: u64 },
}

/// A registry file that cannot be trusted to describe the supported assets.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("asset registry is not valid JSON: {0}")]
    Parse(String),
    #[error("invalid asset registry: {0}")]
    Invalid(String),
}

impl RegistryError {
    fn invalid(reason: impl Into<String>) -> Self {
        Self::Invalid(reason.into())
    }
}

/// Lowercases an address and accepts it only in the canonical `0x` + 40 hex-digit form.
fn canonical_erc20_address(address: &str) -> Option<String> {
    let canonical = address.to_ascii_lowercase();
    let digits = canonical.strip_prefix("0x")?;
    (digits.len() == 40 && digits.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(canonical)
}

fn valid_chain(caip2: &str) -> bool {
    match caip2.split_once(':') {
        Some(("eip155", reference)) => reference
            .parse::<u64>()
            .is_ok_and(|id| id > 0 && id.to_string() == reference),
        Some(("solana", reference)) => {
            reference.len() == 32 && bs58::decode(reference).into_vec().is_ok()
        }
        Some(("hlcore", "mainnet")) => true,
        _ => false,
    }
}

fn valid_solana_mint(mint: &str) -> bool {
    (32..=44).contains(&mint.len()) && bs58::decode(mint).onto(&mut [0_u8; 32]) == Ok(32)
}

fn build_chains(chains: Vec<ChainInfo>) -> Result<Vec<ChainInfo>, RegistryError> {
    if chains.is_empty() {
        return Err(RegistryError::invalid("registry defines no chains"));
    }
    risk::unique_indices(chains.iter().map(|chain| u32::from(chain.index)))?;
    let mut seen = HashSet::new();
    for chain in &chains {
        if !valid_chain(&chain.caip2) {
            return Err(RegistryError::invalid(format!(
                "unsupported or non-canonical chain {:?}",
                chain.caip2
            )));
        }
        if chain.name.trim().is_empty() {
            return Err(RegistryError::invalid(format!(
                "chain {} has an empty name",
                chain.caip2
            )));
        }
        if !seen.insert(&chain.caip2) {
            return Err(RegistryError::invalid(format!(
                "chain {} is defined more than once",
                chain.caip2
            )));
        }
    }
    Ok(chains)
}

fn build_assets(
    definitions: &[RegisteredAsset],
    chains: &[ChainInfo],
) -> Result<Vec<AssetInfo>, RegistryError> {
    let mut assets: Vec<AssetInfo> = Vec::with_capacity(definitions.len());
    let mut defined = HashSet::new();
    let mut identities = HashSet::new();
    for definition in definitions {
        let chain = chains
            .iter()
            .find(|chain| chain.caip2 == definition.caip2)
            .ok_or_else(|| {
                RegistryError::invalid(format!(
                    "asset {:?} names undefined chain {:?}",
                    definition.symbol, definition.caip2
                ))
            })?;
        let is_hypercore = chain.caip2 == HYPERCORE_CAIP2;
        if let Some(asset) = definition.ledger_asset {
            if definition.symbol != asset.code() || !defined.insert(asset) {
                return Err(RegistryError::invalid(
                    "accounting asset must use its unique wire code",
                ));
            }
            if is_hypercore {
                return Err(RegistryError::invalid(
                    "HyperCore assets are not supported by Ledger",
                ));
            }
        } else if !is_hypercore || definition.collateral.is_none() {
            return Err(RegistryError::invalid(
                "only configured HyperCore assets may omit Ledger support",
            ));
        }
        if definition.symbol.trim().is_empty() {
            return Err(RegistryError::invalid("asset symbol must not be empty"));
        }
        if definition.decimals > MAX_DECIMALS {
            return Err(RegistryError::invalid(format!(
                "asset {:?} declares {} decimals; the maximum is {MAX_DECIMALS}",
                definition.symbol, definition.decimals
            )));
        }
        // Derive CAIP-19 only for token kinds valid on this chain family. A mint is an address,
        // not necessarily an Ed25519 signing key, so it must not require an on-curve check.
        let is_evm = chain.caip2.starts_with("eip155:");
        let is_solana = chain.caip2.starts_with("solana:");
        let expected_caip19 = match (&definition.token, is_evm) {
            (TokenIdentity::Native, _) if is_evm || is_solana => {
                let coin = if is_evm { 60 } else { 501 };
                format!("{}/slip44:{coin}", chain.caip2)
            }
            (TokenIdentity::Erc20 { address }, true)
                if canonical_erc20_address(address).as_ref() == Some(address) =>
            {
                format!("{}/erc20:{address}", chain.caip2)
            }
            (TokenIdentity::Spl { mint }, false) if is_solana && valid_solana_mint(mint) => {
                format!("{}/token:{mint}", chain.caip2)
            }
            (TokenIdentity::Hip1 { token_id }, false)
                if is_hypercore && canonical_hip1_token(token_id) =>
            {
                format!("{}/hip1:{token_id}", chain.caip2)
            }
            _ => {
                return Err(RegistryError::invalid(format!(
                    "asset {:?} has an invalid token identity for chain {}",
                    definition.symbol, chain.caip2
                )));
            }
        };
        if definition.caip19 != expected_caip19 {
            return Err(RegistryError::invalid(format!(
                "asset {:?} declares caip19 {:?} but its chain and token require {expected_caip19:?}",
                definition.symbol, definition.caip19
            )));
        }
        // Distinct accounting assets must not name the same thing on chain. The identifier is
        // derived from the chain and the token above, so this rejects a repeated contract
        // address and a second native currency alike. Without it a lookup would resolve to
        // whichever entry the file happens to list first, and a transfer of one asset could be
        // credited as another.
        if !identities.insert(definition.caip19.clone()) {
            return Err(RegistryError::invalid(format!(
                "asset {:?} repeats the on-chain identity {:?}",
                definition.symbol, definition.caip19
            )));
        }
        definition
            .classification
            .validate(&definition.symbol, &definition.token)?;
        if matches!(definition.pricing, AssetPricing::FixedUsd { price_e8: 0 }) {
            return Err(RegistryError::invalid("fixed USD price must be positive"));
        }
        if let Some(asset) = definition.ledger_asset {
            assets.push(AssetInfo {
                asset,
                classification: definition.classification.clone(),
                pricing: definition.pricing,
                symbol: definition.symbol.clone(),
                caip2: chain.caip2.clone(),
                caip19: definition.caip19.clone(),
                token: definition.token.clone(),
                decimals: definition.decimals,
                collateral: definition.collateral.clone(),
            });
        }
    }
    // Every accounting asset needs an identity: a missing entry would leave balances the registry
    // cannot describe, and callers resolving that asset would panic rather than fail closed.
    if let Some(missing) = LedgerAsset::ALL
        .iter()
        .find(|asset| !defined.contains(*asset))
    {
        return Err(RegistryError::invalid(format!(
            "registry defines no entry for accounting asset {:?}",
            missing.code()
        )));
    }
    Ok(assets)
}

fn canonical_hip1_token(token: &str) -> bool {
    token.strip_prefix("0x").is_some_and(|digits| {
        digits.len() == 32
            && digits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryFile {
    chains: Vec<ChainInfo>,
    assets: Vec<RegisteredAsset>,
    venues: Vec<risk::VenueDefinition>,
    price_factors: Vec<risk::PriceFactorDefinition>,
    perp_markets: Vec<risk::PerpMarketDefinition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    const USDC_CONTRACT: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";

    #[test]
    fn fixed_usd_pricing_is_explicit_for_every_usdc_representation() {
        for asset in registry().catalog() {
            let expected = if asset.classification.economic_asset == EconomicAsset::Usdc {
                AssetPricing::FixedUsd {
                    price_e8: 100_000_000,
                }
            } else {
                AssetPricing::Aggregate {}
            };
            assert_eq!(asset.pricing, expected);
            if let Some(ledger_asset) = asset.ledger_asset {
                assert_eq!(info(ledger_asset).pricing, expected);
            }
        }
    }

    #[test]
    fn pricing_rejects_missing_unknown_and_invalid_fixed_values() {
        for pricing in [
            Value::Null,
            json!({"kind": "unknown"}),
            json!({"kind": "aggregate", "price_e8": 100000000}),
            json!({"kind": "fixed_usd"}),
            json!({"kind": "fixed_usd", "price_e8": 0}),
            json!({"kind": "fixed_usd", "price_e8": -1}),
            json!({"kind": "fixed_usd", "price_e8": 1.5}),
            json!({"kind": "fixed_usd", "price_e8": 100000000, "override": true}),
        ] {
            let mut file = valid_file();
            file["assets"][2]["pricing"] = pricing;
            assert!(parse(&file).is_err());
        }
        let mut file = valid_file();
        file["assets"][2]
            .as_object_mut()
            .expect("asset")
            .remove("pricing");
        assert!(
            parse(&file).is_err(),
            "classification must not imply a fixed price"
        );
    }

    /// A complete, valid registry that the failure cases below mutate one field at a time.
    fn valid_file() -> Value {
        let mut file: Value = serde_json::from_str(REGISTRY_JSON).expect("registry JSON");
        // Keep USDC at index 2 for the existing mutation tests.
        file["assets"].as_array_mut().expect("assets").swap(2, 3);
        file
    }

    fn parse(file: &Value) -> Result<AssetRegistry, RegistryError> {
        AssetRegistry::parse(&file.to_string())
    }

    #[test]
    fn embedded_registry_describes_every_accounting_asset_exactly_once() {
        assert_eq!(all().len(), LedgerAsset::ALL.len());
        for asset in LedgerAsset::ALL {
            let entry = info(asset);
            assert_eq!(entry.asset, asset);
            // The wire code and the registry symbol are separate declarations; a build where they
            // drift would store balances under a code the registry cannot resolve.
            assert_eq!(entry.symbol, asset.code());
            assert_eq!(by_symbol(asset.code()), Ok(entry));
            assert_eq!(by_caip19(&entry.caip19), Ok(entry));
        }
    }

    #[test]
    fn economic_groups_preserve_distinct_accounting_and_chain_identities() {
        for (economic_asset, mainnet, base) in [
            (EconomicAsset::Eth, LedgerAsset::Eth, LedgerAsset::EthBase),
            (
                EconomicAsset::Usdc,
                LedgerAsset::Usdc,
                LedgerAsset::UsdcBase,
            ),
        ] {
            let representations: Vec<_> = registry().representations(economic_asset).collect();
            let expected = if economic_asset == EconomicAsset::Usdc {
                vec![info(mainnet), info(base), info(LedgerAsset::UsdcSolana)]
            } else {
                vec![
                    info(mainnet),
                    info(base),
                    info(LedgerAsset::Weth),
                    info(LedgerAsset::Steth),
                    info(LedgerAsset::Wsteth),
                    info(LedgerAsset::Cbeth),
                ]
            };
            assert_eq!(representations, expected);
            assert_ne!(info(mainnet).asset, info(base).asset);
            assert_ne!(info(mainnet).caip19, info(base).caip19);
            assert_eq!(by_caip19(&info(base).caip19).unwrap().asset, base);
            // The durable accounting codes remain representation-specific.
            assert_eq!(serde_json::to_value(base).unwrap(), json!(base.code()));
        }
        assert_eq!(
            registry()
                .representations(EconomicAsset::Sol)
                .collect::<Vec<_>>(),
            vec![
                info(LedgerAsset::Sol),
                info(LedgerAsset::SolSolana),
                info(LedgerAsset::Wsol),
                info(LedgerAsset::Jitosol),
                info(LedgerAsset::Msol),
                info(LedgerAsset::Jupsol)
            ]
        );
        assert_eq!(
            registry()
                .representations(EconomicAsset::Btc)
                .collect::<Vec<_>>(),
            vec![
                info(LedgerAsset::Btc),
                info(LedgerAsset::Cbbtc),
                info(LedgerAsset::Tbtc),
                info(LedgerAsset::Lbtc),
                info(LedgerAsset::Solvbtc)
            ]
        );
    }

    #[test]
    fn economic_group_is_required_and_unknown_groups_fail_closed() {
        let mut missing = valid_file();
        missing["assets"][0]["classification"]
            .as_object_mut()
            .unwrap()
            .remove("economic_asset");
        assert!(parse(&missing).is_err());

        for invalid in [
            json!(""),
            json!("eth"),
            json!("DOGE"),
            json!(null),
            json!(1),
        ] {
            let mut file = valid_file();
            file["assets"][0]["classification"]["economic_asset"] = invalid;
            assert!(parse(&file).is_err());
        }
    }

    #[test]
    fn economic_group_comes_from_the_registry_not_the_accounting_symbol() {
        let mut file = valid_file();
        // A synthetic reassignment demonstrates that the field is the sole grouping authority.
        // Economic correctness of a mapping is reviewed when editing the locked registry.
        file["assets"][4]["classification"]["economic_asset"] = json!("SOL");
        let registry = parse(&file).unwrap();
        assert_eq!(
            registry
                .asset(LedgerAsset::EthBase)
                .classification
                .economic_asset,
            EconomicAsset::Sol
        );
        assert_eq!(
            registry
                .representations(EconomicAsset::Sol)
                .map(|entry| entry.asset)
                .collect::<Vec<_>>(),
            vec![
                LedgerAsset::Sol,
                LedgerAsset::EthBase,
                LedgerAsset::SolSolana,
                LedgerAsset::Wsol,
                LedgerAsset::Jitosol,
                LedgerAsset::Msol,
                LedgerAsset::Jupsol
            ]
        );
    }

    #[test]
    fn embedded_classifications_preserve_current_instrument_meanings() {
        for (asset, economic_asset, instrument_type) in [
            (LedgerAsset::Eth, EconomicAsset::Eth, InstrumentType::Native),
            (
                LedgerAsset::EthBase,
                EconomicAsset::Eth,
                InstrumentType::Native,
            ),
            (
                LedgerAsset::Btc,
                EconomicAsset::Btc,
                InstrumentType::Wrapped,
            ),
            (
                LedgerAsset::Sol,
                EconomicAsset::Sol,
                InstrumentType::Wrapped,
            ),
            (
                LedgerAsset::SolSolana,
                EconomicAsset::Sol,
                InstrumentType::Native,
            ),
            (
                LedgerAsset::Usdc,
                EconomicAsset::Usdc,
                InstrumentType::Stablecoin,
            ),
            (
                LedgerAsset::UsdcBase,
                EconomicAsset::Usdc,
                InstrumentType::Stablecoin,
            ),
            (
                LedgerAsset::UsdcSolana,
                EconomicAsset::Usdc,
                InstrumentType::Stablecoin,
            ),
        ] {
            let classification = &info(asset).classification;
            assert_eq!(classification.economic_asset, economic_asset);
            assert_eq!(classification.instrument_type, instrument_type);
            assert!(classification.traits.is_empty());
        }
        assert_eq!(registry().with_trait(AssetTrait::YieldBearing).count(), 7);
        assert_eq!(
            registry().with_instrument_type(InstrumentType::Lst).count(),
            5
        );
    }

    #[test]
    fn classification_queries_combine_family_type_and_explicit_traits() {
        assert_eq!(
            registry()
                .representations(EconomicAsset::Sol)
                .filter(|entry| entry.classification.has_trait(AssetTrait::YieldBearing))
                .map(|entry| entry.asset)
                .collect::<Vec<_>>(),
            vec![LedgerAsset::Jitosol, LedgerAsset::Msol, LedgerAsset::Jupsol]
        );
        assert_eq!(
            registry()
                .with_instrument_type(InstrumentType::Lst)
                .map(|entry| entry.asset)
                .collect::<Vec<_>>(),
            vec![
                LedgerAsset::Jitosol,
                LedgerAsset::Msol,
                LedgerAsset::Jupsol,
                LedgerAsset::Steth,
                LedgerAsset::Cbeth
            ]
        );
        assert_eq!(
            registry()
                .with_trait(AssetTrait::YieldBearing)
                .map(|entry| entry.asset)
                .collect::<Vec<_>>(),
            vec![
                LedgerAsset::Jitosol,
                LedgerAsset::Msol,
                LedgerAsset::Jupsol,
                LedgerAsset::Steth,
                LedgerAsset::Wsteth,
                LedgerAsset::Cbeth,
                LedgerAsset::Lbtc
            ]
        );
        assert_eq!(
            serde_json::to_value(&info(LedgerAsset::Wsteth).classification).expect("serialize"),
            json!({
                "economic_asset": "ETH", "instrument_type": "wrapped_lst", "traits": ["yield_bearing"]
            })
        );
    }

    #[test]
    fn classification_fields_are_required_and_strict() {
        for field in ["economic_asset", "instrument_type", "traits"] {
            let mut file = valid_file();
            file["assets"][0]["classification"]
                .as_object_mut()
                .expect("classification")
                .remove(field);
            assert!(parse(&file).is_err(), "missing {field}");
        }
        for (field, invalid) in [
            ("instrument_type", json!("LST")),
            ("instrument_type", json!("unknown")),
            ("instrument_type", json!(["native", "wrapped"])),
            ("instrument_type", json!(null)),
            ("traits", json!(["unknown"])),
            ("traits", json!(["YieldBearing"])),
            ("traits", json!("yield_bearing")),
            ("traits", json!(null)),
            ("unexpected", json!(true)),
        ] {
            let mut file = valid_file();
            file["assets"][0]["classification"][field] = invalid;
            assert!(parse(&file).is_err(), "invalid {field}");
        }
        let mut legacy = valid_file();
        legacy["assets"][0]
            .as_object_mut()
            .expect("asset")
            .remove("classification");
        legacy["assets"][0]["economic_asset"] = json!("ETH");
        assert!(parse(&legacy).is_err(), "legacy flat classification");
    }

    #[test]
    fn onboarded_instruments_resolve_to_the_verified_deployments() {
        // Pins the reviewed issuer deployments. Address/decimal facts live only in primitives;
        // the source links for this onboarding are recorded in the root README.
        let solana = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp";
        for (asset, mint) in [
            (
                LedgerAsset::Wsol,
                "So11111111111111111111111111111111111111112",
            ),
            (
                LedgerAsset::Jitosol,
                "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
            ),
            (
                LedgerAsset::Msol,
                "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
            ),
            (
                LedgerAsset::Jupsol,
                "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v",
            ),
        ] {
            let entry = info(asset);
            assert_eq!(entry.caip2, solana);
            assert_eq!(entry.token, TokenIdentity::Spl { mint: mint.into() });
            assert_eq!(entry.decimals, 9);
            assert_eq!(by_caip19(&format!("{solana}/token:{mint}")), Ok(entry));
            assert_ne!(entry.asset, LedgerAsset::SolSolana);
            assert_eq!(entry.classification.economic_asset, EconomicAsset::Sol);
        }
        for (asset, address, decimals, family) in [
            (
                LedgerAsset::Weth,
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                18,
                EconomicAsset::Eth,
            ),
            (
                LedgerAsset::Steth,
                "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",
                18,
                EconomicAsset::Eth,
            ),
            (
                LedgerAsset::Wsteth,
                "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
                18,
                EconomicAsset::Eth,
            ),
            (
                LedgerAsset::Cbeth,
                "0xbe9895146f7af43049ca1c1ae358b0541ea49704",
                18,
                EconomicAsset::Eth,
            ),
            (
                LedgerAsset::Cbbtc,
                "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
                8,
                EconomicAsset::Btc,
            ),
            (
                LedgerAsset::Tbtc,
                "0x18084fba666a33d37592fa2633fd49a74dd93a88",
                18,
                EconomicAsset::Btc,
            ),
            (
                LedgerAsset::Lbtc,
                "0x8236a87084f8b84306f72007f36f2618a5634494",
                8,
                EconomicAsset::Btc,
            ),
            (
                LedgerAsset::Solvbtc,
                "0x7a56e1c57c7475ccf742a1832b028f0456652f97",
                18,
                EconomicAsset::Btc,
            ),
        ] {
            let entry = info(asset);
            assert_eq!(entry.evm_chain_id(), Some(1));
            assert_eq!(entry.decimals, decimals);
            assert_eq!(entry.classification.economic_asset, family);
            assert_eq!(by_erc20_contract(1, address), Ok(entry));
            assert_eq!(by_caip19(&format!("eip155:1/erc20:{address}")), Ok(entry));
            assert!(by_erc20_contract(8453, address).is_err());
        }
        assert_eq!(all().len(), 20);
        for asset in LedgerAsset::ALL {
            let encoded = serde_json::to_value(asset).expect("serialize accounting code");
            assert_eq!(encoded, json!(asset.code()));
            assert_eq!(
                serde_json::from_value::<LedgerAsset>(encoded).expect("decode"),
                asset
            );
        }
    }

    #[test]
    fn inconsistent_classification_names_the_asset_and_reason() {
        for (index, field, value, reason) in [
            (
                0,
                "traits",
                json!(["yield_bearing", "yield_bearing"]),
                "repeats classification trait",
            ),
            (
                0,
                "instrument_type",
                json!("wrapped"),
                "native token identity",
            ),
            (
                1,
                "instrument_type",
                json!("native"),
                "native token identity",
            ),
            (
                2,
                "instrument_type",
                json!("yield_bearing_btc"),
                "non-BTC economic asset",
            ),
        ] {
            let mut file = valid_file();
            file["assets"][index]["classification"][field] = value;
            let Err(RegistryError::Invalid(message)) = parse(&file) else {
                panic!("expected classification validation error");
            };
            assert!(message.contains(reason), "{message}");
            assert!(message.contains(file["assets"][index]["symbol"].as_str().expect("symbol")));
        }
    }

    #[test]
    fn embedded_registry_locks_the_release_1a_ethereum_assets() {
        let eth = info(LedgerAsset::Eth);
        assert_eq!(eth.token, TokenIdentity::Native);
        assert_eq!(eth.decimals, 18);
        assert_eq!(eth.caip19, "eip155:1/slip44:60");

        // BTC and SOL are accounted through their wrapped Ethereum representations.
        let btc = info(LedgerAsset::Btc);
        assert_eq!(btc.decimals, 8);
        assert_eq!(
            btc.erc20_address(),
            Some("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599")
        );
        let sol = info(LedgerAsset::Sol);
        assert_eq!(sol.decimals, 9);
        assert_eq!(
            sol.erc20_address(),
            Some("0xd31a59c85ae9d8edefec411d448f90841571b89c")
        );
        let usdc = info(LedgerAsset::Usdc);
        assert_eq!(usdc.decimals, 6);
        assert_eq!(usdc.erc20_address(), Some(USDC_CONTRACT));

        // Base assets are distinct accounting identities on eip155:8453.
        let eth_base = info(LedgerAsset::EthBase);
        assert_eq!(eth_base.token, TokenIdentity::Native);
        assert_eq!(eth_base.evm_chain_id(), Some(8453));
        assert_eq!(eth_base.caip19, "eip155:8453/slip44:60");
        let usdc_base = info(LedgerAsset::UsdcBase);
        assert_eq!(usdc_base.evm_chain_id(), Some(8453));
        assert_eq!(usdc_base.decimals, 6);
        assert_eq!(
            usdc_base.erc20_address(),
            Some("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
        );
    }

    #[test]
    fn solana_representations_keep_their_own_identity_and_case() {
        let sol = info(LedgerAsset::SolSolana);
        let usdc = info(LedgerAsset::UsdcSolana);
        let chain = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp";
        let mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        assert_eq!(sol.caip19, format!("{chain}/slip44:501"));
        assert_eq!(sol.token, TokenIdentity::Native);
        assert_eq!(sol.decimals, 9);
        assert_eq!(usdc.caip19, format!("{chain}/token:{mint}"));
        assert_eq!(usdc.token, TokenIdentity::Spl { mint: mint.into() });
        assert_eq!(usdc.decimals, 6);
        assert_eq!(
            usdc.classification.economic_asset,
            info(LedgerAsset::UsdcBase).classification.economic_asset
        );
        assert_ne!(usdc.asset, LedgerAsset::UsdcBase);
        assert_ne!(sol.asset, LedgerAsset::Sol);

        for (entry, code) in [(sol, "SOL_SOLANA"), (usdc, "USDC_SOLANA")] {
            assert_eq!(entry.caip2, chain);
            assert_eq!(entry.evm_chain_id(), None);
            assert_eq!(entry.erc20_address(), None);
            assert_eq!(by_caip19(&entry.caip19), Ok(entry));
            assert_eq!(by_symbol(code), Ok(entry));
            assert_eq!(serde_json::to_value(entry.asset).unwrap(), json!(code));
            assert!(by_caip19(&entry.caip19.to_ascii_lowercase()).is_err());
        }
        assert!(by_caip19(&format!("{chain}/token:{}", mint.to_ascii_lowercase())).is_err());
        assert!(
            by_caip19(&format!(
                "{chain}/token:{}",
                bs58::encode([7; 32]).into_string()
            ))
            .is_err()
        );
        assert!(by_erc20_contract(8453, mint).is_err());
        assert!(
            by_caip19(&format!(
                "solana:EtWTRABZaYq6iMfeYKouRu166VU2xqa1/token:{mint}"
            ))
            .is_err()
        );
    }

    #[test]
    fn solana_registry_rejects_invalid_mints_and_cross_chain_token_kinds() {
        for mint in [
            String::new(),
            "0OIl".into(),
            "1".repeat(31),
            "1".repeat(33),
            bs58::encode([1; 31]).into_string(),
            bs58::encode([1; 33]).into_string(),
        ] {
            let mut file = valid_file();
            file["assets"][7]["token"] = json!({"kind": "spl", "mint": mint});
            // Keep the identifier consistent, so rejection must come from mint validation.
            file["assets"][7]["caip19"] = json!(format!(
                "{}/token:{mint}",
                file["chains"][2]["caip2"].as_str().unwrap()
            ));
            assert!(parse(&file).is_err(), "mint {mint:?}");
        }
        for (asset, token, suffix) in [
            (
                4,
                json!({"kind": "spl", "mint": "11111111111111111111111111111111"}),
                "token:11111111111111111111111111111111".to_owned(),
            ),
            (
                7,
                json!({"kind": "erc20", "address": USDC_CONTRACT}),
                format!("erc20:{USDC_CONTRACT}"),
            ),
        ] {
            let mut file = valid_file();
            file["assets"][asset]["token"] = token;
            file["assets"][asset]["caip19"] = json!(format!(
                "{}/{suffix}",
                file["assets"][asset]["chain"].as_str().unwrap()
            ));
            assert!(parse(&file).is_err());
        }
        let mut duplicate = valid_file();
        duplicate["assets"][6]["classification"] = duplicate["assets"][7]["classification"].clone();
        duplicate["assets"][6]["token"] = duplicate["assets"][7]["token"].clone();
        duplicate["assets"][6]["caip19"] = duplicate["assets"][7]["caip19"].clone();
        assert!(
            parse(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("repeats the on-chain identity")
        );
    }

    #[test]
    fn chain_identifiers_are_canonical_and_supported() {
        for caip2 in [
            "eip155:0",
            "eip155:01",
            "eip155:+1",
            "eip155:18446744073709551616",
            "cosmos:mainnet",
            "solana:mainnet",
            "solana:00000000000000000000000000000000",
        ] {
            let mut file = valid_file();
            file["chains"]
                .as_array_mut()
                .unwrap()
                .push(json!({"caip2": caip2, "name": "invalid"}));
            assert!(parse(&file).is_err(), "chain {caip2}");
        }
    }

    #[test]
    fn unknown_assets_fail_closed() {
        for identifier in ["DOGE", "eth", "", " ETH"] {
            assert!(by_symbol(identifier).is_err(), "{identifier:?} resolved");
            assert!(LedgerAsset::from_code(identifier).is_err());
            assert!(LedgerAsset::from_symbol(identifier).is_none());
        }
        assert!(by_caip19("eip155:1/erc20:0xdead").is_err());
        assert!(by_caip19("eip155:137/slip44:60").is_err());
    }

    #[test]
    fn erc20_lookup_normalizes_case_and_refuses_everything_unregistered() {
        let usdc = info(LedgerAsset::Usdc);
        assert_eq!(by_caip19(&usdc.caip19.to_ascii_uppercase()), Ok(usdc));
        assert_eq!(by_erc20_contract(1, USDC_CONTRACT), Ok(usdc));
        // Ethereum tooling emits checksummed (mixed-case) addresses.
        assert_eq!(
            by_erc20_contract(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            Ok(usdc)
        );
        // A wrapped token must never credit the native custody balance, and native ETH has no
        // contract to be found under.
        assert!(
            all()
                .iter()
                .filter(|entry| entry.evm_chain_id().is_some())
                .all(|entry| {
                    entry.erc20_address().is_some()
                        || matches!(entry.asset, LedgerAsset::Eth | LedgerAsset::EthBase)
                })
        );
        for address in [
            "0xcccccccccccccccccccccccccccccccccccccccc", // unregistered contract
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb4",  // one digit short
            "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   // no 0x prefix
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606ebZZ", // not hex
            "",
        ] {
            assert!(
                matches!(
                    by_erc20_contract(1, address),
                    Err(AssetError::UnsupportedAsset { .. })
                ),
                "{address:?} resolved"
            );
        }
    }

    #[test]
    fn foreign_chains_fail_closed() {
        assert_eq!(ensure_chain(1), Ok(()));
        for chain_id in [0, 10, 137, 11_155_111] {
            assert!(matches!(
                ensure_chain(chain_id),
                Err(AssetError::UnsupportedChain { .. })
            ));
            // A registered contract address on an unregistered chain is still unregistered.
            assert!(by_erc20_contract(chain_id, USDC_CONTRACT).is_err());
        }
    }

    #[test]
    fn collateral_and_accounting_metadata_live_on_the_same_asset() {
        let usdc = info(LedgerAsset::Usdc);
        assert_eq!(
            usdc.collateral.as_ref().expect("USDC collateral").factor,
            900_000
        );
        let canonical = registry()
            .catalog()
            .iter()
            .find(|asset| asset.ledger_asset == Some(LedgerAsset::Usdc))
            .expect("USDC entry");
        assert_eq!(canonical.caip19, usdc.caip19);
        assert_eq!(canonical.collateral, usdc.collateral);
        assert_eq!(canonical.classification, usdc.classification);
        assert_eq!(registry().catalog().len(), 22);
        assert_eq!(all().len(), 20);
        for asset in registry()
            .catalog()
            .iter()
            .filter(|asset| asset.ledger_asset.is_none())
        {
            assert!(
                by_caip19(&asset.caip19).is_err(),
                "Risk-only assets must not become accounting assets"
            );
        }
        let mut duplicate = valid_file();
        duplicate["risk"] = json!({});
        assert!(
            parse(&duplicate).is_err(),
            "separate Risk registries are rejected"
        );
        let mut promoted = valid_file();
        promoted["assets"][8]["ledger_asset"] = json!("USDC");
        assert!(
            parse(&promoted).is_err(),
            "HyperCore cannot be promoted to Ledger implicitly"
        );
    }

    #[test]
    fn common_registry_rejects_broken_collateral_references_and_hip1_identity() {
        let mut missing_price = valid_file();
        missing_price["assets"][2]["collateral"]["price_index"] = json!(999);
        assert!(parse(&missing_price).is_err());
        let mut duplicate_index = valid_file();
        duplicate_index["assets"][8]["collateral"]["index"] = json!(2);
        assert!(parse(&duplicate_index).is_err());
        let mut wrong_identity = valid_file();
        wrong_identity["assets"][8]["token"]["token_id"] = json!("0x00");
        assert!(parse(&wrong_identity).is_err());
        let mut cross_chain = valid_file();
        cross_chain["assets"][8]["chain"] = json!("eip155:1");
        assert!(parse(&cross_chain).is_err());
    }

    #[test]
    fn asset_references_round_trip_without_compatibility_metadata() {
        for asset in LedgerAsset::ALL {
            let reference = AssetReference::new(asset);
            let wire = serde_json::to_value(reference).expect("serialize");
            assert_eq!(wire, json!({"asset": asset.code()}));
            let decoded: AssetReference = serde_json::from_value(wire).expect("deserialize");
            assert_eq!(decoded, reference);
            assert_eq!(decoded.info(), info(asset));
        }
        assert!(serde_json::from_value::<AssetReference>(json!({"asset":"UNKNOWN"})).is_err());
    }

    #[test]
    fn the_reference_file_parses() {
        let parsed = parse(&valid_file()).expect("valid registry");
        assert_eq!(parsed.chains().len(), 4);
        assert_eq!(parsed.assets().len(), LedgerAsset::ALL.len());
    }

    #[test]
    fn the_embedded_file_matches_what_this_build_validates() {
        assert_eq!(
            AssetRegistry::parse(REGISTRY_JSON).expect("embedded registry"),
            *registry()
        );
    }

    #[test]
    fn inconsistent_registry_files_are_refused() {
        // Each case breaks exactly one invariant of `valid_file`; all must fail closed rather
        // than load a registry the services would then disagree about.
        type Break = Box<dyn Fn(&mut Value)>;
        let cases: Vec<(&str, Break)> = vec![
            (
                "no chains",
                Box::new(|file: &mut Value| file["chains"] = json!([])),
            ),
            (
                "legacy numeric chain identity",
                Box::new(|file: &mut Value| file["chains"][0]["chain_id"] = json!(137)),
            ),
            (
                "duplicate chain",
                Box::new(|file: &mut Value| {
                    let chain = file["chains"][0].clone();
                    file["chains"].as_array_mut().expect("chains").push(chain);
                }),
            ),
            (
                "legacy chain kind",
                Box::new(|file: &mut Value| file["chains"][0]["kind"] = json!("solana")),
            ),
            (
                "asset on an undeclared chain",
                Box::new(|file: &mut Value| file["assets"][0]["chain"] = json!("eip155:137")),
            ),
            (
                "asset with no accounting identity",
                Box::new(|file: &mut Value| file["assets"][0]["symbol"] = json!("DOGE")),
            ),
            (
                "missing accounting asset",
                Box::new(|file: &mut Value| {
                    file["assets"].as_array_mut().expect("assets").remove(0);
                }),
            ),
            (
                "duplicate asset",
                Box::new(|file: &mut Value| {
                    let asset = file["assets"][2].clone();
                    file["assets"].as_array_mut().expect("assets").push(asset);
                }),
            ),
            (
                "two assets share one contract address",
                Box::new(|file: &mut Value| {
                    let contract = file["assets"][2]["token"]["address"].clone();
                    let caip19 = file["assets"][2]["caip19"].clone();
                    file["assets"][1]["token"]["address"] = contract;
                    file["assets"][1]["caip19"] = caip19;
                }),
            ),
            (
                "two native currencies on one chain",
                Box::new(|file: &mut Value| {
                    file["assets"][1]["token"] = json!({"kind": "native"});
                    file["assets"][1]["caip19"] = json!("eip155:1/slip44:60");
                }),
            ),
            (
                "caip19 disagrees with the token",
                Box::new(|file: &mut Value| {
                    file["assets"][2]["caip19"] =
                        json!("eip155:1/erc20:0xcccccccccccccccccccccccccccccccccccccccc");
                }),
            ),
            (
                "caip19 disagrees with the chain",
                Box::new(|file: &mut Value| {
                    file["assets"][0]["caip19"] = json!("eip155:137/slip44:60");
                }),
            ),
            (
                "uppercase contract address",
                Box::new(|file: &mut Value| {
                    file["assets"][2]["token"]["address"] = json!(USDC_CONTRACT.to_uppercase());
                }),
            ),
            (
                "malformed contract address",
                Box::new(|file: &mut Value| {
                    file["assets"][2]["token"]["address"] = json!("0xdead")
                }),
            ),
            (
                "impossible decimals",
                Box::new(|file: &mut Value| file["assets"][2]["decimals"] = json!(39)),
            ),
            (
                "unknown token kind",
                Box::new(|file: &mut Value| file["assets"][0]["token"] = json!({"kind": "hip1"})),
            ),
            (
                "unexpected field",
                Box::new(|file: &mut Value| file["assets"][2]["collateral_factor"] = json!(1)),
            ),
        ];
        for (name, break_it) in cases {
            let mut file = valid_file();
            break_it(&mut file);
            assert!(parse(&file).is_err(), "{name} was accepted");
        }
    }

    #[test]
    fn a_repeated_on_chain_identity_names_the_offender() {
        // The table above proves these are refused; this pins *why*, so the case cannot start
        // passing for an unrelated reason if validation is reordered.
        let mut file = valid_file();
        let contract = file["assets"][2]["token"]["address"].clone();
        let caip19 = file["assets"][2]["caip19"].clone();
        file["assets"][1]["token"]["address"] = contract;
        file["assets"][1]["caip19"] = caip19;
        let Err(RegistryError::Invalid(reason)) = parse(&file) else {
            panic!("a repeated contract address must be refused");
        };
        assert!(
            reason.contains("repeats the on-chain identity") && reason.contains("USDC"),
            "{reason}"
        );
    }

    #[test]
    fn native_transfers_resolve_by_chain_and_fail_closed() {
        assert_eq!(native_asset(1), Ok(info(LedgerAsset::Eth)));
        assert!(matches!(
            native_asset(137),
            Err(AssetError::UnsupportedChain { .. })
        ));

        // A chain may be registered for its tokens alone. Callers that credit native transfers
        // must not fall back to another chain's currency, so the lookup fails instead.
        let mut file = valid_file();
        file["chains"].as_array_mut().expect("chains").push(json!({
            "index": 5,
            "caip2": "eip155:10",
            "name": "OP Mainnet",
        }));
        let parsed = parse(&file).expect("a token-only chain is a valid registry");
        assert_eq!(
            parsed.native_asset(10),
            Err(AssetError::MissingNativeAsset { chain_id: 10 })
        );
        assert_eq!(
            parsed.native_asset(1).map(|entry| entry.asset),
            Ok(LedgerAsset::Eth)
        );
    }

    #[test]
    fn malformed_json_is_refused() {
        assert!(matches!(
            AssetRegistry::parse("{"),
            Err(RegistryError::Parse(_))
        ));
    }
}

/// Fixtures for dependent crates testing malformed registry definitions.
#[cfg(feature = "test-support")]
pub mod test_support {
    /// The canonical embedded JSON, exposed only for test fixture mutation.
    pub const fn registry_json() -> &'static str {
        super::REGISTRY_JSON
    }
}
