use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString};
use tackt_primitives::accounts::AccountSubject;
use tackt_primitives::risk::RiskClassification;

/// Dense index of an account in the configured account range.
pub type AccountIndex = u32;
/// Registry index of a standalone asset.
pub type AssetIndex = u32;
/// Registry index of a chain.
pub type ChainIndex = u16;
/// Registry index of a venue.
pub type VenueIndex = u16;
/// Registry index of a price factor.
pub type PriceIndex = u32;
/// Registry index of a Hyperliquid perp market.
pub type PerpMarketIndex = u32;
/// Source-assigned index of a Hyperliquid margin domain.
pub type MarginDomainIndex = u32;
/// Unique identifier of one position record within a portfolio generation.
pub type PositionIndex = u32;
/// Monotonic internal epoch assigned to each accepted update.
pub type ProcessingEpoch = u64;
/// Monotonic per-source input sequence number.
pub type SourceSequence = u64;
/// Monotonic revision of mutable portfolio or account state.
pub type StateVersion = u64;

macro_rules! fixed_point_type {
    ($(#[$documentation:meta])* $name:ident) => {
        $(#[$documentation])*
        #[derive(
            Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[repr(transparent)]
        pub struct $name(i128);

        impl $name {
            #[must_use]
            pub const fn from_raw(raw: i128) -> Self {
                Self(raw)
            }

            #[must_use]
            pub const fn raw(self) -> i128 {
                self.0
            }

            #[must_use]
            pub const fn zero() -> Self {
                Self(0)
            }
        }
    };
}

fixed_point_type! {
    /// Non-negative asset quantity in `1e9` fixed point.
    Amount
}
fixed_point_type! {
    /// Signed position size in `1e9` fixed point.
    SignedAmount
}
fixed_point_type! {
    /// Source price in `1e8` fixed point.
    Price
}
fixed_point_type! {
    /// Risk factor or health ratio in `1e6` fixed point.
    Factor
}
fixed_point_type! {
    /// Monetary value in `1e8` fixed point.
    Value
}

pub use tackt_primitives::assets::risk::PriceSourceKind;

/// Settlement status of an Ethereum balance record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Display, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Finality {
    /// Finalized on chain; contributes to collateral.
    Finalized,
    /// Observed but not yet finalized; contributes nothing.
    InFlight,
    /// Rolled back by a reorganization; contributes nothing.
    Reverted,
}

/// Hyperliquid account margining configuration.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Display, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AccountMode {
    /// Perp margin only.
    Standard,
    /// Spot balances are included in reported account value.
    Unified,
}

/// Isolation mode of a Hyperliquid margin domain.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Display, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MarginMode {
    /// Shared cross margin.
    Cross,
    /// Per-position isolated margin.
    Isolated,
    /// Builder-deployed HIP-3 market margin.
    Hip3,
}

/// One Ethereum ERC-20 balance record in a portfolio generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EthereumBalance {
    /// Owning account.
    pub account: AccountIndex,
    /// Registry index of the ERC-20 asset.
    pub asset: AssetIndex,
    /// Balance amount in `1e9` fixed point.
    pub amount: Amount,
    /// Settlement status; only finalized balances contribute.
    pub finality: Finality,
    /// Generation-unique position identifier.
    pub position: PositionIndex,
    /// Version of the normalized source data.
    pub source_version: StateVersion,
}

/// One `HyperCore` HIP-1 spot balance record in a portfolio generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HyperCoreBalance {
    /// Owning account.
    pub account: AccountIndex,
    /// Registry index of the HIP-1 asset.
    pub asset: AssetIndex,
    /// Balance amount in `1e9` fixed point.
    pub amount: Amount,
    /// Whether the balance is already counted in reported Hyperliquid account value.
    pub included_in_account_value: bool,
    /// Generation-unique position identifier.
    pub position: PositionIndex,
    /// Version of the normalized source data.
    pub source_version: StateVersion,
}

/// Authoritative Hyperliquid account-value and maintenance snapshot for one margin domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HyperliquidSnapshot {
    /// Source-reported account value; may be negative.
    pub account_value: Value,
    /// Source-reported maintenance requirement.
    pub maintenance: Value,
    /// Source sequence at which the snapshot was taken.
    pub source_sequence: SourceSequence,
}

/// Diagnostic view of one Hyperliquid margin domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarginDomainSnapshot {
    /// Owning account.
    pub account: AccountIndex,
    /// Source-assigned domain index.
    pub domain: MarginDomainIndex,
    /// Account margining configuration.
    pub account_mode: AccountMode,
    /// Domain isolation mode.
    pub margin_mode: MarginMode,
    /// Current account value including local deltas.
    pub account_value: Value,
    /// Current maintenance including local deltas.
    pub maintenance: Value,
    /// Domain health ratio.
    pub health: Factor,
    /// Domain-local risk state; diagnostic only.
    pub risk_state: RiskState,
    /// Count of missing inputs affecting the domain.
    pub uncertain_inputs: u32,
}

/// One account's perpetual margin health at a venue, excluding collateral held elsewhere.
/// This is diagnostic evidence, not authorization or the account's Tackt credit health.
/// Measurements are provisional when `risk_state` is `DataIncomplete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueMarginSnapshot {
    /// Owning account.
    pub account: AccountIndex,
    /// Venue whose margin pools are included.
    pub venue: super::VenueKind,
    /// Signed equity summed once per pool, including P&L, funding, and fees.
    pub account_value: Value,
    /// Sum of the pools' current maintenance requirements.
    pub maintenance: Value,
    /// Largest maintenance / equity ratio across independent pools, rounded up.
    /// A value of 1 is the standard liquidation boundary; higher is worse.
    /// `i128::MAX` denotes negative equity or zero equity with positive maintenance.
    pub margin_ratio: Factor,
    /// Sum of absolute position sizes times their current mark prices, including longs and shorts.
    /// Provisional when a required price is missing.
    pub gross_notional: Value,
    /// Gross notional / total venue equity, rounded up; higher is more leveraged.
    /// `i128::MAX` denotes negative equity or zero equity with positive exposure.
    pub leverage: Factor,
    /// Weakest pool's equity / maintenance; independent pools cannot share equity.
    pub health: Factor,
    /// Weakest pool ratio, so healthy pools cannot conceal an isolated pool's shortfall.
    pub minimum_margin_health: Factor,
    /// Worst pool classification, with missing inputs taking precedence. Liquidatable means
    /// equity < maintenance; Watch uses Tackt's warning threshold, not a venue rule.
    pub risk_state: RiskState,
    /// Number of missing inputs in this venue, independent of other account collateral.
    pub uncertain_inputs: u32,
}

/// Margin-domain membership and its optional authoritative snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarginDomain {
    /// Owning account.
    pub account: AccountIndex,
    /// Source-assigned domain index.
    pub domain: MarginDomainIndex,
    /// Account margining configuration.
    pub account_mode: AccountMode,
    /// Domain isolation mode.
    pub margin_mode: MarginMode,
    /// Authoritative snapshot; `None` marks the domain as data-incomplete.
    pub snapshot: Option<HyperliquidSnapshot>,
    /// Version of the normalized source data.
    pub source_version: StateVersion,
}

/// One signed position in a registered Hyperliquid perp market.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerpPosition {
    /// Owning account.
    pub account: AccountIndex,
    /// Margin domain the position belongs to.
    pub domain: MarginDomainIndex,
    /// Registry index of the perp market.
    pub market: PerpMarketIndex,
    /// Signed position size in `1e9` fixed point.
    pub signed_size: SignedAmount,
    /// Mark price at the snapshot anchor; `None` marks the position as data-incomplete.
    pub anchor_price: Option<Price>,
    /// Generation-unique position identifier.
    pub position: PositionIndex,
    /// Version of the normalized source data.
    pub source_version: StateVersion,
}

/// Immutable, versioned portfolio input covering every tracked account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioGeneration {
    /// Monotonic generation version.
    pub version: StateVersion,
    /// Stable Core subjects keyed by the dense indexes used in this generation.
    /// Every account referenced by a record must be present.
    pub account_subjects: BTreeMap<AccountIndex, AccountSubject>,
    /// Ethereum ERC-20 balance records.
    pub ethereum_balances: Vec<EthereumBalance>,
    /// `HyperCore` HIP-1 spot balance records.
    pub hypercore_balances: Vec<HyperCoreBalance>,
    /// Hyperliquid margin-domain memberships and snapshots.
    pub margin_domains: Vec<MarginDomain>,
    /// Signed Hyperliquid perp positions.
    pub perp_positions: Vec<PerpPosition>,
}

impl PortfolioGeneration {
    #[must_use]
    pub const fn new(
        version: StateVersion,
        ethereum_balances: Vec<EthereumBalance>,
        hypercore_balances: Vec<HyperCoreBalance>,
        margin_domains: Vec<MarginDomain>,
        perp_positions: Vec<PerpPosition>,
    ) -> Self {
        Self {
            version,
            account_subjects: BTreeMap::new(),
            ethereum_balances,
            hypercore_balances,
            margin_domains,
            perp_positions,
        }
    }

    #[must_use]
    pub const fn empty(version: StateVersion) -> Self {
        Self::new(version, Vec::new(), Vec::new(), Vec::new(), Vec::new())
    }

    /// Adds the immutable stable-account bindings for this generation.
    #[must_use]
    pub fn with_account_subjects(
        mut self,
        account_subjects: BTreeMap<AccountIndex, AccountSubject>,
    ) -> Self {
        self.account_subjects = account_subjects;
        self
    }
}

/// One source-bound price update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceUpdate {
    /// Registry price index the update applies to.
    pub price_index: PriceIndex,
    /// Source authority publishing the price.
    pub source: PriceSourceKind,
    /// Monotonic per-source sequence number.
    pub source_sequence: SourceSequence,
    /// New price in `1e8` fixed point.
    pub price: Price,
}

/// Risk classification of an account or margin domain.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum RiskState {
    /// Health is above the warning threshold.
    #[default]
    Healthy,
    /// Health breaches the warning threshold.
    Watch,
    /// Health breaches the liquidation threshold.
    Liquidatable,
    /// A required price, snapshot, anchor, or margin input is missing.
    DataIncomplete,
}

impl RiskState {
    /// The shared advisory classification this internal state publishes as.
    #[must_use]
    pub const fn classification(self) -> RiskClassification {
        match self {
            Self::Healthy => RiskClassification::Healthy,
            Self::Watch => RiskClassification::Watch,
            Self::Liquidatable => RiskClassification::Liquidatable,
            Self::DataIncomplete => RiskClassification::DataIncomplete,
        }
    }
}

/// Cause attached to a risk-state transition.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Display, EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum RiskReason {
    /// Overall health dropped below the warning threshold.
    HealthBelowWarning,
    /// Overall health dropped below the liquidation threshold.
    HealthBelowLiquidation,
    /// A required input is missing.
    MissingInput,
    /// The account returned to a healthy state.
    Recovered,
}

/// One deterministic risk-state transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskEvent {
    /// Affected account.
    pub account: AccountIndex,
    /// Processing epoch that produced the transition.
    pub epoch: ProcessingEpoch,
    /// Account state version after the transition.
    pub state_version: StateVersion,
    /// State before the transition.
    pub previous_state: RiskState,
    /// State after the transition.
    pub current_state: RiskState,
    /// Cause of the transition.
    pub reason: RiskReason,
    /// Pooled collateral at evaluation.
    pub collateral_value: Value,
    /// Pooled liabilities at evaluation.
    pub debt_value: Value,
    /// Total Hyperliquid maintenance at evaluation.
    pub maintenance_value: Value,
    /// Overall account health ratio.
    pub health: Factor,
    /// Weakest individual margin-domain health.
    pub minimum_margin_health: Factor,
}

/// Whether a price update mutated state or repeated the committed cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochStatus {
    /// The update was validated and applied.
    Applied,
    /// The update exactly repeated the last committed update for its source.
    Duplicate,
}

/// Result of applying one price update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochOutcome {
    /// Processing epoch assigned to the update.
    pub epoch: ProcessingEpoch,
    /// Source authority of the update.
    pub source: PriceSourceKind,
    /// Source sequence of the update.
    pub source_sequence: SourceSequence,
    /// Whether the update was applied or was a duplicate.
    pub status: EpochStatus,
    /// Accounts whose aggregates changed.
    pub affected_accounts: usize,
    /// Balance lane records revalued.
    pub affected_balances: usize,
    /// Perp lane records revalued.
    pub affected_perp_positions: usize,
    /// Margin domains whose values changed.
    pub affected_margin_domains: usize,
    /// Worker shards that processed at least one record.
    pub acknowledged_shards: usize,
    /// Risk-state transitions produced by the update.
    pub events: Vec<RiskEvent>,
}

impl EpochOutcome {
    #[must_use]
    pub const fn duplicate(
        epoch: ProcessingEpoch,
        source: PriceSourceKind,
        source_sequence: SourceSequence,
    ) -> Self {
        Self {
            epoch,
            source,
            source_sequence,
            status: EpochStatus::Duplicate,
            affected_accounts: 0,
            affected_balances: 0,
            affected_perp_positions: 0,
            affected_margin_domains: 0,
            acknowledged_shards: 0,
            events: Vec::new(),
        }
    }
}

/// Result of installing a portfolio generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationInstall {
    /// Whether the candidate replaced the installed generation.
    pub installed: bool,
    /// Risk-state transitions caused by the reconstruction.
    pub events: Vec<RiskEvent>,
}

/// One account's complete portfolio, installable alone as an incremental patch.
///
/// A patch wholly replaces the account's previous records; an empty patch removes the
/// account from the portfolio.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountPortfolio {
    /// Account the patch replaces.
    pub account: AccountIndex,
    /// Stable Core subject permanently bound to [`Self::account`].
    pub account_subject: AccountSubject,
    /// Monotonic per-account content version, sharing the generation version space.
    pub version: StateVersion,
    /// Ethereum ERC-20 balance records.
    pub ethereum_balances: Vec<EthereumBalance>,
    /// `HyperCore` HIP-1 spot balance records.
    pub hypercore_balances: Vec<HyperCoreBalance>,
    /// Hyperliquid margin-domain memberships and snapshots.
    pub margin_domains: Vec<MarginDomain>,
    /// Signed Hyperliquid perp positions.
    pub perp_positions: Vec<PerpPosition>,
}

/// Result of applying one account patch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchOutcome {
    /// Processing epoch assigned to the patch, shared with price-update epochs.
    ///
    /// For a duplicate whose content was committed by a generation install rather than an
    /// earlier patch, this is `0`: installs predate patch epoch tracking.
    pub epoch: ProcessingEpoch,
    /// Patched account.
    pub account: AccountIndex,
    /// Whether the patch was applied or repeated already-committed content.
    pub status: EpochStatus,
    /// Risk-state transition caused by the patch, if any.
    pub events: Vec<RiskEvent>,
}
