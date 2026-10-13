//! Deterministic Ethereum and Hyperliquid risk-processing core.

mod arithmetic;
mod asset_registry;
pub mod credit;
mod digest;
mod engine;
mod evaluation;
mod policy;
mod portfolio;
mod state;
mod types;

pub use arithmetic::{
    AMOUNT_SCALE, FACTOR_SCALE, MAX_AMOUNT_RAW, MAX_FACTOR_RAW, MAX_PRICE_RAW, MAX_VALUE_RAW,
    PRICE_SCALE, VALUE_SCALE, account_health_ratio, average_maintenance_factor,
    collateral_contribution, debt_contribution, health_ratio, maintenance_delta,
    maintenance_margin, margin_health_ratio, notional_value, perp_pnl,
};
pub use asset_registry::{
    AssetIdentifierProfile, AssetMetadata, AssetRegistry, Caip2ChainId, Caip19AssetId, ChainKind,
    ChainMetadata, MarginTier, PerpMarketMetadata, PriceFactorMetadata, VenueKind, VenueMetadata,
};
pub use engine::RiskProcessor;
pub use evaluation::{
    EvaluationInput, MAX_EVALUATION_RECORDS, ProjectedAccount, ProjectedEthereumBalance,
    ProjectedHyperCoreBalance, ProjectedHyperliquidSnapshot, ProjectedMarginDomain,
    ProjectedPerpPosition,
};
pub use policy::RiskPolicy;
pub use portfolio::PortfolioIndex;
pub use state::{AccountSnapshot, AccountStore};
pub use types::{
    AccountIndex, AccountMode, AccountPortfolio, Amount, AssetIndex, ChainIndex, EpochOutcome,
    EpochStatus, EthereumBalance, Factor, Finality, GenerationInstall, HyperCoreBalance,
    HyperliquidSnapshot, MarginDomain, MarginDomainIndex, MarginDomainSnapshot, MarginMode,
    PatchOutcome, PerpMarketIndex, PerpPosition, PortfolioGeneration, PositionIndex, Price,
    PriceIndex, PriceSourceKind, PriceUpdate, ProcessingEpoch, RiskEvent, RiskReason, RiskState,
    SignedAmount, SourceSequence, StateVersion, Value, VenueIndex, VenueMarginSnapshot,
};
