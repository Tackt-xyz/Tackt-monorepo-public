use thiserror::Error;

use crate::processing::{
    AccountIndex, AssetIndex, MarginDomainIndex, PerpMarketIndex, PositionIndex, PriceIndex,
    PriceSourceKind, ProcessingEpoch, SourceSequence, StateVersion, VenueKind,
};
use tackt_primitives::accounts::AccountSubject;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Validation, arithmetic, and processing failures reported by the risk engine.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("arithmetic overflow while {operation}")]
    ArithmeticOverflow { operation: &'static str },

    #[error("account {account} is outside the configured account range")]
    AccountOutOfRange { account: AccountIndex },

    #[error("portfolio records for account {account} have no stable account subject")]
    MissingAccountSubject { account: AccountIndex },

    #[error("stable account subject {subject} is bound to more than one dense account index")]
    DuplicateAccountSubject { subject: AccountSubject },

    #[error("dense account index {account} is permanently bound to {committed}, not {received}")]
    AccountSubjectRebind {
        account: AccountIndex,
        committed: AccountSubject,
        received: AccountSubject,
    },

    #[error("risk recommendation contract rejected engine output: {reason}")]
    RecommendationContract { reason: String },

    #[error("risk evaluation contract rejected engine output: {reason}")]
    EvaluationContract { reason: String },

    #[error("evaluation action_id must not be nil")]
    InvalidEvaluationActionId,

    #[error("stable account subject {subject} is not installed in the risk engine")]
    UnknownAccountSubject { subject: AccountSubject },

    #[error(
        "evaluation for {subject} expected account version {received}, but the engine has {committed}"
    )]
    EvaluationAccountVersionMismatch {
        subject: AccountSubject,
        received: StateVersion,
        committed: StateVersion,
    },

    #[error("evaluation input version must be greater than zero")]
    InvalidEvaluationInputVersion,

    #[error("evaluation contains {received} portfolio records; maximum is {maximum}")]
    EvaluationRecordLimit { received: usize, maximum: usize },

    #[error("account count must be greater than zero")]
    EmptyAccountSet,

    #[error("account count exceeds AccountIndex capacity")]
    AccountSetTooLarge,

    #[error("shard count must be between 1 and the account count, got {shard_count}")]
    InvalidShardCount { shard_count: usize },

    #[error("price {price_index} must be positive and within bounds")]
    InvalidPrice { price_index: PriceIndex },

    #[error("price {price_index} is not present in the configured registry")]
    UnknownPrice { price_index: PriceIndex },

    #[error("price {price_index} requires source {expected}, but received {received}")]
    PriceSourceMismatch {
        price_index: PriceIndex,
        expected: PriceSourceKind,
        received: PriceSourceKind,
    },

    #[error(
        "stale {price_source} source sequence {received}; last accepted sequence is {committed}"
    )]
    StaleSourceSequence {
        price_source: PriceSourceKind,
        received: SourceSequence,
        committed: SourceSequence,
    },

    #[error("{price_source} source sequence {sequence} conflicts with the accepted update")]
    SourceSequenceConflict {
        price_source: PriceSourceKind,
        sequence: SourceSequence,
    },

    #[error("amount for account {account}, asset {asset} is invalid")]
    InvalidAmount {
        account: AccountIndex,
        asset: AssetIndex,
    },

    #[error("asset {asset} is not present in the configured asset registry")]
    UnknownAsset { asset: AssetIndex },

    #[error("asset {asset} is not valid for {expected} balances")]
    AssetVenueMismatch {
        asset: AssetIndex,
        expected: VenueKind,
    },

    #[error("asset {asset} is not enabled as standalone collateral")]
    CollateralAssetDisabled { asset: AssetIndex },

    #[error("perp market {market} is not present in the configured registry")]
    UnknownPerpMarket { market: PerpMarketIndex },

    #[error("position {position} appears more than once in a portfolio generation")]
    DuplicatePosition { position: PositionIndex },

    #[error("margin domain {domain} for account {account} appears more than once")]
    DuplicateMarginDomain {
        account: AccountIndex,
        domain: MarginDomainIndex,
    },

    #[error("perp position references missing margin domain {domain} for account {account}")]
    MissingMarginDomain {
        account: AccountIndex,
        domain: MarginDomainIndex,
    },

    #[error(
        "account {account} includes asset {asset} in Hyperliquid account value \
         but has no margin domain"
    )]
    IncludedBalanceWithoutMarginDomain {
        account: AccountIndex,
        asset: AssetIndex,
    },

    #[error("source version must be greater than zero for account {account}")]
    InvalidSourceVersion { account: AccountIndex },

    #[error("Hyperliquid snapshot is inconsistent for account {account}, domain {domain}")]
    InconsistentMarginSnapshot {
        account: AccountIndex,
        domain: MarginDomainIndex,
    },

    /// Native venue margin currently supports standard cross and isolated pools only.
    #[error("native venue margin for account {account} requires standard account mode")]
    UnsupportedVenueAccountMode { account: AccountIndex },

    #[error("risk policy thresholds are invalid")]
    InvalidPolicy,

    /// A credit snapshot has invalid quantities or runtime revisions.
    #[error("invalid credit input: {reason}")]
    InvalidCreditInput { reason: &'static str },

    #[error("worker thread panicked while processing epoch {epoch}")]
    WorkerPanicked { epoch: ProcessingEpoch },

    /// A failed apply phase may have left aggregates partially updated; the processor refuses
    /// further price updates until a rebuild or portfolio-generation install succeeds.
    #[error("processor state is poisoned by a failed apply; rebuild or install a generation")]
    ProcessorPoisoned,

    #[error("snapshot epoch {recovered} is behind reconstructed epoch {replayed}")]
    InvalidRecoveredEpoch {
        recovered: ProcessingEpoch,
        replayed: ProcessingEpoch,
    },

    #[error("failed to create the processing worker pool: {0}")]
    WorkerPoolCreation(String),

    #[error("portfolio generation version must be greater than zero")]
    InvalidPortfolioGenerationVersion,

    #[error(
        "reconciliation sweep {received} is older than the newest account patch {newest_patch}"
    )]
    StaleReconciliationSweep {
        received: StateVersion,
        newest_patch: StateVersion,
    },

    #[error("stale patch for account {account}: received {received}, committed {committed}")]
    StaleAccountPatch {
        account: AccountIndex,
        received: StateVersion,
        committed: StateVersion,
    },

    #[error("patch for account {account} at version {version} conflicts with committed content")]
    AccountPatchConflict {
        account: AccountIndex,
        version: StateVersion,
    },

    #[error("patch for account {account} contains a record belonging to another account")]
    PatchAccountMismatch { account: AccountIndex },

    #[error("position {position} already belongs to account {owner}")]
    PositionOwnedByOtherAccount {
        position: PositionIndex,
        owner: AccountIndex,
    },

    #[error("portfolio generation {received} is older than generation {current}")]
    StalePortfolioGeneration {
        received: StateVersion,
        current: StateVersion,
    },

    #[error("portfolio generation {generation} conflicts with the installed generation")]
    PortfolioGenerationConflict { generation: StateVersion },

    #[error("invalid asset registry: {reason}")]
    InvalidAssetRegistry { reason: String },
}
