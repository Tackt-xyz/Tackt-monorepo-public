use std::collections::BTreeMap;

use tackt_primitives::assets::EconomicAsset;
use tackt_primitives::ledger::{BalanceBucket, LedgerAsset};

use crate::processing::{
    Factor, MarginDomainIndex, PositionIndex, Price, RiskState, StateVersion, Value,
};

/// Minimal account inputs; credit debt has no chain or collateral-asset scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditAccount {
    /// Monotonic version of this complete account snapshot.
    pub version: StateVersion,
    /// Complete Ledger balance snapshot, keyed uniquely by representation and bucket.
    /// Quantities stay in registry-defined atomic units; Risk selects and pools collateral.
    pub balances: BTreeMap<(LedgerAsset, BalanceBucket), CollateralBalance>,
    /// Observation time of the coherent balance snapshot, in Unix milliseconds.
    pub balances_as_of_ms: u64,
    /// Pledged margin pools, each counted once. `None` means a required snapshot is missing.
    /// Pool identifiers must be unique within the account, including across venues.
    pub margin_pools: BTreeMap<MarginDomainIndex, Option<PerpMarginSnapshot>>,
    /// Current positions, uniquely keyed within this account and belonging to exactly one pool.
    /// This must be the complete position set covered by the pool snapshots.
    pub perp_positions: BTreeMap<PositionIndex, CreditPerpPosition>,
    /// Total outstanding account debt, including any liabilities recognized by its authority.
    pub debt: Value,
    /// Undrawn commitments only; drawn debt must not also appear here.
    pub pending_draws: Value,
}

/// Authoritative current equity for a pledged margin pool and its complete position set.
///
/// Equity includes unrealized P&L and accrued funding/fees exactly once. It excludes the Tackt
/// loan itself. Callers must have enforceable control of the pool and its closing proceeds;
/// include all positions sharing its equity, including positions consuming that equity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerpMarginSnapshot {
    /// Authoritative evidence of exclusive Tackt control over this pool and closing proceeds.
    pub control: CollateralControl,
    /// Non-zero source version shared with every position in this complete pool snapshot.
    pub source_version: StateVersion,
    /// Signed current equity in the debt quote unit, conservatively normalized to `Value`.
    pub equity: Value,
    /// Observation time of the coherent equity and position snapshot, in Unix milliseconds.
    pub observed_at_ms: u64,
}

/// An authoritative balance and its custody evidence; funding origin does not change eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollateralBalance {
    /// Signed Ledger quantity in this representation's atomic units, not the Risk Amount scale.
    /// Collateral buckets must be non-negative; liability buckets may retain Ledger's sign.
    pub quantity: i128,
    /// Custody evidence covering this entire balance at the account snapshot version.
    pub control: CollateralControl,
}

/// Input-authority custody facts. Risk does not discover or grant control itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollateralControl {
    /// Exclusive, enforceable control over the collateral and its recovery proceeds is verified.
    Controlled,
    /// Known to be outside Tackt control; cannot back credit as free spot.
    Uncontrolled,
    /// Required control evidence is missing or unverified.
    Unknown,
}

/// On-demand explanation of how a Ledger balance contributes to the account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollateralEligibility {
    /// Count for borrowing and liquidation.
    Included,
    /// Still controlled, but reserved: count only for liquidation.
    LiquidationOnly,
    /// Pinned policy excludes deposits that Ledger has not finalized.
    Pending,
    /// Pinned policy excludes reserved balances from both limits.
    Reserved,
    /// Counted through perp pool equity, never as additional spot collateral.
    MarginEquity,
    /// Accounting bucket is not collateral (for example, account debt).
    NotCollateral,
    /// No configured spot policy for this economic asset.
    Unconfigured,
    /// Confirmed outside Tackt custody.
    Uncontrolled,
    /// Eligibility cannot be established from the supplied evidence.
    DataIncomplete,
}

/// Current exposure of one position; equity belongs to its margin pool, never this record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditPerpPosition {
    /// Source version of the owning pool snapshot; mixing versions is rejected.
    pub source_version: StateVersion,
    /// Pool sharing the position's collateral, P&L, funding, and fees.
    pub margin_pool: MarginDomainIndex,
    /// Signed current marked notional in the debt quote unit; magnitude must be rounded up.
    /// Both directions consume reserves. No entry-price or original-leverage input is used.
    pub notional: Value,
}

/// One quote and its observation time, provided explicitly for deterministic freshness checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditPrice {
    /// Price in the same quote unit as account debt.
    pub price: Price,
    /// Observation time in milliseconds since the Unix epoch.
    pub observed_at_ms: u64,
}

/// Shared market inputs and evaluation time. The evaluator never reads the system clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditMarketSnapshot {
    /// Version of this price snapshot, including its evaluation time and freshness limit.
    pub version: StateVersion,
    /// Evaluation time in milliseconds since the Unix epoch.
    pub as_of_ms: u64,
    /// Maximum accepted balance, price, and margin-snapshot age, inclusive, in milliseconds.
    pub max_age_ms: u64,
    /// At most one accepted quote per economic asset.
    pub prices: BTreeMap<EconomicAsset, CreditPrice>,
}

/// Complete calculated measurements; none are retained by the evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditHealth {
    /// Signed spot borrowing contributions plus pool equity minus opening reserves.
    pub borrowing_capacity: Value,
    /// Signed spot liquidation contributions plus pool equity minus maintenance reserves.
    pub liquidation_capacity: Value,
    /// Remaining capacity after outstanding debt and undrawn commitments, floored at zero.
    pub available_credit: Value,
    /// Debt plus undrawn commitments in excess of borrowing capacity, floored at zero.
    pub capacity_shortfall: Value,
    /// Liquidation capacity / debt, rounded down; `i128::MAX` represents zero-debt health.
    /// Classification uses direct value comparisons, never this rounded display ratio.
    pub health_factor: Factor,
    /// Healthy within borrowing capacity, Watch above it, Liquidatable at the debt boundary.
    pub state: RiskState,
}

/// Missing required evidence or policy cannot produce usable credit capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreditEvaluation {
    /// All required inputs were valid and fresh.
    Complete(CreditHealth),
    /// Required balance, custody, price, pool evidence, or perp policy is unavailable. No partial capacity is returned.
    DataIncomplete,
}
