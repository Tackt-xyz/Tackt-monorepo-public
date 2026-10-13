//! Read-only, versioned financial-evaluation input.

use serde::{Deserialize, Serialize};
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

use super::{
    AccountIndex, AccountMode, AccountPortfolio, Amount, AssetIndex, EthereumBalance, Finality,
    HyperCoreBalance, HyperliquidSnapshot, MarginDomain, MarginDomainIndex, MarginMode,
    PerpMarketIndex, PerpPosition, PositionIndex, Price, SignedAmount, StateVersion,
};

/// Maximum aggregate portfolio records accepted by one synchronous evaluation.
pub const MAX_EVALUATION_RECORDS: usize = 1_024;

/// Validated proposed account state evaluated without mutating the engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluationInput {
    /// Stable Core action identifier, used only for correlation and evidence.
    pub action_id: Uuid,
    /// Stable Core account subject.
    pub account: AccountSubject,
    /// Committed account-input version the projection was based on.
    pub account_version: StateVersion,
    /// Caller-owned version of this projected action input.
    pub input_version: StateVersion,
    /// Complete account state expected after the proposed action.
    pub projected_account: ProjectedAccount,
}

/// Account-local portfolio records; the engine binds them to its private dense index.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedAccount {
    /// Complete projected Ethereum balances for the account.
    pub ethereum_balances: Vec<ProjectedEthereumBalance>,
    /// Complete projected `HyperCore` balances for the account.
    pub hypercore_balances: Vec<ProjectedHyperCoreBalance>,
    /// Complete projected margin domains for the account.
    pub margin_domains: Vec<ProjectedMarginDomain>,
    /// Complete projected perpetual positions for the account.
    pub perp_positions: Vec<ProjectedPerpPosition>,
}

/// Projected Ethereum balance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedEthereumBalance {
    /// Engine asset-registry index.
    pub asset: AssetIndex,
    /// Projected balance amount.
    pub amount: Amount,
    /// Observed Ethereum finality.
    pub finality: Finality,
    /// Stable position index within the account.
    pub position: PositionIndex,
    /// Source-owned record version.
    pub source_version: StateVersion,
}

/// Projected `HyperCore` balance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedHyperCoreBalance {
    /// Engine asset-registry index.
    pub asset: AssetIndex,
    /// Projected balance amount.
    pub amount: Amount,
    /// Whether Hyperliquid includes this balance in account value.
    pub included_in_account_value: bool,
    /// Stable position index within the account.
    pub position: PositionIndex,
    /// Source-owned record version.
    pub source_version: StateVersion,
}

/// Projected Hyperliquid margin domain.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedMarginDomain {
    /// Stable account-local margin-domain index.
    pub domain: MarginDomainIndex,
    /// Projected account mode.
    pub account_mode: AccountMode,
    /// Projected margin mode.
    pub margin_mode: MarginMode,
    /// Complete margin snapshot, or `None` when unavailable.
    pub snapshot: Option<ProjectedHyperliquidSnapshot>,
    /// Source-owned record version.
    pub source_version: StateVersion,
}

/// Strict HTTP representation of a Hyperliquid margin snapshot.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedHyperliquidSnapshot {
    /// Source-reported account value; may be negative.
    pub account_value: super::Value,
    /// Source-reported maintenance requirement.
    pub maintenance: super::Value,
    /// Source sequence at which the snapshot was taken.
    pub source_sequence: super::SourceSequence,
}

impl From<ProjectedHyperliquidSnapshot> for HyperliquidSnapshot {
    fn from(snapshot: ProjectedHyperliquidSnapshot) -> Self {
        Self {
            account_value: snapshot.account_value,
            maintenance: snapshot.maintenance,
            source_sequence: snapshot.source_sequence,
        }
    }
}

/// Projected Hyperliquid perp position.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedPerpPosition {
    /// Owning margin-domain index.
    pub domain: MarginDomainIndex,
    /// Engine perpetual-market index.
    pub market: PerpMarketIndex,
    /// Signed projected position size.
    pub signed_size: SignedAmount,
    /// Entry or anchor price, or `None` when unavailable.
    pub anchor_price: Option<Price>,
    /// Stable position index within the account.
    pub position: PositionIndex,
    /// Source-owned record version.
    pub source_version: StateVersion,
}

impl EvaluationInput {
    pub(crate) fn record_count(&self) -> usize {
        self.projected_account.ethereum_balances.len()
            + self.projected_account.hypercore_balances.len()
            + self.projected_account.margin_domains.len()
            + self.projected_account.perp_positions.len()
    }

    pub(crate) fn bind(self, account: AccountIndex) -> AccountPortfolio {
        let projected = self.projected_account;
        AccountPortfolio {
            account,
            account_subject: self.account,
            version: self.input_version,
            ethereum_balances: projected
                .ethereum_balances
                .into_iter()
                .map(|balance| EthereumBalance {
                    account,
                    asset: balance.asset,
                    amount: balance.amount,
                    finality: balance.finality,
                    position: balance.position,
                    source_version: balance.source_version,
                })
                .collect(),
            hypercore_balances: projected
                .hypercore_balances
                .into_iter()
                .map(|balance| HyperCoreBalance {
                    account,
                    asset: balance.asset,
                    amount: balance.amount,
                    included_in_account_value: balance.included_in_account_value,
                    position: balance.position,
                    source_version: balance.source_version,
                })
                .collect(),
            margin_domains: projected
                .margin_domains
                .into_iter()
                .map(|domain| MarginDomain {
                    account,
                    domain: domain.domain,
                    account_mode: domain.account_mode,
                    margin_mode: domain.margin_mode,
                    snapshot: domain.snapshot.map(Into::into),
                    source_version: domain.source_version,
                })
                .collect(),
            perp_positions: projected
                .perp_positions
                .into_iter()
                .map(|position| PerpPosition {
                    account,
                    domain: position.domain,
                    market: position.market,
                    signed_size: position.signed_size,
                    anchor_price: position.anchor_price,
                    position: position.position,
                    source_version: position.source_version,
                })
                .collect(),
        }
    }
}
