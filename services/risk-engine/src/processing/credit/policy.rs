use std::collections::BTreeMap;

use tackt_primitives::assets::{self, EconomicAsset};
use tackt_primitives::ledger::LedgerAsset;

use crate::processing::{FACTOR_SCALE, Factor};
use crate::{Error, Result};

/// Credit limits for one economic asset, expressed in `1e6` fixed point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditAssetPolicy {
    /// Maximum debt plus pending draws as a fraction of collateral value.
    pub max_ltv: Factor,
    /// Outstanding debt boundary as a fraction of collateral value.
    pub liquidation_threshold: Factor,
}

/// Validated policy shared by every account. Missing assets contribute zero capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditPolicy {
    pub(super) assets: BTreeMap<EconomicAsset, CreditAssetPolicy>,
    pub(super) perps: Option<PerpReservePolicy>,
    pub(super) reserved_spot: ReservedSpotPolicy,
    pub(super) pending_deposits: PendingDepositPolicy,
}

/// Eligibility of observed deposits that Ledger has not finalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingDepositPolicy {
    /// Wait until Ledger moves the deposit into Available.
    Excluded,
    /// Count controlled pending deposits for borrowing and liquidation, accepting reorg exposure.
    Included,
}

/// Treatment of Ledger reservations that are still under verified Tackt control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservedSpotPolicy {
    /// Exclude from both borrowing and liquidation capacity.
    Excluded,
    /// Exclude from new borrowing, retain for liquidation until custody ends.
    LiquidationOnly,
}

/// Portfolio lending reserves against each position's current absolute notional.
/// These are Tackt policy rates, independent of the venue's own margin requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerpReservePolicy {
    /// Reserve deducted from pool equity for borrowing capacity.
    pub opening: Factor,
    /// Reserve deducted from pool equity for the liquidation limit.
    pub maintenance: Factor,
}

impl CreditPolicy {
    /// Representations reviewed for the policy's one-to-one economic-asset valuation.
    /// Registry membership alone never opts a new instrument into collateral. New wrappers
    /// need an explicit custody/peg review; yield instruments also need conversion-rate policy.
    pub(super) fn supports_representation(&self, asset: LedgerAsset) -> bool {
        if !self
            .assets
            .contains_key(&assets::info(asset).classification.economic_asset)
        {
            return false;
        }
        match asset {
            LedgerAsset::Eth
            | LedgerAsset::Btc
            | LedgerAsset::Sol
            | LedgerAsset::Usdc
            | LedgerAsset::EthBase
            | LedgerAsset::UsdcBase
            | LedgerAsset::SolSolana
            | LedgerAsset::UsdcSolana => true,
            LedgerAsset::Wsol
            | LedgerAsset::Jitosol
            | LedgerAsset::Msol
            | LedgerAsset::Jupsol
            | LedgerAsset::Weth
            | LedgerAsset::Steth
            | LedgerAsset::Wsteth
            | LedgerAsset::Cbeth
            | LedgerAsset::Cbbtc
            | LedgerAsset::Tbtc
            | LedgerAsset::Lbtc
            | LedgerAsset::Solvbtc => false,
        }
    }

    /// Validates an immutable policy.
    ///
    /// # Errors
    /// Rejects spot rates outside `0 <= LTV < liquidation threshold <= 1`,
    /// or perp rates outside `0 <= maintenance < opening <= 1`.
    pub fn new(
        assets: BTreeMap<EconomicAsset, CreditAssetPolicy>,
        perps: Option<PerpReservePolicy>,
        reserved_spot: ReservedSpotPolicy,
        pending_deposits: PendingDepositPolicy,
    ) -> Result<Self> {
        if assets.values().any(|rates| {
            rates.max_ltv.raw() < 0
                || rates.max_ltv >= rates.liquidation_threshold
                || rates.liquidation_threshold.raw() > FACTOR_SCALE
        }) || perps.is_some_and(|rates| {
            rates.maintenance.raw() < 0
                || rates.maintenance >= rates.opening
                || rates.opening.raw() > FACTOR_SCALE
        }) {
            return Err(Error::InvalidPolicy);
        }
        Ok(Self {
            assets,
            perps,
            reserved_spot,
            pending_deposits,
        })
    }

    /// Current credit policy: 85% spot LTV, 90% liquidation threshold,
    /// 10% opening and 5% maintenance perp reserves, controlled reservations retained
    /// for liquidation, and controlled pending deposits included in both limits.
    ///
    /// # Panics
    /// Panics only if the built-in rates violate policy invariants.
    #[must_use]
    pub fn current() -> Self {
        let rates = CreditAssetPolicy {
            max_ltv: Factor::from_raw(850_000),
            liquidation_threshold: Factor::from_raw(900_000),
        };
        Self::new(
            [EconomicAsset::Eth, EconomicAsset::Btc, EconomicAsset::Sol]
                .into_iter()
                .map(|asset| (asset, rates))
                .collect(),
            Some(PerpReservePolicy {
                opening: Factor::from_raw(100_000),
                maintenance: Factor::from_raw(50_000),
            }),
            ReservedSpotPolicy::LiquidationOnly,
            PendingDepositPolicy::Included,
        )
        .expect("current credit policy is valid")
    }
}
