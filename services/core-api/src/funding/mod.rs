//! Safe boundary for external funding.
//!
//! The real provider and topology are intentionally deferred. Normal runtime calls wait for
//! funding; local/test simulation returns a visibly fake receipt and never creates a balance or
//! observed transfer.

use async_trait::async_trait;
use tackt_primitives::{accounts::SourceAccount, assets::AssetReference};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct FundingRequest {
    action_id: Uuid,
    account: SourceAccount,
    asset: AssetReference,
    quantity: u128,
}

impl FundingRequest {
    /// Constructs a funding request for an already validated Core action.
    ///
    /// Returns `None` for nil action IDs or zero quantities so malformed work never reaches a
    /// provider boundary.
    #[must_use]
    pub fn new(
        action_id: Uuid,
        account: SourceAccount,
        asset: AssetReference,
        quantity: u128,
    ) -> Option<Self> {
        if action_id.is_nil() || quantity == 0 {
            return None;
        }
        Some(Self {
            action_id,
            account,
            asset,
            quantity,
        })
    }

    pub fn action_id(&self) -> Uuid {
        self.action_id
    }

    pub fn account(&self) -> &SourceAccount {
        &self.account
    }

    pub fn asset(&self) -> AssetReference {
        self.asset
    }

    pub fn quantity(&self) -> u128 {
        self.quantity
    }
}

/// A funding boundary outcome. Neither variant is accounting or transfer evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FundingOutcome {
    WaitingForFunding,
    Simulated { receipt: String },
}

#[async_trait]
pub trait FundingCapability: Send + Sync {
    async fn fund(&self, request: &FundingRequest) -> FundingOutcome;
}

/// Test-scaffolding selection for the unwired funding boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FundingMode {
    #[default]
    Unavailable,
    Simulated,
}

static UNAVAILABLE_FUNDING: UnavailableFunding = UnavailableFunding;
static SIMULATED_FUNDING: SimulatedFunding = SimulatedFunding;

#[must_use]
pub fn capability(mode: FundingMode) -> &'static dyn FundingCapability {
    match mode {
        FundingMode::Unavailable => &UNAVAILABLE_FUNDING,
        FundingMode::Simulated => &SIMULATED_FUNDING,
    }
}

struct UnavailableFunding;

#[async_trait]
impl FundingCapability for UnavailableFunding {
    async fn fund(&self, _request: &FundingRequest) -> FundingOutcome {
        FundingOutcome::WaitingForFunding
    }
}

struct SimulatedFunding;

#[async_trait]
impl FundingCapability for SimulatedFunding {
    async fn fund(&self, request: &FundingRequest) -> FundingOutcome {
        FundingOutcome::Simulated {
            receipt: format!("SIMULATED-FUNDING-NOT-A-TRANSFER:{}", request.action_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use tackt_primitives::{Source, accounts::AccountSubject, ledger::LedgerAsset};

    use super::*;

    fn account() -> SourceAccount {
        SourceAccount::new(
            AccountSubject::new(Uuid::from_u128(2)).expect("non-nil user"),
            Source::Hyperliquid,
            "0x2222222222222222222222222222222222222222",
        )
        .expect("valid account")
    }

    fn request() -> FundingRequest {
        FundingRequest::new(
            Uuid::from_u128(1),
            account(),
            AssetReference::new(LedgerAsset::Usdc),
            1_000_000,
        )
        .expect("valid request")
    }

    #[test]
    fn malformed_requests_do_not_reach_the_capability() {
        assert!(
            FundingRequest::new(
                Uuid::nil(),
                account(),
                AssetReference::new(LedgerAsset::Usdc),
                1,
            )
            .is_none()
        );
        assert!(
            FundingRequest::new(
                Uuid::from_u128(1),
                account(),
                AssetReference::new(LedgerAsset::Usdc),
                0,
            )
            .is_none()
        );
    }

    #[test]
    fn request_accessors_round_trip_constructor_values() {
        let account = account();
        let asset = AssetReference::new(LedgerAsset::Usdc);
        let request = FundingRequest::new(Uuid::from_u128(1), account.clone(), asset, 42)
            .expect("valid request");

        assert_eq!(request.action_id(), Uuid::from_u128(1));
        assert_eq!(request.account(), &account);
        assert_eq!(request.asset(), asset);
        assert_eq!(request.quantity(), 42);
    }

    #[tokio::test]
    async fn unavailable_funding_waits_without_creating_external_evidence() {
        assert_eq!(
            capability(FundingMode::Unavailable).fund(&request()).await,
            FundingOutcome::WaitingForFunding
        );
    }

    #[tokio::test]
    async fn simulation_is_unmistakably_fake() {
        let FundingOutcome::Simulated { receipt } =
            capability(FundingMode::Simulated).fund(&request()).await
        else {
            panic!("simulation did not return a simulated result");
        };
        assert_eq!(
            receipt,
            "SIMULATED-FUNDING-NOT-A-TRANSFER:00000000-0000-0000-0000-000000000001"
        );
    }
}
