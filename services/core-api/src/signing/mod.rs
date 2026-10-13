//! Demo wallet provisioning and transaction signing.
//! The custody-exit workflow capability below remains separate and unwired.

use async_trait::async_trait;
use tackt_primitives::{accounts::SourceAccount, assets::AssetReference, ledger::CommandResult};
use uuid::Uuid;

mod keys;
pub use keys::{addresses_for_keys, backfill_wallet_addresses, generate_user_signing_keys};
pub mod evm;
pub mod hypercore;
pub mod svm;

#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    #[error("{0}")]
    InvalidTransaction(&'static str),
    #[error("user has no signing keys")]
    MissingKeys,
    #[error("stored signing key is invalid")]
    InvalidKey,
    #[error("signing operation failed")]
    SigningFailed,
    #[error(transparent)]
    Repository(#[from] tackt_postgres::UserRepositoryError),
}

/// Call-site ordering marker for a committed Ledger withdrawal hold.
///
/// This token is not authorization and does not independently prove that its action and
/// reservation match Ledger state. Its crate-private constructor requires a Ledger-produced
/// transaction ID and must be called only from the successful Ledger-response branch.
#[derive(Debug, PartialEq, Eq)]
pub struct CommittedWithdrawalHold {
    action_id: Uuid,
    reservation_id: Uuid,
    ledger_transaction_id: Uuid,
}

impl CommittedWithdrawalHold {
    /// Records Ledger's committed reservation result for one Core action.
    ///
    /// Returns `None` for nil identifiers.
    #[must_use]
    // The durable custody-exit workflow is not wired yet.
    #[allow(dead_code)]
    pub(crate) const fn from_ledger_commit(
        action_id: Uuid,
        reservation_id: Uuid,
        result: &CommandResult,
    ) -> Option<Self> {
        if action_id.is_nil() || reservation_id.is_nil() || result.transaction_id.is_nil() {
            return None;
        }
        Some(Self {
            action_id,
            reservation_id,
            ledger_transaction_id: result.transaction_id,
        })
    }
}

/// Typed context required before a custody-exit signing attempt.
#[derive(Debug)]
pub struct SigningRequest {
    pub hold: CommittedWithdrawalHold,
    pub account: SourceAccount,
    pub asset: AssetReference,
}

/// A signing boundary outcome. Neither variant claims that a real transaction exists.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SigningOutcome {
    WaitingForSigning,
    Simulated { receipt: String },
}

#[async_trait]
pub trait SigningCapability: Send + Sync {
    async fn sign(&self, request: SigningRequest) -> SigningOutcome;
}

/// Test-scaffolding selection for the unwired signing boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SigningMode {
    #[default]
    Unavailable,
    Simulated,
}

static UNAVAILABLE_SIGNING: UnavailableSigning = UnavailableSigning;
static SIMULATED_SIGNING: SimulatedSigning = SimulatedSigning;

#[must_use]
pub fn capability(mode: SigningMode) -> &'static dyn SigningCapability {
    match mode {
        SigningMode::Unavailable => &UNAVAILABLE_SIGNING,
        SigningMode::Simulated => &SIMULATED_SIGNING,
    }
}

struct UnavailableSigning;

#[async_trait]
impl SigningCapability for UnavailableSigning {
    async fn sign(&self, _request: SigningRequest) -> SigningOutcome {
        SigningOutcome::WaitingForSigning
    }
}

struct SimulatedSigning;

#[async_trait]
impl SigningCapability for SimulatedSigning {
    async fn sign(&self, request: SigningRequest) -> SigningOutcome {
        SigningOutcome::Simulated {
            receipt: format!(
                "SIMULATED-SIGNING-NOT-A-TRANSACTION:{}",
                request.hold.action_id
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tackt_primitives::{Source, accounts::AccountSubject, ledger::LedgerAsset};

    use super::*;

    fn request() -> SigningRequest {
        let action_id = Uuid::from_u128(1);
        let subject = AccountSubject::new(Uuid::from_u128(2)).expect("non-nil user");
        SigningRequest {
            hold: CommittedWithdrawalHold::from_ledger_commit(
                action_id,
                Uuid::from_u128(3),
                &CommandResult {
                    transaction_id: Uuid::from_u128(4),
                    duplicate: false,
                    recorded_at: Utc::now(),
                },
            )
            .expect("committed hold"),
            account: SourceAccount::new(
                subject,
                Source::Ethereum,
                "0x1111111111111111111111111111111111111111",
            )
            .expect("valid account"),
            asset: AssetReference::new(LedgerAsset::Eth),
        }
    }

    #[test]
    fn nil_identifiers_cannot_become_hold_evidence() {
        assert!(
            CommittedWithdrawalHold::from_ledger_commit(
                Uuid::nil(),
                Uuid::from_u128(1),
                &CommandResult {
                    transaction_id: Uuid::from_u128(2),
                    duplicate: false,
                    recorded_at: Utc::now()
                }
            )
            .is_none()
        );
        assert!(
            CommittedWithdrawalHold::from_ledger_commit(
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                &CommandResult {
                    transaction_id: Uuid::nil(),
                    duplicate: false,
                    recorded_at: Utc::now()
                }
            )
            .is_none()
        );
        assert!(
            CommittedWithdrawalHold::from_ledger_commit(
                Uuid::from_u128(1),
                Uuid::nil(),
                &CommandResult {
                    transaction_id: Uuid::from_u128(2),
                    duplicate: false,
                    recorded_at: Utc::now()
                }
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn unavailable_signing_waits_without_creating_external_evidence() {
        assert_eq!(
            capability(SigningMode::Unavailable).sign(request()).await,
            SigningOutcome::WaitingForSigning
        );
    }

    #[tokio::test]
    async fn simulation_is_unmistakably_fake() {
        let SigningOutcome::Simulated { receipt } =
            capability(SigningMode::Simulated).sign(request()).await
        else {
            panic!("simulation did not return a simulated result");
        };
        assert_eq!(
            receipt,
            "SIMULATED-SIGNING-NOT-A-TRANSACTION:00000000-0000-0000-0000-000000000001"
        );
    }
}
