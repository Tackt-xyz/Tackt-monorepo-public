//! One audit record for every executed ledger command.
//!
//! Financial HTTP runs typed commands against the repository and records every outcome here, so
//! an operator can reconstruct a command's fate from one place with one field set and one set of
//! levels.
//!
//! The settlement worker deliberately does not: it drives `execute` in process and resolves a
//! failure by parking the observation for review, so it keeps its own `event_id`-keyed logging.

use crate::LedgerError;
use tackt_primitives::ledger::CommandResult;

/// Recorded as the `ingress` log field, separating these records from settlement's own.
const INGRESS: &str = "http";

/// Audit `outcome` at the level its variant requires, then hand it back unchanged.
///
/// `idempotency_key` and `kind` are read from the command before execution consumes it. Every
/// branch records both, so a burst of failures can be traced back to the exact commands it
/// covered.
pub fn record(
    idempotency_key: &str,
    kind: &'static str,
    outcome: Result<CommandResult, LedgerError>,
) -> Result<CommandResult, LedgerError> {
    match &outcome {
        Ok(result) if result.duplicate => {
            tracing::debug!(
                ingress = INGRESS,
                idempotency_key,
                kind,
                "duplicate command replayed"
            )
        }
        Ok(result) => {
            tracing::info!(ingress = INGRESS, idempotency_key, kind, transaction_id = %result.transaction_id, "command executed")
        }
        // The caller is answered 503 with the commit state unknown, which is an incident here.
        Err(LedgerError::Storage(error)) => {
            tracing::error!(%error, ingress = INGRESS, idempotency_key, kind, "storage failure")
        }
        Err(LedgerError::Integrity(error)) => {
            tracing::error!(%error, ingress = INGRESS, idempotency_key, kind, "ledger integrity failure")
        }
        // The caller receives this verdict in its response, so a client must not be able to
        // drive this service's error volume.
        Err(
            error @ (LedgerError::Conflict(_)
            | LedgerError::IdempotencyConflict
            | LedgerError::InsufficientFunds
            | LedgerError::InvalidQuantity
            | LedgerError::MissingApproval),
        ) => {
            tracing::warn!(%error, ingress = INGRESS, idempotency_key, kind, "command rejected")
        }
    }
    outcome
}
