//! Cold-start persistence boundary.
//!
//! Persists the engine's *inputs* — never derived state — so a restart reconstructs the
//! processor from local disk instead of re-querying external sources. The module runs beside
//! the processing core at the same boundary layer as [`crate::ingest`] and [`crate::egress`]:
//! nothing here executes on the price-update hot path.
//!
//! Components:
//!
//! - [`Fold`]: the latest accepted input per account and per price index, mutated only when
//!   the engine accepts an event;
//! - [`EgressLedger`]: the last risk state published downstream per account;
//! - [`Snapshot`]: canonical binary image of fold and ledger with a SHA-256 trailer;
//! - [`SnapshotStore`]: atomic rotated snapshot files on local disk;
//! - [`ApplyLoop`]: binds a processor to its fold and ledger for steady-state operation;
//! - [`recover`]: rebuilds a processor from the newest verifiable snapshot and returns the
//!   corrections downstream needs.
//!
//! Transport stays out of scope: stream cursors are opaque `(stream name, sequence)` pairs
//! recorded on acknowledgement, and tail replay after recovery is the embedding service's
//! responsibility.

mod apply;
mod fold;
mod ledger;
mod recovery;
mod snapshot;
mod store;

use thiserror::Error;

pub use apply::ApplyLoop;
pub use fold::Fold;
pub use ledger::{EgressLedger, LedgerEntry};
pub use recovery::{Recovery, recover};
pub use snapshot::{Snapshot, SnapshotView};
pub use store::SnapshotStore;

/// Result alias for persistence-boundary operations.
pub type Result<T, E = PersistenceError> = std::result::Result<T, E>;

/// Failures reported by the persistence boundary.
#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("snapshot io failure at {path}: {reason}")]
    Io { path: String, reason: String },

    #[error("snapshot encode failed: {reason}")]
    Encode { reason: String },

    #[error("snapshot decode failed: {reason}")]
    Decode { reason: String },

    /// Snapshot files exist but none passed digest verification and decoding.
    #[error("no readable snapshot: all {rejected} candidates failed verification")]
    NoValidSnapshot { rejected: usize },

    /// The store-assigned write ordinal cannot advance past `u64::MAX`.
    #[error("snapshot ordinal space exhausted")]
    OrdinalSpaceExhausted,

    /// Validation or reconstruction failures raised by the processing core.
    #[error(transparent)]
    Processing(#[from] crate::error::Error),
}
