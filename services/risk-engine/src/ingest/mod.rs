//! Ingest boundary.
//!
//! Source-specific oracle validation, chain indexing, transport, and retries are deliberately
//! out of scope. Implementations only hand normalized, versioned events to the processing core.

use thiserror::Error;

use crate::processing::{AccountPortfolio, PortfolioGeneration, PriceUpdate};

/// One normalized, versioned input handed to the processing core.
///
/// Account patches and portfolio generations share one `StateVersion` space and MUST be
/// stamped by a single monotonic authority: a reconciliation sweep is rejected as stale
/// unless its version is at least the newest patch version it subsumes, so mixing version
/// domains (for example timestamps for patches but small integers for sweeps) permanently
/// wedges the reconciliation path.
#[derive(Debug)]
pub enum IngestEvent {
    /// A source-bound price update.
    Price(PriceUpdate),
    /// A complete immutable portfolio generation (reconciliation sweep).
    Portfolio(PortfolioGeneration),
    /// One account's complete portfolio from a realtime position stream.
    AccountPatch(AccountPortfolio),
}

/// Source of normalized, versioned inputs for the processing core.
pub trait RiskIngest: Send {
    /// Returns the next normalized input, or `None` when no input is currently available.
    ///
    /// # Errors
    ///
    /// Returns an ingest error when the source is unavailable or yields invalid data.
    fn next_event(&mut self) -> Result<Option<IngestEvent>, IngestError>;
}

/// Failures reported by an ingest boundary implementation.
#[derive(Debug, Error)]
pub enum IngestError {
    #[error("ingest source is unavailable: {0}")]
    Unavailable(String),

    #[error("ingest payload is invalid: {0}")]
    InvalidPayload(String),
}
