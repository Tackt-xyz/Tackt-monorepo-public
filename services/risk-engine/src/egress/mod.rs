//! Egress boundary.
//!
//! Delivery protocols, persistence, retries, and customer-facing APIs are deliberately out of
//! scope. Implementations publish only the shared advisory contract; internal dense-index
//! events are never an egress API.

use tackt_primitives::risk::RiskRecommendation;
use thiserror::Error;

/// IO-free display-history capture; the host owns persistence.
pub mod history;

pub trait RiskEgress: Send {
    /// Publishes one validated advisory recommendation.
    ///
    /// # Errors
    ///
    /// Returns an egress error when the destination is unavailable or rejects the event.
    fn publish(&mut self, recommendation: &RiskRecommendation) -> Result<(), EgressError>;

    /// Confirms that every event for an epoch has been submitted.
    ///
    /// # Errors
    ///
    /// Returns an egress error when the destination cannot be flushed.
    fn flush_epoch(&mut self, epoch: u64) -> Result<(), EgressError>;
}

#[derive(Debug, Error)]
pub enum EgressError {
    #[error("egress destination is unavailable: {0}")]
    Unavailable(String),

    #[error("risk recommendation was rejected: {0}")]
    Rejected(String),
}
