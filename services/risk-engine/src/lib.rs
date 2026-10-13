//! Deterministic in-memory risk engine.
//!
//! The crate is split into four pipeline sections:
//! - [`ingest`]: placeholder boundary for normalized input sources;
//! - [`processing`]: implemented risk state and calculation core;
//! - [`persistence`]: local-disk snapshots of engine inputs for cold-start recovery;
//! - [`egress`]: placeholder boundary for publishing risk decisions.

pub mod egress;
pub mod error;
pub mod ingest;
pub mod persistence;
pub mod processing;

pub use error::{Error, Result};
