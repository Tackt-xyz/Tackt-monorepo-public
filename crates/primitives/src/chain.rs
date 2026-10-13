//! Minimal chain position shared by ingestion and accounting.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlockRef {
    pub number: u64,
    pub hash: String,
}
