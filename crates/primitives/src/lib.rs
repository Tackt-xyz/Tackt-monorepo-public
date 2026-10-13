use chrono::{DateTime, Utc};
pub mod chain;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use uuid::Uuid;

use accounts::SourceAccount;
use assets::AssetReference;
use ledger::CustodyExitDestination;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreActionKind {
    CustodyExit(CustodyExitDestination),
    VenueOrder,
}

impl CoreActionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CustodyExit(destination) => destination.code(),
            Self::VenueOrder => "venue_order",
        }
    }

    pub const fn is_custody_exit(self) -> bool {
        matches!(self, Self::CustodyExit(_))
    }

    pub const fn custody_exit_destination(self) -> Option<CustodyExitDestination> {
        match self {
            Self::CustodyExit(destination) => Some(destination),
            Self::VenueOrder => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CoreActionState {
    Pending,
    WaitingForFunding,
    WaitingForSigning,
    ExternallyPending,
    UnknownOutcome,
    Failed,
    Succeeded,
}

impl CoreActionState {
    pub const ALL: [Self; 7] = [
        Self::Pending,
        Self::WaitingForFunding,
        Self::WaitingForSigning,
        Self::ExternallyPending,
        Self::UnknownOutcome,
        Self::Failed,
        Self::Succeeded,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::WaitingForFunding => "waiting_for_funding",
            Self::WaitingForSigning => "waiting_for_signing",
            Self::ExternallyPending => "externally_pending",
            Self::UnknownOutcome => "unknown_outcome",
            Self::Failed => "failed",
            Self::Succeeded => "succeeded",
        }
    }

    pub const fn is_active(self) -> bool {
        !matches!(self, Self::Failed | Self::Succeeded)
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::WaitingForFunding
                    | Self::WaitingForSigning
                    | Self::ExternallyPending
                    | Self::UnknownOutcome
                    | Self::Failed
            ) | (
                Self::WaitingForFunding,
                Self::WaitingForSigning
                    | Self::ExternallyPending
                    | Self::UnknownOutcome
                    | Self::Failed
            ) | (
                Self::WaitingForSigning,
                Self::ExternallyPending | Self::UnknownOutcome | Self::Failed
            ) | (
                Self::ExternallyPending,
                Self::UnknownOutcome | Self::Failed | Self::Succeeded
            ) | (
                Self::UnknownOutcome,
                Self::ExternallyPending | Self::Failed | Self::Succeeded
            )
        )
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    Pending,
    Active,
    Restricted,
    Closed,
}

impl UserStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Restricted => "restricted",
            Self::Closed => "closed",
        }
    }

    /// Allowed lifecycle moves: `closed` is terminal, nothing returns to
    /// `pending`, and restriction only applies to users that were activated.
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Active)
                | (Self::Pending, Self::Closed)
                | (Self::Active, Self::Restricted)
                | (Self::Active, Self::Closed)
                | (Self::Restricted, Self::Active)
                | (Self::Restricted, Self::Closed)
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CoreUser {
    pub id: Uuid,
    pub external_id: String,
    pub status: UserStatus,
    /// Explicit beta enrollment; older user payloads default to no access.
    #[serde(default)]
    pub beta_access: bool,
    /// At most one dedicated account per source; keyed so new sources extend the
    /// map without changing this shape.
    pub accounts: BTreeMap<Source, String>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateCoreUser {
    pub external_id: String,
    #[serde(default = "empty_json_object")]
    pub metadata: Value,
}

pub fn empty_json_object() -> Value {
    Value::Object(Default::default())
}

/// Indexer-facing read projection of a Core user. Writes are owned by the Core
/// API (`CoreUser`); the Indexer only consumes this shape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub id: Uuid,
    pub external_id: String,
    pub enabled: bool,
    pub ethereum_addresses: Vec<String>,
    pub hyperliquid_accounts: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Ethereum,
    Hyperliquid,
}

impl Source {
    pub fn as_str(&self) -> &'static str {
        match self {
            Source::Ethereum => "ethereum",
            Source::Hyperliquid => "hyperliquid",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    EthereumBlock,
    EthereumTransaction,
    EthereumReceipt,
    EthTransfer,
    Erc20Transfer,
    HyperliquidBalance,
    HyperliquidPosition,
    HyperliquidOrder,
    HyperliquidFill,
    HyperliquidFunding,
    HyperliquidFee,
    HyperliquidTransfer,
}

impl EventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::EthereumBlock => "ethereum_block",
            EventKind::EthereumTransaction => "ethereum_transaction",
            EventKind::EthereumReceipt => "ethereum_receipt",
            EventKind::EthTransfer => "eth_transfer",
            EventKind::Erc20Transfer => "erc20_transfer",
            EventKind::HyperliquidBalance => "hyperliquid_balance",
            EventKind::HyperliquidPosition => "hyperliquid_position",
            EventKind::HyperliquidOrder => "hyperliquid_order",
            EventKind::HyperliquidFill => "hyperliquid_fill",
            EventKind::HyperliquidFunding => "hyperliquid_funding",
            EventKind::HyperliquidFee => "hyperliquid_fee",
            EventKind::HyperliquidTransfer => "hyperliquid_transfer",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub event_id: String,
    pub source: Source,
    pub kind: EventKind,
    /// Validated user/source account for account-scoped evidence.
    pub account: Option<SourceAccount>,
    /// Supported Release 1A asset, validated against the locked registry.
    pub asset: Option<AssetReference>,
    /// Source-native asset evidence (for example an ERC-20 contract or Hyperliquid symbol).
    /// Retained for every asset, including tokens outside the accounting registry.
    pub external_asset: Option<String>,
    pub quantity: Option<String>,
    pub source_timestamp: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
    pub confirmations: Option<u64>,
    pub canonical: bool,
    pub finalized: bool,
    pub raw: Value,
    pub provenance: Provenance,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Provenance {
    pub endpoint: String,
    pub method: String,
    pub source_id: String,
}

impl Observation {
    /// The block this fact was observed in, when the source reported both coordinates.
    pub fn block_ref(&self) -> Option<chain::BlockRef> {
        Some(chain::BlockRef {
            number: self.block_number?,
            hash: self.block_hash.clone()?,
        })
    }

    /// Deterministic event identity. Each part is length-prefixed so distinct part
    /// vectors can never collide, regardless of delimiter characters in the data.
    pub fn stable_id(source: &Source, kind: &EventKind, parts: &[&str]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"tackt-observation");
        for part in [source.as_str(), kind.as_str()]
            .into_iter()
            .chain(parts.iter().copied())
        {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        format!("{OBSERVATION_ID_PREFIX}{}", hex::encode(hasher.finalize()))
    }
    /// Deterministic digest of a JSON value, for event identities where the upstream
    /// supplies no stable identifier. serde_json maps are key-sorted, so equal values
    /// always digest identically.
    pub fn content_digest(value: &Value) -> String {
        hex::encode(Sha256::digest(value.to_string().as_bytes()))
    }
}

/// Decodes a SHA-256 digest in its canonical lowercase hexadecimal form.
///
/// Canonical lowercase matters because identities are compared, logged, and hashed as text, so
/// two spellings of one digest must not both be accepted. `hex::decode_to_slice` alone would
/// accept uppercase, which is why the charset is checked first. Callers map `None` onto their own
/// typed error.
#[must_use]
pub(crate) fn decode_sha256_hex(encoded: &str) -> Option<[u8; 32]> {
    if !encoded
        .bytes()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    let mut decoded = [0_u8; 32];
    hex::decode_to_slice(encoded, &mut decoded).ok()?;
    Some(decoded)
}

/// Namespace that [`Observation::stable_id`] owns. Identities derived from an observation carry
/// it, and nothing else may claim a name inside it; the literal value is itself the invariant,
/// because identities on both sides of the boundary are already persisted under it.
pub const OBSERVATION_ID_PREFIX: &str = "obs_";

/// Whether a reference belongs to the namespace reserved above.
#[must_use]
pub fn is_reserved_observation_reference(value: &str) -> bool {
    value.starts_with(OBSERVATION_ID_PREFIX)
}

pub mod accounts;
pub mod assets;
pub mod ledger;
pub mod risk;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_identities_stay_inside_the_reserved_namespace() {
        let event_id = Observation::stable_id(
            &Source::Ethereum,
            &EventKind::EthTransfer,
            &["0xabc", "0", "0xdef"],
        );
        let phase_key = format!("{event_id}:pending");
        // The literal is pinned deliberately: changing it would silently stop recognising every
        // identity already persisted under the old prefix.
        assert_eq!(OBSERVATION_ID_PREFIX, "obs_");
        assert!(event_id.starts_with("obs_"));
        assert!(is_reserved_observation_reference(&event_id));
        assert!(is_reserved_observation_reference(&phase_key));
        for outside in ["authorization-456", "ob_", "obs", "OBS_x", " obs_x"] {
            assert!(!is_reserved_observation_reference(outside), "{outside}");
        }
    }
}
