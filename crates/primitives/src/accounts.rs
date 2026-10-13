//! Stable account identities shared across service boundaries.
//!
//! Dense numeric indexes are service-local implementation details. Public contracts use the
//! Core user UUID plus a validated source account, so an index cannot be rebound to another user
//! or interpreted under another source.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::Source;

/// Stable account subject used by account-level contracts.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(try_from = "Uuid")]
pub struct AccountSubject(Uuid);

impl AccountSubject {
    /// Creates a stable subject from a non-nil Core user ID.
    ///
    /// # Errors
    ///
    /// Returns [`AccountError::NilUserId`] for the nil UUID.
    pub fn new(user_id: Uuid) -> Result<Self, AccountError> {
        if user_id.is_nil() {
            Err(AccountError::NilUserId)
        } else {
            Ok(Self(user_id))
        }
    }

    /// The authoritative Core user ID.
    #[must_use]
    pub const fn user_id(self) -> Uuid {
        self.0
    }
}

impl TryFrom<Uuid> for AccountSubject {
    type Error = AccountError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Display for AccountSubject {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

/// One source-owned account belonging to a stable Core user.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(try_from = "SourceAccountWire")]
pub struct SourceAccount {
    subject: AccountSubject,
    source: Source,
    external_id: Box<str>,
}

impl SourceAccount {
    /// Creates a source account and canonicalizes its external identifier.
    ///
    /// Ethereum and Hyperliquid accounts are both EVM addresses in the currently supported
    /// sources. The address is lowercased only after its exact shape is validated.
    ///
    /// # Errors
    ///
    /// Returns [`AccountError::InvalidExternalId`] when the source identifier is not a
    /// `0x`-prefixed 20-byte hexadecimal address.
    pub fn new(
        subject: AccountSubject,
        source: Source,
        external_id: &str,
    ) -> Result<Self, AccountError> {
        let canonical =
            canonical_evm_address(external_id).ok_or_else(|| AccountError::InvalidExternalId {
                account_source: source,
                external_id: external_id.to_owned(),
            })?;
        Ok(Self {
            subject,
            source,
            external_id: canonical.into_boxed_str(),
        })
    }

    /// Stable Core user subject.
    #[must_use]
    pub const fn subject(&self) -> AccountSubject {
        self.subject
    }

    /// External system that owns this account.
    #[must_use]
    pub const fn source(&self) -> Source {
        self.source
    }

    /// Canonical lowercase source identifier.
    #[must_use]
    pub fn external_id(&self) -> &str {
        &self.external_id
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceAccountWire {
    subject: AccountSubject,
    source: Source,
    external_id: String,
}

impl TryFrom<SourceAccountWire> for SourceAccount {
    type Error = AccountError;

    fn try_from(wire: SourceAccountWire) -> Result<Self, Self::Error> {
        Self::new(wire.subject, wire.source, &wire.external_id)
    }
}

/// Invalid shared account identity.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum AccountError {
    #[error("Core user ID must not be nil")]
    NilUserId,
    #[error("{account_source:?} account {external_id:?} is not a canonicalizable EVM address")]
    InvalidExternalId {
        account_source: Source,
        external_id: String,
    },
}

fn canonical_evm_address(value: &str) -> Option<String> {
    let canonical = value.to_ascii_lowercase();
    let digits = canonical.strip_prefix("0x")?;
    (digits.len() == 40 && digits.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(canonical)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn subject() -> AccountSubject {
        AccountSubject::new(Uuid::from_u128(1)).expect("non-nil")
    }

    #[test]
    fn source_accounts_canonicalize_only_valid_addresses() {
        let account = SourceAccount::new(
            subject(),
            Source::Ethereum,
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        )
        .expect("valid address");
        assert_eq!(
            account.external_id(),
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        );
        for invalid in ["", "0xdead", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"] {
            assert!(
                SourceAccount::new(subject(), Source::Ethereum, invalid).is_err(),
                "{invalid:?} was accepted"
            );
        }
    }

    #[test]
    fn deserialization_reapplies_every_invariant() {
        assert!(serde_json::from_value::<AccountSubject>(json!(Uuid::nil())).is_err());
        assert!(
            serde_json::from_value::<SourceAccount>(json!({
                "subject": Uuid::from_u128(1),
                "source": "ethereum",
                "external_id": "0xdead"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<SourceAccount>(json!({
                "subject": Uuid::from_u128(1),
                "source": "ethereum",
                "external_id": "0x1111111111111111111111111111111111111111",
                "bypass": true
            }))
            .is_err()
        );
    }
}
