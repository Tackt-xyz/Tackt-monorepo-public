//! Last-published risk state per account.

use std::collections::BTreeMap;

use crate::processing::{AccountIndex, RiskEvent, RiskState, StateVersion};

/// The most recent risk state acknowledged as delivered downstream for one account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerEntry {
    /// Last published risk classification.
    pub risk_state: RiskState,
    /// Account state version carried by the published event.
    pub state_version: StateVersion,
}

/// Tracks what downstream last heard, so recovery emits only genuine corrections.
///
/// The ledger is persisted inside each snapshot rather than on every publish: a ledger stale
/// by one snapshot interval only re-emits transitions downstream already saw, and consumers
/// dedupe on `(account, state_version)` for at-least-once delivery regardless.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EgressLedger {
    entries: BTreeMap<AccountIndex, LedgerEntry>,
}

impl EgressLedger {
    /// Creates an empty ledger: downstream has heard nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one event as acknowledged by the egress destination.
    pub fn record(&mut self, event: &RiskEvent) {
        self.entries.insert(
            event.account,
            LedgerEntry {
                risk_state: event.current_state,
                state_version: event.state_version,
            },
        );
    }

    /// Returns the last published state for one account, if any was ever published.
    #[must_use]
    pub fn entry(&self, account: AccountIndex) -> Option<LedgerEntry> {
        self.entries.get(&account).copied()
    }

    /// Number of accounts with a published state.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no account has a published state.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) const fn entries(&self) -> &BTreeMap<AccountIndex, LedgerEntry> {
        &self.entries
    }

    pub(crate) const fn from_entries(entries: BTreeMap<AccountIndex, LedgerEntry>) -> Self {
        Self { entries }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::{Factor, RiskReason, Value};

    fn event(account: AccountIndex, state: RiskState, version: StateVersion) -> RiskEvent {
        RiskEvent {
            account,
            epoch: 1,
            state_version: version,
            previous_state: RiskState::Healthy,
            current_state: state,
            reason: RiskReason::HealthBelowWarning,
            collateral_value: Value::zero(),
            debt_value: Value::zero(),
            maintenance_value: Value::zero(),
            health: Factor::zero(),
            minimum_margin_health: Factor::zero(),
        }
    }

    #[test]
    fn record_keeps_the_latest_state_per_account() {
        let mut ledger = EgressLedger::new();
        assert!(ledger.is_empty());
        assert_eq!(ledger.entry(7), None);

        ledger.record(&event(7, RiskState::Watch, 3));
        ledger.record(&event(7, RiskState::Liquidatable, 4));
        ledger.record(&event(9, RiskState::Healthy, 2));

        assert_eq!(ledger.len(), 2);
        assert_eq!(
            ledger.entry(7),
            Some(LedgerEntry {
                risk_state: RiskState::Liquidatable,
                state_version: 4,
            })
        );
        assert_eq!(
            ledger.entry(9),
            Some(LedgerEntry {
                risk_state: RiskState::Healthy,
                state_version: 2,
            })
        );
    }
}
