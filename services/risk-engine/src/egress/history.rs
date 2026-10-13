//! Best-effort display sampling of streamed account health.
//!
//! The engine thread copies scalar state into a bounded channel between completed applies;
//! a host worker owns identity allocation and persistence. Capture never waits on a consumer,
//! allocates, or performs I/O, so it stays outside the processing budget.

use chrono::{DateTime, Utc};
use tackt_primitives::{accounts::AccountSubject, risk::RiskClassification};
use tokio::sync::mpsc;

use crate::processing::{Factor, RiskProcessor};

const METRIC_SAMPLES_DROPPED: &str = "risk_history_samples_dropped_total";
/// A fixed memory bound, independent of outage duration or engine account capacity.
pub const CHANNEL_CAPACITY: usize = 64;
/// Demo history explicitly opts in at most this many stable accounts.
pub const MAX_ACCOUNTS: usize = 32;

/// Scalar-only handoff; sample identity allocation and persistence run off-thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapturedHealth {
    /// Stable account identity resolved through the processor's subject map.
    pub subject: AccountSubject,
    /// Finite ratio in 1e6 fixed point; `None` for incomplete inputs or an unbounded ratio.
    pub health: Option<i128>,
    /// Classification copied from the engine, so history never depends on later thresholds.
    pub classification: RiskClassification,
    /// Host capture time, not source observation time.
    pub sampled_at: DateTime<Utc>,
}

/// Lives exclusively on the engine thread; never scans the full account allocation.
pub struct HistorySampler {
    accounts: Vec<AccountSubject>,
    sender: mpsc::Sender<CapturedHealth>,
}

impl HistorySampler {
    #[must_use]
    pub fn new(accounts: &[AccountSubject], sender: mpsc::Sender<CapturedHealth>) -> Self {
        Self {
            accounts: accounts.to_vec(),
            sender,
        }
    }

    /// Call only between completed applies. A full or closed queue drops a display sample.
    pub fn capture(&self, processor: &RiskProcessor, sampled_at: DateTime<Utc>) {
        if processor.is_poisoned() {
            return;
        }
        for &subject in &self.accounts {
            let Some(index) = processor.account_index(subject) else {
                metrics::counter!(METRIC_SAMPLES_DROPPED, "reason" => "unknown_account")
                    .increment(1);
                continue;
            };
            let Ok(snapshot) = processor.account(index) else {
                continue;
            };
            let captured = CapturedHealth {
                subject,
                health: snapshot.finite_health().map(Factor::raw),
                classification: snapshot.risk_state.classification(),
                sampled_at,
            };
            if self.sender.try_send(captured).is_err() {
                metrics::counter!(METRIC_SAMPLES_DROPPED, "reason" => "queue_unavailable")
                    .increment(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::{
        AccountSnapshot, Amount, AssetRegistry, HyperCoreBalance, PortfolioGeneration, Price,
        PriceSourceKind, PriceUpdate, RiskPolicy, RiskState,
    };
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn fixture() -> (RiskProcessor, AccountSubject) {
        let subject = AccountSubject::new(Uuid::from_u128(1)).expect("subject");
        let registry = AssetRegistry::bundled().expect("registry");
        let mut generation =
            PortfolioGeneration::empty(1).with_account_subjects(BTreeMap::from([(0, subject)]));
        generation.hypercore_balances.push(HyperCoreBalance {
            account: 0,
            asset: 3,
            amount: Amount::from_raw(1_000_000_000),
            included_in_account_value: false,
            position: 0,
            source_version: 1,
        });
        let processor = RiskProcessor::new(
            2,
            1,
            RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(1_000_000))
                .expect("policy"),
            registry,
            &generation,
        )
        .expect("processor");
        (processor, subject)
    }

    fn price(processor: &mut RiskProcessor, sequence: u64, raw: i128) {
        processor
            .apply_price_update(PriceUpdate {
                price_index: 3,
                source: PriceSourceKind::Hyperliquid,
                source_sequence: sequence,
                price: Price::from_raw(raw),
            })
            .expect("price update");
    }

    #[test]
    fn captures_incomplete_and_unbounded_health_without_fabricating_a_number() {
        let (mut processor, subject) = fixture();
        let (sender, mut receiver) = mpsc::channel(4);
        let sampler = HistorySampler::new(&[subject], sender);
        let at = DateTime::from_timestamp_millis(1_800_000_000_000).expect("time");
        sampler.capture(&processor, at);
        let incomplete = receiver.try_recv().expect("sample");
        assert_eq!(
            incomplete.classification,
            RiskClassification::DataIncomplete
        );
        assert!(incomplete.health.is_none());
        price(&mut processor, 1, 100_000_000);
        sampler.capture(&processor, at + chrono::Duration::minutes(1));
        let first = receiver.try_recv().expect("sample");
        assert_eq!(first.classification, RiskClassification::Healthy);
        assert_eq!(first.health, None);
        sampler.capture(&processor, at + chrono::Duration::minutes(2));
        let unchanged = receiver.try_recv().expect("sample");
        assert_eq!(unchanged.health, first.health);
        assert!(unchanged.sampled_at > first.sampled_at);
    }

    #[test]
    fn finite_health_keeps_exact_ratio_and_omits_incomplete_or_unbounded_values() {
        let (processor, _) = fixture();
        let base = processor.account(0).expect("account");
        let snapshot = |risk_state, raw| AccountSnapshot {
            risk_state,
            health: Factor::from_raw(raw),
            ..base.clone()
        };
        assert_eq!(
            snapshot(RiskState::Healthy, 1_234_567).finite_health(),
            Some(Factor::from_raw(1_234_567))
        );
        assert_eq!(
            snapshot(RiskState::Liquidatable, -123).finite_health(),
            Some(Factor::from_raw(-123))
        );
        assert_eq!(
            snapshot(RiskState::Healthy, i128::MAX).finite_health(),
            None
        );
        assert_eq!(
            snapshot(RiskState::Liquidatable, i128::MIN).finite_health(),
            None
        );
        assert_eq!(
            snapshot(RiskState::DataIncomplete, 1_234_567).finite_health(),
            None
        );
    }

    #[test]
    fn saturated_closed_and_unknown_destinations_do_not_mutate_or_block_processing() {
        let (mut processor, subject) = fixture();
        let (sender, mut receiver) = mpsc::channel(1);
        let unknown = AccountSubject::new(Uuid::from_u128(99)).expect("subject");
        let sampler = HistorySampler::new(&[subject, unknown], sender);
        let at = DateTime::from_timestamp_millis(1_800_000_000_000).expect("time");
        let before = processor.account(0).expect("account");
        for _ in 0..1000 {
            sampler.capture(&processor, at);
        }
        assert_eq!(receiver.len(), 1);
        assert_eq!(processor.account(0).expect("account"), before);
        assert_eq!(receiver.try_recv().expect("sample").subject, subject);
        drop(receiver);
        sampler.capture(&processor, at);
        price(&mut processor, 1, 100_000_000);
        assert_eq!(processor.last_committed_epoch(), 1);
    }
}
