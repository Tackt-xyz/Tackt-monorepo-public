//! Steady-state binding of a processor to its fold and egress ledger.

use crate::error::Result;
use crate::ingest::IngestEvent;
use crate::processing::{AssetRegistry, PortfolioGeneration, RiskEvent, RiskPolicy, RiskProcessor};
use tackt_primitives::risk::{RecommendationReason, RiskMeasurements, RiskRecommendation};

use super::{EgressLedger, Fold, Snapshot, SnapshotView};

/// Feeds ingest events to the engine and keeps the fold and ledger in lock-step.
///
/// The fold mutates only when the engine accepts an event, so at every acknowledgement
/// boundary the fold equals committed engine input state. Deterministic rejections
/// propagate as errors; per the ingest contract the caller acknowledges and drops them,
/// since replaying them cannot change the verdict. The one exception is
/// [`Error::ProcessorPoisoned`]: it reports the *processor's* health, not the event's
/// validity, so the caller must heal via [`Self::rebuild`] (or a fresh generation install)
/// and re-apply instead of dropping the event.
///
/// [`Error::ProcessorPoisoned`]: crate::error::Error::ProcessorPoisoned
pub struct ApplyLoop {
    processor: RiskProcessor,
    fold: Fold,
    ledger: EgressLedger,
}

impl ApplyLoop {
    /// First-ever boot: constructs the processor and seeds the fold from one generation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or portfolio input.
    pub fn boot(
        account_count: usize,
        shard_count: usize,
        policy: RiskPolicy,
        registry: AssetRegistry,
        generation: &PortfolioGeneration,
    ) -> Result<Self> {
        let processor =
            RiskProcessor::new(account_count, shard_count, policy, registry, generation)?;
        // Group only after the processor has validated every record and account binding.
        let fold = Fold::from_generation(generation);
        Ok(Self {
            processor,
            fold,
            ledger: EgressLedger::new(),
        })
    }

    pub(crate) const fn from_parts(
        processor: RiskProcessor,
        fold: Fold,
        ledger: EgressLedger,
    ) -> Self {
        Self {
            processor,
            fold,
            ledger,
        }
    }

    /// Applies one normalized ingest event and returns the risk events to publish.
    ///
    /// # Errors
    ///
    /// Propagates the engine's rejection unchanged; the fold is untouched on error.
    pub fn apply(&mut self, event: IngestEvent) -> Result<Vec<RiskEvent>> {
        match event {
            IngestEvent::Price(update) => {
                let outcome = self.processor.apply_price_update(update)?;
                self.fold.record_price(update, &outcome);
                Ok(outcome.events)
            }
            IngestEvent::Portfolio(generation) => {
                let install = self.processor.install_portfolio_generation(&generation)?;
                if install.installed {
                    // Group only after the engine accepted the install, consuming the
                    // generation so no record is copied; rejected and duplicate sweeps
                    // never pay for grouping.
                    let version = generation.version;
                    let account_subjects = generation.account_subjects.clone();
                    self.fold.commit_generation(
                        version,
                        account_subjects,
                        Fold::group_generation_owned(generation),
                    );
                }
                Ok(install.events)
            }
            IngestEvent::AccountPatch(patch) => {
                let outcome = self.processor.apply_account_patch(&patch)?;
                self.fold.record_patch(&patch, &outcome);
                Ok(outcome.events)
            }
        }
    }

    /// Records the acknowledged position on one input stream.
    pub fn acknowledge(&mut self, stream: &str, sequence: u64) {
        self.fold.set_cursor(stream, sequence);
    }

    /// Records one risk event as delivered downstream.
    pub fn record_published(&mut self, event: &RiskEvent) {
        self.ledger.record(event);
    }

    /// Converts an internal dense-index transition into the sealed public advisory contract.
    ///
    /// The stable account binding is owned by the processor and cannot be supplied by the
    /// caller. This prevents a publisher from relabelling an event for another account.
    ///
    /// # Errors
    ///
    /// Returns an error if the event has no committed account binding or violates the shared
    /// recommendation contract.
    pub fn recommendation(&self, event: &RiskEvent) -> Result<RiskRecommendation> {
        let account = self.processor.account_subject(event.account).ok_or(
            crate::Error::MissingAccountSubject {
                account: event.account,
            },
        )?;
        let measurements = RiskMeasurements::new(
            event.collateral_value.raw(),
            event.debt_value.raw(),
            event.maintenance_value.raw(),
            event.health.raw(),
            event.minimum_margin_health.raw(),
        )
        .map_err(recommendation_error)?;
        let portfolio_version = self
            .processor
            .account_portfolio_version(event.account)
            .ok_or(crate::Error::AccountOutOfRange {
                account: event.account,
            })?;
        RiskRecommendation::new(
            account,
            event.previous_state.classification(),
            event.current_state.classification(),
            reason(event.reason),
            measurements,
            portfolio_version,
            event.epoch,
        )
        .map_err(recommendation_error)
    }

    /// Heals a poisoned processor by reconstructing derived state from committed inputs.
    ///
    /// [`Error::ProcessorPoisoned`] is transient: the installed portfolio and committed
    /// inputs are intact, only derived aggregates may be partially updated, so a rebuild
    /// restores a consistent processor without external help. Events rejected while
    /// poisoned must not be acknowledged and dropped — heal first, then re-apply them.
    ///
    /// Returns the risk-state transitions the reconstruction caused; publish them like any
    /// other events. The fold is untouched: it already equals committed input state.
    ///
    /// # Errors
    ///
    /// Returns an error if the installed values cannot be reconstructed safely.
    ///
    /// [`Error::ProcessorPoisoned`]: crate::error::Error::ProcessorPoisoned
    pub fn rebuild(&mut self) -> Result<Vec<RiskEvent>> {
        self.processor.rebuild()
    }

    /// Produces the canonical snapshot of the current fold and ledger.
    ///
    /// Clones every map; prefer [`Self::snapshot_view`] for writing to disk.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.fold.snapshot(&self.ledger)
    }

    /// Borrows the current fold and ledger as an encodable snapshot view for
    /// [`SnapshotStore::write_view`].
    ///
    /// [`SnapshotStore::write_view`]: super::SnapshotStore::write_view
    #[must_use]
    pub fn snapshot_view(&self) -> SnapshotView<'_> {
        self.fold.snapshot_view(&self.ledger)
    }

    /// The underlying processor, for account and domain queries.
    #[must_use]
    pub const fn processor(&self) -> &RiskProcessor {
        &self.processor
    }

    /// The committed-input fold.
    #[must_use]
    pub const fn fold(&self) -> &Fold {
        &self.fold
    }

    /// The last-published ledger.
    #[must_use]
    pub const fn ledger(&self) -> &EgressLedger {
        &self.ledger
    }
}

fn reason(reason: crate::processing::RiskReason) -> RecommendationReason {
    match reason {
        crate::processing::RiskReason::HealthBelowWarning => {
            RecommendationReason::HealthBelowWarning
        }
        crate::processing::RiskReason::HealthBelowLiquidation => {
            RecommendationReason::HealthBelowLiquidation
        }
        crate::processing::RiskReason::MissingInput => RecommendationReason::MissingInput,
        crate::processing::RiskReason::Recovered => RecommendationReason::Recovered,
    }
}

fn recommendation_error(error: impl std::fmt::Display) -> crate::Error {
    crate::Error::RecommendationContract {
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::processing::{
        AccountMode, FACTOR_SCALE, Factor, HyperliquidSnapshot, MarginDomain, MarginMode,
        PRICE_SCALE, Price, PriceSourceKind, PriceUpdate, Value,
    };
    use tackt_primitives::accounts::AccountSubject;
    use uuid::Uuid;

    fn registry() -> AssetRegistry {
        AssetRegistry::bundled().expect("bundled registry")
    }

    fn policy() -> RiskPolicy {
        RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(FACTOR_SCALE))
            .expect("valid policy")
    }

    fn generation(version: u64) -> PortfolioGeneration {
        PortfolioGeneration::new(
            version,
            Vec::new(),
            Vec::new(),
            vec![MarginDomain {
                account: 0,
                domain: 1,
                account_mode: AccountMode::Standard,
                margin_mode: MarginMode::Cross,
                snapshot: Some(HyperliquidSnapshot {
                    account_value: Value::from_raw(100 * PRICE_SCALE),
                    maintenance: Value::from_raw(20 * PRICE_SCALE),
                    source_sequence: 1,
                }),
                source_version: 1,
            }],
            Vec::new(),
        )
        .with_account_subjects(std::collections::BTreeMap::from([(
            0,
            AccountSubject::new(Uuid::from_u128(1)).expect("subject"),
        )]))
    }

    fn price(sequence: u64) -> IngestEvent {
        IngestEvent::Price(PriceUpdate {
            price_index: 3,
            source: PriceSourceKind::Hyperliquid,
            source_sequence: sequence,
            price: Price::from_raw(100 * PRICE_SCALE),
        })
    }

    #[test]
    fn rebuild_heals_a_poisoned_processor() {
        let mut apply_loop =
            ApplyLoop::boot(1, 1, policy(), registry(), &generation(1)).expect("boot");
        apply_loop
            .apply(price(1))
            .expect("healthy processor accepts prices");

        apply_loop.processor.poison_for_tests();
        assert!(matches!(
            apply_loop
                .processor
                .venue_margin(0, crate::processing::VenueKind::HyperliquidPerps),
            Err(Error::ProcessorPoisoned)
        ));
        assert!(apply_loop.processor().is_poisoned());
        // A poisoned rejection reports processor health, not event validity: the event must
        // be retried after healing, never acknowledged and dropped.
        assert!(matches!(
            apply_loop.apply(price(2)),
            Err(Error::ProcessorPoisoned)
        ));

        apply_loop.rebuild().expect("rebuild heals");
        assert!(!apply_loop.processor().is_poisoned());
        apply_loop
            .apply(price(2))
            .expect("healed processor accepts the retried event");
    }

    #[test]
    fn rejected_sweeps_never_commit_to_the_fold() {
        let mut apply_loop =
            ApplyLoop::boot(1, 1, policy(), registry(), &generation(5)).expect("boot");
        let before = apply_loop.fold().clone();

        let stale = apply_loop.apply(IngestEvent::Portfolio(generation(4)));
        assert!(stale.is_err(), "stale sweep must be rejected");
        assert_eq!(*apply_loop.fold(), before);

        // A same-version duplicate reports installed == false and leaves the fold alone.
        apply_loop
            .apply(IngestEvent::Portfolio(generation(5)))
            .expect("duplicate sweep is acknowledged");
        assert_eq!(*apply_loop.fold(), before);
    }
}
