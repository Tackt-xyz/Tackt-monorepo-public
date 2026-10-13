//! Latest accepted input per account and per price index.

use std::collections::BTreeMap;
use tackt_primitives::accounts::AccountSubject;

use crate::processing::{
    AccountIndex, AccountPortfolio, EpochOutcome, EpochStatus, PatchOutcome, PortfolioGeneration,
    PriceIndex, PriceUpdate, ProcessingEpoch, StateVersion,
};

use super::{EgressLedger, Snapshot, SnapshotView};

/// The persisted view of committed engine inputs.
///
/// The fold mutates only on events the engine accepted, so it equals committed engine input
/// state at every acknowledgement boundary. Replaying the fold — the base generation at its
/// installed version, then every account patched past it in stamp order — reproduces, via
/// the engine's own deterministic paths, exactly the state the inputs produced, including
/// each account's committed-version cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fold {
    accounts: BTreeMap<AccountIndex, AccountPortfolio>,
    /// Permanent dense-index to stable-subject bindings. Entries are never removed or rebound.
    account_subjects: BTreeMap<AccountIndex, AccountSubject>,
    /// Accounts removed by an applied empty patch, at the removing patch's version. The
    /// tombstone preserves the engine's per-account cursor across restarts so a patch older
    /// than the removal is still rejected after recovery.
    removed: BTreeMap<AccountIndex, StateVersion>,
    prices: BTreeMap<PriceIndex, PriceUpdate>,
    cursors: BTreeMap<String, u64>,
    version_high_water_mark: StateVersion,
    /// Version of the last installed generation; accounts stamped past it are patches.
    generation_version: StateVersion,
    last_committed_epoch: ProcessingEpoch,
}

impl Fold {
    /// Seeds a fold from the initial portfolio generation of a first-ever boot.
    #[must_use]
    pub(crate) fn from_generation(generation: &PortfolioGeneration) -> Self {
        Self {
            accounts: Self::group_generation(generation),
            account_subjects: generation.account_subjects.clone(),
            removed: BTreeMap::new(),
            prices: BTreeMap::new(),
            cursors: BTreeMap::new(),
            version_high_water_mark: generation.version,
            generation_version: generation.version,
            last_committed_epoch: 0,
        }
    }

    /// Splits a fold and ledger back out of a decoded snapshot.
    #[must_use]
    pub fn from_snapshot(snapshot: Snapshot) -> (Self, EgressLedger) {
        let Snapshot {
            engine_epoch,
            cursors,
            version_high_water_mark,
            generation_version,
            prices,
            accounts,
            account_subjects,
            removed,
            ledger,
        } = snapshot;
        (
            Self {
                accounts,
                account_subjects,
                removed,
                prices,
                cursors,
                version_high_water_mark,
                generation_version,
                last_committed_epoch: engine_epoch,
            },
            EgressLedger::from_entries(ledger),
        )
    }

    /// Groups a generation's record vectors into one complete portfolio per account.
    ///
    /// Every per-account portfolio is stamped at the generation version, matching the
    /// engine's own per-account cursor reset on a generation install.
    #[must_use]
    pub(crate) fn group_generation(
        generation: &PortfolioGeneration,
    ) -> BTreeMap<AccountIndex, AccountPortfolio> {
        Self::group_generation_owned(generation.clone())
    }

    /// Groups an owned generation's record vectors without copying any record.
    #[must_use]
    pub(crate) fn group_generation_owned(
        generation: PortfolioGeneration,
    ) -> BTreeMap<AccountIndex, AccountPortfolio> {
        fn entry(
            accounts: &mut BTreeMap<AccountIndex, AccountPortfolio>,
            account: AccountIndex,
            account_subject: AccountSubject,
            version: StateVersion,
        ) -> &mut AccountPortfolio {
            accounts.entry(account).or_insert_with(|| AccountPortfolio {
                account,
                account_subject,
                version,
                ethereum_balances: Vec::new(),
                hypercore_balances: Vec::new(),
                margin_domains: Vec::new(),
                perp_positions: Vec::new(),
            })
        }

        let version = generation.version;
        let mut accounts: BTreeMap<AccountIndex, AccountPortfolio> = generation
            .account_subjects
            .iter()
            .map(|(&account, &account_subject)| {
                (
                    account,
                    AccountPortfolio {
                        account,
                        account_subject,
                        version,
                        ethereum_balances: Vec::new(),
                        hypercore_balances: Vec::new(),
                        margin_domains: Vec::new(),
                        perp_positions: Vec::new(),
                    },
                )
            })
            .collect();
        for balance in generation.ethereum_balances {
            let subject = generation.account_subjects[&balance.account];
            entry(&mut accounts, balance.account, subject, version)
                .ethereum_balances
                .push(balance);
        }
        for balance in generation.hypercore_balances {
            let subject = generation.account_subjects[&balance.account];
            entry(&mut accounts, balance.account, subject, version)
                .hypercore_balances
                .push(balance);
        }
        for domain in generation.margin_domains {
            let subject = generation.account_subjects[&domain.account];
            entry(&mut accounts, domain.account, subject, version)
                .margin_domains
                .push(domain);
        }
        for position in generation.perp_positions {
            let subject = generation.account_subjects[&position.account];
            entry(&mut accounts, position.account, subject, version)
                .perp_positions
                .push(position);
        }
        accounts
    }

    /// Replaces the whole account map after a successful generation install.
    pub fn commit_generation(
        &mut self,
        version: StateVersion,
        account_subjects: BTreeMap<AccountIndex, AccountSubject>,
        accounts: BTreeMap<AccountIndex, AccountPortfolio>,
    ) {
        self.accounts = accounts;
        self.account_subjects.extend(account_subjects);
        // The install reset every per-account cursor to the generation version, subsuming
        // all prior patches and removals.
        self.removed.clear();
        self.version_high_water_mark = version;
        self.generation_version = version;
    }

    /// Records one account patch the engine reported as applied.
    ///
    /// Duplicate outcomes leave the fold untouched; an applied empty patch removes the
    /// account, mirroring the engine, and leaves a tombstone at the patch version so the
    /// account's cursor survives a restart.
    pub fn record_patch(&mut self, patch: &AccountPortfolio, outcome: &PatchOutcome) {
        if outcome.status != EpochStatus::Applied {
            return;
        }
        let empty = patch.ethereum_balances.is_empty()
            && patch.hypercore_balances.is_empty()
            && patch.margin_domains.is_empty()
            && patch.perp_positions.is_empty();
        if empty {
            self.accounts.insert(patch.account, patch.clone());
            self.removed.insert(patch.account, patch.version);
        } else {
            self.accounts.insert(patch.account, patch.clone());
            self.removed.remove(&patch.account);
        }
        self.account_subjects
            .entry(patch.account)
            .or_insert(patch.account_subject);
        self.version_high_water_mark = self.version_high_water_mark.max(patch.version);
        self.last_committed_epoch = outcome.epoch;
    }

    /// Records one price update the engine reported as applied.
    pub fn record_price(&mut self, update: PriceUpdate, outcome: &EpochOutcome) {
        if outcome.status != EpochStatus::Applied {
            return;
        }
        self.prices.insert(update.price_index, update);
        self.last_committed_epoch = outcome.epoch;
    }

    /// Records the acknowledged position on one input stream.
    pub fn set_cursor(&mut self, stream: &str, sequence: u64) {
        self.cursors.insert(stream.to_owned(), sequence);
    }

    /// Acknowledged position per input stream.
    #[must_use]
    pub const fn cursors(&self) -> &BTreeMap<String, u64> {
        &self.cursors
    }

    /// Last accepted update per price index.
    #[must_use]
    pub const fn prices(&self) -> &BTreeMap<PriceIndex, PriceUpdate> {
        &self.prices
    }

    /// Max `StateVersion` across accepted generations and patches.
    #[must_use]
    pub const fn version_high_water_mark(&self) -> StateVersion {
        self.version_high_water_mark
    }

    /// Engine epoch at the last accepted epoch-consuming event.
    ///
    /// Mirrors [`RiskProcessor::last_committed_epoch`]: prices and patches allocate a new
    /// processing epoch, while generation installs deliberately do not — so this field is
    /// unchanged by [`Self::commit_generation`], keeping fold and engine in lock-step.
    ///
    /// [`RiskProcessor::last_committed_epoch`]: crate::processing::RiskProcessor::last_committed_epoch
    #[must_use]
    pub const fn last_committed_epoch(&self) -> ProcessingEpoch {
        self.last_committed_epoch
    }

    /// Assembles the accounts still at the installed generation version into a generation.
    ///
    /// Accounts stamped past the generation version are patches and are excluded; replaying
    /// them via [`Self::pending_patches`] restores their content *and* their per-account
    /// version cursors, so post-restart accept/reject verdicts match a processor that never
    /// restarted.
    #[must_use]
    pub fn base_generation(&self) -> PortfolioGeneration {
        let mut generation = PortfolioGeneration::empty(self.generation_version)
            .with_account_subjects(self.account_subjects.clone());
        for portfolio in self.accounts.values() {
            if portfolio.version != self.generation_version {
                continue;
            }
            generation
                .ethereum_balances
                .extend(portfolio.ethereum_balances.iter().cloned());
            generation
                .hypercore_balances
                .extend(portfolio.hypercore_balances.iter().cloned());
            generation
                .margin_domains
                .extend(portfolio.margin_domains.iter().cloned());
            generation
                .perp_positions
                .extend(portfolio.perp_positions.iter().cloned());
        }
        generation
    }

    /// Every account patched past the installed generation, in ascending stamp order.
    ///
    /// Removed accounts reappear as empty patches at their tombstone version, so replaying
    /// the returned list over [`Self::base_generation`] reproduces both the content and the
    /// per-account cursors of the pre-restart engine — including the removal's guard against
    /// older late-delivered patches.
    #[must_use]
    pub fn pending_patches(&self) -> Vec<AccountPortfolio> {
        let mut patches: Vec<AccountPortfolio> = self
            .accounts
            .values()
            .filter(|portfolio| portfolio.version != self.generation_version)
            .cloned()
            .collect();
        patches.sort_unstable_by_key(|patch| (patch.version, patch.account));
        patches
    }

    /// Version of the last installed generation.
    #[must_use]
    pub const fn generation_version(&self) -> StateVersion {
        self.generation_version
    }

    /// Produces the canonical snapshot of this fold and the given ledger.
    ///
    /// Clones every map; prefer [`Self::snapshot_view`] for writing to disk, which encodes
    /// straight from borrowed state.
    #[must_use]
    pub fn snapshot(&self, ledger: &EgressLedger) -> Snapshot {
        Snapshot {
            engine_epoch: self.last_committed_epoch,
            cursors: self.cursors.clone(),
            version_high_water_mark: self.version_high_water_mark,
            generation_version: self.generation_version,
            prices: self.prices.clone(),
            accounts: self.accounts.clone(),
            account_subjects: self.account_subjects.clone(),
            removed: self.removed.clone(),
            ledger: ledger.entries().clone(),
        }
    }

    /// Borrows this fold and the given ledger as an encodable snapshot view.
    ///
    /// [`SnapshotStore::write_view`] streams the view straight to the staged snapshot
    /// file, so a snapshot write copies no account state.
    ///
    /// [`SnapshotStore::write_view`]: super::SnapshotStore::write_view
    #[must_use]
    pub fn snapshot_view<'fold>(&'fold self, ledger: &'fold EgressLedger) -> SnapshotView<'fold> {
        SnapshotView {
            engine_epoch: self.last_committed_epoch,
            cursors: &self.cursors,
            version_high_water_mark: self.version_high_water_mark,
            generation_version: self.generation_version,
            prices: &self.prices,
            accounts: &self.accounts,
            account_subjects: &self.account_subjects,
            removed: &self.removed,
            ledger: ledger.entries(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::{
        Amount, EpochStatus, EthereumBalance, Finality, Price, PriceSourceKind,
    };
    use uuid::Uuid;

    fn subject(account: AccountIndex) -> AccountSubject {
        AccountSubject::new(Uuid::from_u128(u128::from(account) + 1)).expect("subject")
    }

    fn balance(account: AccountIndex, position: u32) -> EthereumBalance {
        EthereumBalance {
            account,
            asset: 2,
            amount: Amount::from_raw(1),
            finality: Finality::Finalized,
            position,
            source_version: 1,
        }
    }

    fn portfolio(account: AccountIndex, version: StateVersion) -> AccountPortfolio {
        AccountPortfolio {
            account,
            account_subject: subject(account),
            version,
            ethereum_balances: vec![balance(account, account * 10)],
            hypercore_balances: Vec::new(),
            margin_domains: Vec::new(),
            perp_positions: Vec::new(),
        }
    }

    fn generation() -> PortfolioGeneration {
        PortfolioGeneration::new(
            5,
            vec![balance(0, 1), balance(2, 2), balance(0, 3)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_account_subjects(BTreeMap::from([(0, subject(0)), (2, subject(2))]))
    }

    fn patch_outcome(status: EpochStatus, epoch: ProcessingEpoch) -> PatchOutcome {
        PatchOutcome {
            epoch,
            account: 0,
            status,
            events: Vec::new(),
        }
    }

    fn price_update(sequence: u64) -> PriceUpdate {
        PriceUpdate {
            price_index: 2,
            source: PriceSourceKind::Ethereum,
            source_sequence: sequence,
            price: Price::from_raw(sequence.into()),
        }
    }

    #[test]
    fn generation_grouping_stamps_every_account_at_the_generation_version() {
        let fold = Fold::from_generation(&generation());
        assert_eq!(fold.version_high_water_mark(), 5);
        assert_eq!(fold.generation_version(), 5);
        assert!(fold.pending_patches().is_empty());

        let rebuilt = fold.base_generation();
        assert_eq!(rebuilt.version, 5);
        // Grouped per ascending account: account 0's two balances precede account 2's.
        let accounts: Vec<AccountIndex> = rebuilt
            .ethereum_balances
            .iter()
            .map(|balance| balance.account)
            .collect();
        assert_eq!(accounts, vec![0, 0, 2]);
    }

    #[test]
    fn applied_patches_replace_remove_and_raise_the_high_water_mark() {
        let mut fold = Fold::from_generation(&generation());

        fold.record_patch(&portfolio(1, 7), &patch_outcome(EpochStatus::Applied, 4));
        assert_eq!(fold.version_high_water_mark(), 7);
        assert_eq!(fold.generation_version(), 5);
        assert_eq!(fold.last_committed_epoch(), 4);
        // The patched account leaves the base generation and becomes a pending patch.
        assert_eq!(fold.base_generation().ethereum_balances.len(), 3);
        assert_eq!(fold.pending_patches().len(), 1);
        assert_eq!(fold.pending_patches()[0].version, 7);

        // An applied empty patch removes the account and leaves a tombstone at its version.
        let removal = AccountPortfolio {
            ethereum_balances: Vec::new(),
            ..portfolio(1, 8)
        };
        fold.record_patch(&removal, &patch_outcome(EpochStatus::Applied, 5));
        assert_eq!(fold.version_high_water_mark(), 8);
        let patches = fold.pending_patches();
        assert_eq!(patches.len(), 1);
        assert_eq!((patches[0].account, patches[0].version), (1, 8));
        assert!(patches[0].ethereum_balances.is_empty());

        // A later non-empty patch to the same account supersedes the tombstone; pending
        // patches replay in ascending stamp order.
        fold.record_patch(&portfolio(3, 6), &patch_outcome(EpochStatus::Applied, 6));
        fold.record_patch(&portfolio(1, 9), &patch_outcome(EpochStatus::Applied, 7));
        assert_eq!(fold.version_high_water_mark(), 9);
        let stamps: Vec<(StateVersion, AccountIndex)> = fold
            .pending_patches()
            .iter()
            .map(|patch| (patch.version, patch.account))
            .collect();
        assert_eq!(stamps, vec![(6, 3), (9, 1)]);
    }

    #[test]
    fn committing_a_generation_clears_tombstones_and_pending_patches() {
        let mut fold = Fold::from_generation(&generation());
        let removal = AccountPortfolio {
            ethereum_balances: Vec::new(),
            ..portfolio(2, 6)
        };
        fold.record_patch(&portfolio(1, 7), &patch_outcome(EpochStatus::Applied, 4));
        fold.record_patch(&removal, &patch_outcome(EpochStatus::Applied, 5));

        let sweep =
            PortfolioGeneration::new(9, vec![balance(0, 1)], Vec::new(), Vec::new(), Vec::new())
                .with_account_subjects(BTreeMap::from([(0, subject(0))]));
        fold.commit_generation(
            sweep.version,
            sweep.account_subjects.clone(),
            Fold::group_generation(&sweep),
        );

        assert_eq!(fold.generation_version(), 9);
        assert_eq!(fold.version_high_water_mark(), 9);
        assert!(fold.pending_patches().is_empty());
        assert_eq!(fold.base_generation().ethereum_balances.len(), 1);
    }

    #[test]
    fn duplicate_outcomes_leave_the_fold_untouched() {
        let mut fold = Fold::from_generation(&generation());
        let before = fold.clone();

        fold.record_patch(&portfolio(1, 9), &patch_outcome(EpochStatus::Duplicate, 0));
        fold.record_price(
            price_update(1),
            &EpochOutcome::duplicate(1, PriceSourceKind::Ethereum, 1),
        );
        assert_eq!(fold, before);
    }

    #[test]
    fn applied_prices_keep_the_latest_update_per_index() {
        let mut fold = Fold::from_generation(&generation());
        let mut outcome = EpochOutcome::duplicate(1, PriceSourceKind::Ethereum, 1);
        outcome.status = EpochStatus::Applied;

        fold.record_price(price_update(1), &outcome);
        outcome.epoch = 2;
        fold.record_price(price_update(2), &outcome);

        assert_eq!(fold.prices().len(), 1);
        assert_eq!(
            fold.prices().get(&2).expect("stored price").source_sequence,
            2
        );
        assert_eq!(fold.last_committed_epoch(), 2);
    }
}
