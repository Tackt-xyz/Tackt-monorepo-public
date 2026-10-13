use std::collections::{BTreeMap, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

use metrics::{counter, histogram};
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tackt_primitives::{
    accounts::AccountSubject,
    risk::{EvaluationReason, EvaluationVersions, RiskEvaluation, RiskMeasurements},
};
use tracing::instrument;

use crate::error::{Error, Result};

use super::{
    AccountIndex, AccountMode, AccountPortfolio, AccountSnapshot, AccountStore, AssetRegistry,
    EpochOutcome, EpochStatus, EvaluationInput, Factor, GenerationInstall, MAX_EVALUATION_RECORDS,
    MAX_PRICE_RAW, MAX_VALUE_RAW, MarginDomainIndex, MarginDomainSnapshot, MarginMode,
    PatchOutcome, PortfolioGeneration, PortfolioIndex, Price, PriceIndex, PriceSourceKind,
    PriceUpdate, ProcessingEpoch, RiskEvent, RiskPolicy, RiskState, SignedAmount, SourceSequence,
    StateVersion, Value, VenueKind, VenueMarginSnapshot, account_health_ratio,
    arithmetic::{
        checked_uncertain_add, collateral_contribution_weighted, margin_health_ratio,
        native_maintenance_margin, notional_value, perp_pnl, utilization_ratio,
    },
    digest::{EvaluationDigestInput, evaluation_input_digest},
    portfolio::{CompiledAccount, CompiledPerp, IndexedDomain, PriceShardLanes, compile_account},
    state::{AccountShard, MarginSummary, shard_len},
};

const METRIC_EPOCHS_TOTAL: &str = "risk_processing_epochs_total";
const METRIC_EPOCH_DURATION_SECONDS: &str = "risk_processing_epoch_duration_seconds";
const METRIC_BALANCES_TOTAL: &str = "risk_processing_balances_total";
const METRIC_PERPS_TOTAL: &str = "risk_processing_perps_total";
const METRIC_STATE_CHANGES_TOTAL: &str = "risk_processing_state_changes_total";

/// Deterministic single-writer risk processor over sharded in-memory account state.
pub struct RiskProcessor {
    accounts: AccountStore,
    domains: DomainStore,
    portfolio: PortfolioIndex,
    registry: AssetRegistry,
    policy: RiskPolicy,
    prices: BTreeMap<u32, Price>,
    last_committed_epoch: ProcessingEpoch,
    ethereum_cursor: Option<SourceCursor>,
    hyperliquid_cursor: Option<SourceCursor>,
    workers: ThreadPool,
    scratch: Vec<ShardScratch>,
    poisoned: bool,
    account_versions: Vec<StateVersion>,
    account_subjects: Vec<Option<AccountSubject>>,
    account_by_subject: HashMap<AccountSubject, AccountIndex>,
    patch_epochs: HashMap<AccountIndex, ProcessingEpoch>,
    max_account_version: StateVersion,
}

impl RiskProcessor {
    /// Creates a processor and reconstructs the initial portfolio.
    ///
    /// No risk events are emitted for construction; initial risk states are available through
    /// account snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, portfolio input, or arithmetic.
    pub fn new(
        account_count: usize,
        shard_count: usize,
        policy: RiskPolicy,
        registry: AssetRegistry,
        generation: &PortfolioGeneration,
    ) -> Result<Self> {
        let portfolio = PortfolioIndex::build(generation, account_count, shard_count, &registry)?;
        let workers = ThreadPoolBuilder::new()
            .num_threads(shard_count)
            .thread_name(|worker| format!("risk-worker-{worker}"))
            .build()
            .map_err(|error| Error::WorkerPoolCreation(error.to_string()))?;
        let scratch = build_scratch(&portfolio);
        let prices = BTreeMap::new();
        let (accounts, domains, _) = rebuild_stores(
            &workers,
            account_count,
            shard_count,
            &portfolio,
            &registry,
            &prices,
            &policy,
            0,
            None,
        )?;
        let generation_version = portfolio.generation();
        let mut account_subjects = vec![None; account_count];
        for (&account, &subject) in &generation.account_subjects {
            account_subjects[account as usize] = Some(subject);
        }
        let account_by_subject = generation
            .account_subjects
            .iter()
            .map(|(&account, &subject)| (subject, account))
            .collect();
        Ok(Self {
            accounts,
            domains,
            portfolio,
            registry,
            policy,
            prices,
            last_committed_epoch: 0,
            ethereum_cursor: None,
            hyperliquid_cursor: None,
            workers,
            scratch,
            poisoned: false,
            account_versions: vec![generation_version; account_count],
            account_subjects,
            account_by_subject,
            patch_epochs: HashMap::new(),
            max_account_version: generation_version,
        })
    }

    #[must_use]
    pub const fn last_committed_epoch(&self) -> ProcessingEpoch {
        self.last_committed_epoch
    }

    /// Restores the durable epoch after condensed snapshot inputs have been
    /// replayed. This keeps occurrence identifiers stable for tail redelivery.
    pub(crate) fn restore_committed_epoch(&mut self, recovered: ProcessingEpoch) -> Result<()> {
        if recovered < self.last_committed_epoch {
            return Err(Error::InvalidRecoveredEpoch {
                recovered,
                replayed: self.last_committed_epoch,
            });
        }
        self.last_committed_epoch = recovered;
        Ok(())
    }

    /// Whether a failed apply or patch commit left state partially updated.
    ///
    /// A poisoned processor rejects further updates until a rebuild or generation install
    /// succeeds; snapshots read from a poisoned processor may mix old and new values.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Marks the processor poisoned so heal paths can be exercised without forcing a
    /// mid-commit failure.
    #[cfg(test)]
    pub(crate) const fn poison_for_tests(&mut self) {
        self.poisoned = true;
    }

    #[must_use]
    pub const fn portfolio_generation(&self) -> StateVersion {
        self.portfolio.generation()
    }

    #[must_use]
    pub const fn asset_registry(&self) -> &AssetRegistry {
        &self.registry
    }

    #[must_use]
    pub const fn policy(&self) -> &RiskPolicy {
        &self.policy
    }

    #[must_use]
    pub const fn account_count(&self) -> usize {
        self.accounts.account_count()
    }

    /// Stable subject permanently bound to one dense account index.
    #[must_use]
    pub fn account_subject(&self, account: AccountIndex) -> Option<AccountSubject> {
        self.account_subjects
            .get(account as usize)
            .copied()
            .flatten()
    }

    /// Resolves a stable subject without scanning allocated account slots.
    #[must_use]
    pub fn account_index(&self, subject: AccountSubject) -> Option<AccountIndex> {
        self.account_by_subject.get(&subject).copied()
    }

    /// Persisted portfolio-input version for one dense account index.
    #[must_use]
    pub fn account_portfolio_version(&self, account: AccountIndex) -> Option<StateVersion> {
        self.account_versions.get(account as usize).copied()
    }

    #[must_use]
    pub fn last_source_sequence(&self, source: PriceSourceKind) -> Option<SourceSequence> {
        self.cursor(source).map(|cursor| cursor.sequence)
    }

    /// Evaluates a complete projected account without mutating committed state.
    ///
    /// # Errors
    ///
    /// Rejects poisoned state, unsupported or stale versions, unknown subjects,
    /// invalid portfolio input, and unsafe arithmetic.
    pub fn evaluate(&self, request: EvaluationInput) -> Result<RiskEvaluation> {
        let account = self.evaluation_account(&request)?;
        let committed = self.account_versions[account as usize];
        let action_id = request.action_id;
        let subject = request.account;
        let input_version = request.input_version;
        let portfolio = request.bind(account);
        let compiled = compile_account(&self.registry, self.accounts.account_count(), &portfolio)?;
        let input_digest = evaluation_input_digest(EvaluationDigestInput {
            action_id,
            subject,
            account_version: committed,
            input_version,
            portfolio_digest: compiled.content_digest,
        });
        self.portfolio
            .validate_position_owners(account, &compiled.positions)?;
        let staged = stage_account(&compiled, &self.prices, &self.policy, &self.registry)?;
        let mut uncertain_inputs = staged.uncertain;
        let mut minimum_margin_health = i128::MAX;
        for domain in &staged.domains {
            uncertain_inputs =
                checked_uncertain_add(uncertain_inputs, i64::from(domain.uncertain))?;
            minimum_margin_health = minimum_margin_health.min(domain.health);
        }
        let collateral = Value::from_raw(staged.collateral);
        let debt = Value::from_raw(staged.debt);
        let maintenance = Value::from_raw(staged.maintenance);
        let classification =
            self.policy
                .evaluate_account(collateral, debt, maintenance, uncertain_inputs > 0);
        let measurements = RiskMeasurements::new(
            collateral.raw(),
            debt.raw(),
            maintenance.raw(),
            account_health_ratio(collateral, debt, maintenance)?.raw(),
            minimum_margin_health,
        )
        .map_err(evaluation_contract_error)?;
        RiskEvaluation::new(
            action_id,
            subject,
            input_digest,
            classification.classification(),
            evaluation_reason(classification),
            measurements,
            uncertain_inputs,
            EvaluationVersions {
                account_version: committed,
                input_version,
                processing_epoch: self.last_committed_epoch,
                ethereum_price_sequence: self.last_source_sequence(PriceSourceKind::Ethereum),
                hyperliquid_price_sequence: self.last_source_sequence(PriceSourceKind::Hyperliquid),
            },
        )
        .map_err(evaluation_contract_error)
    }

    fn evaluation_account(&self, request: &EvaluationInput) -> Result<AccountIndex> {
        if self.poisoned {
            return Err(Error::ProcessorPoisoned);
        }
        if request.action_id.is_nil() {
            return Err(Error::InvalidEvaluationActionId);
        }
        if request.input_version == 0 {
            return Err(Error::InvalidEvaluationInputVersion);
        }
        let record_count = request.record_count();
        if record_count > MAX_EVALUATION_RECORDS {
            return Err(Error::EvaluationRecordLimit {
                received: record_count,
                maximum: MAX_EVALUATION_RECORDS,
            });
        }
        let account = self
            .account_by_subject
            .get(&request.account)
            .copied()
            .ok_or(Error::UnknownAccountSubject {
                subject: request.account,
            })?;
        let committed = self.account_versions[account as usize];
        if request.account_version != committed {
            return Err(Error::EvaluationAccountVersionMismatch {
                subject: request.account,
                received: request.account_version,
                committed,
            });
        }
        Ok(account)
    }

    /// Returns one account snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the account is outside the configured range.
    pub fn account(&self, account: AccountIndex) -> Result<AccountSnapshot> {
        self.accounts.snapshot(account)
    }

    /// Returns one Hyperliquid margin-domain snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the account is outside the configured range.
    pub fn margin_domain(
        &self,
        account: AccountIndex,
        domain: MarginDomainIndex,
    ) -> Result<Option<MarginDomainSnapshot>> {
        let (shard, _) = self.accounts.location(account)?;
        Ok(self.domains.shards[shard].snapshot(account, domain))
    }

    /// Enumerates this account's Hyperliquid pools in domain order without allocating.
    /// Work is proportional to this account's pools, independent of portfolio size.
    ///
    /// # Errors
    /// Rejects poisoned state and accounts outside the configured range.
    pub fn margin_domains(
        &self,
        account: AccountIndex,
    ) -> Result<impl Iterator<Item = MarginDomainSnapshot> + '_> {
        if self.poisoned {
            return Err(Error::ProcessorPoisoned);
        }
        let (shard, local) = self.accounts.location(account)?;
        let domains = &self.domains.shards[shard];
        Ok(domains.domains_by_account[local]
            .slots()
            .map(|slot| domains.snapshot_at(slot as usize)))
    }

    /// Calculates standard-account venue health and gross leverage without allocation or mutation.
    /// Only Hyperliquid has perp margin inputs in the current registry/input contract. Other
    /// venues, and accounts without pools at this venue, return `None`, never invented health.
    /// Inspect `margin_domains` to identify the individual pools behind the worst classification.
    ///
    /// # Errors
    /// Rejects poisoned state, out-of-range accounts, unified accounts, and unsafe arithmetic.
    pub fn venue_margin(
        &self,
        account: AccountIndex,
        venue: VenueKind,
    ) -> Result<Option<VenueMarginSnapshot>> {
        let mut pools = self.margin_domains(account)?;
        if venue != VenueKind::HyperliquidPerps {
            return Ok(None);
        }
        let Some(first) = pools.next() else {
            return Ok(None);
        };
        if first.account_mode != AccountMode::Standard {
            return Err(Error::UnsupportedVenueAccountMode { account });
        }
        let mut result = VenueMarginSnapshot {
            account,
            venue,
            account_value: first.account_value,
            maintenance: first.maintenance,
            margin_ratio: utilization_ratio(first.maintenance, first.account_value)?,
            gross_notional: Value::zero(),
            leverage: Factor::zero(),
            health: first.health,
            minimum_margin_health: first.health,
            risk_state: self.policy.evaluate_venue_margin(
                first.account_value,
                first.maintenance,
                first.uncertain_inputs > 0,
            ),
            uncertain_inputs: first.uncertain_inputs,
        };
        for pool in pools {
            if pool.account_mode != AccountMode::Standard {
                return Err(Error::UnsupportedVenueAccountMode { account });
            }
            result.margin_ratio = result
                .margin_ratio
                .max(utilization_ratio(pool.maintenance, pool.account_value)?);
            result.account_value = Value::from_raw(checked_add(
                result.account_value.raw(),
                pool.account_value.raw(),
                "summing venue equity",
            )?);
            result.maintenance = Value::from_raw(checked_add(
                result.maintenance.raw(),
                pool.maintenance.raw(),
                "summing venue maintenance",
            )?);
            result.minimum_margin_health = result.minimum_margin_health.min(pool.health);
            result.risk_state = result.risk_state.max(self.policy.evaluate_venue_margin(
                pool.account_value,
                pool.maintenance,
                pool.uncertain_inputs > 0,
            ));
            result.uncertain_inputs =
                checked_uncertain_add(result.uncertain_inputs, i64::from(pool.uncertain_inputs))?;
        }
        result.health = result.minimum_margin_health;
        result.gross_notional = self.venue_gross_notional(account)?;
        result.leverage = utilization_ratio(result.gross_notional, result.account_value)?;
        Ok(Some(result))
    }

    /// Sum only this account's perp positions, using sorted lane account indexes.
    fn venue_gross_notional(&self, account: AccountIndex) -> Result<Value> {
        let (shard, _) = self.accounts.location(account)?;
        let mut total = 0_i128;
        for (price_index, shards) in self.portfolio.prices() {
            let lane = &shards[shard].perps;
            let start = lane.accounts.partition_point(|&other| other < account);
            let end = lane.accounts.partition_point(|&other| other <= account);
            // Missing prices already mark the associated pools DataIncomplete.
            if let Some(price) = self.prices.get(&price_index) {
                for size in &lane.signed_sizes[start..end] {
                    total = bounded_aggregate_add(
                        total,
                        notional_value(*size, *price)?.raw(),
                        "summing venue gross notional",
                    )?;
                }
            }
        }
        Ok(Value::from_raw(total))
    }

    /// Atomically installs and rebuilds a newer immutable portfolio generation.
    ///
    /// Reconstruction can change account risk states without a price update; the returned
    /// events report every transition so downstream consumers stay consistent.
    ///
    /// # Errors
    ///
    /// Returns an error for stale, conflicting, or invalid portfolio input or arithmetic.
    pub fn install_portfolio_generation(
        &mut self,
        generation: &PortfolioGeneration,
    ) -> Result<GenerationInstall> {
        for (&account, &subject) in &generation.account_subjects {
            self.validate_account_subject(account, subject)?;
        }
        let candidate = PortfolioIndex::build(
            generation,
            self.accounts.account_count(),
            self.accounts.shard_count(),
            &self.registry,
        )?;
        let received = candidate.generation();
        let current = self.portfolio.generation();
        if received < current {
            return Err(Error::StalePortfolioGeneration { received, current });
        }
        // A reconciliation sweep must be stamped at least as new as every patch it subsumes;
        // otherwise an older whole-portfolio snapshot would silently roll accounts back.
        if received < self.max_account_version {
            return Err(Error::StaleReconciliationSweep {
                received,
                newest_patch: self.max_account_version,
            });
        }
        if received == current {
            if candidate == self.portfolio {
                return Ok(GenerationInstall {
                    installed: false,
                    events: Vec::new(),
                });
            }
            return Err(Error::PortfolioGenerationConflict {
                generation: received,
            });
        }

        let (accounts, domains, events) = rebuild_stores(
            &self.workers,
            self.accounts.account_count(),
            self.accounts.shard_count(),
            &candidate,
            &self.registry,
            &self.prices,
            &self.policy,
            self.last_committed_epoch,
            Some(&self.accounts),
        )?;
        self.scratch = build_scratch(&candidate);
        self.portfolio = candidate;
        self.accounts = accounts;
        self.domains = domains;
        self.poisoned = false;
        self.account_versions.fill(received);
        for (&account, &subject) in &generation.account_subjects {
            self.account_subjects[account as usize] = Some(subject);
            self.account_by_subject.insert(subject, account);
        }
        self.patch_epochs.clear();
        self.max_account_version = received;
        Ok(GenerationInstall {
            installed: true,
            events,
        })
    }

    /// Reconstructs the installed generation from current source prices.
    ///
    /// Returns the risk-state transitions caused by the reconstruction.
    ///
    /// # Errors
    ///
    /// Returns an error if the installed values cannot be reconstructed safely.
    pub fn rebuild(&mut self) -> Result<Vec<RiskEvent>> {
        // Patched lanes reference free-list slab slots; the rebuilt stores use the dense
        // numbering derived from the retained domain lists, so the lanes must be renumbered
        // first or perp deltas would route to other accounts' domains.
        let shards = &self.domains.shards;
        self.portfolio
            .renumber_domain_offsets(|shard, slot| shards[shard].external_indices[slot]);
        let (accounts, domains, events) = rebuild_stores(
            &self.workers,
            self.accounts.account_count(),
            self.accounts.shard_count(),
            &self.portfolio,
            &self.registry,
            &self.prices,
            &self.policy,
            self.last_committed_epoch,
            Some(&self.accounts),
        )?;
        self.accounts = accounts;
        self.domains = domains;
        self.poisoned = false;
        Ok(events)
    }

    /// Validates and applies one source price update.
    ///
    /// # Errors
    ///
    /// Returns an error for stale, conflicting, cross-source, invalid, or unsafe updates, and
    /// [`Error::ProcessorPoisoned`] after a failed apply until a rebuild or install succeeds.
    #[instrument(
        skip(self),
        fields(
            price_index = update.price_index,
            source = %update.source,
            source_sequence = update.source_sequence,
        )
    )]
    pub fn apply_price_update(&mut self, update: PriceUpdate) -> Result<EpochOutcome> {
        if self.poisoned {
            return Err(Error::ProcessorPoisoned);
        }
        let started = Instant::now();
        if let Some(epoch) = self.validate_update(update)? {
            return Ok(EpochOutcome::duplicate(
                epoch,
                update.source,
                update.source_sequence,
            ));
        }
        let epoch = self
            .last_committed_epoch
            .checked_add(1)
            .ok_or(Error::ArithmeticOverflow {
                operation: "assigning processing epoch",
            })?;
        let old_price = self.prices.get(&update.price_index).copied();

        let Some(by_shard) = self.portfolio.for_price(update.price_index) else {
            self.commit_price(update, epoch);
            record_metrics(0, 0, 0, started);
            return Ok(EpochOutcome {
                epoch,
                source: update.source,
                source_sequence: update.source_sequence,
                status: EpochStatus::Applied,
                affected_accounts: 0,
                affected_balances: 0,
                affected_perp_positions: 0,
                affected_margin_domains: 0,
                acknowledged_shards: 0,
                events: Vec::new(),
            });
        };

        calculate_deltas(
            &self.workers,
            &self.domains.shards,
            by_shard,
            &self.registry,
            &mut self.scratch,
            old_price,
            update.price,
            epoch,
        )?;
        preflight_deltas(
            &self.workers,
            self.accounts.shards(),
            &self.domains.shards,
            &self.scratch,
            epoch,
        )?;
        let applied = apply_deltas(
            &self.workers,
            self.accounts.shards_mut(),
            &mut self.domains.shards,
            &mut self.scratch,
            &self.policy,
            epoch,
        );
        let ApplyResult {
            affected_accounts,
            affected_domains,
            events,
        } = match applied {
            Ok(applied) => applied,
            Err(error) => {
                // The apply phase mutates shard state in place; a failure here may leave
                // aggregates partially updated, so refuse further updates until a rebuild.
                self.poisoned = true;
                return Err(error);
            }
        };

        let affected_balances = by_shard
            .iter()
            .map(|lanes| lanes.balances.accounts.len())
            .sum();
        let affected_perps = by_shard
            .iter()
            .map(|lanes| lanes.perps.accounts.len())
            .sum();
        let acknowledged_shards = by_shard
            .iter()
            .filter(|lanes| !lanes.balances.accounts.is_empty() || !lanes.perps.accounts.is_empty())
            .count();

        self.commit_price(update, epoch);
        record_metrics(affected_balances, affected_perps, events.len(), started);
        Ok(EpochOutcome {
            epoch,
            source: update.source,
            source_sequence: update.source_sequence,
            status: EpochStatus::Applied,
            affected_accounts,
            affected_balances,
            affected_perp_positions: affected_perps,
            affected_margin_domains: affected_domains,
            acknowledged_shards,
            events,
        })
    }

    /// Validates and applies one account patch, wholly replacing the account's records.
    ///
    /// The patch consumes the next processing epoch, so patches and price updates share one
    /// total replay order. An empty patch removes the account from the portfolio.
    ///
    /// # Errors
    ///
    /// Returns an error for stale, conflicting, or invalid patches, positions owned by
    /// another account, or unsafe arithmetic. Validation completes before any mutation; a
    /// failure inside the commit itself poisons the processor.
    #[instrument(skip(self, patch), fields(account = patch.account, version = patch.version))]
    pub fn apply_account_patch(&mut self, patch: &AccountPortfolio) -> Result<PatchOutcome> {
        if self.poisoned {
            return Err(Error::ProcessorPoisoned);
        }
        let started = Instant::now();
        let account = patch.account;
        let (shard, local) = self.accounts.location(account)?;
        self.validate_account_subject(account, patch.account_subject)?;
        let committed = self.account_versions[account as usize];
        if patch.version < committed {
            return Err(Error::StaleAccountPatch {
                account,
                received: patch.version,
                committed,
            });
        }
        let compiled = compile_account(&self.registry, self.accounts.account_count(), patch)?;
        if patch.version == committed {
            if compiled.digest == self.portfolio.account_digest(account) {
                return Ok(PatchOutcome {
                    epoch: self.patch_epochs.get(&account).copied().unwrap_or(0),
                    account,
                    status: EpochStatus::Duplicate,
                    events: Vec::new(),
                });
            }
            return Err(Error::AccountPatchConflict {
                account,
                version: patch.version,
            });
        }
        self.portfolio
            .validate_position_owners(account, &compiled.positions)?;
        let staged = stage_account(&compiled, &self.prices, &self.policy, &self.registry)?;
        let epoch = self
            .last_committed_epoch
            .checked_add(1)
            .ok_or(Error::ArithmeticOverflow {
                operation: "assigning processing epoch",
            })?;

        // Commit phase: plain writes of precomputed values. The poison flag stays set until
        // the final bookkeeping succeeds, so a panic or error mid-commit refuses further
        // processing until a rebuild or install restores consistency.
        self.poisoned = true;
        self.portfolio.remove_account_lanes(account);
        self.portfolio
            .replace_account_domains(account, &compiled.domains);
        self.portfolio
            .commit_positions(account, compiled.positions.clone(), compiled.digest);
        let domain_shard = &mut self.domains.shards[shard];
        domain_shard.free_account_domains(local);
        let mut slots = Vec::with_capacity(staged.domains.len());
        for domain in &staged.domains {
            slots.push((domain.external_index, domain_shard.allocate_domain(domain)));
        }
        self.portfolio.insert_account_lanes(&compiled, &slots);
        self.accounts.set_aggregates(
            account,
            staged.collateral,
            staged.debt,
            staged.maintenance,
            staged.uncertain,
        )?;
        let margin = self.domains.shards[shard].summary(account);
        let event = self
            .accounts
            .evaluate(account, &self.policy, epoch, margin)?;
        self.account_versions[account as usize] = patch.version;
        self.account_subjects[account as usize] = Some(patch.account_subject);
        self.account_by_subject
            .insert(patch.account_subject, account);
        self.max_account_version = self.max_account_version.max(patch.version);
        self.patch_epochs.insert(account, epoch);
        self.last_committed_epoch = epoch;
        self.poisoned = false;
        let events: Vec<RiskEvent> = event.into_iter().collect();
        record_metrics(
            compiled.balance_records.len(),
            compiled.perp_records.len(),
            events.len(),
            started,
        );
        Ok(PatchOutcome {
            epoch,
            account,
            status: EpochStatus::Applied,
            events,
        })
    }

    fn validate_update(&self, update: PriceUpdate) -> Result<Option<ProcessingEpoch>> {
        let factor = self
            .registry
            .price(update.price_index)
            .ok_or(Error::UnknownPrice {
                price_index: update.price_index,
            })?;
        if factor.source != update.source {
            return Err(Error::PriceSourceMismatch {
                price_index: update.price_index,
                expected: factor.source,
                received: update.source,
            });
        }
        if update.price.raw() <= 0 || update.price.raw() > MAX_PRICE_RAW {
            return Err(Error::InvalidPrice {
                price_index: update.price_index,
            });
        }
        if update.source_sequence == 0 {
            return Err(Error::StaleSourceSequence {
                price_source: update.source,
                received: 0,
                committed: self
                    .cursor(update.source)
                    .map_or(0, |cursor| cursor.sequence),
            });
        }
        if let Some(cursor) = self.cursor(update.source) {
            if update.source_sequence < cursor.sequence {
                return Err(Error::StaleSourceSequence {
                    price_source: update.source,
                    received: update.source_sequence,
                    committed: cursor.sequence,
                });
            }
            if update.source_sequence == cursor.sequence {
                if cursor.update == update {
                    return Ok(Some(cursor.epoch));
                }
                return Err(Error::SourceSequenceConflict {
                    price_source: update.source,
                    sequence: update.source_sequence,
                });
            }
        }
        Ok(None)
    }

    fn validate_account_subject(
        &self,
        account: AccountIndex,
        received: AccountSubject,
    ) -> Result<()> {
        let slot = self
            .account_subjects
            .get(account as usize)
            .ok_or(Error::AccountOutOfRange { account })?;
        if let Some(committed) = slot {
            if *committed != received {
                return Err(Error::AccountSubjectRebind {
                    account,
                    committed: *committed,
                    received,
                });
            }
            return Ok(());
        }
        if self.account_by_subject.contains_key(&received) {
            return Err(Error::DuplicateAccountSubject { subject: received });
        }
        Ok(())
    }

    fn cursor(&self, source: PriceSourceKind) -> Option<&SourceCursor> {
        match source {
            PriceSourceKind::Ethereum => self.ethereum_cursor.as_ref(),
            PriceSourceKind::Hyperliquid => self.hyperliquid_cursor.as_ref(),
        }
    }

    fn commit_price(&mut self, update: PriceUpdate, epoch: ProcessingEpoch) {
        self.prices.insert(update.price_index, update.price);
        self.last_committed_epoch = epoch;
        let cursor = SourceCursor {
            sequence: update.source_sequence,
            update,
            epoch,
        };
        match update.source {
            PriceSourceKind::Ethereum => self.ethereum_cursor = Some(cursor),
            PriceSourceKind::Hyperliquid => self.hyperliquid_cursor = Some(cursor),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn rebuild_stores(
    workers: &ThreadPool,
    account_count: usize,
    shard_count: usize,
    portfolio: &PortfolioIndex,
    registry: &AssetRegistry,
    prices: &BTreeMap<u32, Price>,
    policy: &RiskPolicy,
    epoch: ProcessingEpoch,
    previous: Option<&AccountStore>,
) -> Result<(AccountStore, DomainStore, Vec<RiskEvent>)> {
    let mut accounts = AccountStore::new(account_count, shard_count)?;
    if let Some(previous) = previous {
        accounts.inherit_metadata(previous);
    }
    accounts.clear_aggregates();
    let mut domains = DomainStore::new(account_count, portfolio.domains());
    let price_lanes: Vec<(PriceIndex, &[PriceShardLanes])> = portfolio.prices().collect();

    // Accounts and domains are modulo-sharded, so every write below is shard-local and the
    // shards can rebuild concurrently; per-shard iteration order is unchanged, and events
    // are re-sorted into ascending account order afterwards, so the result is bit-identical
    // to the serial rebuild.
    let result = catch_unwind(AssertUnwindSafe(|| {
        workers.install(|| {
            accounts
                .shards_mut()
                .par_iter_mut()
                .zip(domains.shards.par_iter_mut())
                .enumerate()
                .map(|(shard_index, (account_shard, domain_shard))| {
                    for (price_index, shards) in &price_lanes {
                        let price = prices.get(price_index).copied();
                        let lanes = &shards[shard_index];
                        for (&account, &weighted) in lanes
                            .balances
                            .accounts
                            .iter()
                            .zip(&lanes.balances.weighted_amounts)
                        {
                            let offset = account as usize / shard_count;
                            match price {
                                Some(price) => account_shard.accumulate(
                                    offset,
                                    collateral_contribution_weighted(weighted, price)?.raw(),
                                    0,
                                    0,
                                    0,
                                )?,
                                None => account_shard.accumulate(offset, 0, 0, 0, 1)?,
                            }
                        }
                        for index in 0..lanes.perps.accounts.len() {
                            let domain_offset = lanes.perps.domain_offsets[index];
                            let (deltas, uncertain) = perp_deltas_from_anchor(
                                lanes.perps.signed_sizes[index],
                                lanes.perps.anchor_prices[index],
                                registry
                                    .perp_market(lanes.perps.markets[index])
                                    .expect("compiled market exists"),
                                price,
                            )?;
                            if uncertain > 0 {
                                domain_shard
                                    .add_uncertainty(domain_offset, i64::from(uncertain))?;
                            }
                            if let Some((pnl, maintenance)) = deltas {
                                domain_shard.accumulate(domain_offset, pnl, maintenance, 0)?;
                            }
                        }
                    }

                    domain_shard.evaluate_all(policy)?;
                    for offset in 0..domain_shard.len() {
                        if !domain_shard.occupied[offset] {
                            continue;
                        }
                        let (collateral, debt) =
                            split_account_value(domain_shard.account_values[offset])?;
                        let local = domain_shard.accounts[offset] as usize / shard_count;
                        account_shard.accumulate(
                            local,
                            collateral,
                            debt,
                            domain_shard.maintenance_values[offset],
                            0,
                        )?;
                    }

                    let mut events = Vec::new();
                    for local in 0..shard_len(account_count, shard_index, shard_count) {
                        let account = u32::try_from(local * shard_count + shard_index)
                            .expect("account count validated");
                        let margin = domain_shard.summary(account);
                        if let Some(event) =
                            account_shard.evaluate(account, local, policy, epoch, margin)?
                        {
                            events.push(event);
                        }
                    }
                    Ok(events)
                })
                .collect::<Vec<Result<Vec<RiskEvent>>>>()
        })
    }));
    let per_shard = result.map_err(|_| Error::WorkerPanicked { epoch })?;
    // Shard results are collected in shard order, so error selection is deterministic.
    let mut events = Vec::new();
    for shard_events in per_shard {
        events.extend(shard_events?);
    }
    events.sort_unstable_by_key(|event| event.account);
    Ok((accounts, domains, events))
}

#[allow(clippy::too_many_arguments)]
fn calculate_deltas(
    workers: &ThreadPool,
    domain_shards: &[DomainShard],
    price_shards: &[PriceShardLanes],
    registry: &AssetRegistry,
    scratch: &mut [ShardScratch],
    old_price: Option<Price>,
    new_price: Price,
    epoch: ProcessingEpoch,
) -> Result<()> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        workers.install(|| {
            domain_shards
                .par_iter()
                .zip(price_shards.par_iter())
                .zip(scratch.par_iter_mut())
                .try_for_each(|((domains, lanes), scratch)| {
                    calculate_shard_deltas(domains, lanes, registry, scratch, old_price, new_price)
                })
        })
    }));
    result.map_err(|_| Error::WorkerPanicked { epoch })?
}

fn calculate_shard_deltas(
    domains: &DomainShard,
    lanes: &PriceShardLanes,
    registry: &AssetRegistry,
    scratch: &mut ShardScratch,
    old_price: Option<Price>,
    new_price: Price,
) -> Result<()> {
    scratch.account_deltas.clear();
    scratch.domain_deltas.clear();
    scratch.events.clear();

    let mut cursor = 0;
    while cursor < lanes.balances.accounts.len() {
        let account = lanes.balances.accounts[cursor];
        let mut collateral = 0_i128;
        let mut uncertain = 0_i64;
        while cursor < lanes.balances.accounts.len() && lanes.balances.accounts[cursor] == account {
            let weighted = lanes.balances.weighted_amounts[cursor];
            let new_value = collateral_contribution_weighted(weighted, new_price)?.raw();
            let old_value = if let Some(price) = old_price {
                collateral_contribution_weighted(weighted, price)?.raw()
            } else {
                uncertain -= 1;
                0
            };
            collateral = checked_add(
                collateral,
                checked_sub(new_value, old_value, "calculating balance delta")?,
                "combining balance deltas",
            )?;
            cursor += 1;
        }
        scratch.account_deltas.push(AccountDelta {
            account,
            collateral,
            debt: 0,
            maintenance: 0,
            uncertain,
        });
    }

    cursor = 0;
    while cursor < lanes.perps.accounts.len() {
        let account = lanes.perps.accounts[cursor];
        let domain_offset = lanes.perps.domain_offsets[cursor];
        let mut value = 0_i128;
        let mut maintenance = 0_i128;
        let mut uncertain = 0_i64;
        while cursor < lanes.perps.accounts.len()
            && lanes.perps.domain_offsets[cursor] == domain_offset
        {
            let anchor = lanes.perps.anchor_prices[cursor];
            if old_price.is_none() {
                uncertain -= 1;
            }
            // Difference of anchor-relative values makes rounding independent of the price path.
            // Anchorless positions remain uncertain and contribute no delta, matching rebuilds.
            if let Some(anchor) = anchor {
                let market = registry
                    .perp_market(lanes.perps.markets[cursor])
                    .expect("compiled market exists");
                let size = lanes.perps.signed_sizes[cursor];
                let (pnl_delta, maintenance_delta) = perp_deltas_between_marks(
                    size,
                    anchor,
                    market,
                    old_price.unwrap_or(anchor),
                    new_price,
                )?;
                value = checked_add(value, pnl_delta, "combining perp PnL deltas")?;
                maintenance = checked_add(
                    maintenance,
                    maintenance_delta,
                    "combining maintenance deltas",
                )?;
            }
            cursor += 1;
        }
        debug_assert_eq!(domains.accounts[domain_offset], account);
        scratch.domain_deltas.push(DomainDelta {
            account,
            offset: domain_offset,
            account_value: value,
            maintenance,
            uncertain,
        });
    }

    merge_account_and_domain_deltas(domains, scratch)?;
    Ok(())
}

fn merge_account_and_domain_deltas(
    domains: &DomainShard,
    scratch: &mut ShardScratch,
) -> Result<()> {
    scratch.domain_account_deltas.clear();
    for delta in &scratch.domain_deltas {
        let old = domains.account_values[delta.offset];
        let new = checked_add(old, delta.account_value, "updating domain account value")?;
        let (old_collateral, old_debt) = split_account_value(old)?;
        let (new_collateral, new_debt) = split_account_value(new)?;
        scratch.domain_account_deltas.push(AccountDelta {
            account: delta.account,
            collateral: checked_sub(
                new_collateral,
                old_collateral,
                "calculating domain collateral delta",
            )?,
            debt: checked_sub(new_debt, old_debt, "calculating domain debt delta")?,
            maintenance: delta.maintenance,
            uncertain: 0,
        });
    }
    combine_account_deltas(
        &scratch.account_deltas,
        &scratch.domain_account_deltas,
        &mut scratch.combined,
    )?;
    std::mem::swap(&mut scratch.account_deltas, &mut scratch.combined);
    Ok(())
}

fn combine_account_deltas(
    left: &[AccountDelta],
    right: &[AccountDelta],
    output: &mut Vec<AccountDelta>,
) -> Result<()> {
    output.clear();
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() || right_index < right.len() {
        let account = match (left.get(left_index), right.get(right_index)) {
            (Some(left), Some(right)) => left.account.min(right.account),
            (Some(left), None) => left.account,
            (None, Some(right)) => right.account,
            (None, None) => break,
        };
        let mut combined = AccountDelta {
            account,
            ..AccountDelta::default()
        };
        while left
            .get(left_index)
            .is_some_and(|delta| delta.account == account)
        {
            add_account_delta(&mut combined, &left[left_index])?;
            left_index += 1;
        }
        while right
            .get(right_index)
            .is_some_and(|delta| delta.account == account)
        {
            add_account_delta(&mut combined, &right[right_index])?;
            right_index += 1;
        }
        output.push(combined);
    }
    Ok(())
}

fn add_account_delta(target: &mut AccountDelta, source: &AccountDelta) -> Result<()> {
    target.collateral = checked_add(
        target.collateral,
        source.collateral,
        "combining account collateral",
    )?;
    target.debt = checked_add(target.debt, source.debt, "combining account debt")?;
    target.maintenance = checked_add(
        target.maintenance,
        source.maintenance,
        "combining account maintenance",
    )?;
    target.uncertain =
        target
            .uncertain
            .checked_add(source.uncertain)
            .ok_or(Error::ArithmeticOverflow {
                operation: "combining account uncertainty",
            })?;
    Ok(())
}

fn preflight_deltas(
    workers: &ThreadPool,
    account_shards: &[AccountShard],
    domain_shards: &[DomainShard],
    scratch: &[ShardScratch],
    epoch: ProcessingEpoch,
) -> Result<()> {
    let shard_count = account_shards.len();
    // Deltas are shard-local, so shards validate concurrently; nothing mutates until every
    // shard passes. Results are collected in shard order and scanned serially, so the error
    // a failing epoch reports does not depend on worker scheduling.
    let result = catch_unwind(AssertUnwindSafe(|| {
        workers.install(|| {
            account_shards
                .par_iter()
                .zip(domain_shards.par_iter())
                .zip(scratch.par_iter())
                .map(|((accounts, domains), scratch)| {
                    for delta in &scratch.domain_deltas {
                        domains.validate_update(delta)?;
                    }
                    for delta in &scratch.account_deltas {
                        let offset = delta.account as usize / shard_count;
                        accounts.validate_update(
                            offset,
                            delta.collateral,
                            delta.debt,
                            delta.maintenance,
                            delta.uncertain,
                        )?;
                    }
                    Ok(())
                })
                .collect::<Vec<Result<()>>>()
        })
    }));
    for shard_result in result.map_err(|_| Error::WorkerPanicked { epoch })? {
        shard_result?;
    }
    Ok(())
}

fn apply_deltas(
    workers: &ThreadPool,
    account_shards: &mut [AccountShard],
    domain_shards: &mut [DomainShard],
    scratch: &mut [ShardScratch],
    policy: &RiskPolicy,
    epoch: ProcessingEpoch,
) -> Result<ApplyResult> {
    let shard_count = account_shards.len();
    // Cleared serially so a shard whose worker never runs restores nothing rather than
    // pairing a previous epoch's saves with this epoch's deltas.
    for scratch in scratch.iter_mut() {
        scratch.saved_states.clear();
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        workers.install(|| {
            account_shards
                .par_iter_mut()
                .zip(domain_shards.par_iter_mut())
                .zip(scratch.par_iter_mut())
                .map(|((accounts, domains), scratch)| {
                    // Save the risk metadata of every account this shard touches before any
                    // mutation, so a failed apply can roll the epoch back completely;
                    // otherwise a partially committed state survives the post-poison rebuild
                    // and the lost transitions are never re-emitted.
                    for delta in &scratch.account_deltas {
                        scratch
                            .saved_states
                            .push(accounts.risk_metadata(delta.account as usize / shard_count));
                    }
                    for delta in &scratch.domain_deltas {
                        domains.apply(delta, policy)?;
                    }
                    scratch.events.clear();
                    for delta in &scratch.account_deltas {
                        let offset = delta.account as usize / shard_count;
                        accounts.accumulate(
                            offset,
                            delta.collateral,
                            delta.debt,
                            delta.maintenance,
                            delta.uncertain,
                        )?;
                        let margin = domains.summary(delta.account);
                        if let Some(event) =
                            accounts.evaluate(delta.account, offset, policy, epoch, margin)?
                        {
                            scratch.events.push(event);
                        }
                    }
                    Ok((scratch.account_deltas.len(), scratch.domain_deltas.len()))
                })
                .collect::<Result<Vec<_>>>()
        })
    }));
    let counts = match result.map_err(|_| Error::WorkerPanicked { epoch }) {
        Ok(Ok(counts)) => counts,
        Ok(Err(error)) | Err(error) => {
            restore_risk_states(account_shards, scratch, shard_count);
            return Err(error);
        }
    };
    let affected_accounts = counts.iter().map(|counts| counts.0).sum();
    let affected_domains = counts.iter().map(|counts| counts.1).sum();
    let event_count = scratch.iter().map(|scratch| scratch.events.len()).sum();
    let mut events = Vec::with_capacity(event_count);
    for scratch in scratch {
        events.append(&mut scratch.events);
    }
    events.sort_unstable_by_key(|event| event.account);
    Ok(ApplyResult {
        affected_accounts,
        affected_domains,
        events,
    })
}

fn restore_risk_states(
    account_shards: &mut [AccountShard],
    scratch: &[ShardScratch],
    shard_count: usize,
) {
    for (accounts, scratch) in account_shards.iter_mut().zip(scratch) {
        for (delta, &(state, version)) in scratch.account_deltas.iter().zip(&scratch.saved_states) {
            accounts.restore_risk_metadata(delta.account as usize / shard_count, state, version);
        }
    }
}

fn evaluation_reason(state: RiskState) -> EvaluationReason {
    match state {
        RiskState::Healthy => EvaluationReason::AboveWarningThreshold,
        RiskState::Watch => EvaluationReason::HealthBelowWarning,
        RiskState::Liquidatable => EvaluationReason::HealthBelowLiquidation,
        RiskState::DataIncomplete => EvaluationReason::MissingInput,
    }
}

fn evaluation_contract_error(error: impl std::fmt::Display) -> Error {
    Error::EvaluationContract {
        reason: error.to_string(),
    }
}

/// One domain's values recomputed from a patch, ready for infallible slab insertion.
struct StagedDomain {
    account: AccountIndex,
    external_index: MarginDomainIndex,
    account_mode: AccountMode,
    margin_mode: MarginMode,
    account_value: i128,
    maintenance: i128,
    uncertain: u32,
    health: i128,
    risk_state: RiskState,
}

/// One account's aggregates recomputed from a patch against current prices.
struct StagedAccount {
    collateral: i128,
    debt: i128,
    maintenance: i128,
    uncertain: u32,
    domains: Vec<StagedDomain>,
}

/// Recomputes one account's state from scratch: the per-account restriction of
/// `rebuild_stores`, with identical arithmetic, uncertainty, and bound rules.
#[allow(clippy::too_many_lines)]
fn stage_account(
    compiled: &CompiledAccount,
    prices: &BTreeMap<u32, Price>,
    policy: &RiskPolicy,
    registry: &AssetRegistry,
) -> Result<StagedAccount> {
    let mut collateral = 0_i128;
    let mut debt = 0_i128;
    let mut maintenance_total = 0_i128;
    let mut uncertain = 0_u32;

    for (price_index, weighted) in &compiled.balance_records {
        match prices.get(price_index) {
            Some(price) => {
                collateral = bounded_aggregate_add(
                    collateral,
                    collateral_contribution_weighted(*weighted, *price)?.raw(),
                    "updating collateral aggregate",
                )?;
            }
            None => uncertain = checked_uncertain_add(uncertain, 1)?,
        }
    }

    let mut domains = Vec::with_capacity(compiled.domains.len());
    let mut perps_by_domain: BTreeMap<MarginDomainIndex, Vec<&CompiledPerp>> = BTreeMap::new();
    for perp in &compiled.perp_records {
        perps_by_domain.entry(perp.domain).or_default().push(perp);
    }
    for domain in &compiled.domains {
        let (mut value, mut maintenance, mut domain_uncertain) = match domain.snapshot {
            Some(snapshot) => (
                snapshot.account_value.raw(),
                snapshot.maintenance.raw(),
                0_u32,
            ),
            None => (0, 0, 1_u32),
        };
        for perp in perps_by_domain
            .get(&domain.external_index)
            .into_iter()
            .flatten()
        {
            let (deltas, uncertain) = perp_deltas_from_anchor(
                perp.signed_size,
                perp.anchor_price,
                registry
                    .perp_market(perp.market)
                    .expect("compiled market exists"),
                prices.get(&perp.price).copied(),
            )?;
            domain_uncertain = checked_uncertain_add(domain_uncertain, i64::from(uncertain))?;
            if let Some((pnl, maintenance_add)) = deltas {
                value = checked_add(value, pnl, "updating domain account value")?;
                maintenance =
                    checked_add(maintenance, maintenance_add, "updating domain maintenance")?;
            }
        }
        if value.unsigned_abs() > MAX_VALUE_RAW as u128 {
            return Err(Error::ArithmeticOverflow {
                operation: "enforcing domain account value bound",
            });
        }
        if !(0..=MAX_VALUE_RAW).contains(&maintenance) {
            return Err(Error::InconsistentMarginSnapshot {
                account: domain.account,
                domain: domain.external_index,
            });
        }
        let health =
            margin_health_ratio(Value::from_raw(value), Value::from_raw(maintenance))?.raw();
        let risk_state = policy.evaluate_margin(
            Value::from_raw(value),
            Value::from_raw(maintenance),
            domain_uncertain > 0,
        );
        let (domain_collateral, domain_debt) = split_account_value(value)?;
        collateral = bounded_aggregate_add(
            collateral,
            domain_collateral,
            "updating collateral aggregate",
        )?;
        debt = bounded_aggregate_add(debt, domain_debt, "updating debt aggregate")?;
        maintenance_total = bounded_aggregate_add(
            maintenance_total,
            maintenance,
            "updating maintenance aggregate",
        )?;
        domains.push(StagedDomain {
            account: domain.account,
            external_index: domain.external_index,
            account_mode: domain.account_mode,
            margin_mode: domain.margin_mode,
            account_value: value,
            maintenance,
            uncertain: domain_uncertain,
            health,
            risk_state,
        });
    }
    Ok(StagedAccount {
        collateral,
        debt,
        maintenance: maintenance_total,
        uncertain,
        domains,
    })
}

/// Values one perp position at the current price relative to its snapshot anchor.
///
/// Returns the raw (pnl, maintenance) deltas when both the price and the anchor are
/// present, plus the count of uncertain inputs (missing anchor and/or missing price).
/// Shared by whole-store rebuilds and per-account staging so both value positions with
/// identical arithmetic.
fn perp_deltas_from_anchor(
    signed_size: SignedAmount,
    anchor: Option<Price>,
    market: &super::PerpMarketMetadata,
    price: Option<Price>,
) -> Result<(Option<(i128, i128)>, u32)> {
    let mut uncertain = 0_u32;
    if anchor.is_none() {
        uncertain += 1;
    }
    if price.is_none() {
        uncertain += 1;
    }
    let deltas = if let (Some(price), Some(anchor)) = (price, anchor) {
        Some(perp_deltas_between_marks(
            signed_size,
            anchor,
            market,
            anchor,
            price,
        )?)
    } else {
        None
    };
    Ok((deltas, uncertain))
}

/// Difference of rounded values, not rounding of the difference. Maintenance's anchor
/// terms cancel exactly, so only the old and new tier requirements need evaluation.
fn perp_deltas_between_marks(
    size: SignedAmount,
    anchor: Price,
    market: &super::PerpMarketMetadata,
    previous: Price,
    current: Price,
) -> Result<(i128, i128)> {
    let current_pnl = perp_pnl(
        size,
        checked_sub(
            current.raw(),
            anchor.raw(),
            "calculating current anchor delta",
        )?,
    )?;
    let previous_pnl = perp_pnl(
        size,
        checked_sub(
            previous.raw(),
            anchor.raw(),
            "calculating previous anchor delta",
        )?,
    )?;
    let current_maintenance =
        native_maintenance_margin(notional_value(size, current)?, &market.margin_tiers)?;
    let previous_maintenance =
        native_maintenance_margin(notional_value(size, previous)?, &market.margin_tiers)?;
    Ok((
        checked_sub(
            current_pnl.raw(),
            previous_pnl.raw(),
            "calculating perp PnL delta",
        )?,
        checked_sub(
            current_maintenance.raw(),
            previous_maintenance.raw(),
            "calculating maintenance delta",
        )?,
    ))
}

fn bounded_aggregate_add(current: i128, delta: i128, operation: &'static str) -> Result<i128> {
    let value = checked_add(current, delta, operation)?;
    if !(0..=MAX_VALUE_RAW).contains(&value) {
        return Err(Error::ArithmeticOverflow { operation });
    }
    Ok(value)
}

fn split_account_value(raw: i128) -> Result<(i128, i128)> {
    if raw >= 0 {
        Ok((raw, 0))
    } else {
        Ok((
            0,
            raw.checked_abs().ok_or(Error::ArithmeticOverflow {
                operation: "converting negative account value to liability",
            })?,
        ))
    }
}

fn build_scratch(portfolio: &PortfolioIndex) -> Vec<ShardScratch> {
    portfolio
        .max_records_per_shard()
        .into_iter()
        .map(ShardScratch::with_capacity)
        .collect()
}

fn record_metrics(balances: usize, perps: usize, events: usize, started: Instant) {
    counter!(METRIC_EPOCHS_TOTAL).increment(1);
    counter!(METRIC_BALANCES_TOTAL).increment(balances as u64);
    counter!(METRIC_PERPS_TOTAL).increment(perps as u64);
    counter!(METRIC_STATE_CHANGES_TOTAL).increment(events as u64);
    histogram!(METRIC_EPOCH_DURATION_SECONDS).record(started.elapsed().as_secs_f64());
}

fn checked_add(left: i128, right: i128, operation: &'static str) -> Result<i128> {
    left.checked_add(right)
        .ok_or(Error::ArithmeticOverflow { operation })
}

fn checked_sub(left: i128, right: i128, operation: &'static str) -> Result<i128> {
    left.checked_sub(right)
        .ok_or(Error::ArithmeticOverflow { operation })
}

#[derive(Clone, Copy)]
struct SourceCursor {
    sequence: SourceSequence,
    update: PriceUpdate,
    epoch: ProcessingEpoch,
}

#[derive(Default)]
struct AccountDelta {
    account: AccountIndex,
    collateral: i128,
    debt: i128,
    maintenance: i128,
    uncertain: i64,
}

struct DomainDelta {
    account: AccountIndex,
    offset: usize,
    account_value: i128,
    maintenance: i128,
    uncertain: i64,
}

#[derive(Default)]
struct ShardScratch {
    account_deltas: Vec<AccountDelta>,
    domain_deltas: Vec<DomainDelta>,
    domain_account_deltas: Vec<AccountDelta>,
    combined: Vec<AccountDelta>,
    saved_states: Vec<(RiskState, StateVersion)>,
    events: Vec<RiskEvent>,
}

impl ShardScratch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            account_deltas: Vec::with_capacity(capacity),
            domain_deltas: Vec::with_capacity(capacity),
            domain_account_deltas: Vec::with_capacity(capacity),
            combined: Vec::with_capacity(capacity),
            saved_states: Vec::with_capacity(capacity),
            events: Vec::new(),
        }
    }
}

struct ApplyResult {
    affected_accounts: usize,
    affected_domains: usize,
    events: Vec<RiskEvent>,
}

#[derive(Debug)]
struct DomainStore {
    shards: Vec<DomainShard>,
}

impl DomainStore {
    fn new(account_count: usize, indexed: &[Vec<IndexedDomain>]) -> Self {
        let shard_count = indexed.len();
        let shards = indexed
            .iter()
            .enumerate()
            .map(|(shard, domains)| {
                DomainShard::new(
                    shard_count,
                    shard_len(account_count, shard, shard_count),
                    domains,
                )
            })
            .collect();
        Self { shards }
    }
}

/// Per-account domain-slot list with the common 1-2 domain case stored inline, so the
/// hot-path summary never chases a heap pointer for typical accounts.
#[derive(Debug, Clone, Default)]
struct DomainSlots {
    count: u32,
    inline: [u32; 2],
    spill: Vec<u32>,
}

impl DomainSlots {
    fn push(&mut self, slot: u32) {
        match self.count {
            0 | 1 => self.inline[self.count as usize] = slot,
            _ => self.spill.push(slot),
        }
        self.count += 1;
    }

    fn slots(&self) -> impl Iterator<Item = u32> + '_ {
        let inline = self.count.min(2) as usize;
        self.inline[..inline]
            .iter()
            .copied()
            .chain(self.spill.iter().copied())
    }
}

/// Slot-stable margin-domain storage: slots keep their index for the lifetime of the
/// installed generation, so perp lanes can reference domains without dense-offset coupling.
#[derive(Debug)]
struct DomainShard {
    shard_count: usize,
    accounts: Vec<AccountIndex>,
    external_indices: Vec<MarginDomainIndex>,
    account_modes: Vec<AccountMode>,
    margin_modes: Vec<MarginMode>,
    account_values: Vec<i128>,
    maintenance_values: Vec<i128>,
    uncertain_inputs: Vec<u32>,
    health_values: Vec<i128>,
    risk_states: Vec<RiskState>,
    occupied: Vec<bool>,
    free_slots: Vec<u32>,
    // Slot lists per local account offset, ordered by external domain index.
    domains_by_account: Vec<DomainSlots>,
}

impl DomainShard {
    fn new(shard_count: usize, account_count: usize, domains: &[IndexedDomain]) -> Self {
        let mut shard = Self {
            shard_count,
            accounts: Vec::with_capacity(domains.len()),
            external_indices: Vec::with_capacity(domains.len()),
            account_modes: Vec::with_capacity(domains.len()),
            margin_modes: Vec::with_capacity(domains.len()),
            account_values: Vec::with_capacity(domains.len()),
            maintenance_values: Vec::with_capacity(domains.len()),
            uncertain_inputs: Vec::with_capacity(domains.len()),
            health_values: vec![i128::MAX; domains.len()],
            risk_states: vec![RiskState::Healthy; domains.len()],
            occupied: Vec::with_capacity(domains.len()),
            free_slots: Vec::new(),
            domains_by_account: vec![DomainSlots::default(); account_count],
        };
        for domain in domains {
            let slot = u32::try_from(shard.accounts.len()).expect("domain count fits u32");
            shard.accounts.push(domain.account);
            shard.external_indices.push(domain.external_index);
            shard.account_modes.push(domain.account_mode);
            shard.margin_modes.push(domain.margin_mode);
            if let Some(snapshot) = domain.snapshot {
                shard.account_values.push(snapshot.account_value.raw());
                shard.maintenance_values.push(snapshot.maintenance.raw());
                shard.uncertain_inputs.push(0);
            } else {
                shard.account_values.push(0);
                shard.maintenance_values.push(0);
                shard.uncertain_inputs.push(1);
            }
            shard.occupied.push(true);
            let local = domain.account as usize / shard_count;
            // Input is sorted by (account, external index), so slot lists stay ordered.
            shard
                .domains_by_account
                .get_mut(local)
                .expect("domain account within shard range")
                .push(slot);
        }
        shard
    }

    fn len(&self) -> usize {
        self.accounts.len()
    }

    /// Frees every slot owned by the local account. Infallible.
    fn free_account_domains(&mut self, local: usize) {
        let slots = std::mem::take(&mut self.domains_by_account[local]);
        for slot in slots.slots() {
            self.occupied[slot as usize] = false;
            self.free_slots.push(slot);
        }
    }

    /// Stores one staged domain in a stable slot and returns the slot id. Infallible.
    ///
    /// Callers insert an account's domains in ascending external-index order after freeing
    /// its previous slots, which keeps the per-account slot list ordered.
    fn allocate_domain(&mut self, staged: &StagedDomain) -> u32 {
        let slot = if let Some(slot) = self.free_slots.pop() {
            let offset = slot as usize;
            self.accounts[offset] = staged.account;
            self.external_indices[offset] = staged.external_index;
            self.account_modes[offset] = staged.account_mode;
            self.margin_modes[offset] = staged.margin_mode;
            self.account_values[offset] = staged.account_value;
            self.maintenance_values[offset] = staged.maintenance;
            self.uncertain_inputs[offset] = staged.uncertain;
            self.health_values[offset] = staged.health;
            self.risk_states[offset] = staged.risk_state;
            self.occupied[offset] = true;
            slot
        } else {
            let slot = u32::try_from(self.accounts.len()).expect("domain count fits u32");
            self.accounts.push(staged.account);
            self.external_indices.push(staged.external_index);
            self.account_modes.push(staged.account_mode);
            self.margin_modes.push(staged.margin_mode);
            self.account_values.push(staged.account_value);
            self.maintenance_values.push(staged.maintenance);
            self.uncertain_inputs.push(staged.uncertain);
            self.health_values.push(staged.health);
            self.risk_states.push(staged.risk_state);
            self.occupied.push(true);
            slot
        };
        let local = staged.account as usize / self.shard_count;
        self.domains_by_account[local].push(slot);
        slot
    }

    fn add_uncertainty(&mut self, offset: usize, delta: i64) -> Result<()> {
        self.uncertain_inputs[offset] =
            checked_uncertain_add(self.uncertain_inputs[offset], delta)?;
        Ok(())
    }

    fn accumulate(
        &mut self,
        offset: usize,
        account_value_delta: i128,
        maintenance_delta: i128,
        uncertain_delta: i64,
    ) -> Result<()> {
        self.account_values[offset] = checked_add(
            self.account_values[offset],
            account_value_delta,
            "updating domain account value",
        )?;
        if self.account_values[offset].unsigned_abs() > MAX_VALUE_RAW as u128 {
            return Err(Error::ArithmeticOverflow {
                operation: "enforcing domain account value bound",
            });
        }
        self.maintenance_values[offset] = checked_add(
            self.maintenance_values[offset],
            maintenance_delta,
            "updating domain maintenance",
        )?;
        if !(0..=MAX_VALUE_RAW).contains(&self.maintenance_values[offset]) {
            return Err(Error::InconsistentMarginSnapshot {
                account: self.accounts[offset],
                domain: self.external_indices[offset],
            });
        }
        self.add_uncertainty(offset, uncertain_delta)
    }

    fn validate_update(&self, delta: &DomainDelta) -> Result<()> {
        let account_value = checked_add(
            self.account_values[delta.offset],
            delta.account_value,
            "updating domain account value",
        )?;
        let maintenance = checked_add(
            self.maintenance_values[delta.offset],
            delta.maintenance,
            "updating domain maintenance",
        )?;
        if account_value.unsigned_abs() > MAX_VALUE_RAW as u128
            || !(0..=MAX_VALUE_RAW).contains(&maintenance)
            || checked_uncertain_add(self.uncertain_inputs[delta.offset], delta.uncertain).is_err()
        {
            return Err(Error::InconsistentMarginSnapshot {
                account: delta.account,
                domain: self.external_indices[delta.offset],
            });
        }
        Ok(())
    }

    fn apply(&mut self, delta: &DomainDelta, policy: &RiskPolicy) -> Result<()> {
        self.accumulate(
            delta.offset,
            delta.account_value,
            delta.maintenance,
            delta.uncertain,
        )?;
        self.evaluate(delta.offset, policy)
    }

    fn evaluate_all(&mut self, policy: &RiskPolicy) -> Result<()> {
        for offset in 0..self.len() {
            if self.occupied[offset] {
                self.evaluate(offset, policy)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, offset: usize, policy: &RiskPolicy) -> Result<()> {
        let account_value = Value::from_raw(self.account_values[offset]);
        let maintenance = Value::from_raw(self.maintenance_values[offset]);
        self.health_values[offset] = margin_health_ratio(account_value, maintenance)?.raw();
        self.risk_states[offset] = policy.evaluate_margin(
            account_value,
            maintenance,
            self.uncertain_inputs[offset] > 0,
        );
        Ok(())
    }

    fn summary(&self, account: AccountIndex) -> MarginSummary {
        let local = account as usize / self.shard_count;
        let Some(slots) = self.domains_by_account.get(local) else {
            return MarginSummary::default();
        };
        let mut summary = MarginSummary::default();
        for slot in slots.slots() {
            let offset = slot as usize;
            summary.has_uncertain_data |= self.uncertain_inputs[offset] > 0;
            summary.minimum_health =
                Factor::from_raw(summary.minimum_health.raw().min(self.health_values[offset]));
        }
        summary
    }

    fn snapshot(
        &self,
        account: AccountIndex,
        domain: MarginDomainIndex,
    ) -> Option<MarginDomainSnapshot> {
        let local = account as usize / self.shard_count;
        let offset = self
            .domains_by_account
            .get(local)?
            .slots()
            .map(|slot| slot as usize)
            .find(|offset| self.external_indices[*offset] == domain)?;
        Some(self.snapshot_at(offset))
    }

    fn snapshot_at(&self, offset: usize) -> MarginDomainSnapshot {
        MarginDomainSnapshot {
            account: self.accounts[offset],
            domain: self.external_indices[offset],
            account_mode: self.account_modes[offset],
            margin_mode: self.margin_modes[offset],
            account_value: Value::from_raw(self.account_values[offset]),
            maintenance: Value::from_raw(self.maintenance_values[offset]),
            health: Factor::from_raw(self.health_values[offset]),
            risk_state: self.risk_states[offset],
            uncertain_inputs: self.uncertain_inputs[offset],
        }
    }
}
