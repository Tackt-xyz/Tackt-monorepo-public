use crate::error::{Error, Result};

use super::{
    AccountIndex, Factor, MAX_VALUE_RAW, ProcessingEpoch, RiskEvent, RiskPolicy, RiskState,
    StateVersion, Value, account_health_ratio, arithmetic::checked_uncertain_add,
};

/// Point-in-time view of one account's pooled risk aggregates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    /// Account the snapshot describes.
    pub account: AccountIndex,
    /// Pooled collateral value.
    pub collateral_value: Value,
    /// Pooled liability value.
    pub debt_value: Value,
    /// Total Hyperliquid maintenance.
    pub maintenance_value: Value,
    /// Overall account health ratio.
    pub health: Factor,
    /// Weakest individual margin-domain health.
    pub minimum_margin_health: Factor,
    /// Current risk classification.
    pub risk_state: RiskState,
    /// Monotonic account state version.
    pub state_version: StateVersion,
    /// Processing epoch of the last evaluation.
    pub applied_epoch: ProcessingEpoch,
    /// Count of missing inputs affecting the account.
    pub uncertain_inputs: u32,
}

impl AccountSnapshot {
    /// Health as a finite ratio. `None` while inputs are incomplete or when zero maintenance
    /// saturated the ratio to the `i128::MAX` / `i128::MIN` sentinels produced by arithmetic.
    #[must_use]
    pub fn finite_health(&self) -> Option<Factor> {
        let raw = self.health.raw();
        (self.risk_state != RiskState::DataIncomplete && raw != i128::MAX && raw != i128::MIN)
            .then_some(self.health)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MarginSummary {
    pub has_uncertain_data: bool,
    pub minimum_health: Factor,
}

impl Default for MarginSummary {
    fn default() -> Self {
        Self {
            has_uncertain_data: false,
            minimum_health: Factor::from_raw(i128::MAX),
        }
    }
}

/// Dense modulo-sharded store of per-account risk aggregates and state metadata.
#[derive(Debug)]
pub struct AccountStore {
    account_count: usize,
    shard_count: usize,
    shards: Vec<AccountShard>,
}

impl AccountStore {
    /// Allocates dense account shards.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or unrepresentable account set or invalid shard count.
    pub fn new(account_count: usize, shard_count: usize) -> Result<Self> {
        if account_count == 0 {
            return Err(Error::EmptyAccountSet);
        }
        if AccountIndex::try_from(account_count - 1).is_err() {
            return Err(Error::AccountSetTooLarge);
        }
        if shard_count == 0 || shard_count > account_count {
            return Err(Error::InvalidShardCount { shard_count });
        }
        let shards = (0..shard_count)
            .map(|shard| AccountShard::new(shard_len(account_count, shard, shard_count)))
            .collect();
        Ok(Self {
            account_count,
            shard_count,
            shards,
        })
    }

    #[must_use]
    pub const fn account_count(&self) -> usize {
        self.account_count
    }

    #[must_use]
    pub const fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Returns one account's current aggregate state.
    ///
    /// # Errors
    ///
    /// Returns an error when the account is outside the configured range.
    pub fn snapshot(&self, account: AccountIndex) -> Result<AccountSnapshot> {
        let (shard, offset) = self.location(account)?;
        self.shards[shard].snapshot(account, offset)
    }

    pub(crate) fn clear_aggregates(&mut self) {
        for shard in &mut self.shards {
            shard.clear_aggregates();
        }
    }

    pub(crate) fn inherit_metadata(&mut self, previous: &Self) {
        debug_assert_eq!(self.account_count, previous.account_count);
        for (shard, previous_shard) in self.shards.iter_mut().zip(&previous.shards) {
            shard.risk_states.clone_from(&previous_shard.risk_states);
            shard
                .state_versions
                .clone_from(&previous_shard.state_versions);
        }
    }

    pub(crate) fn evaluate(
        &mut self,
        account: AccountIndex,
        policy: &RiskPolicy,
        epoch: ProcessingEpoch,
        margin: MarginSummary,
    ) -> Result<Option<RiskEvent>> {
        let (shard, offset) = self.location(account)?;
        self.shards[shard].evaluate(account, offset, policy, epoch, margin)
    }

    /// Overwrites one account's aggregates with freshly recomputed values.
    ///
    /// # Errors
    ///
    /// Returns an error when the account is outside the configured range.
    pub(crate) fn set_aggregates(
        &mut self,
        account: AccountIndex,
        collateral: i128,
        debt: i128,
        maintenance: i128,
        uncertain: u32,
    ) -> Result<()> {
        if [collateral, debt, maintenance]
            .into_iter()
            .any(|value| !(0..=super::MAX_VALUE_RAW).contains(&value))
        {
            return Err(Error::ArithmeticOverflow {
                operation: "enforcing non-negative account aggregate bounds",
            });
        }
        let (shard, offset) = self.location(account)?;
        self.shards[shard].set_aggregates(offset, collateral, debt, maintenance, uncertain);
        Ok(())
    }

    pub(crate) fn shards_mut(&mut self) -> &mut [AccountShard] {
        &mut self.shards
    }

    pub(crate) fn shards(&self) -> &[AccountShard] {
        &self.shards
    }

    /// Returns the shard and in-shard offset of an in-range account.
    pub(crate) fn location(&self, account: AccountIndex) -> Result<(usize, usize)> {
        if account as usize >= self.account_count {
            return Err(Error::AccountOutOfRange { account });
        }
        Ok((
            account as usize % self.shard_count,
            account as usize / self.shard_count,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct AccountShard {
    collateral_values: Vec<i128>,
    debt_values: Vec<i128>,
    maintenance_values: Vec<i128>,
    minimum_margin_health: Vec<i128>,
    risk_states: Vec<RiskState>,
    state_versions: Vec<StateVersion>,
    applied_epochs: Vec<ProcessingEpoch>,
    uncertain_inputs: Vec<u32>,
}

impl AccountShard {
    fn new(len: usize) -> Self {
        Self {
            collateral_values: vec![0; len],
            debt_values: vec![0; len],
            maintenance_values: vec![0; len],
            minimum_margin_health: vec![i128::MAX; len],
            risk_states: vec![RiskState::Healthy; len],
            state_versions: vec![0; len],
            applied_epochs: vec![0; len],
            uncertain_inputs: vec![0; len],
        }
    }

    fn clear_aggregates(&mut self) {
        self.collateral_values.fill(0);
        self.debt_values.fill(0);
        self.maintenance_values.fill(0);
        self.minimum_margin_health.fill(i128::MAX);
        self.applied_epochs.fill(0);
        self.uncertain_inputs.fill(0);
    }

    pub(crate) fn accumulate(
        &mut self,
        offset: usize,
        collateral_delta: i128,
        debt_delta: i128,
        maintenance_delta: i128,
        uncertain_delta: i64,
    ) -> Result<()> {
        self.collateral_values[offset] = checked_non_negative_add(
            self.collateral_values[offset],
            collateral_delta,
            "updating collateral aggregate",
        )?;
        self.debt_values[offset] = checked_non_negative_add(
            self.debt_values[offset],
            debt_delta,
            "updating debt aggregate",
        )?;
        self.maintenance_values[offset] = checked_non_negative_add(
            self.maintenance_values[offset],
            maintenance_delta,
            "updating maintenance aggregate",
        )?;
        self.uncertain_inputs[offset] =
            checked_uncertain_add(self.uncertain_inputs[offset], uncertain_delta)?;
        Ok(())
    }

    pub(crate) fn validate_update(
        &self,
        offset: usize,
        collateral_delta: i128,
        debt_delta: i128,
        maintenance_delta: i128,
        uncertain_delta: i64,
    ) -> Result<()> {
        checked_non_negative_add(
            self.collateral_values[offset],
            collateral_delta,
            "updating collateral aggregate",
        )?;
        checked_non_negative_add(
            self.debt_values[offset],
            debt_delta,
            "updating debt aggregate",
        )?;
        checked_non_negative_add(
            self.maintenance_values[offset],
            maintenance_delta,
            "updating maintenance aggregate",
        )?;
        checked_uncertain_add(self.uncertain_inputs[offset], uncertain_delta)?;
        self.state_versions[offset]
            .checked_add(1)
            .ok_or(Error::ArithmeticOverflow {
                operation: "incrementing account state version",
            })?;
        Ok(())
    }

    pub(crate) fn risk_metadata(&self, offset: usize) -> (RiskState, StateVersion) {
        (self.risk_states[offset], self.state_versions[offset])
    }

    /// Overwrites one account's aggregates with freshly recomputed values, leaving its risk
    /// metadata for the following `evaluate` to advance.
    pub(crate) fn set_aggregates(
        &mut self,
        offset: usize,
        collateral: i128,
        debt: i128,
        maintenance: i128,
        uncertain: u32,
    ) {
        self.collateral_values[offset] = collateral;
        self.debt_values[offset] = debt;
        self.maintenance_values[offset] = maintenance;
        self.uncertain_inputs[offset] = uncertain;
    }

    pub(crate) fn restore_risk_metadata(
        &mut self,
        offset: usize,
        state: RiskState,
        version: StateVersion,
    ) {
        self.risk_states[offset] = state;
        self.state_versions[offset] = version;
    }

    pub(crate) fn evaluate(
        &mut self,
        account: AccountIndex,
        offset: usize,
        policy: &RiskPolicy,
        epoch: ProcessingEpoch,
        margin: MarginSummary,
    ) -> Result<Option<RiskEvent>> {
        let previous_state = self.risk_states[offset];
        let collateral = Value::from_raw(self.collateral_values[offset]);
        let debt = Value::from_raw(self.debt_values[offset]);
        let maintenance = Value::from_raw(self.maintenance_values[offset]);
        let current_state = policy.evaluate_account(
            collateral,
            debt,
            maintenance,
            self.uncertain_inputs[offset] > 0 || margin.has_uncertain_data,
        );
        let next_version =
            self.state_versions[offset]
                .checked_add(1)
                .ok_or(Error::ArithmeticOverflow {
                    operation: "incrementing account state version",
                })?;

        self.minimum_margin_health[offset] = margin.minimum_health.raw();
        self.risk_states[offset] = current_state;
        self.state_versions[offset] = next_version;
        self.applied_epochs[offset] = epoch;

        if previous_state == current_state {
            return Ok(None);
        }
        Ok(Some(RiskEvent {
            account,
            epoch,
            state_version: next_version,
            previous_state,
            current_state,
            reason: RiskPolicy::reason(current_state),
            collateral_value: collateral,
            debt_value: debt,
            maintenance_value: maintenance,
            health: account_health_ratio(collateral, debt, maintenance)?,
            minimum_margin_health: margin.minimum_health,
        }))
    }

    fn snapshot(&self, account: AccountIndex, offset: usize) -> Result<AccountSnapshot> {
        let collateral = Value::from_raw(self.collateral_values[offset]);
        let debt = Value::from_raw(self.debt_values[offset]);
        let maintenance = Value::from_raw(self.maintenance_values[offset]);
        Ok(AccountSnapshot {
            account,
            collateral_value: collateral,
            debt_value: debt,
            maintenance_value: maintenance,
            health: account_health_ratio(collateral, debt, maintenance)?,
            minimum_margin_health: Factor::from_raw(self.minimum_margin_health[offset]),
            risk_state: self.risk_states[offset],
            state_version: self.state_versions[offset],
            applied_epoch: self.applied_epochs[offset],
            uncertain_inputs: self.uncertain_inputs[offset],
        })
    }
}

/// Returns the number of accounts assigned to a modulo-partitioned shard.
pub(crate) fn shard_len(account_count: usize, shard: usize, shard_count: usize) -> usize {
    account_count.saturating_sub(shard).div_ceil(shard_count)
}

fn checked_non_negative_add(current: i128, delta: i128, operation: &'static str) -> Result<i128> {
    let value = current
        .checked_add(delta)
        .ok_or(Error::ArithmeticOverflow { operation })?;
    if value < 0 {
        return Err(Error::ArithmeticOverflow { operation });
    }
    if value > MAX_VALUE_RAW {
        return Err(Error::ArithmeticOverflow { operation });
    }
    Ok(value)
}
