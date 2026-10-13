//! Trusted facts are recorded first; accounting and the durable cursor share their transaction.
use crate::state::DepositPhase;
use crate::{Ledger, LedgerError, accounting::post_deposit};
use anyhow::{Context, Result};
use tackt_nats::{Consumer, EvmBatch, NatsClient, ObservationEvent, ObservationFeed};
use tackt_postgres::{ChainState, Deposit, LedgerWrite};
use tackt_primitives::{EventKind, Observation};

/// Recorded but never posted: the initial pending posting failed or was rejected first.
const NEW: &str = "new";

/// A single durable consumer; committed duplicates are safe after a lost acknowledgement.
pub async fn consume(
    ledger: std::sync::Arc<Ledger>,
    feed: ObservationFeed,
    mut consumer: Consumer<ObservationEvent>,
    mut lease: tackt_postgres::ObservationLease,
) -> Result<()> {
    loop {
        let delivery = consumer.next_strict().await?;
        lease.check().await?;
        ledger
            .observe(
                &feed.stream().name,
                delivery.stream_sequence(),
                delivery.message(),
            )
            .await?;
        delivery.ack().await?;
    }
}

impl Ledger {
    /// Resume only the same stream incarnation as the committed accounting cursor.
    /// The caller holds the observer lease before binding its consumer.
    pub async fn observation_consumer(
        &self,
        client: &NatsClient,
        feed: &ObservationFeed,
    ) -> Result<Consumer<ObservationEvent>> {
        let stream = &feed.stream().name;
        let mut write = self.repository.begin_ledger_write().await?;
        let sequence = write.observation_sequence(stream).await?;
        let consumer = client.ledger_observations(feed.stream()).await?;
        let stored = write.observation_stream_created_at(stream).await?;
        anyhow::ensure!(
            stored.as_deref() == Some(consumer.stream_created_at())
                || (stored.is_none() && sequence == 0),
            "observation stream {stream} identity missing or changed at cursor {sequence}; restore matching JetStream data and database state before resuming"
        );
        anyhow::ensure!(
            consumer.acknowledged_sequence() <= sequence,
            "observation consumer on {stream} has acknowledged through {}, ahead of database cursor {sequence}; stop Ledger and explicitly reposition the durable to {} before resuming",
            consumer.acknowledged_sequence(),
            sequence.saturating_add(1)
        );
        if stored.is_none() {
            write
                .store_observation_stream_created_at(stream, consumer.stream_created_at())
                .await?;
        }
        write.commit().await?;
        Ok(consumer)
    }

    pub async fn observe(
        &self,
        stream: &str,
        sequence: u64,
        event: &ObservationEvent,
    ) -> Result<()> {
        let mut write = self.repository.begin_ledger_write().await?;
        let previous = write.observation_sequence(stream).await?;
        if sequence <= previous {
            return Ok(());
        }
        anyhow::ensure!(
            previous.checked_add(1) == Some(sequence),
            "observation stream gap after {previous}; restore missing history before continuing"
        );
        if let Some(batch) = &event.batch {
            apply_batch(&mut write, sequence, &event.observation, batch).await?;
        } else {
            write.store_observation(&event.observation, None).await?;
        }
        write.store_observation_sequence(stream, sequence).await?;
        write.commit().await
    }
}

async fn apply_batch(
    write: &mut LedgerWrite,
    sequence: u64,
    block: &Observation,
    batch: &EvmBatch,
) -> Result<()> {
    let chain = batch.chain_id;
    if let Some(previous) = write.chain_state(chain).await? {
        anyhow::ensure!(
            previous.sequence == batch.previous_sequence,
            "chain {chain} delivery gap after {}; restore missing updates",
            previous.sequence
        );
    }
    if let Some(from) = batch.revert_from {
        for deposit in write
            .deposits(chain, from, i64::MAX as u64, &[NEW, "pending", "finalized"])
            .await?
        {
            transition(write, deposit, DepositPhase::Rejected, sequence).await?;
        }
        write.mark_orphaned(chain, from).await?;
    }
    let facts = std::iter::once(block)
        .chain(&batch.observations)
        .collect::<Vec<_>>();
    write.store_observations(&facts, chain).await?;
    for observation in &batch.observations {
        let Some(candidate) = deposit(observation) else {
            continue;
        };
        let mut deposit = match write.deposit(&candidate.event_id).await? {
            Some(known) => known,
            None if credits_tracked_recipient(write, observation).await? => candidate,
            None => continue,
        };
        deposit.chain_id = chain;
        if deposit.status == NEW || deposit.status == DepositPhase::Rejected.as_ref() {
            transition(write, deposit, DepositPhase::Pending, sequence).await?;
        }
    }
    finalize_deposits(write, chain, batch.finalized.number, sequence).await?;
    write
        .store_chain_state(
            chain,
            &ChainState {
                tip: block
                    .block_ref()
                    .context("batch header missing block coordinates")?,
                finalized: batch.finalized.clone(),
                sequence,
            },
        )
        .await
}

/// Only an inbound transfer to a tracked account from an untracked sender is external credit.
async fn credits_tracked_recipient(
    write: &mut LedgerWrite,
    observation: &Observation,
) -> Result<bool> {
    let Some((sender, recipient)) = parties(observation) else {
        return Ok(false);
    };
    let inbound = observation
        .account
        .as_ref()
        .is_some_and(|account| account.external_id() == recipient);
    Ok(inbound && !write.tracked_sender(&sender).await?)
}

async fn finalize_deposits(
    write: &mut LedgerWrite,
    chain: u64,
    height: u64,
    revision: u64,
) -> Result<()> {
    for deposit in write.deposits(chain, 0, height, &["pending"]).await? {
        transition(write, deposit, DepositPhase::Finalized, revision).await?;
    }
    write.mark_finalized(chain, height).await
}

fn deposit(observation: &Observation) -> Option<Deposit> {
    if !observation.canonical
        || !matches!(
            observation.kind,
            EventKind::EthTransfer | EventKind::Erc20Transfer
        )
    {
        return None;
    }
    let asset = observation.asset?.asset();
    let quantity = observation.quantity.as_deref()?;
    let quantity = u128::from_str_radix(
        quantity
            .strip_prefix("0x")
            .or_else(|| quantity.strip_prefix("0X"))
            .unwrap_or(quantity),
        16,
    )
    .ok()?;
    if quantity == 0 {
        return None;
    }
    Some(Deposit {
        event_id: observation.event_id.clone(),
        chain_id: asset.info().evm_chain_id()?,
        block_number: observation.block_number?,
        user_id: observation.account.as_ref()?.subject().user_id(),
        asset,
        quantity,
        effective_at: observation.source_timestamp,
        status: NEW.into(),
    })
}

async fn transition(
    write: &mut LedgerWrite,
    mut deposit: Deposit,
    phase: DepositPhase,
    revision: u64,
) -> Result<Deposit> {
    let status = phase.as_ref();
    if status == deposit.status {
        return Ok(deposit);
    }
    // A failed initial posting has nothing to reverse or finalize.
    if deposit.status == NEW && phase != DepositPhase::Pending {
        if phase == DepositPhase::Rejected {
            deposit.status = status.into();
            write.store_deposit(&deposit, None).await?;
        }
        return Ok(deposit);
    }
    write.accounting_savepoint().await?;
    match post_deposit(write, &deposit, phase, revision).await {
        Ok(()) => {
            write.finish_accounting(false).await?;
            deposit.status = status.into();
            write.store_deposit(&deposit, None).await?;
        }
        Err(LedgerError::Storage(error)) => return Err(error),
        Err(error) => {
            // A reorg cannot commit canonicality/cursor while leaving an orphan credited.
            if phase == DepositPhase::Rejected {
                return Err(error.into());
            }
            write.finish_accounting(true).await?;
            let detail = format!("{} -> {status}: {error}", deposit.status);
            write.store_deposit(&deposit, Some(&detail)).await?;
            tracing::error!(event_id = %deposit.event_id, %detail, "observation retained; accounting needs review");
        }
    }
    Ok(deposit)
}

fn parties(observation: &Observation) -> Option<(String, String)> {
    match observation.kind {
        EventKind::EthTransfer => Some((
            observation.raw.get("from")?.as_str()?.to_ascii_lowercase(),
            observation.raw.get("to")?.as_str()?.to_ascii_lowercase(),
        )),
        EventKind::Erc20Transfer => {
            let topics = observation.raw.get("topics")?.as_array()?;
            let address = |index: usize| {
                let topic = topics.get(index)?.as_str()?;
                Some(format!(
                    "0x{}",
                    topic
                        .get(topic.len().checked_sub(40)?..)?
                        .to_ascii_lowercase()
                ))
            };
            Some((address(1)?, address(2)?))
        }
        _ => None,
    }
}
