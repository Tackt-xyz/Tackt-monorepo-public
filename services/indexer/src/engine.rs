use crate::{
    sink::ObservationSink,
    source::{
        evm::adapter::{EthereumAdapter, EvmBlock},
        hyperliquid::adapter::{HyperliquidAdapter, PORTFOLIO_BATCH_LIMIT},
    },
    users::UserRegistry,
};
use anyhow::{Context, Result, bail};
use chrono::Utc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tackt_nats::{EvmBatch, ObservationEvent, PublishOutcome};
use tackt_postgres::Repository;
use tackt_primitives::chain::BlockRef;
use tackt_primitives::*;

/// Bounds one reconciliation pass; the WebSocket worker immediately schedules the
/// next pass while a larger durable gap remains.
const MAX_BLOCKS_PER_PASS: u64 = 200;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EthereumPoll {
    /// Observations the broker newly stored (duplicates were already durable).
    pub published: u64,
    pub caught_up: bool,
}

struct EvmChain {
    adapter: EthereumAdapter,
    verified: AtomicBool,
    poll_lock: tokio::sync::Mutex<()>,
}

/// Where one pass continues from, derived from the durable checkpoint alone.
struct Resume {
    sequence: u64,
    history: Vec<BlockRef>,
    /// First height to publish; above `head` when only finality or a rollback moved.
    start: u64,
    revert_from: Option<u64>,
}

impl EvmChain {
    fn new(adapter: EthereumAdapter) -> Self {
        Self {
            adapter,
            verified: AtomicBool::new(false),
            poll_lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[derive(Clone)]
pub struct Indexer<R: Repository, S: ObservationSink> {
    pub repo: Arc<R>,
    evm: Arc<Vec<EvmChain>>,
    pub hyperliquid: HyperliquidAdapter,
    users: UserRegistry,
    /// The acknowledged durable publish is the only record of an observation,
    /// so a publish failure fails the pass and supervision retries it.
    sink: S,
    /// Prevents the periodic and activity-triggered portfolio reconciliations from
    /// issuing duplicate concurrent account snapshots.
    hyperliquid_reconcile_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<R: Repository, S: ObservationSink> Indexer<R, S> {
    pub fn new(
        repo: Arc<R>,
        sink: S,
        evm_adapters: Vec<EthereumAdapter>,
        hyperliquid: HyperliquidAdapter,
    ) -> Self {
        Self {
            repo,
            evm: Arc::new(evm_adapters.into_iter().map(EvmChain::new).collect()),
            hyperliquid,
            users: UserRegistry::default(),
            sink,
            hyperliquid_reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// The chain ids this indexer ingests, in configuration order.
    pub fn evm_chain_ids(&self) -> Vec<u64> {
        self.evm
            .iter()
            .map(|chain| chain.adapter.configured_chain_id())
            .collect()
    }

    fn evm_chain(&self, chain_id: u64) -> Result<&EvmChain> {
        self.evm
            .iter()
            .find(|chain| chain.adapter.configured_chain_id() == chain_id)
            .with_context(|| format!("EVM chain {chain_id} is not configured"))
    }

    /// The process-local user registry read by ingestion and maintained by
    /// [`crate::users`] from PostgreSQL and NATS lifecycle events.
    pub fn users(&self) -> &UserRegistry {
        &self.users
    }

    /// Non-EVM observations retain their individual durable event identities.
    async fn publish_observations(&self, observations: &[Observation]) -> Result<u64> {
        let mut stored = 0;
        for observation in observations {
            let event = ObservationEvent::new(observation.clone());
            let source = event.observation.source.as_str();
            let outcome = self.sink.publish(&event).await;
            if outcome.is_err() {
                metrics::counter!(
                    "indexer_observation_publish_failures_total",
                    "source" => source,
                    "chain_id" => "none",
                )
                .increment(1);
            }
            match outcome
                .with_context(|| format!("publishing observation {}", event.observation.event_id))?
            {
                PublishOutcome::Stored => {
                    metrics::counter!(
                        "indexer_observations_published_total",
                        "source" => source,
                        "chain_id" => "none",
                    )
                    .increment(1);
                    stored += 1;
                }
                PublishOutcome::Duplicate => {
                    metrics::counter!(
                        "indexer_observation_duplicates_total",
                        "source" => source,
                        "chain_id" => "none",
                    )
                    .increment(1);
                }
            }
        }
        Ok(stored)
    }
    /// Reconciles one chain to its HTTP endpoint's current head. Used once at
    /// WebSocket startup/reconnect and by the finite `poll-once` command.
    pub async fn poll_evm(&self, chain_id: u64) -> Result<EthereumPoll> {
        let chain = self.evm_chain(chain_id)?;
        let _poll_guard = chain.poll_lock.lock().await;
        let head = chain.adapter.head().await?;
        self.sync_evm(chain, head).await.inspect_err(|_| {
            metrics::counter!("indexer_evm_sync_failures_total", "chain_id" => chain_id.to_string()).increment(1);
        })
    }

    /// A batch is both evidence and the durable chain checkpoint. No second write.
    async fn sync_evm(&self, chain: &EvmChain, head: u64) -> Result<EthereumPoll> {
        let chain_id = chain.adapter.configured_chain_id();
        if !chain.verified.load(Ordering::Relaxed) {
            anyhow::ensure!(
                chain.adapter.chain_id().await? == chain_id,
                "EVM chain mismatch"
            );
            chain.verified.store(true, Ordering::Relaxed);
        }
        let finalized = chain.adapter.finalized_header().await?.block;
        anyhow::ensure!(finalized.number <= head, "finalized block exceeds RPC head");
        let last = self.sink.last_evm_event(chain_id).await?;
        let Resume {
            mut sequence,
            mut history,
            start,
            mut revert_from,
        } = match &last {
            None => Resume {
                sequence: 0,
                history: chain.adapter.history(finalized.number, head).await?,
                start: head,
                revert_from: None,
            },
            Some((sequence, event)) => {
                let tip = event
                    .observation
                    .block_ref()
                    .context("checkpoint missing block coordinates")?;
                let batch = event
                    .batch
                    .as_ref()
                    .context("EVM checkpoint requires a block batch")?;
                resume_after_batch(&chain.adapter, *sequence, batch, tip, head, &finalized).await?
            }
        };
        let reorg = revert_from.is_some();
        let registrations = self.users.registrations();
        let accounts = registrations.ethereum_owners();
        let mut published = 0;
        let end = head.min(start.saturating_add(MAX_BLOCKS_PER_PASS - 1));
        for number in start..=end {
            let EvmBlock {
                header,
                block,
                facts,
            } = chain
                .adapter
                .block(number, accounts, head, finalized.number)
                .await?;
            if let Some(previous) = history
                .iter()
                .find(|block| block.number.checked_add(1) == Some(number))
            {
                anyhow::ensure!(
                    previous.hash == header.parent_hash,
                    "RPC branch changed before block publication; retry"
                );
            }
            history.retain(|block| block.number < number);
            history.push(header.block);
            let fact_count = facts.len() as u64 + 1;
            let update = block_update(
                chain_id,
                sequence,
                block,
                facts,
                &finalized,
                &mut history,
                revert_from.take(),
            )?;
            sequence = self.sink.publish_batch(&update).await?;
            published += fact_count;
            metrics::counter!("indexer_observations_published_total", "source" => "ethereum", "chain_id" => chain_id.to_string())
                .increment(fact_count);
        }
        // No new block is required for a finality advance or a shorter canonical tip.
        let finality_moved = last
            .as_ref()
            .and_then(|(_, event)| event.batch.as_ref())
            .is_some_and(|batch| batch.finalized != finalized);
        if start > head && (revert_from.is_some() || finality_moved) {
            let tip = history.last().context("empty chain history")?;
            let block = chain.adapter.block_header(tip, finalized.number).await?;
            let update = block_update(
                chain_id,
                sequence,
                block,
                Vec::new(),
                &finalized,
                &mut history,
                revert_from,
            )?;
            self.sink.publish_batch(&update).await?;
        }
        if reorg {
            metrics::counter!("indexer_evm_reorg_recoveries_total", "chain_id" => chain_id.to_string()).increment(1);
        }
        let tip = history.last().context("empty chain history")?.number;
        record_evm_pass(chain_id, head, finalized.number, tip);
        Ok(EthereumPoll {
            published,
            caught_up: tip == head,
        })
    }
    pub async fn reconcile_hyperliquid_accounts(
        &self,
        requested: Option<&HashSet<String>>,
    ) -> Result<u64> {
        let _reconciliation_guard = self.hyperliquid_reconcile_lock.lock().await;
        let mut pending = Vec::new();
        for user in self
            .repo
            .users()
            .await?
            .into_iter()
            .filter(|user| user.enabled)
        {
            for account in user.hyperliquid_accounts {
                let account = account.to_lowercase();
                if requested.is_none_or(|requested| requested.contains(&account)) {
                    pending.push((user.id, account));
                }
            }
        }
        let attempted = pending.len();
        let mut total = 0;
        for attempt in 0..3 {
            let mut failures = HashSet::new();
            for chunk in pending.chunks(PORTFOLIO_BATCH_LIMIT) {
                match self.hyperliquid.portfolio_snapshots(chunk).await {
                    Ok(batch) => {
                        total += self.publish_observations(&batch.observations).await?;
                        failures.extend(batch.failed_accounts);
                    }
                    Err(error) => {
                        tracing::warn!(%error, accounts = chunk.len(), attempt = attempt + 1, "Hyperliquid portfolio batch failed");
                        failures.extend(chunk.iter().map(|(_, account)| account.clone()));
                    }
                }
            }
            if failures.is_empty() {
                return Ok(total);
            }
            if attempt == 2 {
                bail!(
                    "Hyperliquid portfolio reconciliation failed for {} of {attempted} accounts after three attempts",
                    failures.len()
                )
            }
            pending.retain(|(_, account)| failures.contains(account));
            tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
        }
        unreachable!("bounded reconciliation attempts either return or fail")
    }

    pub(crate) async fn hyperliquid_resume_block(
        &self,
        stream: tackt_nats::HyperliquidObservationStream,
    ) -> Result<Option<u64>> {
        self.sink.hyperliquid_resume_block(stream).await
    }

    pub(crate) async fn publish_hyperliquid_observations(
        &self,
        observations: &[Observation],
    ) -> Result<u64> {
        self.publish_observations(observations).await
    }
}

/// The per-chain sync surface, refreshed at the end of every successful pass.
/// Readiness stays scoped to the durable publish path (see [`crate::api`]), so
/// a stalled chain is visible only here.
fn record_evm_pass(chain_id: u64, head: u64, finalized_head: u64, checkpoint: u64) {
    let chain = chain_id.to_string();
    metrics::gauge!("indexer_evm_head", "chain_id" => chain.clone()).set(head as f64);
    metrics::gauge!("indexer_evm_checkpoint", "chain_id" => chain.clone()).set(checkpoint as f64);
    metrics::gauge!("indexer_evm_blocks_behind", "chain_id" => chain.clone())
        .set(head.saturating_sub(checkpoint) as f64);
    metrics::gauge!("indexer_evm_finalized_lag_blocks", "chain_id" => chain.clone())
        .set(head.saturating_sub(finalized_head) as f64);
    metrics::gauge!("indexer_evm_last_success_timestamp_seconds", "chain_id" => chain)
        .set(Utc::now().timestamp() as f64);
}

/// Reconnect the RPC branch to the published suffix. Every height past the
/// common ancestor is reverted; the previously finalized anchor may never move.
async fn resume_after_batch(
    adapter: &EthereumAdapter,
    sequence: u64,
    batch: &EvmBatch,
    tip: BlockRef,
    head: u64,
    finalized: &BlockRef,
) -> Result<Resume> {
    anyhow::ensure!(
        finalized.number >= batch.finalized.number,
        "finality regressed; refusing chain update"
    );
    let anchor = if finalized.number == batch.finalized.number {
        finalized.clone()
    } else {
        adapter.header(batch.finalized.number).await?.block
    };
    anyhow::ensure!(
        anchor == batch.finalized,
        "previously finalized block changed; operator reconciliation required"
    );
    let mut history = batch.history.clone();
    let mut candidate = adapter.header(tip.number.min(head)).await?;
    while !history.contains(&candidate.block) {
        anyhow::ensure!(
            candidate.block.number > batch.finalized.number,
            "reorg crosses finalized checkpoint; operator reconciliation required"
        );
        let parent = adapter.header(candidate.block.number - 1).await?;
        anyhow::ensure!(
            parent.block.hash == candidate.parent_hash,
            "RPC branch changed during reorg scan; retry"
        );
        candidate = parent;
    }
    let mut revert_from = None;
    if candidate.block != tip {
        revert_from = Some(candidate.block.number + 1);
        history.retain(|block| block.number <= candidate.block.number);
    }
    Ok(Resume {
        sequence,
        history,
        start: candidate
            .block
            .number
            .checked_add(1)
            .context("block height exhausted")?,
        revert_from,
    })
}

fn block_update(
    chain_id: u64,
    previous_sequence: u64,
    mut observation: Observation,
    mut observations: Vec<Observation>,
    finalized: &BlockRef,
    history: &mut Vec<BlockRef>,
    revert_from: Option<u64>,
) -> Result<ObservationEvent> {
    let tip = history.last().context("empty chain history")?;
    anyhow::ensure!(
        observation.block_hash.as_deref() == Some(tip.hash.as_str()),
        "RPC branch changed before checkpoint publication; retry"
    );
    let finalized = if finalized.number > tip.number {
        // Catch-up has not connected to the new finalized hash yet.
        history.first().context("empty chain history")?.clone()
    } else {
        finalized.clone()
    };
    anyhow::ensure!(
        history.contains(&finalized),
        "finalized hash is not on the published branch; retry"
    );
    history.retain(|block| block.number >= finalized.number);
    for fact in std::iter::once(&mut observation).chain(&mut observations) {
        fact.finalized = fact
            .block_number
            .is_some_and(|height| height <= finalized.number);
    }
    Ok(ObservationEvent::with_batch(
        observation,
        EvmBatch {
            chain_id,
            previous_sequence,
            finalized,
            history: history.clone(),
            revert_from,
            observations,
        },
    ))
}

#[cfg(test)]
#[path = "evm_tests.rs"]
mod evm_tests;
