//! Restart reconstruction from the newest verifiable snapshot.

use crate::processing::{AssetRegistry, PriceUpdate, RiskEvent, RiskPolicy, RiskProcessor};

use super::{ApplyLoop, Fold, Result, SnapshotStore};

/// A recovered engine plus everything the embedding service needs to resume.
///
/// Resume each input stream from `apply_loop.fold().cursors()`.
pub struct Recovery {
    /// Processor, fold, and ledger, reconstructed and ready for the tail replay.
    pub apply_loop: ApplyLoop,
    /// Transitions downstream has not heard: state recomputed at recovery differed from the
    /// ledger. Publish these before replaying the tail.
    pub corrections: Vec<RiskEvent>,
}

/// Rebuilds a processor from the newest verifiable snapshot.
///
/// Returns `Ok(None)` when no snapshot exists — a first-ever boot that must assemble its
/// initial generation externally. Otherwise the recovered engine is fully opinionated: the
/// base generation installs at its original version, accounts patched past it replay
/// through the public patch path in stamp order (so every per-account version cursor —
/// and with it the stale-patch and stale-sweep guards — matches a processor that never
/// restarted), stored prices replay through the normal update path in ascending per-source
/// sequence order, and the ledger diff yields exactly the transitions downstream never
/// heard.
///
/// # Errors
///
/// Returns an error when every existing snapshot fails verification or when the processing
/// core rejects the reconstructed inputs.
pub fn recover(
    store: &SnapshotStore,
    account_count: usize,
    shard_count: usize,
    policy: RiskPolicy,
    registry: AssetRegistry,
) -> Result<Option<Recovery>> {
    let Some(snapshot) = store.load_latest()? else {
        return Ok(None);
    };

    let (fold, mut ledger) = Fold::from_snapshot(snapshot);
    let recovered_epoch = fold.last_committed_epoch();
    let mut processor = RiskProcessor::new(
        account_count,
        shard_count,
        policy,
        registry,
        &fold.base_generation(),
    )?;

    // Replay accounts patched past the base generation through the public patch path, in
    // stamp order. This restores per-account version cursors exactly — a late-delivered
    // patch or a same-version reconciliation sweep meets the same verdict it would have
    // met had the process never restarted.
    for patch in fold.pending_patches() {
        processor.apply_account_patch(&patch)?;
    }

    // Seed prices through the public update path: cursors start at None, so each source's
    // stored updates are accepted as long as they replay in ascending sequence order.
    let mut updates: Vec<PriceUpdate> = fold.prices().values().copied().collect();
    updates.sort_unstable_by_key(|update| (update.source, update.source_sequence));
    for update in updates {
        processor.apply_price_update(update)?;
    }
    // Snapshot replay is condensed (latest inputs only), so it consumes fewer
    // epochs than the original history. Restore the durable high-water mark:
    // replaying an unacked tail input after restart then reproduces the same
    // occurrence identity it had before the crash.
    processor.restore_committed_epoch(recovered_epoch)?;

    // Emit a correction wherever downstream's last-heard state disagrees with the state the
    // inputs reproduce. Accounts downstream never heard about need no correction: there is
    // no stale belief to fix, and the tail replay reports their transitions normally. A
    // ledger entry outside the configured account range fails the account query loudly:
    // the snapshot and the configuration disagree.
    let mut corrections = Vec::new();
    let entries: Vec<_> = ledger
        .entries()
        .iter()
        .map(|(&account, &entry)| (account, entry))
        .collect();
    for (account, entry) in entries {
        let current = processor.account(account)?;
        if entry.risk_state == current.risk_state {
            continue;
        }
        let correction = RiskEvent {
            account,
            epoch: current.applied_epoch,
            state_version: current.state_version,
            previous_state: entry.risk_state,
            current_state: current.risk_state,
            reason: RiskPolicy::reason(current.risk_state),
            collateral_value: current.collateral_value,
            debt_value: current.debt_value,
            maintenance_value: current.maintenance_value,
            health: current.health,
            minimum_margin_health: current.minimum_margin_health,
        };
        ledger.record(&correction);
        corrections.push(correction);
    }

    Ok(Some(Recovery {
        apply_loop: ApplyLoop::from_parts(processor, fold, ledger),
        corrections,
    }))
}
