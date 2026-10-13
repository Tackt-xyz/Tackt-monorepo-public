use std::collections::BTreeMap;
use std::path::PathBuf;

use risk_engine::Error;
use risk_engine::ingest::IngestEvent;
use risk_engine::persistence::{ApplyLoop, SnapshotStore, recover};
use risk_engine::processing::{
    AMOUNT_SCALE, AccountMode, AccountPortfolio, AccountSnapshot, AssetRegistry, FACTOR_SCALE,
    Factor, HyperliquidSnapshot, MarginDomain, MarginMode, PRICE_SCALE, PerpPosition,
    PortfolioGeneration, Price, PriceSourceKind, PriceUpdate, RiskEvent, RiskPolicy, RiskProcessor,
    RiskReason, RiskState, SignedAmount, Value,
};
use tackt_primitives::accounts::AccountSubject;
use uuid::Uuid;

fn subject(account: u32) -> AccountSubject {
    AccountSubject::new(Uuid::from_u128(u128::from(account) + 1)).expect("non-zero subject")
}

fn registry() -> AssetRegistry {
    AssetRegistry::bundled().expect("bundled registry")
}

fn policy() -> RiskPolicy {
    RiskPolicy::new(Factor::from_raw(1_200_000), Factor::from_raw(FACTOR_SCALE))
        .expect("valid policy")
}

fn test_directory(name: &str) -> PathBuf {
    let directory = std::env::temp_dir()
        .join("risk-engine-recovery-tests")
        .join(format!("{}-{name}", std::process::id()));
    if directory.exists() {
        std::fs::remove_dir_all(&directory).expect("clean test directory");
    }
    directory
}

fn update(price_index: u32, price: i128, sequence: u64) -> PriceUpdate {
    PriceUpdate {
        price_index,
        source: PriceSourceKind::Hyperliquid,
        source_sequence: sequence,
        price: Price::from_raw(price * PRICE_SCALE),
    }
}

fn domain(
    account: u32,
    index: u32,
    account_value: i128,
    maintenance: i128,
    source_version: u64,
) -> MarginDomain {
    MarginDomain {
        account,
        domain: index,
        account_mode: AccountMode::Unified,
        margin_mode: MarginMode::Cross,
        snapshot: Some(HyperliquidSnapshot {
            account_value: Value::from_raw(account_value * PRICE_SCALE),
            maintenance: Value::from_raw(maintenance * PRICE_SCALE),
            source_sequence: source_version,
        }),
        source_version,
    }
}

fn perp(account: u32, size: i128, anchor: i128, source_version: u64) -> PerpPosition {
    PerpPosition {
        account,
        domain: 1,
        market: 1,
        signed_size: SignedAmount::from_raw(size * AMOUNT_SCALE),
        anchor_price: Some(Price::from_raw(anchor * PRICE_SCALE)),
        position: account + 1,
        source_version,
    }
}

fn patch(
    account: u32,
    version: u64,
    domains: Vec<MarginDomain>,
    perps: Vec<PerpPosition>,
) -> AccountPortfolio {
    AccountPortfolio {
        account,
        account_subject: subject(account),
        version,
        ethereum_balances: Vec::new(),
        hypercore_balances: Vec::new(),
        margin_domains: domains,
        perp_positions: perps,
    }
}

fn base_generation(accounts: u32) -> PortfolioGeneration {
    let domains = (0..accounts)
        .map(|account| domain(account, 1, 100, 20, 1))
        .collect();
    let perps = (0..accounts)
        .map(|account| perp(account, if account % 2 == 0 { 1 } else { -1 }, 100, 1))
        .collect();
    PortfolioGeneration::new(1, Vec::new(), Vec::new(), domains, perps).with_account_subjects(
        (0..accounts)
            .map(|account| (account, subject(account)))
            .collect(),
    )
}

/// The deterministic financial state of an account; per-account applied epochs
/// and state versions are reconstruction bookkeeping and deliberately excluded.
fn comparable(snapshot: &AccountSnapshot) -> impl PartialEq + std::fmt::Debug + use<> {
    (
        snapshot.account,
        snapshot.collateral_value,
        snapshot.debt_value,
        snapshot.maintenance_value,
        snapshot.health,
        snapshot.minimum_margin_health,
        snapshot.risk_state,
        snapshot.uncertain_inputs,
    )
}

/// Transition essence for cross-lifetime comparison. The durable epoch is
/// included because it also identifies the public recommendation occurrence;
/// only per-account state-version counters are reconstruction-local.
fn essence(event: &RiskEvent) -> impl PartialEq + std::fmt::Debug {
    (
        event.account,
        event.epoch,
        event.previous_state,
        event.current_state,
        event.reason,
        event.collateral_value,
        event.debt_value,
        event.maintenance_value,
        event.health,
        event.minimum_margin_health,
    )
}

fn prefix_events() -> Vec<IngestEvent> {
    vec![
        IngestEvent::Price(update(3, 110, 1)),
        IngestEvent::AccountPatch(patch(
            1,
            2,
            vec![domain(1, 1, 60, 25, 2)],
            vec![perp(1, 2, 110, 2)],
        )),
        IngestEvent::Price(update(3, 80, 2)),
    ]
}

fn tail_events() -> Vec<IngestEvent> {
    vec![
        IngestEvent::Price(update(3, 105, 3)),
        IngestEvent::AccountPatch(patch(2, 3, Vec::new(), Vec::new())),
        IngestEvent::Price(update(3, 45, 4)),
    ]
}

#[test]
fn full_cycle_recovery_matches_an_uninterrupted_reference() {
    let accounts = 4;
    let store = SnapshotStore::open(test_directory("full-cycle")).expect("store");

    // Reference: one processor that never restarts, fed the identical event sequence.
    let mut reference = RiskProcessor::new(accounts, 2, policy(), registry(), &base_generation(4))
        .expect("reference");
    let mut reference_tail_events = Vec::new();
    for event in prefix_events() {
        match event {
            IngestEvent::Price(update) => {
                reference
                    .apply_price_update(update)
                    .expect("reference price");
            }
            IngestEvent::AccountPatch(patch) => {
                reference
                    .apply_account_patch(&patch)
                    .expect("reference patch");
            }
            IngestEvent::Portfolio(generation) => {
                reference
                    .install_portfolio_generation(&generation)
                    .expect("reference install");
            }
        }
    }
    for event in tail_events() {
        match event {
            IngestEvent::Price(update) => reference_tail_events.extend(
                reference
                    .apply_price_update(update)
                    .expect("reference price")
                    .events,
            ),
            IngestEvent::AccountPatch(patch) => reference_tail_events.extend(
                reference
                    .apply_account_patch(&patch)
                    .expect("reference patch")
                    .events,
            ),
            IngestEvent::Portfolio(generation) => reference_tail_events.extend(
                reference
                    .install_portfolio_generation(&generation)
                    .expect("reference install")
                    .events,
            ),
        }
    }

    // Persistent run: apply the prefix, publish every event, snapshot, and crash.
    let mut live =
        ApplyLoop::boot(accounts, 2, policy(), registry(), &base_generation(4)).expect("boot");
    for (sequence, event) in prefix_events().into_iter().enumerate() {
        let events = live.apply(event).expect("live apply");
        for event in &events {
            live.record_published(event);
        }
        live.acknowledge("events", sequence as u64 + 1);
    }
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    // Recover from disk. Everything published was reproduced, so no corrections.
    let recovery = recover(&store, accounts, 2, policy(), registry())
        .expect("recover")
        .expect("snapshot exists");
    assert!(recovery.corrections.is_empty());
    assert_eq!(
        recovery.apply_loop.fold().cursors(),
        &BTreeMap::from([("events".to_owned(), 3)])
    );
    let mut recovered = recovery.apply_loop;

    // At-least-once redelivery across the snapshot boundary: the last acknowledged event
    // arrives again and must be a silent duplicate.
    let redelivered = prefix_events().pop().expect("prefix has events");
    assert!(recovered.apply(redelivered).expect("duplicate").is_empty());

    // Replay the tail; the emitted transitions must match the uninterrupted reference.
    let mut recovered_tail_events = Vec::new();
    for event in tail_events() {
        recovered_tail_events.extend(recovered.apply(event).expect("tail apply"));
    }
    assert_eq!(
        recovered_tail_events
            .iter()
            .map(essence)
            .collect::<Vec<_>>(),
        reference_tail_events
            .iter()
            .map(essence)
            .collect::<Vec<_>>(),
    );

    // Final account state must match the reference exactly.
    for account in 0..4 {
        assert_eq!(
            comparable(&recovered.processor().account(account).expect("recovered")),
            comparable(&reference.account(account).expect("reference")),
            "account {account} diverged after recovery"
        );
        assert_eq!(
            recovered
                .processor()
                .margin_domain(account, 1)
                .expect("recovered"),
            reference.margin_domain(account, 1).expect("reference"),
        );
    }
}

#[test]
fn unpublished_transitions_surface_as_corrections() {
    let store = SnapshotStore::open(test_directory("corrections")).expect("store");
    let generation = PortfolioGeneration::new(
        1,
        Vec::new(),
        Vec::new(),
        vec![domain(0, 1, 100, 50, 1)],
        vec![perp(0, 1, 100, 1)],
    )
    .with_account_subjects(BTreeMap::from([(0, subject(0))]));
    let mut live = ApplyLoop::boot(1, 1, policy(), registry(), &generation).expect("boot");

    // The first price resolves boot-time uncertainty: DataIncomplete -> Healthy.
    let first_events = live
        .apply(IngestEvent::Price(update(3, 100, 1)))
        .expect("anchor price");
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].previous_state, RiskState::DataIncomplete);
    assert_eq!(first_events[0].current_state, RiskState::Healthy);
    live.record_published(&first_events[0]);

    // Establish a published belief downstream: the crash drives the account liquidatable.
    let crash_events = live
        .apply(IngestEvent::Price(update(3, 45, 2)))
        .expect("crash");
    assert_eq!(crash_events.len(), 1);
    assert_eq!(crash_events[0].current_state, RiskState::Liquidatable);
    live.record_published(&crash_events[0]);

    // The account recovers, but the process dies between applying and publishing.
    let recovered_events = live
        .apply(IngestEvent::Price(update(3, 100, 3)))
        .expect("recovery");
    assert_eq!(recovered_events.len(), 1);
    assert_eq!(recovered_events[0].current_state, RiskState::Healthy);
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    let recovery = recover(&store, 1, 1, policy(), registry())
        .expect("recover")
        .expect("snapshot exists");
    assert_eq!(recovery.corrections.len(), 1);
    let correction = &recovery.corrections[0];
    assert_eq!(correction.account, 0);
    assert_eq!(correction.previous_state, RiskState::Liquidatable);
    assert_eq!(correction.current_state, RiskState::Healthy);
    assert_eq!(correction.reason, RiskReason::Recovered);
    assert_eq!(
        recovery
            .apply_loop
            .ledger()
            .entry(0)
            .expect("ledger updated")
            .risk_state,
        RiskState::Healthy
    );
}

#[test]
fn recovery_restores_the_stale_sweep_guard() {
    let store = SnapshotStore::open(test_directory("stale-sweep")).expect("store");
    let mut live = ApplyLoop::boot(1, 1, policy(), registry(), &base_generation(1)).expect("boot");
    live.apply(IngestEvent::AccountPatch(patch(
        0,
        5,
        vec![domain(0, 1, 80, 20, 2)],
        Vec::new(),
    )))
    .expect("patch");
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    let mut recovered = recover(&store, 1, 1, policy(), registry())
        .expect("recover")
        .expect("snapshot exists")
        .apply_loop;

    // A reconciliation sweep older than the pre-crash patch must still be rejected, with
    // the same verdict a never-restarted processor gives: the replayed patch restored
    // `max_account_version`, so the sweep is stale against the newest patch.
    let stale = PortfolioGeneration::new(3, Vec::new(), Vec::new(), Vec::new(), Vec::new());
    assert!(matches!(
        recovered.apply(IngestEvent::Portfolio(stale)),
        Err(Error::StaleReconciliationSweep {
            received: 3,
            newest_patch: 5
        })
    ));

    // A genuinely newer sweep still installs.
    let fresh = PortfolioGeneration::new(6, Vec::new(), Vec::new(), Vec::new(), Vec::new());
    recovered
        .apply(IngestEvent::Portfolio(fresh))
        .expect("fresh sweep");
}

#[test]
fn late_cross_stream_patch_is_accepted_after_recovery() {
    let store = SnapshotStore::open(test_directory("late-patch")).expect("store");

    // The authority stamped account 0's patch v2 before account 1's v3, but sharded
    // delivery got v3 to the engine first; the crash hits before v2 arrives.
    let early = patch(1, 3, vec![domain(1, 1, 70, 30, 2)], Vec::new());
    let late = patch(0, 2, vec![domain(0, 1, 90, 10, 2)], Vec::new());

    let mut reference =
        RiskProcessor::new(2, 1, policy(), registry(), &base_generation(2)).expect("reference");
    reference.apply_account_patch(&early).expect("early");

    let mut live = ApplyLoop::boot(2, 1, policy(), registry(), &base_generation(2)).expect("boot");
    live.apply(IngestEvent::AccountPatch(early)).expect("early");
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    let mut recovered = recover(&store, 2, 1, policy(), registry())
        .expect("recover")
        .expect("snapshot exists")
        .apply_loop;

    // The late patch is older than the high-water mark but newer than account 0's own
    // committed version; recovery must preserve that distinction, not flatten every
    // account's cursor to the high-water mark.
    reference
        .apply_account_patch(&late)
        .expect("reference late");
    recovered
        .apply(IngestEvent::AccountPatch(late))
        .expect("recovered late");

    for account in 0..2 {
        assert_eq!(
            comparable(&recovered.processor().account(account).expect("recovered")),
            comparable(&reference.account(account).expect("reference")),
            "account {account} diverged after recovery"
        );
    }
}

#[test]
fn same_version_sweep_installs_after_recovery() {
    let store = SnapshotStore::open(test_directory("same-version-sweep")).expect("store");
    let patched = patch(0, 5, vec![domain(0, 1, 80, 20, 2)], Vec::new());

    let mut reference =
        RiskProcessor::new(2, 1, policy(), registry(), &base_generation(2)).expect("reference");
    reference.apply_account_patch(&patched).expect("patch");

    let mut live = ApplyLoop::boot(2, 1, policy(), registry(), &base_generation(2)).expect("boot");
    live.apply(IngestEvent::AccountPatch(patched))
        .expect("patch");
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    let mut recovered = recover(&store, 2, 1, policy(), registry())
        .expect("recover")
        .expect("snapshot exists")
        .apply_loop;

    // A reconciliation sweep stamped exactly at the newest patch version is legal and must
    // install after recovery exactly as it does on the uninterrupted reference — the
    // recovered generation keeps its original version instead of absorbing the patch's.
    let sweep = || {
        PortfolioGeneration::new(
            5,
            Vec::new(),
            Vec::new(),
            vec![domain(0, 1, 80, 20, 3), domain(1, 1, 120, 30, 3)],
            Vec::new(),
        )
        .with_account_subjects(BTreeMap::from([(0, subject(0)), (1, subject(1))]))
    };
    reference
        .install_portfolio_generation(&sweep())
        .expect("reference sweep");
    recovered
        .apply(IngestEvent::Portfolio(sweep()))
        .expect("recovered sweep");

    for account in 0..2 {
        assert_eq!(
            comparable(&recovered.processor().account(account).expect("recovered")),
            comparable(&reference.account(account).expect("reference")),
            "account {account} diverged after recovery"
        );
    }
}

#[test]
fn patch_older_than_a_removal_stays_rejected_after_recovery() {
    let store = SnapshotStore::open(test_directory("removal-tombstone")).expect("store");
    let removal = patch(1, 4, Vec::new(), Vec::new());

    let mut reference =
        RiskProcessor::new(2, 1, policy(), registry(), &base_generation(2)).expect("reference");
    reference.apply_account_patch(&removal).expect("removal");

    let mut live = ApplyLoop::boot(2, 1, policy(), registry(), &base_generation(2)).expect("boot");
    live.apply(IngestEvent::AccountPatch(removal))
        .expect("removal");
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    let mut recovered = recover(&store, 2, 1, policy(), registry())
        .expect("recover")
        .expect("snapshot exists")
        .apply_loop;

    // A patch stamped before the removal must stay rejected: the tombstone preserved the
    // removed account's version cursor across the restart.
    let stale = patch(1, 3, vec![domain(1, 1, 70, 30, 2)], Vec::new());
    assert!(matches!(
        reference.apply_account_patch(&stale),
        Err(Error::StaleAccountPatch {
            account: 1,
            received: 3,
            committed: 4
        })
    ));
    assert!(matches!(
        recovered.apply(IngestEvent::AccountPatch(stale)),
        Err(Error::StaleAccountPatch {
            account: 1,
            received: 3,
            committed: 4
        })
    ));

    // A patch stamped past the removal re-creates the account on both.
    let fresh = patch(1, 5, vec![domain(1, 1, 90, 10, 3)], Vec::new());
    reference
        .apply_account_patch(&fresh)
        .expect("reference fresh");
    recovered
        .apply(IngestEvent::AccountPatch(fresh))
        .expect("recovered fresh");
    for account in 0..2 {
        assert_eq!(
            comparable(&recovered.processor().account(account).expect("recovered")),
            comparable(&reference.account(account).expect("reference")),
            "account {account} diverged after recovery"
        );
    }
}

#[test]
fn snapshot_after_recovery_supersedes_the_pre_crash_snapshot() {
    let store = SnapshotStore::open(test_directory("second-crash")).expect("store");
    let mut reference =
        RiskProcessor::new(1, 1, policy(), registry(), &base_generation(1)).expect("reference");

    // First lifetime: three accepted prices leave a high engine epoch behind.
    let mut live = ApplyLoop::boot(1, 1, policy(), registry(), &base_generation(1)).expect("boot");
    for (sequence, price) in [(1, 110), (2, 90), (3, 105)] {
        reference
            .apply_price_update(update(3, price, sequence))
            .expect("reference price");
        live.apply(IngestEvent::Price(update(3, price, sequence)))
            .expect("live price");
        live.acknowledge("prices", sequence);
    }
    store.write(&live.snapshot()).expect("first snapshot");
    drop(live);

    // Second lifetime: epochs restart below the first lifetime's, and the high-water mark
    // has not moved; the new snapshot must still supersede the pre-crash file.
    let mut recovered = recover(&store, 1, 1, policy(), registry())
        .expect("first recover")
        .expect("snapshot exists")
        .apply_loop;
    reference
        .apply_price_update(update(3, 45, 4))
        .expect("reference price");
    recovered
        .apply(IngestEvent::Price(update(3, 45, 4)))
        .expect("recovered price");
    recovered.acknowledge("prices", 4);
    store.write(&recovered.snapshot()).expect("second snapshot");
    drop(recovered);

    let recovery = recover(&store, 1, 1, policy(), registry())
        .expect("second recover")
        .expect("snapshot exists");
    assert_eq!(
        recovery.apply_loop.fold().cursors(),
        &BTreeMap::from([("prices".to_owned(), 4)])
    );
    assert_eq!(
        comparable(
            &recovery
                .apply_loop
                .processor()
                .account(0)
                .expect("recovered")
        ),
        comparable(&reference.account(0).expect("reference")),
    );
}

#[test]
fn empty_directory_recovers_to_none_and_snapshots_restore_prices() {
    let store = SnapshotStore::open(test_directory("edge-cases")).expect("store");
    assert!(
        recover(&store, 1, 1, policy(), registry())
            .expect("recover")
            .is_none()
    );

    let mut live = ApplyLoop::boot(1, 1, policy(), registry(), &base_generation(1)).expect("boot");
    live.apply(IngestEvent::Price(update(3, 100, 1)))
        .expect("price");
    store.write(&live.snapshot()).expect("snapshot");
    drop(live);

    let recovery = recover(&store, 1, 1, policy(), registry())
        .expect("recover")
        .expect("snapshot exists");
    let account = recovery.apply_loop.processor().account(0).expect("account");
    assert_eq!(account.risk_state, RiskState::Healthy);
    assert_eq!(account.uncertain_inputs, 0);
}
