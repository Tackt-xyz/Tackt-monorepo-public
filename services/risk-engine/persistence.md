# Cold-Start Persistence

Status: implemented in [`src/persistence/`](src/persistence/)

This document describes the durability layer that lets a restart reconstruct the engine
without re-querying external sources. It lives at the same boundary layer as ingest and
egress — `Fold`, `EgressLedger`, `Snapshot`, `SnapshotStore`, `ApplyLoop`, and `recover` —
and never executes on the processing hot path. The processing core stays synchronous,
in-memory, and free of filesystem work.

## Principle

The engine is deterministic over versioned inputs, so the persistence layer stores **inputs,
never derived state**: no compiled lanes, no aggregates, no health. Calculated health is a
pure function of (portfolio fold, prices, policy, registry) and is recomputed identically by
`RiskProcessor::new` at boot. The one state that is persisted but not an engine input is the
egress ledger — what downstream last heard — which exists so a restart emits only genuine
corrections instead of a million-event re-broadcast.

```text
NATS JetStream (prices, patches, sweeps)
        |
        v
  apply loop ── RiskProcessor (in-memory, hot path, untouched)
        |               |
        | accepted      | events
        v               v
      fold          egress publisher ──> downstream
        |               |
        |               v
        |          egress ledger (last published state per account)
        v
  snapshot writer ──> local NVMe (atomic, rotated)
```

## Persisted state

| Section | Content | Approx. size at 1M accounts |
| --- | --- | ---: |
| Portfolio fold | latest `AccountPortfolio` per account | 200–500 MB raw, ~100 MB zstd |
| Removal tombstones | version of the removing empty patch per removed account | bytes–KBs |
| Prices | last accepted `PriceUpdate` per price index | KBs |
| Stream cursors | JetStream stream sequence per consumed stream | bytes |
| Version high-water mark | max `StateVersion` across installed generations and patches | bytes |
| Generation version | `StateVersion` of the last installed generation | bytes |
| Processing | engine epoch counter | bytes |
| Egress ledger | last published `(risk_state, state_version)` per account | ~16 MB |

Explicitly not persisted: SoA lanes, domain slabs, account aggregates, health ratios, risk
states other than the egress ledger. Persisting derived state would create a second source of
truth that can diverge from what a fresh rebuild produces; the digest machinery cannot detect
that divergence because derived state carries no content digest.

## The fold

The apply loop maintains an in-memory fold beside the engine and mutates it **only after the
engine accepts the event**:

```rust
struct Fold {
    /// Latest complete portfolio per account. Removed on empty patch.
    accounts: BTreeMap<AccountIndex, AccountPortfolio>,
    /// Accounts removed by an applied empty patch, at the removing patch's
    /// version, so the per-account cursor survives a restart.
    removed: BTreeMap<AccountIndex, StateVersion>,
    /// Last accepted update per price index; each carries its own source
    /// sequence.
    prices: BTreeMap<PriceIndex, PriceUpdate>,
    /// JetStream stream sequence per consumed stream, advanced on ack.
    cursors: BTreeMap<String, u64>,
    /// Max StateVersion across accepted generations and patches.
    version_high_water_mark: StateVersion,
    /// Version of the last installed generation; accounts stamped past it
    /// are patches.
    generation_version: StateVersion,
    /// Engine epoch at the last accepted event.
    last_committed_epoch: ProcessingEpoch,
}
```

Fold rules:

- `AccountPatch` accepted (`EpochStatus::Applied`) → replace that account's entry (or, if the
  patch is empty, remove it and record a tombstone at the patch version); raise the
  high-water mark.
- `Portfolio` (reconciliation sweep) accepted → replace the entire `accounts` map with the
  sweep's records grouped per account; clear tombstones; set the high-water mark and the
  generation version to the sweep version.
- `Price` accepted → overwrite `prices[price_index]`.
- Rejected or duplicate events do not mutate the fold. The engine's accept/reject decision is
  deterministic, so the fold tracks committed engine state exactly.

Because a `PortfolioGeneration` is the concatenation of per-account record vectors, the fold
replays mechanically: the accounts still at `generation_version` concatenate into a base
generation stamped at that version, and every account stamped past it (including tombstones,
as empty patches) replays through the public patch path in stamp order. Content digests are
computed inside the engine at install; the persister never hashes portfolio content itself.

## Egress ledger

```rust
struct LedgerEntry {
    risk_state: RiskState,
    state_version: StateVersion,
}
// ledger: BTreeMap<AccountIndex, LedgerEntry> — sparse, keyed by account
```

Updated when the publisher acknowledges delivery of a `RiskEvent`. Persisted inside each
snapshot, not on every update: a ledger stale by up to one snapshot interval only causes
re-emission of transitions downstream already saw. Downstream consumers MUST treat events
idempotently (dedupe on `(account, state_version)`) **within a process lifetime**; this is
required for at-least-once JetStream delivery anyway and makes ledger staleness harmless.
`state_version` is a per-account, per-lifetime evaluation counter — it restarts with the
process, so it must never be compared across a restart. The recovery corrections (below) are
the resynchronization point: downstream accepts them unconditionally as the new baseline.

## Snapshot file

Written to a dedicated NVMe directory, rotated, last two retained:

```text
snapshots/
  snapshot-<ordinal>-<hwm>-<epoch>.bin  # newest; all numbers zero-padded to 20 digits
  snapshot-<ordinal'>-<hwm>-<epoch'>.bin # previous
  tmp/                                  # in-progress writes; cleared on open
```

The leading ordinal is assigned by the store — one past the newest retained file's — so
lexicographic order equals write order. Recency must never derive from snapshot content: the
engine epoch restarts every process lifetime and the high-water mark can stall across a
restart, so a content-derived name would sort post-restart snapshots as older, making
rotation delete the newest file and recovery resurrect the pre-crash one.

Layout (all fixed-width little-endian; body stored raw):

```text
magic "RISKSNAP"
engine epoch u64
stream cursors: count u32, (stream name, sequence u64)*
version high-water mark u64
generation version u64
prices: count u32, PriceUpdate*
accounts: count u32, (canonical per-account record sections)*
removed accounts: count u32, (account u32, version u64)*
egress ledger: count u32, (account u32, risk_state u8, state_version u64)*
trailer: SHA-256 of everything above
```

Write protocol: serialize to `tmp/`, fsync file, rename into `snapshots/`, fsync directory,
delete oldest. A torn write is unreachable by the rename; a corrupt file fails the trailer
digest at load and recovery falls back to the previous snapshot with a longer JetStream
replay.

Cadence is the embedding service's choice: every N minutes or M accepted events, whichever
first. `ApplyLoop::snapshot_view()` borrows a consistent image between events and
`SnapshotStore::write_view` streams it straight to the staged file with the SHA-256 trailer
computed on the fly, so a snapshot write clones no account state and allocates no full
in-memory image — the pause is the streaming write itself. (`ApplyLoop::snapshot()` still
materialises an owned `Snapshot` when one is needed in memory.) Never snapshot mid-event.

## Recovery sequence

1. Load the newest snapshot; verify the trailer digest, else fall back to the previous one.
2. Load the configured asset registry; require its identity to equal the snapshot's. A
   mismatch is an operator decision (registry migrations re-normalize inputs upstream), never
   a silent proceed.
3. Assemble the accounts still at the stored generation version into a base
   `PortfolioGeneration`, call
   `RiskProcessor::new(account_count, shard_count, policy, registry, generation)`, then
   replay every account stamped past the generation version — tombstones as empty patches —
   through `apply_account_patch` in stamp order. This reproduces each account's committed
   version cursor exactly: a late-delivered cross-stream patch, a patch older than a
   removal, or a sweep stamped at the newest patch version all meet the same verdict a
   never-restarted processor gives. (Stamping one flat generation at the high-water mark
   instead would flatten every cursor to the maximum and flip those verdicts.)
4. Seed prices by replaying the stored `PriceUpdate`s through `apply_price_update`, ordered
   per source by ascending `source_sequence` (cross-source order is irrelevant; cursors start
   at `None` and accept each source's first update). The engine is now fully opinionated —
   no `DataIncomplete` warm-up window. Events produced during seeding are not published
   directly; they flow into step 5's diff.
5. Resync egress: for every account with a ledger entry, compare `processor.account(index)`
   against the last-published state; publish a correction for each difference and update the
   ledger. Accounts downstream never heard about need no correction — there is no stale
   belief to fix, and the tail replay reports their transitions normally. Accounts whose
   state changed while the process was down (via deltas about to be replayed) are corrected
   again naturally in step 6.
6. Subscribe each JetStream stream from its stored cursor and replay the tail through the
   normal apply loop, publishing events normally. At-least-once redelivery across the
   snapshot boundary is absorbed by the engine's own idempotence: duplicate prices and
   patches return `EpochStatus::Duplicate` and do not mutate the fold.
7. Live. Steady-state and recovery share one code path from step 6 onward.

Expected wall clock at 1M accounts: snapshot read + verify ~2 s, generation compile and
rebuild ~1.6 s (measured), price seeding one sweep-equivalent (~seconds), ledger diff one
pass over account snapshots, plus JetStream tail proportional to downtime. Order of ten
seconds for a process restart.

## Failure matrix

| Failure | Recovery |
| --- | --- |
| Process crash / deploy | local snapshot + JetStream tail — seconds |
| Newest snapshot corrupt | previous snapshot + longer tail |
| Host loss | out of scope for NVMe tier; requires snapshot upload to object storage |
| JetStream retention exceeded | snapshot + genuine external re-sweep — the only path that ever queries addresses again |

## JetStream contract

> Status: implemented by the host service — the apply/ack loop lives in
> `src/host/engine.rs`, consumer positioning and tail replay in `src/host/ingress.rs`.

- Ack an event only after the engine returned `Ok` **and** the fold is updated. Crash between
  apply and ack → redelivery → engine reports duplicate → safe.
- Patches and sweeps share one `StateVersion` authority (see `src/ingest/mod.rs`); that
  authority's stamping must be durable upstream. The snapshot's high-water mark protects the
  engine locally: a stale reconciliation sweep is rejected after restart exactly as it would
  have been before, closing the pre-persistence gap where `max_account_version` died with the
  process.
- Engine-rejected events (stale, conflicting, cross-source) are acked and dropped after
  logging: redelivering them cannot change the deterministic verdict.
- `ProcessorPoisoned` is the exception: it reports the processor's health, not the event's
  validity, so an event rejected while poisoned must NOT be acked and dropped. Heal with
  `ApplyLoop::rebuild()` (or a fresh generation install), publish the returned transition
  events, then re-apply the rejected event.

## Crate API deltas

The processing core needed no changes beyond `install_portfolio_generation` taking the
generation by reference (so a rejected or duplicate sweep costs no copy): the engine hashes
content at install, price seeding uses `apply_price_update`, and the resync diff uses
`account()` snapshots. The persistence boundary added `ApplyLoop::rebuild()` (the heal path
for a poisoned processor), `ApplyLoop::snapshot_view()` / `Fold::snapshot_view()` (borrowed
encodable views), and `SnapshotStore::write_view` (streaming snapshot writes).

Still optional and unimplemented, only if downstream requires ordering continuity across
restarts: allow seeding `last_committed_epoch` and the per-account `state_version` counters
at construction (today both start at 0). The snapshot already persists the engine epoch
informationally. Until then, neither `epoch` nor `state_version` is comparable across a
restart — both are per-lifetime counters — and downstream must treat the recovery
corrections as an unconditional re-baseline rather than ordering them against pre-crash
events.

## Deviations from the original sketch

The implementation forced three adjustments, none structural:

- **No compression yet.** Snapshot bodies are raw canonical bytes; zstd would be the crate's
  only new dependency and is deferred behind the format version. The sizing estimates above
  therefore describe the uncompressed file today.
- **Prices persist as complete `PriceUpdate` values,** not bare prices: each carries its own
  source sequence, which is exactly what lets seeding
  replay them through the public `apply_price_update` path with normal cursor validation.
- **The correction rule is scoped to published accounts.** The sketch said "for every
  account"; the implementation diffs only accounts with a ledger entry, since an account
  downstream never heard about has no stale belief to correct.

## Invariants

- External sources are for liveness; local durability is for recovery. A restart replays.
  Only a new account or detected divergence queries the outside world.
- The fold mutates only on engine-accepted events; fold state equals committed engine input
  state at every ack boundary.
- Snapshots contain inputs and the egress ledger only; nothing derived.
- Every load path is digest-verified; a snapshot that cannot be verified is discarded, never
  partially used.
- Downstream event consumption is idempotent on `(account, state_version)` within a process
  lifetime; across a restart the recovery corrections re-baseline downstream unconditionally.
