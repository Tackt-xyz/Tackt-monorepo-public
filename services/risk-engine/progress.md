# Compatibility cleanup

- Application schema, registry, and policy versions and handshakes have been removed.
- One current credit policy replaces historical policy constructors. API paths retain `/v1`.
- Runtime revisions, source sequences, account conflict checks, typed validation, and snapshot
  checksums remain. PostgreSQL migration 0011 drops compatibility metadata columns.

# Progress

## Implemented

- Root registry stores identity, accounting support, and collateral settings
  on each canonical asset. Core and Risk use the same asset/chain lists.
  HyperCore entries are explicitly excluded from Ledger; the original accounting representations,
  streamed asset indices, factors, and margin tiers are preserved. Runtime path overrides are rejected.

- Demo history stores only exact numeric health, classification, account, capture time, and retry
  identity. Migration 0010 creates the numeric schema directly, without JSONB, breakdowns,
  thresholds, or version metadata. Null health distinguishes incomplete/unbounded results by classification.

- Opt-in streamed account/margin health history uses a bounded nonblocking scalar handoff at
  completed processing boundaries and a separate PostgreSQL writer/pool. Core exposes scoped
  latest/history reads. Incomplete/unbounded health is null, finite ratios are exact,
  and sampling never claims source freshness. This does not integrate the separate credit
  evaluator. See `architecture/risk-health-history.md` at the workspace root.

- The credit policy explicitly retains its reviewed representation set as the shared registry
  adds SOL/ETH/BTC instruments. Newly registered wrappers and yield tokens remain unconfigured
  for collateral; family membership alone cannot supply a conversion rate or collateral approval.

- The host loads user-owned Ledger balance inputs from PostgreSQL before serving, then refreshes
  the entire snapshot every five seconds by default. Reads use a consistent read-only transaction
  through `tackt-postgres`. Failed refreshes retain the prior inputs and fail readiness until
  recovery. The input loader adds no credit evaluation endpoint or NATS snapshot flow.

- The current policy counts controlled pending deposits for borrowing and liquidation at the same
  asset rates as available balances, explicitly accepting reorg exposure. Finalization counts funds once; a Ledger reversal reduces capacity on the
  next evaluation while retaining debt. Custody, freshness, price, and quantity checks still apply.

- Credit eligibility now runs inside `evaluate_credit` over authoritative Ledger buckets and
  custody facts. It selects pending deposits by supplied policy, excludes non-collateral balances, avoids counting shared margin as
  spot, pools exact registry atomic quantities before rounding, and explains exclusions on demand.
  Missing custody evidence and stale balance snapshots fail closed. The current policy retains
  controlled reservations for liquidation only. Controlled
  borrowed proceeds count with debt once. These two treatments are draft policy
  assumptions for review. The evaluator still stores no derived state; source coherence and
  proving custody remain the input authority's responsibility.

- Portfolio credit now includes pledged perps within the same account evaluator as free spot.
  Shared margin equity is counted once; uniquely identified positions reference its source
  version and consume 10% opening / 5% maintenance reserves on absolute current notional.
  Signed losses reduce portfolio limits, missing/stale pool evidence fails closed, and
  the current policy supplies spot rates and perp reserves. Inputs, policy, and evaluation now live under `processing/credit/`. The market
  snapshot replaces the price-only name, and spot collateral is explicitly separate from pools.

- Stateless `processing::credit::evaluate_credit` calculates account borrowing capacity,
  liquidation capacity, headroom, shortfall, and debt-based health from pooled economic-asset
  quantities and one account debt total. The spot rates are: ETH/BTC/SOL
  at 85% maximum LTV and 90% liquidation threshold; unconfigured assets contribute zero.
  Pending undrawn commitments consume capacity separately from liquidation debt. Explicit
  price times enforce freshness without reading a clock; incomplete evidence returns no
  partial measurements. This is a library API; host/HTTP/NATS integration remains pending.

- The registry covers chains, venues, price factors, standalone assets, collateral factors,
  and Hyperliquid perp markets.
- Only Ethereum and Hyperliquid price sources are representable.
- Ethereum and HyperCore assets with the same symbol retain separate identities and prices.
- HyperEVM is rejected.
- Immutable portfolio generations replace raw exposure generations.
- Ethereum finality, HyperCore account-value inclusion, Hyperliquid snapshots, signed perp
  positions, anchor prices, source versions, account modes, and margin domains are represented.
- Standalone balance, perp PnL, and maintenance sensitivity compile into separate SoA lanes.
- Ethereum and Hyperliquid sequence cursors advance independently.
- Portfolio and price inputs resolve against the embedded registry.
- Accepted updates receive deterministic internal processing epochs.
- Stale, conflicting, cross-source, invalid-policy, and out-of-range updates are rejected before
  mutation.
- Positive Hyperliquid account value counts in full; negative value becomes liability.
- Finalized Ethereum collateral is accepted directly into overall account value without modeling
  a move to Hyperliquid.
- Account health pools Ethereum, standalone HyperCore, and Hyperliquid value against total
  Hyperliquid maintenance.
- Domain health remains diagnostic and no longer independently overrides a healthy overall
  account.
- Missing prices, snapshots, anchors, and margin inputs result in `DataIncomplete`.
- The registry stores per-market maintenance-margin tiers; marginal rates and continuity
  deductions are derived at load.
- Perp lanes use each position's average maintenance rate at its anchor, so extrapolated
  maintenance is conservative on drops and never negative.
- Account patches install one account's portfolio alone: spliced lanes, stable domain slots,
  scoped aggregate recomputation, per-account version/digest cursors, shared epoch order, and
  events for the transitions they cause.
- Generation and account digests use a canonical binary encoding; content identity is
  order-insensitive and per-account.
- Same-version install comparison uses the generation digest alone, so reordered-but-identical
  content is a duplicate, never a conflict.
- Account patches carry the same tracing span and epoch/state-change metrics as price updates.
- Reconciliation sweeps must be stamped at least as new as every patch they subsume.
- Snapshot replacement discards prior local deltas and rebuilds from new anchors.
- Generation installs and rebuilds return the risk-state transitions they cause.
- HyperCore balances marked as included in account value require a margin domain.
- A failed apply phase poisons the processor until a rebuild or install succeeds.
- Changed domains and parent accounts are grouped and evaluated once.
- One-worker and eight-worker outputs are identical.
- Cold-start persistence: a fold of committed inputs and an egress ledger snapshot to local
  disk in a canonical binary format with a SHA-256 trailer; writes are atomic and rotated
  with the previous snapshot retained as the corruption fallback.
- The fold mutates only on engine-accepted events; duplicates and rejections leave it
  untouched, and an applied empty patch removes the account, leaving a tombstone at the
  patch version so the account's cursor survives restarts.
- Recovery rebuilds a fully opinionated processor from the newest verifiable snapshot:
  the base generation installs at its original version and accounts patched past it replay
  through the public patch path in stamp order, so per-account version cursors — and the
  stale-patch and stale-reconciliation-sweep guards — match a never-restarted processor;
  stored prices seed through the public update path, and corrections are emitted only where
  recomputed state disagrees with the last published state.
- Snapshot files are named by a store-assigned write ordinal, never by content, so recency
  survives the per-lifetime epoch reset and rotation can never delete the newest snapshot.
- Snapshots persist inputs and the ledger only — never lanes, aggregates, or health.
- Authenticated `POST /internal/v1/risk/evaluations` requests evaluate a complete
  projected account without mutating, publishing, or persisting engine state. Stable subjects
  bind to private dense indexes, all relevant input identities are pinned, and stale,
  unsupported, malformed, or incomplete input fails closed (`DataIncomplete` is explicit).
- Synchronous evaluation shares the deterministic fixed-point engine and returns measurements,
  uncertainty, observed versions and price cursors, a canonical input digest, and a deterministic
  evidence ID. It deliberately carries neither an approval field nor a signature.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- 43 unit tests
- 31 integration tests
- release benchmark with 1,000,000 mixed affected records, including full-generation install
  latency

Reference mixed-workload result on an AMD Ryzen 9 3900:

- P50: 72.263 ms
- P95: 88.489 ms
- P99: 93.048 ms
- max: 103.532 ms
- throughput at P99: 10.75 million exposures/second
- average CPU: 5.51 logical cores
- measured allocations: 3,606 calls / 1,120,201,120 bytes across 200 updates
- RSS: 526,340 KiB

The measured P99 was below the sub-100 ms target when recorded (2026-07-27); the newest
dated report (`benchmarks/2026-07-28-account-patches.md`) measured price-update P99 at
100.418 ms under different conditions — re-benchmark the current commit before citing
either number.

## Manual stream provisioning (2026-09-07)

- Host startup checks all four required streams before serving HTTP; provisioning is an explicit
  deployment step. Snapshot-positioned consumer replay and the processing path are unchanged.
- The host integration fixture now provisions its isolated streams before launching the service.

## Parallel preflight and rebuild; streaming snapshots (2026-08-05)

- The preflight validation phase of a price update now runs shard-parallel inside the
  worker pool (it was the last serial full-scan on the update path); error selection is
  collected in shard order so failing epochs stay deterministic.
- `rebuild_stores` (boot, generation install, `rebuild()`) is shard-parallel; rebuild
  events are merged and re-sorted by account, so results are bit-identical to the serial
  path.
- Snapshots stream from borrowed state: `ApplyLoop::snapshot_view()` +
  `SnapshotStore::write_view` encode straight to the staged file with the SHA-256 trailer
  computed on the fly — no fold clone, no full in-memory image.
- `ApplyLoop::rebuild()` exposes the heal path for a poisoned processor; `ProcessorPoisoned`
  is documented as transient and must not be acked-and-dropped.
- `install_portfolio_generation` takes the generation by reference; the apply loop groups
  the fold's account map only after a successful install, by consuming the generation.
- The snapshot store validates the full store-written filename shape: foreign files never
  influence ordinals, recovery order, or rotation, and writes refuse to overwrite an
  existing snapshot path.
- The host service (`src/main.rs` + `src/host/`) runs the engine over NATS JetStream:
  cursor-positioned durable consumers for prices/patches/sweeps, a dedicated engine thread
  enforcing publish → record → cursor → input-ack per event, poisoned-processor rebuild and
  in-place re-apply, interval/count snapshot cadence with a final snapshot on graceful
  drain, recovery corrections published before consumers attach, and `/health`, `/ready`,
  `/metrics` endpoints with the engine's counters exported via Prometheus.

### 2026-09-17 — Account-local venue margin diagnostics

- Added allocation-free `venue_margin` and `margin_domains` reads over existing account slots.
  Hyperliquid equity/maintenance is separate from pooled account health and Tackt credit health;
  the worst pool determines venue state, preserving cross/isolated/HIP-3 boundaries.
- Missing snapshots stay visible as incomplete, missing spot prices do not taint venue state,
  and poisoned processors reject reads. Ledger booked margin cannot replace live pool evidence.
- Added isolation, uncertainty, zero-maintenance, removal, and full-install oracle coverage.
  The benchmark measures query cost/allocations separately from the unchanged update path.


### 2026-09-17 — Native standard Hyperliquid margin and gross leverage

- Replaced anchored average-rate maintenance extrapolation with current-tier band calculations
  using exact leverage divisors. Anchor-relative deltas preserve replay/rebuild equality even
  for fractional prices and tier crossings; the current mark and authoritative source snapshot
  remain required inputs.
- Venue reads expose the worst independent pool's maintenance/equity ratio and inverse health;
  standard liquidation uses equity < maintenance independently of Tackt thresholds. Unified
  mode is explicitly rejected until its different collateral-sharing calculation is supported.
- Added gross marked position notional and gross-notional/total-equity venue leverage. Longs
  and shorts both contribute; isolated pools cannot conceal each other's liquidation risk.
- Added formula, precision, tier-crossing, fractional replay, scope, and leverage regression
  coverage. Same-machine baseline/candidate performance is recorded in the native-margin report.
