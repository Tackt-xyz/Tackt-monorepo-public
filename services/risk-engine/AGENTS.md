# risk-engine

Deterministic, in-memory risk mirror for Ethereum ERC-20 balances, HyperCore HIP-1 spot
balances, and Hyperliquid perp positions. It consumes normalized inputs with runtime revisions and emits
risk-state transitions (`Healthy` / `Watch` / `Liquidatable` / `DataIncomplete`). Canonical
authorization and liquidation live outside this crate. Performance target: revalue 1,000,000
affected records under 100 ms at P99; single-account patches in single-digit milliseconds.

## Commands

- `make check` — fmt check, clippy (`pedantic` + `all` at deny, warnings deny), all tests.
  Run before every commit; CI and reviews assume it is green.
- `cargo run --release --bin risk-benchmark` — 1M-record benchmark (price updates,
  full-generation installs, account patches). Env overrides use the `RISK_BENCH_` prefix
  (`ACCOUNTS`, `WORKERS`, `MEASURED_ITERATIONS`, `INSTALL_ITERATIONS`, `PATCH_ITERATIONS`,
  `NEAR_THRESHOLD_PERCENT`). Dated reports live in `benchmarks/`.
- `cargo run --release --bin risk-engine` — the NATS/JetStream host service. Env overrides
  use the `RISK_ENGINE_` prefix; `RISK_ENGINE_SNAPSHOT_DIR` is required (see README).
- Toolchain: Rust 1.91, workspace edition 2024, `unsafe_code = "forbid"`.

## Layout

- `src/processing/` — the implemented core:
  - `engine.rs` — `RiskProcessor`: price updates (3 phases: calculate → preflight → apply,
    parallel over shards), `apply_account_patch` (validate → stage → infallible commit),
    generation installs/rebuilds, `DomainShard` slab, poison handling.
  - `portfolio.rs` — `PortfolioIndex`: whole-generation compile into price×shard SoA lanes;
    per-account compile (`compile_account`) and lane splicing for patches; position
    ownership and per-account digest bookkeeping.
  - `state.rs` — sharded `AccountStore` aggregates (collateral/debt/maintenance/uncertainty).
  - `arithmetic.rs` — all checked fixed-point math, `I256` intermediates, tiered maintenance.
  - `digest.rs` — canonical binary content digests, per account and per generation.
  - `asset_registry.rs` — root `asset-registry.json` through primitives: chains, venues, price
    factors, assets, perp markets with margin tiers (rates + continuity deductions derived
    at load).
  - `policy.rs`, `types.rs` — thresholds; input/output types and fixed-point newtypes.
  - `credit/` — stateless portfolio credit evaluation, eligibility, policy, and input types. Ledger balances,
    pledged perp pools, and positions feed one signed capacity calculation against account debt.
- `src/ingest/`, `src/egress/` — trait boundaries for the processing core; `egress/history.rs`
  is the IO-free display-history sampler the host drives between applies.
- `src/host/ledger.rs` — PostgreSQL credit input load and periodic full refresh through
  `tackt-postgres`; failed reads block readiness while retaining the last successful snapshot.
- `src/main.rs` + `src/host/` — the service binary: JetStream consumers → bounded channel →
  a dedicated engine thread owning the `ApplyLoop` (publish → record → cursor → ack per
  event), snapshot cadence, egress publishing, `/health`/`/ready`/`/metrics`. All network
  and service disk IO lives here; no network/DB IO exists anywhere in the processing path,
  where the registry is embedded through `tackt-primitives` and no runtime file override is accepted.
- `init.md` — the spec and invariant list. `progress.md` — implemented-feature log.
  `rust-standard.md` — coding standard this repo is reviewed against.

## Invariants (violating any of these is a bug, not a style choice)

- Determinism: identical inputs → bit-identical state and events; 1 worker ≡ 8 workers.
  Patches and price updates share one processing-epoch counter — one total replay order.
- No floats in financial code. Fixed point: `Amount`/`SignedAmount` 1e9, `Price`/`Value`
  1e8, `Factor` 1e6, checked arithmetic with `i128`/`I256`, conservative rounding
  (collateral down, liabilities up).
- Validation completes before mutation. Patch commits are infallible writes of staged
  values; a mid-commit failure poisons the processor (`is_poisoned()`), healed only by a
  successful rebuild or install.
- Registry facts come from the embedded primitives registry. Ethereum prices never value Hyperliquid records and vice versa.
- Per-account patches and whole generations share one `StateVersion` space stamped by a
  single monotonic authority (see `IngestEvent` docs). A reconciliation sweep must be
  stamped ≥ the newest patch or it is rejected (`StaleReconciliationSweep`).
- Position ids are globally unique across accounts; a patch cannot claim another account's.
- Hyperliquid processor health = pooled `(collateral − debt) / total Hyperliquid maintenance`;
  per-domain health is diagnostic only. Perp maintenance changes are the difference between
  exact tiered requirements at current and anchor marks, using each tier's maximum leverage
  directly. Incremental changes subtract anchor-relative values; rounding must not accumulate
  with the price path. Native standard venue health preserves independent cross/isolated pools.
- Credit health = `(spot liquidation contributions + pool equity − perp maintenance reserves)
  / account debt`. Equity belongs to a pool once; positions reference that pool with matching
  source versions. Spot eligibility follows Ledger buckets and verified custody; exact atomic
  quantities pool by economic asset before rounding. Pool balances never count as free spot.
  Controlled reservations contribute to liquidation only. Controlled pending deposits count
  for both limits, accepting reorg exposure. Missing required custody evidence and stale balance
  snapshots fail closed. Reserves use absolute current notional per position and round up;
  negative contributions must not be floored before aggregation.
  Pending undrawn commitments reduce available credit, never liquidation health. Classify
  using direct capacity/debt comparisons; missing required price evidence returns no measurements.
- Missing price/snapshot/anchor inputs count as uncertainty → `DataIncomplete`; uncertainty
  increments and decrements must balance exactly across builds, patches, and first price
  arrivals.

## Sharp edges (learned the hard way — keep the guards)

- `rebuild()` must renumber lane `domain_offsets` to dense numbering before rebuilding
  stores (patched lanes carry free-list slab slots). Regression test:
  `rebuild_after_patches_keeps_domain_routing_intact`.
- `PortfolioIndex.generation_digest` is the sole content authority in the manual
  `PartialEq` used by same-version install conflict detection — comparing lanes instead
  would flag reordered-but-identical content as a conflict (lane tie order for equal sort
  keys follows input order) and miss records that compile to no lanes. Tests:
  `same_version_generations_with_lane_invisible_differences_conflict`,
  `same_version_reorder_of_equal_sort_key_records_is_a_duplicate`.
- Perp lanes sort by *external* domain index, not slot id — record order must be a pure
  function of content or patch insertions diverge from full builds.
- The golden oracle test (`patches_match_full_install_of_identical_content`) is the
  load-bearing correctness check for any patch-path change: state after interleaved
  patches/prices must equal a fresh full install of the final content. Extend it rather
  than reasoning informally.
- The pre-apply rollback saves must stay inside the parallel workers (a serial pass over
  1M accounts costs ~15 ms per price update).
- Benchmark numbers are machine-sensitive; when checking for regressions, benchmark the
  baseline commit under current conditions (worktree checkout) rather than comparing
  against numbers from an old report.

## Conventions

- Follow `rust-standard.md` (doc comments on all public types and fields, thiserror,
  `expect("reason")`, enums over strings, typed indexes over bare primitives, metrics names
  as constants).
- All mutation goes through `&mut self` on `RiskProcessor` — no locks; concurrency comes
  from disjoint shard partitioning inside one update. Do not add interior mutability. The
  host realizes the single-writer rule with one dedicated engine thread; the tokio runtime
  never touches the `ApplyLoop`.
- Keep secrets and signing material out of snapshots, events, logs, fixtures, and registry
  configuration. Validate all wire inputs before they reach the processing core, and do not
  weaken NATS transport or operational endpoint protections for local convenience.
- Update `progress.md` when adding features; add a dated report to `benchmarks/` when
  performance-relevant behavior changes; keep `init.md` invariants in sync with reality.
- Commit style: `feat:`/`fix:`/`test:`/`chore:` subjects, body explains why.
