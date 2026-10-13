# Ethereum and Hyperliquid Risk Engine

Status: implemented processing core

Language: Rust

Performance target: revalue 1,000,000 affected mixed balance/perp records below 100 ms at P99

## Scope

The engine is an off-chain risk mirror. It accepts normalized inputs with runtime revisions and emits
deterministic risk-state changes. Canonical authorization and liquidation remain outside this
crate.

Implemented inputs:

- finalized and non-finalized Ethereum ERC-20 balance records;
- HyperCore HIP-1 spot balances with explicit account-value inclusion;
- Hyperliquid account-value and maintenance snapshots;
- standard, unified, cross, isolated, and HIP-3 margin-domain membership;
- signed positions for all registered Hyperliquid perp markets;
- source-bound Ethereum and Hyperliquid price updates.

RPC, WebSocket, chain indexing, LP positions, NFTs, options, open-order simulation, and HyperEVM
are out of scope.

## Ledger input loading

The host loads current user-owned Ledger balance projections from PostgreSQL before accepting
traffic and replaces them on a configurable timer (default five seconds). SQL and snapshot
consistency belong to `tackt-postgres`; the host retains the last successful complete input read.
A failed read prevents readiness until refresh succeeds. This input cache is rebuilt from the
database at every startup, with no NATS snapshot or notification requirement. It contains booked
principal, not inferred interest, custody evidence, or live perp equity. Credit
calculation and its request interface remain separate work.

## Display-history isolation

Optional host sampling captures the scalar streamed account/margin health ratio and classification
only between complete applies. It must never mutate processor state, wait on a channel/database, or scan all allocated
accounts. A bounded nonblocking handoff to a separate worker/pool may drop samples on overload or
storage failure. Capture timestamps never assert source freshness; incomplete and unbounded
samples expose null health. History cannot enter authorization or replace durable Risk streams. See
`architecture/risk-health-history.md` at the workspace root for the demo contract.

## Embedded registry

The root `asset-registry.json` is embedded only by primitives. Risk compiles the common asset entries and their collateral
settings into indexed metadata. Application contracts carry no registry compatibility metadata.
Runtime registry overrides are rejected. Risk-only HyperCore identities never expand Ledger's
supported asset enum. See `architecture/asset-registry.md` for the current registry contract.

## Correctness invariants

The invariants below describe the existing Ethereum/Hyperliquid processor. The separate
on-demand credit calculator in `processing::credit` has these additional rules:

- The caller supplies complete Ledger balances in registry-defined atomic units, custody facts,
  one outstanding account debt value including interest, and separate pending undrawn commitments.
  Risk selects eligible buckets and reviewed representations before pooling exact quantities by
  economic asset. New registry entries remain unconfigured until explicitly admitted by policy;
  in particular, yield-bearing balances are never pooled at an assumed one-to-one exchange rate.
  Pending eligibility follows the supplied policy; non-collateral buckets never count, and margin
  balances count only through pool equity.
  Borrowed proceeds have no separate exclusion: account debt remains counted once.
  Pledged perps supply signed equity once per margin pool and positions keyed by unique IDs.
  Free spot excludes pool balances; pool equity includes P&L/funding/fees but not the Tackt loan.
  Position membership and non-zero pool/position source versions are validated for consistency.
- The caller owns coherent account and price snapshot selection, stale-update rejection, and
  retaining their runtime revisions alongside the result; the calculator stores no revision cursors.
- Borrowing capacity is `sum(quantity * price * max_ltv)`; liquidation capacity uses each
  asset's liquidation threshold instead. Both add pool equity and subtract the appropriate
  per-position reserve on absolute current notional. Spot rounds down; reserves round up.
  Negative contributions remain signed; only headroom and shortfall are floored at zero.
  Wide accumulation enforces output bounds after signed contributions have been combined.
- The current policy uses 10% opening and 5% maintenance perp reserves, retains controlled
  reservations for liquidation only, and counts controlled pending deposits for both limits,
  accepting reorg exposure. Ledger finalization preserves total collateral; reversals remove
  it without erasing drawn debt. A policy without perp rates cannot evaluate perp pools.
- Available credit and shortfall compare borrowing capacity with debt plus pending draws.
  Health is liquidation capacity divided by outstanding debt. Zero debt is never liquidatable.
- Liquidation starts at `debt >= liquidation capacity` for positive debt; classification uses
  direct value comparisons rather than the rounded health ratio.
- Missing spot policy contributes zero. Missing, zero, stale, or future required prices and
  stale/future balance snapshots, missing required custody evidence, uncontrolled pledged pools,
  and missing/stale/future margin-pool snapshots yield
  `DataIncomplete` with no usable partial measurements. Freshness uses explicit input times.
- The calculation is synchronous and read-only, with no persisted or cached derived values.

Existing processor invariants:

- Identical registry, portfolio, price, and policy inputs produce identical results.
- Financial decisions never use floating point.
- Ethereum prices cannot value Hyperliquid records and Hyperliquid prices cannot value Ethereum
  records.
- Source sequences are monotonic and conflict checked independently.
- Each accepted update receives one monotonic internal processing epoch.
- Compiled portfolio state changes only through whole-generation swaps or single-account
  patches; each is validated completely before mutating and applies atomically.
- A balance, account-value snapshot, or perp delta contributes at most once.
- HyperCore spot included in reported account value is not valued separately.
- Positive Hyperliquid account value is full collateral; negative value is a liability.
- Maintenance is never deducted from collateral; total maintenance is the denominator of overall
  account health.
- Finalized Ethereum collateral, standalone HyperCore collateral, and Hyperliquid account value
  are pooled for health without modeling an asset transfer.
- Missing required prices, snapshots, anchors, or margin inputs produce `DataIncomplete`.
- Venue margin reads include only that venue's pools, once each. They report the weakest pool
  health and largest maintenance/equity ratio, so isolated losses cannot be netted away.
  Standard venue liquidation uses equity < maintenance; unified mode is explicitly unsupported
  by the native venue query. Leverage is gross marked notional / total equity. Missing pool
  inputs take precedence, independent of other account uncertainty.
  These allocation-free reads do not change pooled account state or its emitted events.
- Validation completes before an epoch mutates account or domain state.
- Each changed domain and parent account is evaluated once per price update.

## Processing layout

```text
normalized ingest
      |
      v
source cursor -> internal epoch -> price/shard index
                                    | balance SoA lane
                                    | perp PnL SoA lane
                                    | maintenance SoA lane
                                    v
                     shard-owned domains -> shard-owned accounts
                                    |
                                    v
                           changed-state events
```

Strings, CAIP identifiers, market names, venue names, and source metadata live in the registry.
Hot records carry compact indexes and fixed-point integers.

Eight persistent Rayon workers own deterministic account shards. Price lanes are sorted by shard,
account, and domain. Scratch capacity is reused across epochs. A generation rebuild is reserved
for startup, reconciliation, snapshots, trades, transfers, funding, fees, tier changes, and
margin-mode changes.

## Valuation

Standalone collateral:

```text
amount × source price × registry collateral factor
```

Hyperliquid local adjustment from a source snapshot:

```text
account value = snapshot account value
              + sum(signed size × (current mark - anchor mark))

maintenance = snapshot maintenance
            + sum(MM(abs(size) × current mark) - MM(abs(size) × anchor mark))
```

`MM` applies each notional band's exact `1 / (2 × max leverage)` rate, rounded upward
to monetary precision. Price updates subtract anchor-relative values to avoid rounding drift.

The engine pools finalized Ethereum collateral, standalone HyperCore collateral, and all
Hyperliquid domain account values. Overall account health is net pooled value divided by total
Hyperliquid maintenance. Domain health is retained as diagnostic state, but does not independently
trigger account liquidation. Missing data anywhere in the required account inputs takes
precedence and produces `DataIncomplete`.
