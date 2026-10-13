# Risk Engine

This crate implements deterministic, in-memory risk processing for:

- finalized Ethereum ERC-20 balances;
- standalone HyperCore HIP-1 balances;
- Hyperliquid account-value and maintenance snapshots;
- signed positions in every registered Hyperliquid perp market.

The crate ships as a library core plus a host binary. The library's processing path performs
no network, database, RPC, or WebSocket work; `src/main.rs` and `src/host/` wrap it into a
running service that consumes the `TACKT_RISK_{PRICES,PATCHES,SWEEPS}` JetStream streams,
publishes stable-account, advisory-only recommendations onto
`TACKT_RISK_RECOMMENDATIONS`, serves authenticated synchronous evaluations, and persists
snapshots. All IO lives in the host layer. See
"Running the service" below.

## Registry and price identity

For the separate account credit calculation, see [On-demand credit health](#on-demand-credit-health).
The source-bound registry and maintenance model in this section describe the existing processor.

The root [`asset-registry.json`](../../asset-registry.json) defines a shared list of chains,
venues, price factors, assets, and perp markets with their maintenance-margin tiers. Collateral
settings live on each canonical asset; Risk uses those same asset and chain records. Compact
numeric indexes are used in the hot path; CAIP identifiers, symbols, venue names, and source
identifiers remain in cold metadata. Tier rates (half of initial margin at each tier's maximum
leverage) and continuity deductions are derived once at registry load. `AssetRegistry::bundled()`
reads typed settings through `tackt_primitives::assets` from the same embedded file used by Core.
Runtime registry paths are rejected. See
[registry rollout](../../architecture/asset-registry.md) for coordinated registry changes.

Only two price-source kinds are accepted:

- `Ethereum` for Ethereum ERC-20 assets;
- `Hyperliquid` for HyperCore spot assets and Hyperliquid perp markets.

Ethereum and HyperCore forms of the same symbol are distinct assets with distinct `PriceIndex`
values. Distinct assets and perp markets may still share one `PriceIndex`: in the bundled
registry, HyperCore spot BTC and the BTC perp market both reference `perp:BTC`, so spot BTC
collateral is deliberately valued off the perp mark feed and a single BTC mark update reprices
balance and perp lanes together — the mixed-exposure benchmark scenario relies on this coupling.
Independent spot valuation requires a dedicated `spot:BTC` price factor in the registry.
A price update must match the source assigned to its index. Ethereum and Hyperliquid
source sequences advance independently, while the processor assigns a global internal epoch to
each accepted update.

HyperEVM is intentionally unsupported.

## On-demand credit health

`processing::credit::evaluate_credit(&account, &market, &policy)` evaluates one portfolio:
free spot collateral, pledged perp margin pools, their positions, and one account debt total
including accrued interest. It borrows these inputs and a shared `CreditMarketSnapshot`, stores
no derived state, and performs no IO. Policy, input types, and evaluation live together under
`processing/credit/`; spot and perps feed the same signed capacity calculation.

`CreditAccount::balances` holds the complete authoritative Ledger snapshot, uniquely keyed by
`(LedgerAsset, BalanceBucket)`, with quantities in registry-defined atomic units and explicit
custody evidence. Risk selects eligible balances and pools approved representations by economic
asset before rounding once to `Amount`. Asset identities and decimals come only from the locked
registry. `CollateralBalance::eligibility` explains each bucket's treatment on demand.

| Ledger balance | Borrowing | Liquidation |
| --- | --- | --- |
| Controlled, configured `Available` | Included | Included |
| Controlled, configured reservations | Excluded | Included |
| Controlled, configured `Pending` deposits | Included | Included |
| Unconfigured assets, uncontrolled spot | Excluded | Excluded |
| `HyperliquidMargin` | Through pool equity only | Through pool equity only |
| Liability and other non-collateral buckets | Excluded | Excluded |

Ledger owns deposit finality and reservation transitions. Risk never subtracts reservations from
`Available` again. The current policy assumes controlled collateral counts regardless of funding
origin, with debt counted once. Controlled reservations contribute to liquidation capacity only
and cease contributing when custody ends. Controlled pending deposits count toward both limits
at the same asset rates as available balances, explicitly accepting reorg exposure.
Finalization moves the amount between buckets without changing capacity. A Ledger reversal
removes that amount from the next evaluation while outstanding debt remains.
Missing required custody evidence returns `DataIncomplete`;
an uncontrolled pledged pool also returns `DataIncomplete`, so its losses cannot disappear.

`margin_pools` holds current signed equity once per pool; `perp_positions` uniquely identifies positions and
references their owning pool. Pool IDs must be unique within the account across venues.
Every position must reference a pool and match its non-zero source version. Snapshots must cover
all positions sharing that equity, not merely a favorable subset.

Pool equity must include current unrealized P&L, accrued funding, and fees exactly once, before
the Tackt loan. Free spot excludes every balance already included in pool equity. Callers supply
authoritative current equity and signed marked notional, conservatively normalized to the same
quote unit as debt. This path needs neither the originally selected leverage nor loan-origination
prices. Each coherent source update replaces the current inputs, including exposure after closes.

`CreditPolicy::current()` sets ETH/BTC/SOL at 85% maximum LTV and 90% liquidation threshold,
with 10% opening and 5% maintenance reserves on absolute current perp notional. Controlled
reservations contribute to liquidation capacity only; controlled pending deposits contribute
to both limits. These are selected Tackt reserve rates; the 5% reserve is provisional,
not stress-validated. USDC free spot remains unconfigured and contributes zero.

Custom policies use `CreditPolicy::new(spot_rates, perp_reserves, reserved_spot, pending_deposits)`
with `0 <= spot LTV < spot liquidation threshold <= 1` and `0 <= maintenance < opening <= 1`.
Accounts with perp pools under a policy without perp reserves return `DataIncomplete`, so
missing policy cannot hide negative exposure.

```text
borrowing capacity = sum(borrowing-eligible spot value × spot LTV) + sum(pool equity)
                     − sum(10% × absolute current position notional)
liquidation capacity = sum(liquidation-eligible spot value × spot liquidation threshold) + sum(pool equity)
                       − sum(5% × absolute current position notional)
available credit = max(0, borrowing capacity − debt − pending undrawn commitments)
capacity shortfall = max(0, debt + pending undrawn commitments − borrowing capacity)
health factor = liquidation capacity / outstanding debt
```

At $5,000 equity and $25,000 notional, the perp contributes $2,500 borrowing capacity and
$3,750 liquidation capacity. The resulting 50%/75% equity ratios change with current exposure
and equity; the 10%/5% reserve rates remain fixed by policy. Opposing positions do not net their
notionals. Closing releases remaining equity, not notional; after recovered funds repay debt,
the caller supplies the remaining equity, exposure, and account debt for another evaluation.

Pooled eligible quantities use `Amount` scale 1e9; prices, equity, notional, and debt use 1e8; rates and health
use 1e6. Spot contributions round down; each position's reserves round up. Equity and total
capacities remain signed, so losses reduce the portfolio limit. Wide accumulation avoids
order-dependent intermediate bounds; invalid inputs or out-of-range final outputs fail closed.
Only available credit and shortfall are floored at zero. Zero debt has maximum debt health.
Positive debt reaching liquidation capacity is `Liquidatable`; otherwise exposure including
pending draws above borrowing capacity is `Watch`. Direct value comparisons determine the state,
so display-ratio rounding cannot cause premature liquidation.

Missing, stale, future-dated, or zero required prices, stale/future balance snapshots, missing
required control evidence, and missing/stale/future pool snapshots,
return `CreditEvaluation::DataIncomplete` without partial measurements. Freshness uses explicit
`CreditMarketSnapshot::as_of_ms` and `max_age_ms` for balances, prices, and pool evidence; exactly the
maximum age is accepted. Zero account or market snapshot revisions, missing pool references,
zero or mismatched position/pool source revisions, invalid amounts, and arithmetic overflow
return errors.

Callers prove exclusive custody and recovery rights, supply complete Ledger balances and all
pledged pools/positions, and own quote provenance, coherent snapshots, stale-update rejection,
and retaining input identities with results. Fresh timestamps do not prove cross-source coherence:
a transfer into margin must not remain in available spot while also appearing in pool equity. Venue liquidation and withdrawal constraints are
monitored separately. The library does not execute liquidations, choose partial-close sizing,
order positions for closing, or claim a 300ms submission/execution guarantee. Its HTTP/NATS and
monitoring integration remains pending; the existing Hyperliquid maintenance processor is unchanged.

## PostgreSQL credit inputs

Before opening its listener, the host connects through `tackt-postgres` and loads all user-owned
Ledger balance projections in one read-only, consistent database snapshot. The snapshot retains
registry atomic quantities and Ledger signs, including pending funds, reservations, margin,
and booked principal liabilities. System accounts are excluded. Risk does not write these tables.

The host replaces this input snapshot every five seconds by default. Complete reads naturally
pick up new accounts, changed balances, and removals. Failed startup reads stop startup; a failed
refresh retains the last successful inputs and makes `/ready` return 503 until a read succeeds.
There is one refresh worker, no overlapping reads, and no NATS portfolio notification dependency.

This is startup loading and refresh only. The input snapshot is held in `HostState::ledger`,
separate from the existing streamed processor's recovery files. No credit-health endpoint or
stored credit-health projection is added by that input loader. The database supplies booked principal; current accrued
interest, the current credit policy, custody evidence, prices, and perp equity/exposure still need
explicit inputs when the credit calculator is integrated. A Ledger margin balance is not a
substitute for current perp equity.

`RISK_ENGINE_DATABASE_URL` (or `DATABASE_URL`) is required. Production may use
`RISK_ENGINE_MIGRATION_DATABASE_URL` for schema initialization and runtime credentials with
only SELECT access to Ledger accounts and balance projections. Without a separate migration URL,
the runtime URL must also be able to initialize the shared schema. PostgreSQL TLS enforcement
remains in `tackt-postgres`.

## Demo display history

Set `RISK_ENGINE_HISTORY_ACCOUNTS` to comma-separated Core user UUIDs (maximum 32) to enable
history of the existing streamed account/margin model. The default empty list disables it.
`RISK_ENGINE_HISTORY_INTERVAL_SECS` defaults to 60. A bounded nonblocking channel hands scalar
samples to a separate PostgreSQL writer; storage outages drop display samples without waiting
on the processing thread. Source freshness remains unknown because the current input contracts
have no observation timestamps. See [design and API](../../architecture/risk-health-history.md)
for exact semantics, configuration, database permissions, and failure behavior.

## Portfolio generations

Immutable `PortfolioGeneration` values bind each dense internal account index permanently to a
stable Core user UUID and contain Ethereum balances and finality, HyperCore balances and
account-value inclusion, margin-domain membership and mode, authoritative Hyperliquid snapshots,
signed perp sizes, snapshot anchor prices, and source versions. Missing, duplicate, or rebound
account identities are rejected before state mutates.

The compiler validates the generation and creates separate structure-of-arrays lanes for:

- weighted standalone balance value;
- signed perp PnL sensitivity;
- absolute-size maintenance sensitivity.

Lanes are indexed by price, worker shard, margin domain, and account. A generation swap rebuilds
state from the latest source prices and replaces prior local snapshot deltas. Installs and
rebuilds return the risk-state transitions the reconstruction causes, so downstream consumers
observe changes that happen without a price update.

## Account patches

`apply_account_patch` installs one account's complete portfolio alone in single-digit
milliseconds by splicing its records in place: the hot-path lane layout is untouched, domains
occupy stable slab slots, and the account's aggregates are recomputed from scratch against
current prices — exactly what a whole-generation install of the same content would produce.
Patches carry a per-account version and content digest with the same stale, conflict, and
idempotent-duplicate rules as generations, consume processing epochs from the shared counter
(one total replay order), and return any risk-state transition they cause. An empty patch
removes the account. Position identifiers remain globally unique: a patch cannot claim another
account's position. Whole-generation installs remain the reconciliation path and must be
stamped at least as new as every patch they subsume; installing resets all per-account
cursors.

## Risk model

Standalone balances contribute `amount × source price × collateral factor`. Non-finalized
Ethereum balances do not contribute. A HyperCore balance marked as included in Hyperliquid
account value is not counted separately.

Each Hyperliquid domain starts from source-reported account value and maintenance. Between
snapshots:

```text
account value change = signed size × (current mark - snapshot anchor mark)
maintenance change   = MM(abs(size) × current mark) - MM(abs(size) × anchor mark)
```

`MM` charges each notional band at `1 / (2 × maximum leverage)` for that tier. It selects
current tiers, including tier crossings, rather than extrapolating the average anchor rate.
Division uses the leverage directly (so 15x does not become a rounded parts-per-million rate).
Notionals and each band's maintenance round upward to `1e-8`; P&L rounds against the account.
Incremental updates subtract anchor-relative values, so rounding does not accumulate with the
number of price updates. Snapshot P&L, fees, and funding remain authoritative; subsequent
funding, fills, and transfers require a refreshed source snapshot and positions.

Positive Hyperliquid account value counts in full as collateral; negative account value becomes a
liability. Finalized Ethereum balances, standalone HyperCore balances, and Hyperliquid account
value are pooled without moving assets between venues. Maintenance is not deducted from
collateral.

Overall health is:

```text
(pooled collateral - liabilities) / total Hyperliquid maintenance
```

Individual domain health remains available for inspection, but the risk state is determined by
the overall account. An account is:

- `Liquidatable` if overall account health breaches liquidation;
- `Watch` if overall account health breaches warning;
- `DataIncomplete` if a required price, snapshot, anchor, or margin input is missing;
- `Healthy` otherwise.

### Per-account venue margin health

`RiskProcessor::venue_margin(account, VenueKind::HyperliquidPerps)` exposes standard-account
cross and isolated margin health plus overall venue leverage. `margin_domains(account)`
enumerates the independent pools in domain order. Both reads allocate nothing. Leverage uses
binary searches of price lanes to visit only the requested account's positions.

```text
pool margin ratio = pool maintenance / pool equity
venue margin ratio = maximum pool margin ratio
venue health = minimum(pool equity / pool maintenance)
venue gross notional = sum(abs(position size) × current mark)
venue leverage = venue gross notional / sum(pool equity)
```

`margin_ratio`, `health`, and `leverage` use `1e6` fixed point: leverage `5_000_000` means 5x;
margin ratio `250_000` means 25%. Margin ratios and leverage round up; health rounds down.
`minimum_margin_health` also exposes the weakest pool health. The returned `account_value`
and `maintenance` are venue totals for inspection; their quotient must not substitute for
pool-level liquidation risk. Long and short exposures both count toward gross leverage.

Each cross pool must represent one standard-account DEX's cross equity and maintenance;
isolated equity must be excluded from that cross equity and supplied in its own pool.
Each pool is counted once. Ethereum and standalone HyperCore collateral contribute nothing.
A standard HIP-3 DEX also retains its own pool. `Liquidatable` means a pool's equity is
strictly below its maintenance requirement, independent of Tackt's liquidation threshold.
`Watch` is only Tackt's configurable warning band. `DataIncomplete` takes precedence, and all
measurements are provisional when required snapshots, marks, or anchors are missing.

Zero maintenance gives infinite health for nonnegative equity and negative infinite health
for negative equity (`i128::MAX`/`i128::MIN`). Margin ratio and leverage use `i128::MAX` for
negative equity, or zero equity with positive maintenance/exposure; a zero requirement with
nonnegative equity returns zero. An account can have low aggregate leverage while an isolated
position is liquidatable. Overall Tackt account classification retains its pooled semantics.

Unified mode is rejected with `UnsupportedVenueAccountMode`; portfolio margin is not modeled.
Other venue kinds and accounts without pools return `None`. Poisoned processors reject reads.
These are library APIs; no HTTP or NATS endpoint is added. Callers can resolve a stable subject
through `account_index` and retain the portfolio version and processing epoch from the same borrow.
The `margin_domains` view retains its existing Tackt policy classification; `venue_margin`
applies the standard Hyperliquid liquidation boundary directly to each pool's values.

The calculation follows Hyperliquid's [margining](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining)
and [tier rules](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margin-tiers). Matching a
live exchange result requires matching source snapshots, current **mark** prices (not mid/trade
prices), and the embedded registry's tier schedule. This change verifies formulas and replay
behavior locally; it does not establish live exchange parity or change the locked tier registry.

Ledger remains authoritative for booked balances and the Tackt loan. Its user/asset margin bucket
does not identify venue pools or contain live P&L and maintenance, so venue health uses the
Risk portfolio's authoritative snapshots and marked position deltas. Moving collateral into
margin must not count it again as free spot. Venue maintenance is independent of the credit
calculator's Tackt reserve policy; neither the loan nor its pending draws are allocated to venues.
The ledger schema and accounting commands need no changes for this diagnostic.

## Cold-start persistence

The `persistence` module lets a restart reconstruct the processor from local disk instead of
re-querying external sources. It persists *inputs only* — the latest accepted portfolio per
account, the last price update per index, stream cursors, and the version high-water mark —
plus an egress ledger of the last risk state published downstream; compiled lanes, aggregates,
and health are never stored, because determinism recomputes them identically. Snapshots are
canonical binary files with a SHA-256 trailer, written atomically (tmp, fsync, rename,
directory fsync) and rotated keeping two, so a corrupt newest file falls back to the previous
one. Recovery installs the base generation at its original version and replays accounts
patched past it through the public patch path in stamp order — so every per-account version
cursor, and with it the stale-patch and stale-reconciliation-sweep guards, matches a
processor that never restarted — then seeds stored prices through the normal update path so
there is no `DataIncomplete` warm-up window, and emits corrections only for accounts whose
recomputed state disagrees with the ledger. Stream cursors are opaque
`(stream name, sequence)` pairs; the host layer (`src/host/`) implements tail replay and
transport over JetStream. The design rationale is in [`persistence.md`](persistence.md).

## Fixed point

| Type | Scale |
| --- | ---: |
| `Amount`, `SignedAmount` | `1e9` |
| `Price`, `Value` | `1e8` |
| `Factor`, health ratios | `1e6` |

Financial calculations use checked integer arithmetic and signed 256-bit intermediates.
Collateral and positive PnL round down; liabilities and negative PnL round conservatively.

## Running the service

First run the workspace's explicit NATS provisioning command (see root README), with the same
`NATS_URL` and `RISK_ENGINE_STREAM_PREFIX`. Startup fails if any required stream is missing or
incompatible. Snapshot-positioned consumer replay is unchanged; the host never creates streams.

```sh
# DATABASE_URL must point to the application's PostgreSQL database.
RISK_ENGINE_SNAPSHOT_DIR=/var/lib/risk-engine \
RISK_ENGINE_INTERNAL_TOKEN='replace-with-at-least-24-random-bytes' \
cargo run --locked --release -p risk-engine --bin risk-engine
```

Configuration comes from `RISK_ENGINE_`-prefixed environment variables (`.env` is honored):

| Variable | Default | Meaning |
| --- | --- | --- |
| `SNAPSHOT_DIR` | required | snapshot directory (created if absent) |
| `DATABASE_URL` | required; falls back to unprefixed `DATABASE_URL` | PostgreSQL runtime connection |
| `MIGRATION_DATABASE_URL` | unset | optional separate schema-initialization connection |
| `LEDGER_REFRESH_INTERVAL_SECS` | `5` | positive interval between complete Ledger input reads |
| `NATS_URL` | `nats://127.0.0.1:4222` | JetStream server |
| `LISTEN_ADDR` | `127.0.0.1:8083` | operational and internal HTTP endpoints |
| `ACCOUNT_COUNT` | `100000` | fixed account index range |
| `SHARD_COUNT` | cores, max 8 | parallel worker shards |
| `WARNING_HEALTH` / `LIQUIDATION_HEALTH` | `1_200_000` / `1_000_000` | policy thresholds in raw `Factor` (1e6) units |
| `ASSET_REGISTRY_PATH` | unsupported | startup rejects this retired override; the root registry is embedded |
| `SNAPSHOT_INTERVAL_SECS` / `SNAPSHOT_EVENT_COUNT` | `300` / `100000` | snapshot cadence, whichever first |
| `INITIAL_GENERATION_VERSION` | `1` | version of the first-boot empty generation |
| `INGEST_CHANNEL_CAPACITY` | `1024` | bounded ingress buffer (backpressure point) |
| `EVALUATION_CHANNEL_CAPACITY` | `64` | separate bounded synchronous-evaluation queue |
| `STREAM_PREFIX` | `TACKT_RISK` | stream-name prefix; subjects derive from it |
| `INTERNAL_TOKEN` | required | bearer token for internal evaluation requests; at least 24 printable ASCII bytes |
| `ALLOW_CLEARTEXT` | `false` | allow a non-loopback listener only when a proxy or mesh terminates TLS |

Behavior: on boot the host recovers from the newest snapshot (publishing ledger corrections
before attaching consumers) or installs an empty generation at `INITIAL_GENERATION_VERSION` —
**the first real reconciliation sweep must be stamped strictly greater** or it conflicts with
the empty generation and is dropped. Inputs ack only after the fold commits and every emitted
event is broker-acked; deterministic rejections ack-and-drop; a poisoned processor rebuilds
and re-applies in place (an input that keeps poisoning exits loudly after two heals).
`/health` serves 200 for the whole process lifetime, including long snapshot recoveries;
`/ready` returns 503 until recovery finishes and consumers attach, while poisoned, and
during shutdown. PostgreSQL input refresh failures also make readiness return 503. SIGTERM drains in-flight events and writes a final snapshot; if NATS is
unreachable the drain abandons publish retries rather than wedging (unacked inputs
redeliver after restart).

## Synchronous evaluation

`POST /internal/v1/risk/evaluations` accepts `Authorization: Bearer $RISK_ENGINE_INTERNAL_TOKEN`
and a typed [`RiskEvaluationRequest`](src/processing/evaluation.rs). The request identifies
the stable Core account and action, pins the committed account and caller input revisions, and supplies the complete normalized account state
projected after the action. Dense account indexes remain private to the engine.

The endpoint runs that projection through the same fixed-point, I/O-free risk model as streamed
state and returns `healthy`, `watch`, `liquidatable`, or `data_incomplete`, with measurements,
uncertain-input count, observed state revisions and price sequences, a canonical input digest, and a
deterministic evaluation ID. It is read-only: it persists nothing, publishes nothing, and does
not approve or execute the action. `data_incomplete` is a successful 200 classification, not an
approval. Equivalent inputs produce identical evidence; signatures are deliberately
outside this trusted-service contract.

Malformed JSON returns 400, unknown accounts 404, stale account versions 409, invalid financial inputs 422, unavailable or saturated engines 503, and evaluation
timeouts 504. Operational `/health`, `/ready`, and `/metrics` routes remain unauthenticated.

## Engineering-standard deviations

This crate deliberately deviates from the Rust standard's required-dependency table. Financial
math uses the crate's own checked `i128` fixed-point types with signed 256-bit intermediates
instead of `click_primitives::Decimal`, and `dashmap`, `time`, and `tracing-subscriber` are
omitted. `tokio`, `axum`, and `metrics-exporter-prometheus` are present for the host layer
only: the library core stays a synchronous, single-writer, in-memory engine, and the host
provides the HTTP endpoints and exports the engine's `metrics` counters.

## Development

Run all local gates:

```sh
make check
```

The `tests/nats_host_e2e.rs` suite additionally exercises the running service against a live
JetStream server when `NATS_URL` is set (CI always sets it); without it the suite skips.

Run the one-million-exposure release benchmark:

```sh
cargo run --release --bin risk-benchmark
```

The default benchmark uses eight persistent workers, 500,000 HyperCore balance lanes, 500,000
perp lanes, a 10% near-threshold cohort, 10 warmups, and 200 measured updates. Environment
overrides use the `RISK_BENCH_` prefix. The report includes P50/P95/P99/max latency, throughput,
CPU, allocations, RSS, and toolchain.

The measured implementation report is in
[`benchmarks/2026-07-28-margin-tiers.md`](benchmarks/2026-07-28-margin-tiers.md). The default
benchmark also measures full-generation install latency; realtime position streams should
coalesce into batched generation installs sized from that number.
