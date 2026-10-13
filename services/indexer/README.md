# Tackt Indexer — Phase 1

> User management is not owned here. Provision through the Core API. This service reads
> Core-owned associations from PostgreSQL and does not expose them over HTTP.

Standalone Rust process that normalizes external-account facts from Ethereum and Hyperliquid
and price updates from approved venues, and publishes each exactly once through an acknowledged
JetStream contract: observations on `TACKT_OBSERVATIONS`, price evidence on
`TACKT_PRICE_EVIDENCE`. The acknowledged publish is its only durable side effect — the Indexer
writes neither observations nor prices to PostgreSQL. The service deliberately contains no
ledger, risk, credit, ownership, signing, trading, withdrawal, or liquidation decisions.

## What it ingests

- Ethereum blocks plus user-relevant transactions, receipts, native ETH transfers and ERC-20 `Transfer` logs. A `newHeads` WebSocket is the low-latency wake signal; HTTP JSON-RPC remains authoritative for the head, blocks, logs and receipts. Chain ID is verified, confirmation counts are explicit, finality comes from the consensus `finalized` tag rather than a depth heuristic, and a parent-hash mismatch invalidates replaced observations without deleting their raw evidence.
- Hyperliquid fills, funding, fees and transfers from QuickNode's global unfiltered `TRADES` and `EVENTS` gRPC streams, attributed locally against enabled registered accounts. Per-stream cursors close reconnect gaps; broker-maintained NATS KV observations restore progress after process restarts, with paged JSON-RPC backfill and no ingestion-state writes to PostgreSQL. Account activity triggers a batched portfolio refresh, with a five-minute full reconciliation for balances and positions. Order lifecycle data is deliberately not subscribed.

Only enabled, registered Ethereum addresses and Hyperliquid accounts are attributed and collected.
The Indexer owns a global atomic user registry seeded from PostgreSQL at startup, updated
idempotently from Core lifecycle events, and authoritatively reconciled with PostgreSQL every five
minutes; mismatches are logged and repaired. Ethereum reads its address-owner view from this
registry. Hyperliquid additionally maintains a specialized activity-time index for bounded replay.
Stable SHA-256 event IDs — the broker's `Nats-Msg-Id` and every consumer's idempotency key —
make source replay safe. The newest published EVM block batch is the durable resume point.

## Price-evidence boundary

For each accepted price update, the Indexer performs provider authentication, bounded parsing,
stateless authority and identity validation, normalization, and deterministic evidence-ID
construction. Its only durable side effect is then one acknowledged publish through the typed
price-evidence JetStream contract:

```text
approved price source
→ Indexer validation and normalization
→ acknowledged JetStream price-evidence publish
├→ Risk Engine durable consumer
├→ downstream NATS KV latest-state projector
└→ downstream ClickHouse archival consumer
```

The Indexer does not persist price evidence to PostgreSQL, write current price state directly to
NATS KV, connect to ClickHouse, or wait for ClickHouse archival. The KV projection is rebuildable;
the ClickHouse sink batches immutable evidence and acknowledges its durable NATS delivery only
after persistence succeeds. Stateful stale, duplicate, and conflicting-sequence classification is
downstream and rejected evidence must never become an executable price.

## Run locally

Requirements: Rust 1.91+ and PostgreSQL 15+. One convenient database setup is:

```sh
docker compose -f deploy/compose.yaml up -d postgres
cp .env.example .env
set -a; . ./.env; set +a
cargo run -p tackt-indexer -- serve
```

Provision beta users through the Core API as described in `services/core-api/README.md`.

The Postgres crate initializer applies pending shared migrations before any worker or HTTP listener starts. `ETHEREUM_WS_URL` is required: Ethereum `newHeads` notifications coalesce into serialized HTTP reconciliation and a five-minute HTTP safety check. Base ingestion is opt-in: setting `BASE_WS_URL` (with optional `BASE_RPC_URL`, default `https://mainnet.base.org`, and `BASE_CHAIN_ID`, default 8453) runs the same EVM pipeline against Base with its own checkpoint, resume point, and finality boundary; non-mainnet chains publish under `tackt.observations.ethereum.{chain_id}` inside the same stream. Ethereum and Base both use the consensus `finalized` tag. The `BASE_FINALITY_TAG` override is removed; remove it from deployment configuration. `HYPERLIQUID_GRPC_URL` and `HYPERLIQUID_GRPC_TOKEN` are also required; remote transport is TLS-only, gRPC sends the token as sensitive `x-token` metadata, and the QuickNode JSON-RPC recovery URL is derived internally without logging it. `cargo run -p tackt-indexer -- poll-once` performs one Ethereum reconciliation and one Hyperliquid portfolio reconciliation; live economic events remain stream-driven. Production: `cargo build --locked --release` then run `target/release/tackt-indexer serve` with environment variables supplied by the secret manager.

## Operational endpoints

- `GET /health` — process liveness.
- `GET /ready` — readiness: 200 only while the acknowledged JetStream publish path is
  available — the managed NATS connection is established and the JetStream API answers for
  every required stream (observations, plus price evidence and aggregates when price feeds
  are configured). The publish is the Indexer's only durable side effect, so JetStream loss
  or a deleted stream reports 503 even while the core connection is up. Per-chain sync
  health is deliberately excluded: a chain's RPC outage leaves the publish path intact, and
  failing readiness for it would only restart-loop the process.
- `GET /metrics` — Prometheus exposition. Counters cover the three durable publish
  boundaries: observations (per `source` and `chain_id`; `chain_id` is `none` for non-EVM sources),
  price evidence, and aggregate prices. Non-EVM events count stored/duplicate/failed publishes;
  EVM batches count stored facts and report failed sync passes as `indexer_evm_sync_failures_total`. Chain health is the
  `chain_id`-labeled gauges, refreshed at the end of every successful sync pass:
  `indexer_evm_head`, `indexer_evm_checkpoint`, `indexer_evm_blocks_behind`,
  `indexer_evm_finalized_lag_blocks`, and `indexer_evm_last_success_timestamp_seconds`, plus
  the `indexer_evm_reorg_recoveries_total` counter. A stalled chain is a frozen last-success
  timestamp and a growing blocks-behind, not a 503.

The indexer exposes no user or observation endpoints. Consumers use the owning service or
authoritative infrastructure instead of querying the indexer process.

## Messaging (`NATS_URL` required)

- EVM observations use one complete block batch on the chain's existing observation
  subject in `TACKT_OBSERVATIONS`. The batch includes the finalized anchor, canonical hash suffix,
  and an explicit rollback range on reorg. One acknowledged publish is also the restart checkpoint.
  A new adoption of an old block hash is distinct from retrying the same update. Non-EVM facts
  retain event-ID deduplication. A failed publish stops the pass without advancing it.
  See [rollout and payload capacity](../ledger/README.md#block-batch-rollout).
- An ephemeral, per-instance `TACKT_USERS` consumer treats Core API lifecycle events as
  refresh triggers: it idempotently updates affected users in the global registry from PostgreSQL,
  reloads the client-side Hyperliquid address registry, replays the
  configured block overlap with event-ID deduplication, and performs a portfolio reconciliation.
  Lifecycle messages never force a gRPC reconnect; the independently supervised global
  subscriptions reconnect only on stream failure. The Indexer performs a full user-registry comparison
  every five minutes, logging and repairing divergence; Hyperliquid's 60-second database refresh
  remains its fallback for missed lifecycle messages.
- When a price adapter is configured, `NATS_URL` is required and an acknowledged price-evidence
  JetStream publish is the Indexer's completion boundary. ClickHouse and downstream KV projection
  health do not gate the Indexer.
- While `NATS_URL` is configured, the venue price adapters run by default with BTC, ETH, and
  SOL on Binance, Kraken, Bybit, and Coinbase, plus JitoSOL/USD on Kraken as a single-source
  demo feed. USDC gets no feed: it is the base asset,
  valued at par. Per-venue variables override the defaults
  (`BINANCE_MARKETS=BTC:BTCUSDT,ETH:ETHUSDT,SOL:SOLUSDT`, `KRAKEN_MARKETS=ETH:ETH/USD`,
  `BYBIT_MARKETS=ETH:ETHUSDT`, `COINBASE_MARKETS=ETH:ETH-USD`), and setting one empty
  disables that venue. The adapters are supervised, auto-reconnecting WebSocket trade
  streams over one shared connection shell.
  Every decoded trade is normalized into typed price evidence, published through the
  acknowledged JetStream contract, and only then fed to the one in-process aggregation engine —
  an asset configured on several venues is aggregated across them, its weight split evenly per
  source. Each asset's current aggregate (citing its contributing evidence ids and effective
  weights) is published through the acknowledged `tackt.prices.aggregate` contract as soon as
  an update lands — throttled to one publish per asset per 100ms, with an unconditional
  once-per-second heartbeat republish; quorum loss stops publishing rather than degrading
  silently.
  Startup fails closed on an unknown asset code or a duplicate market.
  Aggregation policy (weights, freshness windows, quorum, deviation) is interim service
  configuration pending its move into the locked asset registry.

For a demo that needs only the JitoSOL price, set the following overrides alongside the
normal Indexer configuration:

```sh
export BINANCE_MARKETS=''
export KRAKEN_MARKETS='JITOSOL:JITOSOL/USD'
export BYBIT_MARKETS=''
export COINBASE_MARKETS=''
```

The existing Kraken adapter subscribes to the public `trade` channel at
`wss://ws.kraken.com/v2`; no Kraken API key is required. It routes `JITOSOL/USD` to the
registry's `JITOSOL` accounting asset, preserving the exact USD trade price, source timestamp,
and trade ID in evidence. This is JitoSOL's market price, not a SOL price or redemption rate.
See [Kraken's trade subscription](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade).

Kraken contributes the entire JitoSOL source weight. Updates depend on trades; the one-second
aggregate heartbeat does not imply a new trade. Existing age decay and quorum rules stop
publication when the last trade is too old, including an old snapshot replayed on reconnect.
A single source cannot cross-check venue mispricing. Consumers must expire the last aggregate
when publishing stops. Adding this demo feed does not enable JitoSOL collateral eligibility.

`NATS_URL` is required: without the durable JetStream handoff the Indexer has nowhere to
record an observation, so startup fails closed rather than ingesting into the void.

## Data model and recovery

The Indexer reads users and source-account associations from PostgreSQL; it writes nothing
there. Observations exist only as durable stream messages until a downstream consumer persists
them. Quantities remain exact source strings (Ethereum hex atomic units and Hyperliquid
decimals), never binary floats. Every account fact contains raw source JSON and
endpoint/method/source-ID provenance. All ingestion progress is process-local or derived from
the stream, and stable IDs make overlap replay idempotent.

Ethereum resumes from its latest durable block batch, which includes the finalized anchor and canonical hash suffix. A restart continues at the next height. Reorgs explicitly roll back from the common ancestor and publish replacements oldest first, including a return to a previously seen hash. Same-height forks, shorter heads and finality-only advances are processed. A change to the previously finalized anchor halts ingestion for operator recovery. NATS subject-sequence fencing rejects stale EVM publishers.

Hyperliquid publishes on `tackt.observations.hyperliquid.trades` and
`tackt.observations.hyperliquid.events`. JetStream automatically copies the latest complete
observation envelope on each subject into the `trades` and `events` keys of the
`TACKT_OBSERVATIONS_HYPERLIQUID_RESUME` KV bucket. The bucket retains one value per key without
expiry. Provision it before startup with `cargo run --locked -p tackt-nats --example provision`;
Indexer checks that it exists and is compatible, and never performs a KV put. Portfolio snapshots
remain on the unsuffixed Hyperliquid subject and do not advance either key.

A restarted worker reads its KV key and replays that block **inclusively**: a recorded observation
does not prove every observation in the block was published. Stable event IDs let NATS suppress
near-term duplicates and Ledger persist each observation once even beyond the broker's duplicate
window. Backfill uses `hl_getBatchBlocks` in bounded pages until the live stream is reached;
a missing historical block stops progress for retry instead of silently skipping history.
During an in-process reconnect, the completed-block memory cursor still closes the gap.

KV is the only restart recovery source. Missing/unreadable buckets fail recovery. A missing key
with existing messages on its feed waits for JetStream sourcing; a feed with neither uses
`HYPERLIQUID_INITIAL_LOOKBACK_BLOCKS` for initial bootstrap. This setting also bounds
account-registry overlap (3,600 blocks by default, maximum 100,000); it no longer bounds normal
restart recovery. There is no recovery scan of the old unsuffixed subject. Quiet feeds may replay
more history because blocks without relevant observations leave KV unchanged. Source history
must still be available from the provider.

Each feed is supervised independently. Account activity reconciles a hot account no more than
once every five seconds; periodic full reconciliation remains the state safety net. Order
lifecycle data is outside this ingestion path. Run one active Indexer until distributed ingestion
leasing exists.

## Verification

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo build --release
```

The deterministic fixture suite covers stable IDs, replay/deduplication, user filtering, and stream-derived resume. For a clean PostgreSQL migration test, run `docker compose -f deploy/compose.yaml restart postgres` (which discards all PostgreSQL data), then run `cargo run --locked -p tackt-indexer -- poll-once`. Stopping or restarting NATS also discards its JetStream state in this disposable local stack.
