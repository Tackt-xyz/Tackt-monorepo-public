# Indexer architecture

Status: accepted.

## Scope

The Indexer ingests and normalizes external evidence. It records attributable Ethereum and
Hyperliquid account observations, normalizes trades from approved price venues into price
evidence, and computes each asset's aggregate price from its own evidence — a deterministic,
deviation-guarded weighted composite across venues. It owns ingestion mechanics, evidence
provenance, and that aggregation, but it does not own users, accounting, how a price may move
risk state, risk decisions, signing, trading, withdrawals, or liquidations.

## Persistence and messaging decision

Account observations and price evidence have different workload and recovery requirements and do
not share one persistence path.

| Data | Durable boundary | Long-lived or derived storage |
|---|---|---|
| Users and source-account associations | PostgreSQL | Core-owned lifecycle events accelerate refresh |
| External-account observations | acknowledged typed JetStream publish (`TACKT_OBSERVATIONS`) | downstream durable consumers own long-lived storage |
| Price evidence | acknowledged typed JetStream publish | NATS KV latest-state projection and ClickHouse immutable archive |
| Aggregate prices | acknowledged typed JetStream publish (`TACKT_PRICE_AGGREGATE`) | derived and superseded; recomputable forever from evidence plus registry |

### Account-observation path

EVM observations use one message per complete block on the existing per-chain subject.
Its header, account facts, finalized anchor, recent canonical hash suffix, previous subject sequence,
and optional `revert_from` are one durable publish. That same message is the restart checkpoint:
Indexer reads the last acknowledged batch and resumes at the next height. No second checkpoint
write, partial-block retry path, or process-local hash-window authority exists.

On a fork, Indexer finds the common ancestor against the saved suffix, checks RPC parent links,
and publishes replacements oldest first. The first replacement explicitly invalidates every height
from the fork onward. A shorter tip can publish a rollback without a replacement; finality advances
also publish without waiting for a new head. Recovery is bounded by the *previously published*
finalized anchor, not whether a replacement has since become finalized. Both Ethereum and Base
use the RPC `finalized` tag; `safe` is not settlement finality.

NATS owns publication identity and subject-sequence compare-and-set. An exact retry deduplicates;
a new adoption of the same block hash is a different update. Stale publishers cannot append to
the same chain. Keep one active Indexer: this fence is not leader election for other ingestion.
Ledger commits the whole batch, accounting, per-chain tip and global delivery cursor atomically,
then acknowledges. Fact identity remains stable across reorgs; transition identity includes the
update sequence, so A → B → A can reverse and rebook without duplicating a delivery.

Finality-only advances and shorter-head rollbacks fetch only block headers, not transactions,
receipts or logs. The refreshed tip must match the verified canonical suffix before an empty-fact
update is published; new or replacement blocks still collect all account evidence.

Hyperliquid retains per-observation publication and event-ID deduplication. The stream
keeps limits retention, 7-day age and discard-new. Missing retained history or a changed finalized
anchor stops ingestion for operator recovery; neither service guesses or skips evidence.
See the [Ledger rollout notes](../services/ledger/README.md#block-batch-rollout) for first-deployment
requirements and broker payload capacity. Raw observation archival to ClickHouse is a separate change.

Hyperliquid TRADES and EVENTS observations use distinct
`tackt.observations.hyperliquid.trades` and `.events` subjects within the same stream.
`tackt-nats` provisions `TACKT_OBSERVATIONS_HYPERLIQUID_RESUME`, a file-backed KV bucket with
one full observation envelope per `trades`/`events` key and no age expiry. Its backing stream
sources `TACKT_OBSERVATIONS` with subject transforms; JetStream alone populates the values.
The Indexer still performs one acknowledged publish and no application-side KV put.

On a fresh process each feed reads its KV key and replays the observation's block inclusively,
then catches up through paged JSON-RPC backfill. KV records an observation, not completion of
all observations in that block, so inclusive replay and downstream event-ID deduplication are
required. In-process reconnects retain the completed-block cursor. A lagging KV value causes
extra replay; a missing key with existing feed messages makes startup of that worker retry.
Only a feed with no key and no published messages uses the configured bootstrap lookback.
There is no legacy-subject scan or alternate recovery source. Portfolio snapshots have no block
height and cannot advance a key; blocks without relevant observations leave the key unchanged.
Missing historical blocks stop backfill instead of silently advancing the cursor. KV is provisioned explicitly before startup; Indexer only checks it. Provisioning
inherits the evidence stream's replica count when created and refuses incompatible existing
configuration. Its availability is required to restore a worker, not for each observation publish.

### Price path

For each accepted venue trade, the Indexer's durable side effect for that trade is one
acknowledged publish to the price-evidence JetStream stream. Only after that publish does the
trade enter the in-process aggregation engine — a pure state machine holding each configured
source's latest quote, applying linear staleness decay to source weights, evicting quotes
beyond a deviation band around the weighted median, and failing closed when survivors lack a
quorum or a strict weight majority. The Indexer publishes each asset's current aggregate (citing the
contributing evidence ids and effective weights) to the aggregate-price stream as soon as an
update lands, throttled to one publish per asset per 100ms, plus an unconditional
once-per-second heartbeat; republishing an unchanged price is what separates "stable" from
"stalled", and quorum loss simply stops publishing. The Indexer performs no price write to PostgreSQL or
NATS KV and has no ClickHouse client.

```text
approved venues (Binance, Kraken, Bybit, Coinbase, ...)
      │
      ▼
Indexer: authenticate, validate, normalize, identify
      │
      ▼  one acknowledged durable publish per trade
NATS JetStream price-evidence stream
      ├──► KV projector ──► latest provider/feed state + sequence watermark
      ├──► ClickHouse sink ──► immutable historical evidence
      └──► (in-process) aggregation engine
                 │
                 ▼  acknowledged publish per asset: on update (≤10/s) plus a 1s heartbeat
           NATS JetStream aggregate-price stream ──► Risk Engine and other consumers
```

An evidence publish failure fails the venue's connection cycle so backoff throttles ingestion;
a trade that cannot become evidence is logged and dropped rather than silencing the venue's
healthy markets. Aggregates are derived data: a failed aggregate publish is superseded by the
next tick, never retried. Aggregation policy (per-asset sources, weights, freshness windows,
quorum, deviation bound) is interim service configuration; the accepted follow-up moves it —
with each source's quote currency — into the locked asset registry so policy shares the
registry's version and digest. The engine's state is process-local and rebuilt from live feeds
within seconds of a restart; the single-active-indexer rule is a price-integrity invariant,
because two instances would publish competing aggregates with independent sequences.

## Ownership

### Indexer

The Indexer owns provider adapters, bounded response parsing, configured source-authority checks,
normalization, deterministic evidence identity, and typed publication. It emits logs and metrics
but creates no second durable price write.

An enabled price adapter requires a usable NATS connection and its price-evidence stream. The
Indexer is not ready when it cannot complete the acknowledged publish. It does not include KV
projector or ClickHouse health in readiness because neither is in its completion path.

### NATS and NATS KV

`tackt-nats` owns the price envelope, subject, stream, producer deduplication,
retention, acknowledgement, retry, and durable consumer contracts. The typed contract is
`PriceEvidenceEnvelope` on `tackt.prices.evidence`; `crates/nats/src/prices.rs` states its
complete field list, validation, size bounds, and the identity rule that keeps retries of one
provider update on one identity. The stream is the fan-out and replay boundary; core NATS is not
permitted for price evidence.

`TACKT_PRICE_EVIDENCE` is provisioned idempotently by `NatsClient::ensure_stream` from
`subjects::price_evidence_stream` in the explicit `tackt-nats` provisioning command. Services use
the read-only `NatsClient::require_stream` before accepting work. A configuration that
cannot be reached without destroying retained messages — a retention or discard change, a dropped
subject, a reduced age or capacity limit — is refused rather than applied, so shrinking the stream
is an explicit operator migration.

| Setting | Value | Why |
|---|---|---|
| Retention | limits | Three independent consumers each read every message; a work queue would delete on the first ack |
| Max age | 7 days | The supported downstream outage window: a long-weekend consumer outage plus a working day to notice and drain it |
| Max bytes | 4 GiB | Better than twice the anticipated peak (~3 msg/s at ~1 KiB is ~1.7 GiB a week), so age, not capacity, expires evidence under expected load |
| Discard | new | At capacity the publish is rejected; the Indexer fails closed instead of the broker deleting unarchived evidence |
| Duplicate window | 10 minutes | Covers a retried publish and a provider re-serving one sequence; consumers still deduplicate on `evidence_id` beyond it |

Age expiry is the normal way evidence leaves the stream and is an operational concern — consumer
lag and retention headroom are alerts, not stream settings. Capacity exhaustion is not: it is a
producer error, and price ingestion stops until the backlog drains.

Each consumer binds one durable name owned by `PriceEvidenceConsumer`: `price-evidence-risk`,
`price-evidence-kv`, and `price-evidence-archive`. All three start from the beginning of the
retained stream, have no delivery ceiling, and acknowledge only after their own durable effect is
complete. A transient failure is NAKed with `PriceEvidenceConsumer::retry_delay`, a ladder that
widens to five minutes and stays there. A message is terminated only when this build can never
process it — an undecodable payload — so a
stalled dependency accumulates backlog bounded by retention rather than dropping evidence for
having been retried too often.

A downstream durable consumer owns the NATS KV projection. Each registry/provider/feed key records
the latest accepted sequence, evidence digest, quote, timestamps, and originating stream sequence.
Updates use compare-and-swap. KV is rebuildable current state, not the immutable evidence archive.

### ClickHouse sink

A separate durable NATS consumer owns ClickHouse ingestion. It batches immutable evidence and
acknowledges NATS delivery only after ClickHouse confirms persistence. Deterministic evidence IDs
and an explicit retry strategy make redelivery logically idempotent.

The sink, ClickHouse schema, client, credentials, batching, and archive retention do not belong in
the Indexer. ClickHouse is not queried by the price ingestion or Risk Engine hot paths.

### Risk Engine

The Risk Engine consumes the aggregate-price stream as its price input; it verifies rather than
recomputes. Registry-identity checks, aggregate-sequence monotonicity, freshness, and its own
conservative-price policy remain fail-closed before an aggregate can affect risk state — an
Indexer aggregate is an input to risk, never a bypass of it. The Risk Engine may additionally
consume raw price evidence for cross-checks, but aggregation itself lives in the Indexer.

## Failure behavior

| Failure | Required behavior |
|---|---|
| Price provider unavailable or malformed | Indexer rejects the update, reports source health, and publishes nothing |
| NATS or a durable stream unavailable | Observation and price ingestion fail closed; there is no fallback database write |
| Risk Engine unavailable | Its durable consumer accumulates backlog; Indexer, KV, and archive consumers continue |
| KV projector unavailable | Current-state projection becomes stale and alerts; other consumers continue |
| ClickHouse unavailable | Archive consumer leaves deliveries unacknowledged and backlog accumulates; Indexer, Risk, and KV continue |
| Duplicate delivery | Deterministic evidence identity makes every consumer idempotent |
| Stale or same-sequence conflicting evidence | Stateful consumers classify it deterministically; it never becomes executable |

JetStream retention and capacity cover the supported downstream outage window documented above.
Lag and retention headroom are operational alerts; exhausting the durable handoff fails closed
rather than silently discarding unarchived evidence.

## Prohibited designs

- Persisting price observations, price cursors, or price checkpoints through `tackt-postgres`.
- Having the Indexer update the latest-price KV bucket directly.
- Adding a ClickHouse client, schema, credential, or write retry loop to the Indexer.
- Waiting for ClickHouse archival before acknowledging provider ingestion.
- Sending price evidence over core NATS or another unacknowledged transport.
- Giving the price-evidence stream a delivery ceiling, an old-discard policy, or any limit that
  drops evidence the archive has not yet stored.
- Having Indexer persist account observations to PostgreSQL, or sending them over core NATS or another
  unacknowledged transport.

## Ledger observation consumer

Ledger trusts Indexer's external facts and records all observations, including unknown assets.
It consumes the observation stream in order and commits each observation, its accounting effects,
deposit state and cursor atomically before acknowledging. Core requests enter separately over
authenticated HTTP and use the same accounting internals. No observation-to-command conversion,
settlement poller or second chain validator is involved.

Supported canonical inbound deposits enter pending immediately; trusted finality makes them
available, and replacement blocks reverse their accounting. Internal transfers, unrepresentable
amounts and unknown assets remain recorded without automatic external credit. Accounting failures
leave the observation durable and any attempted postings rolled back, with a review record for a
representable deposit. Storage failures roll back the entire input and leave NATS unacknowledged.
