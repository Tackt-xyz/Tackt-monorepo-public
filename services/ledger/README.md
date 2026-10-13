# Ledger

Two inputs, one accounting authority:

- NATS observations are trusted external facts, stored whether or not they can affect balances.
- Core's authenticated HTTP requests ask Ledger to perform reviewed accounting operations.

PostgreSQL owns SQL and transactions. Ledger owns financial decisions. There is no observation
command envelope, settlement poller, full-history sweep, or separate booking/attachment workflow.

## Layout

- `main.rs`: startup, readiness and supervision.
- `observations.rs`: ordered NATS consumption and direct deposit accounting.
- `http.rs`: Core authentication, typed request decoding and responses.
- `accounting.rs`: posting templates, balancing, reservations, idempotency and shared writes.
- `state.rs`: typed internal reservation changes.
- `outcome.rs`: Core request audit logging.
- `crates/postgres/src/ledger.rs`: opaque transactional accounting storage.
- `crates/postgres/src/observations.rs`: observation, cursor and deposit storage.
- `crates/nats/src/observations.rs`: durable contract, routing and delivery helpers.

## Atomic processing

For each NATS delivery, one PostgreSQL transaction bulk-stores the block's observations, applies
accounting, updates deposit state and the per-chain tip/finalized anchor, and advances the stream cursor. Only then is the delivery acknowledged.
Event IDs identify facts; the update sequence identifies each adoption/reversal of those facts.
A → B → A therefore re-canonicalizes and rebooks A instead of treating its return as a retry. The existing JetStream
durable resumes delivery; the database cursor rejects gaps and deduplicates committed redeliveries.

Supported inbound transfers enter pending immediately. Trusted finality moves pending to available.
Deposits are created, finalized and rejected only by observation processing, not balance-only
deposit commands. Existing audited adjustments remain exceptional operations, not a deposit lifecycle.
Replacement blocks reverse affected deposits. Unknown tokens and quantities outside Ledger's
arithmetic remain observations without automatic balances; internal transfers do not create
external credit. A deterministic accounting failure rolls back only that accounting attempt and
records review detail without losing the last posted phase. A required reorg reversal failure rolls
back the complete input, just like a storage failure: no orphan stays credited behind an acknowledged cursor.

Core requests check authorization evidence and Ledger rules, then commit reservations, journal
entries and balance changes together. Retrying a successful request returns its original result.
The original Core command-hash representation is preserved, including reservation serialization.

One observation worker holds a session lease. All accounting writers share a transaction lock,
including Core requests. This favors predictable correctness over parallel writes to shared system
accounts. Reads remain concurrent. Account reads and account/balance/posting writes are batched per
operation, not performed one query per posting. Deposit resolution uses indexed height/status
ranges, not scans of observation history. Extremely large reorg/finality batches can still delay
Core writes; measure before introducing partitioned writers or an asynchronous settlement queue.

## Core HTTP

Existing routes and response formats are unchanged:

- `POST /internal/v1/credit-draws`
- `POST /internal/v1/custody-exit-reservations`
- `POST /internal/v1/custody-exit-reservations/consume`
- `POST /internal/v1/custody-exit-reservations/release`
- `POST /internal/v1/order-reservations`
- `POST /internal/v1/withdrawal-reservations`

Requests need a Bearer credential, one Idempotency-Key, typed quantities and Core authorization.
There is no arbitrary posting, SQL or generic adjustment HTTP endpoint.
`/health` is liveness; `/ready` checks the database and NATS. An ingress error terminates the
service so its supervisor restarts it on the same durable consumer.

## Configuration

Required: `DATABASE_URL`, `NATS_URL`, `LEDGER_INTERNAL_TOKEN`.

Before startup, run `cargo run --locked -p tackt-nats --example provision` with `NATS_URL` set.
Ledger requires the observation stream and `ledger-deposit-evidence` durable to exist; it never
creates or resets either during startup.
`LEDGER_MIGRATION_DATABASE_URL` optionally selects a separate migration connection. Without it,
startup applies migrations through `DATABASE_URL`. Objects belong to the migration user; no
predefined roles or role memberships are required. If separate credentials are used, the deployment
must provision runtime access itself. Remote database connections require verified TLS.

Optional: `LEDGER_LISTEN_ADDR` (127.0.0.1:8082), `LEDGER_DATABASE_CONNECTIONS` (8, minimum 2),
`LEDGER_HTTP_MAX_BODY_BYTES` (16384), `LEDGER_HTTP_MAX_IN_FLIGHT` (64).
Non-loopback cleartext requires explicit `LEDGER_ALLOW_CLEARTEXT=true` behind TLS termination.
The old settlement enable/chain/confirmation/poll options are removed: recording is always on,
and supported assets come from the embedded registry.

## Block-batch rollout

1. Apply append-only migration 0018; existing migration versions/checksums are unchanged.
2. Deploy the batch-capable Indexer and Ledger together for first use. EVM requires
   block batches; there is no legacy EVM replay or cutover path. Hyperliquid keeps
   individual observations.
3. Provision sufficient NATS `max_payload` for complete blocks. `deploy/nats.conf` sets 8 MiB for
   Compose; production broker configuration must set its own capacity before the Indexer upgrade.
   A larger batch fails with its byte size and required capacity, without publication or checkpoint
   advancement. It is never silently dropped or split into partially committed chunks.

Older Ledger binaries cannot consume v2. Do not roll back the consumer while v2 messages remain.
Raw observations still live in PostgreSQL; an independent ClickHouse archive is outside this change.

## Database recovery

Ledger schema has storage keys and indexes. The typed Ledger API enforces accounting policy
and append-only journal operations; migrations do not install database role-based write restrictions.
The database owner can modify stored data directly. Through the application, accounting history
is immutable; balances are a transactionally maintained projection and can be independently
reconstructed from exported postings. Export is bounded to 10,000 transactions.

Backup/restore uses the repository scripts. Restore assigns objects to the target login and skips
source owners and grants (`pg_restore --no-owner --no-acl`). Stop writers for restore.
If a restored database cursor is behind NATS acknowledgements, startup fails: an operator must
explicitly reposition the stopped Ledger's durable to cursor + 1, with the required history retained.
The cursor is bound to JetStream's stream-creation timestamp. Ordinary service or broker restarts
preserve it; deleting and recreating the stream does not. A mismatch stops startup before processing
or acknowledging observations: restore matching JetStream data and database state, not just a stream
with the same name. An older non-empty cursor without this identity also requires explicit recovery;
startup never guesses which stream it belonged to. Migration 0017 adds this marker without rewriting
prior migrations. PostgreSQL commits before acknowledgement, so an interrupted acknowledgement can
replay an already committed observation without double-booking it.
Missing retained history requires explicit recovery from an archive; the consumer will not skip it.

## Verification

Use only the disposable services in `deploy/compose.yaml`, as required by root AGENTS.md.
Run the root boundary, metadata, formatting, dependency, Clippy and all-target test checks.
Tests cover Core HTTP retries, accounting reconstruction and concurrency, trusted observation
persistence, unknown tokens, failed transfers, overflow rollback, reorgs and consumer restart.
