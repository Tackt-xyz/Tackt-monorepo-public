# Tackt repository instructions

These instructions apply to the entire workspace.

## Repository architecture

Tackt is a Cargo monorepo. Prefer focused workspace crates over separate repositories or one
large feature-gated crate. Feature flags may enable genuinely optional behavior inside a crate,
but they are not architectural or ownership boundaries.

```text
crates/
  primitives/  universal dependency-light domain types
  postgres/    PostgreSQL schema and complete data-access layer
  nats/        NATS transport and all NATS wire contracts
  telemetry/   shared tracing and logging setup
services/
  core-api/   authoritative user lifecycle and source-account provisioning
  indexer/     external-account and price-source ingestion with operational endpoints
  ledger/      immutable double-entry accounting and command processing
  risk-engine/ deterministic risk engine and its NATS/JetStream host service
architecture/ accepted service and data-flow decisions
deploy/        deployment assets
scripts/       CI and architecture checks
asset-registry.json  the locked Release 1A asset list
```

## Ownership rules

### Primitives

`tackt-primitives` contains only universal domain vocabulary such as IDs, sources, timestamps,
amounts, observations, and provenance. It must remain independent of storage and transport
technologies. It must not depend on Postgres, SQLx, NATS, HTTP frameworks, or service crates.

It also owns the locked asset registry. `asset-registry.json` at the workspace root defines the
supported assets and their symbol, chain, CAIP-19 identity, token address, and decimals;
`tackt_primitives::assets` embeds and validates it and is the only way to read those facts. Do
not restate an asset's chain, contract address, or decimals in a service, a configuration value,
an environment variable, or another crate, and do not add a runtime path or override that would
let two processes run against different asset lists. Every lookup fails closed; changing the
supported set means editing the file and rebuilding the services. The preproduction application
contracts have no schema, registry, or policy version metadata or compatibility gates.

### Postgres

`tackt-postgres` is the sole authority for PostgreSQL:

- all migrations live in `crates/postgres/migrations`;
- all SQL and SQLx dependencies live in `crates/postgres`;
- connection pooling, transactions, row mapping, and persistence functions live here;
- APIs exposed to consumers are typed and domain-oriented;
- do not expose `PgPool`, SQLx rows, or SQLx transactions to services;
- do not provide generic raw-SQL escape hatches.

Services must never contain SQL, migrations, SQLx dependencies, or persistence row types.
Schema and query changes must be implemented through the Postgres crate's typed API.

Ledger accounting rules live only in `services/ledger`: command validation, posting templates,
balance admissibility, reservation transitions, idempotency decisions, and settlement outcomes.
The Postgres crate may provide opaque, transaction-scoped typed storage capabilities, but must
not accept financial commands, validate accounting policy, or choose lifecycle outcomes. Ledger
schema business checks, foreign keys, semantic uniqueness constraints, enums, and triggers must
not duplicate Rust rules. Retain storage row keys, indexes, atomic transactions, writer locks,
and application-level write authority. Migrations must not create database roles, transfer object
ownership, or require membership in predefined roles. Objects belong to the migrating database
user; Ledger write policy is enforced through its typed application API.

Migrations are append-only after they have been shared or deployed. Never edit an applied
migration; add a new migration. SQLx records versions and checksums in `_sqlx_migrations` and
locks migration execution, so repeated and concurrent initialization remains idempotent.

### NATS

`tackt-nats` is the sole authority for NATS behavior and wire contracts:

- connection and reconnect behavior;
- subjects and stream names;
- event and command envelopes;
- publisher and consumer helpers;
- delivery, acknowledgement, and retry conventions;
- typed message serialization.
- JetStream-backed key-value bucket definitions and access conventions.

If someone needs to discover what is published or consumed over NATS, the complete answer must
be available in the NATS crate. Do not create a generic `contracts` crate. NATS contracts may
compose types from `tackt-primitives`, but primitives must not depend on NATS.

JetStream streams are provisioned explicitly, never by service startup. Every service must check
its required streams before accepting work and fail if they are missing or incompatible. Ledger
binds its pre-provisioned durable without resetting its position. Ephemeral subscriptions and
Risk's snapshot-positioned consumer recovery remain runtime operations, not stream provisioning.

### Telemetry

`tackt-telemetry` owns shared tracing, logging, and future metrics initialization. Services must
use its public setup rather than independently defining platform-wide telemetry defaults.

### Services

Services own orchestration, adapters, business workflows, and service-specific APIs. A service
composes infrastructure crates through their public typed interfaces. It must not duplicate or
bypass infrastructure owned by a shared crate.

Service authority is deliberately narrow:

- `core-api` is the sole authority for user lifecycle, source-account associations, and
  provider-neutral signing and funding workflow boundaries;
- `indexer` records external-account observations and normalizes approved price-source evidence.
  An accepted observation or price update has exactly one durable side effect: an acknowledged
  publish through the typed observation or price-evidence JetStream contract. The Indexer must not
  persist observations or prices to PostgreSQL, write price state directly to NATS KV, connect to
  ClickHouse, or own accounting, risk, signing, trading, withdrawal, or liquidation decisions;
- `ledger` is the sole accounting authority and owns the immutable double-entry journal, typed
  accounting commands, reservations, and balance projections; callers must never submit arbitrary
  postings;
- `risk-engine` calculates deterministic risk state; canonical authorization and liquidation
  execution remain outside it.

The risk engine is intentionally a synchronous, deterministic processing library. Its nested
`services/risk-engine/AGENTS.md` contains additional correctness, performance, arithmetic, and
benchmark requirements that apply to every change under that directory.

## Initialization convention

Runtime infrastructure crates own an `initialise` entry point that completely prepares their
capability:

- `PgRepository::initialise(...)` connects and applies pending migrations;
- `tackt_nats::initialise(...)` establishes a managed NATS connection;
- `tackt_telemetry::initialise()` installs process telemetry safely.

Services call the crate initializers they need before starting workers or accepting traffic.
Do not create service-level wrappers that merely duplicate a crate initializer. Pure data crates
such as `tackt-primitives` require no runtime initializer.

Initialization must be safe to invoke repeatedly. If initialization fails, startup must fail
before the service accepts work.

## Dependency direction

Allowed dependency flow:

```text
services  -> postgres   -> primitives
services  -> nats       -> primitives
services  -> telemetry
```

Infrastructure and primitive crates must not depend on services. Avoid circular dependencies.

## Messaging reliability

Delivery guarantees must match the authority of the underlying data. Account observations and
price evidence deliberately use different durability paths:

- PostgreSQL is authoritative for users and source-account associations;
- Core user-lifecycle events are an acceleration channel, not a durable source of truth, and
  consumers must reconcile from PostgreSQL;
- for an account observation, the acknowledged publish to the durable observation JetStream
  stream is the Indexer's durable completion boundary; EVM facts share one block batch with a
  durable canonical hash suffix, explicit reorg range and prior subject sequence. Fact IDs remain
  deterministic; batch delivery identities distinguish a new adoption from a retry. Non-EVM
  observations retain individual event-ID deduplication. Typed contracts and publication identities
  are owned by `tackt-nats`;
- for an accepted price update, the acknowledged price-evidence JetStream publish is the Indexer's
  durable completion boundary; publishing must use a deterministic evidence identity and the typed
  contract owned by `tackt-nats`;
- current provider/feed state and sequence watermarks belong in a rebuildable NATS KV projection
  maintained by a downstream durable consumer, never in the Indexer hot path;
- immutable price history belongs in ClickHouse, populated by a separate durable NATS consumer that
  acknowledges only after ClickHouse confirms persistence; the Indexer must neither connect to nor
  wait for ClickHouse;
- a failed optional NATS publish after a committed Core API mutation must be logged but must not
  turn the successful database mutation into an API failure;
- risk streams use durable JetStream delivery with explicit acknowledgement, retry, and
  idempotency behavior owned by `tackt-nats` and the consuming service. Ledger consumes Indexer
  observations durably and records every observation in PostgreSQL, including unknown assets
  and non-EVM activity. Indexer is the trusted authority for external facts, account/asset
  attribution, canonicality and finality; Ledger must not revalidate those facts or gate their
  persistence on asset support. Observation contracts are typed data, not business validators.
  Ledger applies trusted block updates to stored state and owns delivery idempotency and
  accounting rules. The Postgres crate persists typed updates atomically; Ledger owns application
  write authority.
  This evidence ingress does not accept accounting commands. Core's existing command ingress
  remains authenticated HTTP until the separate command/result migration is implemented.

Do not silently weaken a durable flow to core NATS or add a second observation or price write to
the Indexer. See `architecture/indexer.md` for the Indexer's persistence, failure, and ownership
boundaries.

## Dependency management

The root `[workspace.dependencies]` table is the single source of truth for every third-party
dependency version and every internal workspace path. Member crates must opt in with
`dependency.workspace = true`; they must not repeat versions or relative paths.

Keep broadly required features on the root declaration. Add crate-specific features at the
member declaration with `{ workspace = true, features = [...] }`. Remember that Cargo features
are additive and must not be used as ownership boundaries.

The workspace has one committed root `Cargo.lock`. Do not add member lockfiles. CI and release
commands must use `--locked` wherever Cargo resolves or builds dependencies. Update dependencies
deliberately with targeted commands such as `cargo update -p <crate>` and commit the resulting
lockfile change.

All workspace members use Rust edition 2024 through `edition.workspace = true`. Do not introduce
crate-local edition declarations or move a member back to an older edition.

`deny.toml` is the dependency policy authority. New dependencies must come from an approved
source, use an allowed license, and pass RustSec advisory checks. Do not add advisory, license,
crate, or duplicate-version exceptions without a written justification in the configuration and
the pull request.

## Security boundaries

- Never store private keys, seed phrases, credentials, bearer tokens, or other signing material in
  user metadata, domain records, logs, fixtures, or committed configuration.
- Remote PostgreSQL connections must use verified TLS. The insecure-development override is only
  for explicitly controlled development environments and must not become a production default.
- Services carrying credentials must not bind beyond loopback over cleartext unless an explicitly
  configured TLS-terminating proxy or service mesh protects the connection.
- Internal HTTP does not mean trusted input. Preserve authentication boundaries, validate all
  externally supplied identifiers and quantities, and avoid exposing mutation routes from
  read-only services.
- Ledger callers may use only reviewed typed commands. Do not add raw posting, raw SQL, or generic
  adjustment escape hatches; controlled adjustments must retain approval and audit evidence.
- Treat database URLs, backup destinations, restore targets, migration repair tools, and production
  data as sensitive. Operational scripts must validate narrowly scoped targets and fail closed
  rather than guessing.
- Do not weaken transport security, authorization, audit attribution, or financial validation for
  local convenience. Development-only overrides must be explicit and visibly named as insecure.

## Change guidelines

- Put code with the crate that owns the relevant external system or protocol.
- Keep public APIs typed and narrow; expose capabilities rather than implementation details.
- Use one current typed contract for each durable NATS message; no compatibility version stamps.
- Coordinate producers and consumers when changing contracts. Runtime sequence and revision
  checks, validation, and SQLx migration ordering remain required.
- Keep unrelated service behavior out of shared infrastructure crates.
- Update the root README when adding, removing, or materially changing a workspace crate.

## Required verification

**VERY IMPORTANT — DOCKER COMPOSE IS THE ONLY ALLOWED TEST INFRASTRUCTURE.**

Any test that needs PostgreSQL or NATS **MUST** use the disposable services defined in
`deploy/compose.yaml`, locally and in CI. **NEVER** test against host-installed services,
manually started containers (`docker run`), persistent development databases, shared environments,
or production. Do not substitute another database server or broker when Compose is unavailable.
Unit tests that need no external services may still run directly with Cargo.

Agents may choose the database name and create isolated test databases inside the Compose
PostgreSQL instance. `indexer` is only the default, not a required name. Set `POSTGRES_DB`
before starting Compose and use the same name in `DATABASE_URL`; credentials remain `tackt:tackt`.

Start the test infrastructure and configure the test process from the workspace root:

```sh
export POSTGRES_DB=tackt_test # Choose a database name for this test run.
docker compose -f deploy/compose.yaml up -d --wait postgres nats
export DATABASE_URL="postgres://tackt:tackt@127.0.0.1:5432/${POSTGRES_DB}"
export NATS_URL=nats://127.0.0.1:4222
```

**Do not unset these URLs to bypass integration tests or report skipped tests as verified.**
If Docker Compose cannot run, explicitly report infrastructure-dependent verification as blocked.
After testing, run `docker compose -f deploy/compose.yaml down` to stop the disposable stack
and discard its data.

Run these commands from the workspace root before handing off changes:

```sh
./scripts/check-boundaries.sh
cargo metadata --locked --format-version 1 > /dev/null
cargo fmt --all -- --check
cargo deny check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
```

The boundary check must continue rejecting SQLx, embedded SQL, migration directories, and
direct `async-nats` dependencies inside services. Extend it when new architectural rules can
be checked mechanically.
