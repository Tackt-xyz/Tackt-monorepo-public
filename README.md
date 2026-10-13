# Tackt monorepo

The Tackt backend is a Cargo workspace with compiler-enforced platform boundaries.

```text
crates/
  primitives/   dependency-light domain values and identifiers
  postgres/     all migrations, SQLx usage, SQL, and typed persistence APIs
  nats/         NATS transport, subjects, envelopes, and published message contracts
  telemetry/    logging and tracing defaults
services/
  core-api/     user lifecycle, demo custody keys, account API, and signing/funding boundaries
  indexer/      external-account ingestion and approved price-source normalization
  ledger/       immutable PostgreSQL-backed double-entry accounting service
  risk-engine/  deterministic risk engine and its NATS/JetStream host service
architecture/  accepted service and data-flow decisions
deploy/         local and production deployment assets
scripts/        architecture and CI checks
asset-registry.json  the locked Release 1A asset list
```

Core generates Ethereum and Solana custody keys on operator creation and first-login signup.
For the demo, migration `0008` stores these unencrypted in a separate `user_signing_keys` table,
atomically with the new user. Retries preserve the original keys; existing users without keys are not backfilled.
Migration `0009` indexes the Ethereum address and Solana public key for lookup. Core can sign
EIP-1559, Solana legacy/v0 transactions, and supported HyperCore actions by user ID through
separate internal EVM, SVM, and HyperCore signing modules.
See [Core API](services/core-api/README.md#demo-custody-keys) for storage formats and scope.

For the small demo, Risk can periodically sample explicitly configured accounts into PostgreSQL
through a nonblocking handoff. Core serves authenticated latest/history reads. This is the
existing streamed margin-health model, with unknown source freshness, and is separate from
Tackt credit health. See [account-health history](architecture/risk-health-history.md).

## Asset registry

`asset-registry.json` at the workspace root is the single definition of which assets Release 1A
supports and what each one is: symbol, chain, canonical CAIP-19 identity, token address, and
decimals. Services read it only through `tackt_primitives::assets`, which embeds the file at
compile time, validates it once, and fails closed on every asset or chain it
does not describe. Unsupported assets may still be recorded as external evidence; they are never
credited, withdrawn, or signed automatically.

The registry is locked, not configured: no deployment can widen the supported set, and two
services built from the same commit cannot disagree about what an asset is. Changing an entry
means editing the file and rebuilding the services. The registry has one
asset list: each entry holds its identity, decimals, optional `ledger_asset`, and optional
`collateral` settings (factor, enablement, asset index, venue, and price reference). Chains,
venues, price factors, and perp markets are common registry records; there is no separate Risk
asset list. Core can read `assets::info(LedgerAsset::Usdc).collateral` alongside its decimals.
Each entry also declares `pricing`: `{"kind":"aggregate"}` requires a market quote for Core's
account summary; `{"kind":"fixed_usd","price_e8":100000000}` values one token at $1 USD.
All USDC representations explicitly use the latter. Fixed prices must be positive integers in
1e-8 USD units and take precedence over received quotes; classification never implies pricing.
Core values both holdings and debt from this rule without inventing a source timestamp.
Streamed Risk price factors and credit eligibility remain separate policies.
`assets::registry().catalog()` exposes all twenty-two canonical entries; existing accounting lookups
remain limited to the twenty entries with Ledger identities. HyperCore entries have
`ledger_asset: null` and remain unavailable for Ledger accounting. Credit-policy calculation
rules remain in Risk's Rust code. See [registry rollout](architecture/asset-registry.md).

Each representation declares a required structured `classification`:

```json
{
  "classification": {
    "economic_asset": "SOL",
    "instrument_type": "lst",
    "traits": ["yield_bearing"]
  }
}
```

This is the classification used for the registered SOL liquid staking tokens. Economic groups are
`ETH`, `BTC`, `SOL`, and `USDC`. Instrument types are `native`, `wrapped`, `lst`, `wrapped_lst`,
`yield_bearing_btc`, and `stablecoin`. The initial trait vocabulary is `yield_bearing`; an empty
array explicitly assigns no traits. All three fields are required. Unknown fields or values,
duplicate traits, native/token type mismatches, and non-BTC `yield_bearing_btc` classifications
are rejected. Traits are explicit, with no inheritance from instrument types.

Read mappings through `assets::info(asset).classification`. For example, `ETH_BASE` belongs to
`ETH` and `USDC_BASE` belongs to `USDC`, while their Ledger identities remain separate.
`assets::registry().representations(EconomicAsset::Sol)` returns the SOL family;
`with_instrument_type(InstrumentType::Lst)` and `with_trait(AssetTrait::YieldBearing)` filter
across families. These iterators preserve registry order and can be combined with ordinary
iterator filters. Classification is independent of the ERC-20/SPL token standard and chain.

Grouping does not grant collateral eligibility, an LTV, or a conversion rate. Existing Risk
policies 1–4 retain their reviewed representation set: new registry entries contribute no borrowing
or liquidation capacity until explicitly admitted by Risk policy. This also prevents LSTs and LBTC
from being pooled one-for-one with SOL, ETH, or BTC without conversion-rate valuation. Types and
traits describe instruments; they do not authorize financial treatment.

Schema 7 / generation 9 adds required pricing rules. Schema 6 / generation 8 introduced
`classification` and twelve additional representations covering the fifteen SOL/ETH/BTC
instruments below. Application contracts use one current shape without compatibility version
metadata. Deploy matching producers and consumers together when changing them.

To add a representation, follow the existing asset-registration path and explicitly assign its
classification in the registry. New vocabulary needs an `EconomicAsset`, `InstrumentType`, or
`AssetTrait` enum variant as appropriate. Rebuild services after registry changes.
Existing accounting codes retain their identities: `BTC` is Ethereum WBTC, `SOL` is the Ethereum
Wormhole-wrapped token, and `SOL_SOLANA` is native SOL. New accounting codes use uppercase.

The fifteen requested instruments are registered on one selected deployment each:

| Economic asset | Instruments (accounting codes) | Deployment sources |
| --- | --- | --- |
| SOL | SOL (`SOL_SOLANA`), WSOL (`WSOL`), JitoSOL (`JITOSOL`), mSOL (`MSOL`), JupSOL (`JUPSOL`) | [Solana native mint](https://solana.com/docs/tokens/basics/sync-native), [Jito](https://www.jito.network/docs/jitosol/faqs/technical-faqs/), [Marinade](https://docs.marinade.finance/marinade-protocol/protocol-overview/marinade-liquid/msol-token), [Jupiter](https://docs.jup.ag/user-docs/earn/stake-sol/jupsol) |
| ETH | ETH (`ETH`), WETH (`WETH`), stETH (`STETH`), wstETH (`WSTETH`), cbETH (`CBETH`) | [WETH](https://ethereum.org/wrapped-eth/), [Lido](https://docs.lido.fi/deployed-contracts/), [Coinbase](https://www.coinbase.com/en-in/cbeth) |
| BTC | WBTC (`BTC`), cbBTC (`CBBTC`), tBTC (`TBTC`), LBTC (`LBTC`), SolvBTC (`SOLVBTC`) | [WBTC](https://www.wbtc.network/assets/wrapped-tokens-whitepaper.pdf), [Coinbase](https://www.coinbase.com/en-gb/blog/coinbase-wrapped-btc-cbbtc-is-now-live), [Threshold](https://docs.threshold.network/contract-addresses/tbtc), [Lombard](https://docs.lombard.finance/learn/transparency/smart-contracts), [Solv](https://docs.solv.finance/developer-guide/contracts) |

All new SOL-family entries use Solana; all new ETH/BTC-family entries use Ethereum. The existing
Base and USDC representations remain registered, giving twenty distinct accounting assets in all.
Addresses and decimals remain solely in the root registry. Decimal cross-checks use the
[Aave Ethereum V3 address book](https://github.com/bgd-labs/aave-address-book/blob/main/src/AaveV3Ethereum.sol),
[Aave V2 stETH entry](https://github.com/bgd-labs/aave-address-book/blob/main/src/AaveV2Ethereum.sol),
[Optimism cbETH metadata](https://github.com/ethereum-optimism/ethereum-optimism.github.io/blob/master/data/cbETH/data.json),
[Solv's Ethereum data example](https://docs.solv.finance/developer-guide/interfaces-of-services/data-services/user-assets),
and the Solana issuer references above. JupSOL metadata is also listed in
[Jupiter's token list](https://discuss.jup.ag/t/ecosystem-master-token-list/19786).

Chains have one identity, their `caip2`. EVM callers use `AssetInfo::evm_chain_id()`, which returns
`None` for Solana. Solana lookups preserve base58 case; SPL mints must decode to exactly 32 bytes.
This onboarding provides registry identities and typed accounting vocabulary. Solana ingestion
and deposit accounting remain separate work, as before. Registry membership also does not add
staking/rebase accrual to the Ledger or a signing adapter for every instrument. In particular,
[stETH rebases](https://docs.lido.fi/contracts/lido/); its transfer amounts must not be treated as
a complete model of custody balance changes. [LBTC's current strategy](https://docs.lombard.finance/use/faq/lbtc)
is yield-bearing BTC rather than a SOL/ETH-style LST.

## Persistence ownership

`tackt-postgres` is the sole schema and data-access authority. Services must not depend on
SQLx, embed SQL, own migrations, or receive `PgPool`, row, or transaction types. They consume
typed repository APIs and domain values from `tackt-primitives`.

Each infrastructure crate owns a typed `initialise` entry point. Services compose the
initializers they need before starting workers or accepting traffic:

- `tackt_postgres::PgRepository::initialise` connects and applies pending migrations.
- `tackt_nats::initialise` establishes a managed NATS connection.
- `tackt_telemetry::initialise` installs structured logging once per process.

SQLx tracks versions and checksums in `_sqlx_migrations`, making repeated Postgres
initialization idempotent and serializing concurrent migrators. `primitives` remains a pure
data crate and intentionally has no runtime initialization.

Persistence follows the data path rather than forcing every workload through PostgreSQL:

| Data | Durable boundary | Derived/archive stores |
|---|---|---|
| Users and source-account associations | PostgreSQL | user lifecycle events are refresh triggers only |
| External-account observations | acknowledged JetStream publish (one complete EVM block per message) | Ledger atomically persists facts, accounting and chain/delivery cursors in PostgreSQL |
| Price evidence | acknowledged JetStream publish | NATS KV latest-state projection and ClickHouse immutable archive |
| Aggregate prices | acknowledged JetStream publish | Core API per-process Moka cache for display; independent Risk consumption |

For each accepted price update, the Indexer's only durable side effect is its typed JetStream
publish. A downstream durable consumer projects latest provider/feed state into NATS KV, and a
separate consumer batches immutable evidence into ClickHouse. PostgreSQL, NATS KV, and ClickHouse
are not price-ingestion hot-path writes. The complete decision is documented in
[`architecture/indexer.md`](architecture/indexer.md).

The default price feeds also include JitoSOL/USD from Kraken's public WebSocket as a
single-source demo price. See the [Indexer configuration](services/indexer/README.md#messaging-nats_url-required)
for JitoSOL-only overrides and freshness behavior. Pricing does not grant collateral eligibility.

```sh
docker compose -f deploy/compose.yaml up -d --wait postgres nats
cp .env.example .env
set -a; . ./.env; set +a
cargo run --locked -p tackt-nats --example provision
cargo run --locked -p tackt-indexer -- serve
```

The Compose stack is disposable local development infrastructure. PostgreSQL uses username
`tackt`, password `tackt`, and database `${POSTGRES_DB:-indexer}`; NATS has JetStream enabled. Both publish
ports only on loopback (`5432` and `4222`) and have automatic restarts disabled.
PostgreSQL data and NATS JetStream state use temporary `tmpfs` mounts, with no persistent
volumes. Stopping, restarting, or removing either container loses all of that service's data;
starting it again creates fresh state. Services apply PostgreSQL migrations on startup.

The undeployed migration history has been consolidated into four ordered feature baselines:

1. `0001_core_users`: user lifecycle, source accounts, and immutable external identities.
2. `0002_ledger_accounting`: journal, balances, and reservations.
3. `0003_ledger_observations`: observations, deposits, delivery cursors, and chain state.
4. `0004_core_actions`: action workflows, audit records, and Risk evidence.

This is a fresh-database baseline, not an upgrade path from the previous migration history.
Recreate any disposable development database that applied the old files using Compose `down`
and `up`; do not repair its SQLx checksums. Each feature has a matching down migration, with
rollback guards for durable action and observation processing state. The shared `pgcrypto`
extension remains after rollback. Future shared or deployed migrations remain append-only.

Migrations use the database user that runs them as the owner of new objects. They do not create
Ledger roles, transfer ownership, or install role-specific grants. A database owner without
superuser or `CREATEROLE` privileges can run the full history, including its trusted `pgcrypto`
extension and ordinary SQL/PLpgSQL functions. Ledger enforces write policy in its typed API.

The removal of Ledger roles changes migrations 0002 and 0003. A database that has only applied
0001 (including one where 0002 failed and rolled back) can retry with the rebuilt binary.
Databases that already applied the old 0002 or 0003 will fail SQLx checksum validation. Recreate
disposable databases; databases with retained data require a separately planned migration
transition. Do not bypass checksum validation or drop existing cluster roles automatically.

Set `POSTGRES_DB` before starting Compose to choose another database name, and set the database
in `DATABASE_URL` to match. With the default name, use
`DATABASE_URL=postgres://tackt:tackt@127.0.0.1:5432/indexer` in existing `.env` files.

Stop and remove the local stack with:

```sh
docker compose -f deploy/compose.yaml down
```

## Messaging

`tackt-nats` is the sole authority for everything that goes over NATS: subjects, stream
names, envelopes, typed serialization, and delivery conventions. The complete subject/stream
table lives in the crate docs (`crates/nats/src/lib.rs`). In summary:

- `tackt.observations.{source}` — durable JetStream observations from the Indexer;
  Ledger acknowledges only after committing the observation and accounting in PostgreSQL.
- `tackt.observations.hyperliquid.{trades,events}` — the two Hyperliquid feeds. JetStream
  automatically copies their latest observations into the
  `TACKT_OBSERVATIONS_HYPERLIQUID_RESUME` KV bucket. Indexer reads each key on restart and
  replays its block inclusively; persistent event-ID deduplication prevents double counting.
  Indexer performs no additional KV write. Portfolio snapshots remain on the unsuffixed subject.
- `tackt.prices.evidence` — the Indexer's acknowledged JetStream handoff for normalized price
  evidence. One `PriceEvidenceEnvelope` states which configured authority reported which
  fixed-point quote for which asset or market, at which
  provider sequence and timestamps, with the provenance and bounded raw payload behind it, under
  a deterministic `evidence_id` that makes redelivery idempotent. It sits outside the
  `tackt.risk.*` family and shares no required field with the Risk Engine's executable
  `PriceUpdate`. Independent durable consumers feed Risk, project latest state into NATS KV, and
  archive immutable evidence in ClickHouse, each under its own durable name on the
  `TACKT_PRICE_EVIDENCE` stream. That stream is where the price pipeline fails closed: it retains
  seven days so a stalled consumer can catch up, deduplicates one evidence identity for ten
  minutes, and on reaching its 4 GiB capacity rejects the publish rather than deleting evidence
  nobody has archived — which stops Indexer price ingestion instead of losing it. There is no
  core-NATS path to the subject.
- `TACKT_USERS` — JetStream user-lifecycle events published by Core API and consumed by the
  indexer to refresh its client-side Hyperliquid address registry and portfolio state.
- `TACKT_RISK_{PRICES,PATCHES,SWEEPS,EVENTS}` — JetStream streams consumed and published by
  the risk-engine service.

Provision streams explicitly before starting any service:

```sh
NATS_URL=nats://127.0.0.1:4222 cargo run --locked -p tackt-nats --example provision
```

This creates all streams and Ledger's durable consumer, preserving existing delivery positions.
Set `RISK_ENGINE_STREAM_PREFIX` here too if Risk uses a custom prefix. Service startup only checks
required streams; missing or incompatible streams fail startup, never trigger creation or updates.
Core requires users and aggregate prices when NATS is configured; Indexer requires users and observations, plus price
evidence and aggregates when price feeds are enabled; Ledger requires observations and its durable;
Risk requires its four streams. Risk retains snapshot-positioned consumer replay, and live user
signals retain ephemeral consumers. Stream lifecycle is always an operator responsibility.

`NATS_URL` is optional only for core-api, which warns and disables user lifecycle events and the local price cache
without it. The Indexer and the risk-engine service both require it: an accepted observation or price is
incomplete until JetStream acknowledges it. ClickHouse and the downstream KV projector do not
participate in Indexer readiness. Ledger also requires NATS: it records trusted observations and
their accounting effects atomically, acknowledging only after PostgreSQL commits. Core continues
to submit typed requests over authenticated HTTP. Accounting rules live in Ledger, not SQL.

Run the Core API with `cargo run --locked -p tackt-core-api`. Its provisioning routes remain operator-only. User authentication is required at startup, and Solana and EVM wallet login are always available. Configured OIDC providers add login identities; all methods share Core-issued JWTs with refresh and logout. Request authentication verifies JWTs locally without PostgreSQL; handlers load user data as needed. Logout blocks refresh immediately, while issued access JWTs remain valid for up to 15 minutes. See
[`services/core-api/README.md`](services/core-api/README.md) for provisioning endpoints,
authorization, and examples. User/account data belongs to Core; the Indexer reads account
associations from PostgreSQL but exposes no user or observation endpoints.

Core user profiles include beta access, defaulting to false. Authenticated users redeem
`CORE_API_BETA_INVITE_CODE` through `POST /api/v1/users/me/beta-access`; PostgreSQL stores enrollment.

Core's generated [OpenAPI contract](services/core-api/openapi.json) covers user and operator routes.
`GET /api/v1/users/me/account-summary` combines the current Ledger balance snapshot with latest
aggregate prices and 85% demo LTV / 90% liquidation threshold; `/api/v1/users/me/activity` pages the user's Ledger
transactions. See [account page](services/core-api/README.md#account-page-demo) for calculations,
missing prices, and activity semantics.
Authenticated `GET /api/v1/assets` and `GET /api/v1/markets` expose the shared registry's complete
asset catalog and configured perp-market tiers; see [catalogs](services/core-api/README.md#asset-and-market-catalogs).
Regenerate with `cargo run --locked -p tackt-core-api --example export_openapi -- --write`;
CI rejects a stale specification.

Run the internal Ledger Service separately with `cargo run --locked -p tackt-ledger`. Its account model,
typed commands, local setup, audit/export behavior, and recovery procedures are documented in
[`services/ledger/README.md`](services/ledger/README.md).

The risk engine runs as a service consuming the `TACKT_RISK_*` JetStream streams and
publishing validated advisory recommendations on `TACKT_RISK_RECOMMENDATIONS`, while its
financial hot path stays a deterministic library free of network and database I/O. Its required
environment and run command are in
[`services/risk-engine/README.md`](services/risk-engine/README.md). Run its benchmark with:

```sh
cargo run --locked --release -p risk-engine --bin risk-benchmark
```

Its [venue margin read APIs](services/risk-engine/README.md#per-account-venue-margin-health)
expose standard Hyperliquid cross/isolated margin ratios, weakest-pool health, and gross venue
leverage alongside total account health. Exact current-tier maintenance preserves pool isolation;
unified account mode is explicitly unsupported by the native venue query.

Its separate [on-demand credit calculator](services/risk-engine/README.md#on-demand-credit-health)
values free spot and pledged perp equity against one account debt total without storing health.
ETH/BTC/SOL spot policy is 85% maximum LTV and 90% liquidation threshold, with
10% opening / 5% maintenance reserves on absolute current perp notional. It selects eligible
collateral from Ledger buckets and custody evidence, pools registry-defined atomic quantities
across representations, retains controlled reservations for liquidation only, and counts
controlled pending deposits for borrowing and liquidation, accepting reorg exposure. The host initializes Ledger balance inputs from
PostgreSQL and refreshes them periodically; `RISK_ENGINE_DATABASE_URL` or `DATABASE_URL` is
required. This credit library API is not yet wired into service requests.

## Verification

```sh
./scripts/check-boundaries.sh
cargo metadata --locked --format-version 1 > /dev/null
cargo fmt --all -- --check
cargo deny check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
```
