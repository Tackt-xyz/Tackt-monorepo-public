---
name: schema-change
description: Implement or review a Tackt database schema change, routing authoritative transactional data to tackt-postgres and analytical or ingestion data to the future ClickHouse infrastructure crate.
---

# Tackt schema change

First classify the target database from repository ownership and the requested data semantics. Never infer ClickHouse merely from scale, and never move authoritative state out of PostgreSQL as part of an incidental schema change.

## PostgreSQL

PostgreSQL is authoritative for the domains assigned in the root `AGENTS.md`. All SQL, migrations, SQLx, transactions, row mapping, and typed persistence APIs stay in `crates/postgres`.

1. Add a new append-only migration under `crates/postgres/migrations`; never edit an applied migration.
2. Keep `PgRepository::initialise` responsible for connecting and applying migrations safely and repeatedly.
3. Update narrow domain-oriented APIs inside `tackt-postgres`; never expose pools, SQLx rows or transactions, or raw-SQL escape hatches.
4. Update service orchestration only through those typed APIs.
5. Test constraints, compatibility, transactions, initialization idempotency, and failure cases.

## ClickHouse

Use ClickHouse only for analytical, ingestion, or projection workloads explicitly assigned to it. PostgreSQL and ledger authority remain unchanged.

1. If the ClickHouse owning crate, canonical schema, migration layout, and initialization contract do not yet exist, design and establish them deliberately before adding tables. Update the root README and architecture instructions as part of that foundational change.
2. Read `clickhouse-best-practices` rules relevant to the schema, queries, and ingestion format.
3. Add an append-only migration and matching canonical DDL using the repository’s actual conventions.
4. For Rust-backed rows or NATS ingestion, use `clickhouse-struct-sync`; contracts and subjects remain owned by `tackt-nats`.
5. Plan compatibility, rollout order, reconciliation/rebuild behavior, backfills, and recovery.

## Safety and verification

Destructive changes, production backfills, checksum repair, target selection, TTL shortening, partition operations, and live deployment require explicit authorization. Operational tooling must resolve exact targets and fail closed.

Run database-specific checks and every command required by the root `AGENTS.md`. Return the authority decision, migration strategy, compatibility analysis, affected APIs/contracts, rollout plan, and verification.

