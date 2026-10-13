---
name: clickhouse-struct-sync
description: Synchronize a Tackt Rust wire or domain struct with its ClickHouse table, ingestion table, materialized views, migration, and canonical schema. Use only after the Tackt ClickHouse crate and schema layout exist.
---

# Tackt struct-to-ClickHouse synchronization

This skill is future-facing. First discover the actual ClickHouse owner, migrations, canonical schema, and deployment checks in the repository. If they do not yet exist, stop and propose the owning crate and layout; do not generate a migration against invented paths.

## Ownership

- Universal IDs, observations, amounts, timestamps, and provenance remain in `tackt-primitives`.
- NATS subjects, envelopes, serialization, delivery, and retry behavior remain in `tackt-nats`.
- ClickHouse connection management, DDL, migrations, row mapping, and analytical persistence belong in the dedicated Tackt ClickHouse infrastructure crate once established.
- PostgreSQL remains authoritative wherever the root `AGENTS.md` assigns it authority. ClickHouse copies are projections and must be rebuildable or reconcilable.

## Procedure

1. Read the governing instructions and locate the Rust source type, NATS contract, producer, every ClickHouse consumer, canonical DDL, and prior migrations.
2. Compare field names, declaration order, nullability, numeric precision, timestamps, enums, IDs, serde behavior, and the actual ingestion format.
3. Read the applicable `clickhouse-best-practices` rules, especially type mapping and ingestion-format guidance.
4. Classify the change:
   - additive compatible field;
   - removal or rename;
   - type or encoding change;
   - materialized-view-only change;
   - index, partition, ordering-key, or TTL change;
   - backfill or destructive operation.
5. Coordinate producer and consumer contract changes; preproduction uses no compatibility versions. Never assume a position-based format tolerates reordering, insertion, removal, or type changes.
6. Add one append-only migration using the repository’s established naming and metadata format. Update canonical DDL so a fresh environment reaches the same schema.
7. Update all affected ingestion tables and materialized views. For NATS ingestion, confirm subjects and payload shapes through `tackt-nats`.
8. Add static checks or tests that compare the Rust contract to ClickHouse column names, order, types, and nullability where deterministic comparison is possible.
9. Run the ClickHouse-specific repository checks plus the complete root verification suite.

## Fail-closed rules

- Never guess cluster names, database names, credentials, Kubernetes namespaces, table engines, replication paths, or deployment commands.
- Never edit an applied migration.
- Never perform a live migration, backfill, mutation, partition operation, queue cutover, or destructive DDL without explicit authorization and a verified target.
- For an incompatible ingestion change, coordinate all producers and consumers and use fresh disposable state for the cutover.

Return the compatibility analysis, affected producer/consumer matrix, migration and canonical-schema diff, rollout order, rollback/recovery plan, and verification evidence.

