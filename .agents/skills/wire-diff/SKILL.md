---
name: wire-diff
description: Compare Tackt PostgreSQL mappings, NATS contracts, protobuf or serde types, and ClickHouse ingestion surfaces between two revisions or a deployed revision and a candidate. Use to identify pending wire or schema compatibility risks.
---

# Tackt wire-surface diff

The default comparison is the current working revision against `origin/main`. If the user asks for deployed-versus-candidate analysis, discover the live revision from Tackt's actual deployment source; never reuse another repository's tag, GitOps, cluster, or service conventions.

## Inventory

Read the root and nested `AGENTS.md`, then inspect changes affecting:

- `tackt-primitives` domain types and the locked asset registry;
- `tackt-postgres` migrations, row mappings, and typed persistence APIs;
- `tackt-nats` subjects, streams, envelopes, serializers, consumers, acknowledgements, retries, and idempotency;
- protobuf definitions and generated bindings;
- established ClickHouse DDL, ingestion tables, materialized views, row types, and migrations;
- service public request/response types and cross-crate callers.

## Compatibility analysis

For every changed surface, identify producers, consumers, authoritative source, old/new encoding or schema, mixed-version behavior, rollout order, reconciliation or rebuild path, and tests.

- Existing PostgreSQL migrations are append-only.
- Durable NATS messages use one current typed shape without compatibility version stamps. Preserve JetStream delivery semantics and identify producers and consumers that must change together.
- Acceleration events remain non-authoritative and require PostgreSQL reconciliation.
- RowBinary and other positional formats require exact field order, count, null markers, widths, and byte-level encoding; trailing additions are not assumed safe.
- ClickHouse projections do not replace PostgreSQL or ledger authority.
- Asset identity, address, chain, and decimals remain sourced only from the locked registry.

Classify each change as compatible, conditionally compatible with rollout requirements, breaking, or unverified. Cite the exact changed files and evidence. Do not deploy, migrate, replay, publish, or modify external systems during the audit.

Return a producer/consumer matrix, breaking-change findings, required rollout order, missing compatibility tests, and the commands used to establish the revision range.
