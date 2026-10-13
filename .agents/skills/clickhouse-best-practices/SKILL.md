---
name: clickhouse-best-practices
description: Review or design Tackt ClickHouse schemas, queries, ingestion, and configuration using the bundled ClickHouse rules. Use for planned or implemented analytical storage; do not apply ClickHouse advice to authoritative PostgreSQL or ledger state.
license: Apache-2.0
metadata:
  author: ClickHouse Inc., adapted for Tackt
  version: "0.4.0-tackt.1"
---

# Tackt ClickHouse best practices

ClickHouse is a future analytical and ingestion capability. It does not replace PostgreSQL authority for users and external observations, the ledger journal, or other state assigned to PostgreSQL by the root `AGENTS.md`.

Before reviewing ClickHouse work:

1. Read the root and applicable nested `AGENTS.md`.
2. Inspect the actual Tackt ClickHouse layout and configuration; do not assume a database, cluster, engine, topology, deployment platform, or table naming convention.
3. Read only the relevant files under `rules/` and cite their filenames in recommendations.
4. Separate general ClickHouse guidance from Tackt-specific facts verified in the repository.
5. Treat destructive mutations, TTL shortening, partition replacement, schema cutovers, and live-cluster operations as high risk and require explicit authorization before execution.

## Routing

- Schema and type design: read applicable `schema-*.md` rules.
- Query review or performance: read applicable `query-*.md` rules.
- Ingestion, batching, formats, or mutations: read applicable `insert-*.md` rules.
- NATS-engine tables or NATS-fed materialized views: also use `nats-subject-audit`; Tackt NATS subjects and contracts remain owned by `tackt-nats`.
- Rust-struct-backed schema changes: use `clickhouse-struct-sync`.
- Any schema change: use `schema-change` to classify PostgreSQL versus ClickHouse first.

For formal reviews, report the relevant rule, repository evidence, applicability, risk, and recommended verification. Do not copy deployment commands or constants from another environment.

