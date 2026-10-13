---
name: post-deploy-verify
description: Verify a Tackt deployment or database migration using service health, PostgreSQL authority, ClickHouse ingestion and query health, NATS delivery, logs, and domain invariants.
---

# Tackt post-deploy verification

Confirm the exact environment and deployed revision. Prefer read-only observation.

1. Verify changed processes are healthy and completed required initialization before accepting work.
2. Check recent logs for migration, connection, deserialization, materialized-view, acknowledgement, redelivery, retry, arithmetic, authorization, and persistence errors without exposing secrets.
3. Verify authoritative PostgreSQL state for users and observations; do not trust acceleration messages or ClickHouse projections alone.
4. For changed NATS flows, compare live streams and consumers with `tackt-nats`; inspect pending, redelivery, acknowledgement, activity, and durable idempotent processing.
5. For ClickHouse changes, verify the applied migration and schema on every intended node, ingestion freshness, materialized-view progress, query correctness, replication health, part counts, mutation/merge health, resource pressure, and absence of schema/encoding errors.
6. Reconcile representative ClickHouse projections with their authoritative source and verify rebuild/replay assumptions.
7. Check relevant domain invariants, including immutable balanced ledger entries, reservation/balance projections, deterministic risk results, and fail-closed asset lookup.
8. Compare reliable pre-deploy baselines with post-deploy indicators.

Do not mutate data, replay or purge messages, repair migrations, restart services, rebuild tables, or roll back without explicit authorization. Report verified evidence, anomalies, missing access, and the safest next action.
