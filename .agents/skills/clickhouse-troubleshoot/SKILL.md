---
name: clickhouse-troubleshoot
description: Diagnose Tackt ClickHouse errors, ingestion gaps, NATS-engine failures, corruption, memory pressure, replication issues, or slow queries using repository and read-only runtime evidence.
---

# Tackt ClickHouse troubleshooting

Start from the reported symptom and establish the exact environment, revision, database, table, time window, and expected data path. Do not reuse cluster constants or recovery commands from another system.

## Evidence order

1. Trace the repository path: producer → `tackt-nats` contract and subject → ClickHouse ingestion table → materialized view → target table → query.
2. Inspect recent relevant changes, migrations, canonical DDL, configuration, and deployed versions.
3. When live access is authorized, gather read-only evidence from system tables, logs, query plans, stream/consumer state, and table metadata.
4. Classify the cause: contract/column mismatch, orphan subscriber, schema drift, failed materialized view, replication health, excessive parts, mutation pressure, corrupt data, memory/query shape, disk limits, or application misuse.
5. Read and cite the applicable `clickhouse-best-practices/rules/` files.

For NATS ingestion failures, use `nats-subject-audit` and compare every subscriber’s subject, payload shape, and encoding with `tackt-nats`.

Recommend the least destructive verified remedy. Table rebuilds, partition replacement, mutation cancellation, queue recreation, consumer deletion, restarts, configuration changes, and data replay require explicit authorization. Before such work, state impact, exact target, recovery path, and success checks.

Report confirmed cause, evidence, rejected hypotheses, safe remediation, and remaining unknowns. If Tackt’s ClickHouse deployment conventions have not yet been established, identify that gap rather than guessing.

