---
name: pre-deploy-check
description: Assess a Tackt release, PostgreSQL migration, or ClickHouse migration before deployment for ownership, compatibility, security, data safety, messaging reliability, and executable verification.
---

# Tackt pre-deploy check

This is read-only unless the user also requests fixes.

1. Resolve the exact revision, services, migrations, configuration, and environment.
2. Read governing instructions and inspect the complete diff against the deployment base.
3. Verify ownership: PostgreSQL work in `tackt-postgres`; NATS contracts and behavior in `tackt-nats`; universal types and asset facts in `tackt-primitives`; telemetry in `tackt-telemetry`; ClickHouse work in its dedicated infrastructure owner once established.
4. Check rollout compatibility across database migrations, typed APIs, typed messages, producers, consumers, and coordinated contract changes. Existing migrations are immutable.
5. Check PostgreSQL startup migration safety and authoritative reconciliation.
6. For ClickHouse, use `clickhouse-best-practices` and verify canonical DDL matches migrations, Rust/ingestion types match column order and encoding, materialized views cover all affected columns, and cluster/database/engine assumptions come from Tackt configuration.
7. For NATS-fed ClickHouse, use `nats-subject-audit`; verify coordinated cutovers and enumerate every subscriber.
8. Check security, credentials, TLS, bind addresses, authentication, input validation, audit attribution, backups, recovery, and destructive-operation safeguards.
9. Run database-specific checks and the complete root verification suite.

Return blockers, warnings, passed evidence, unverified dependencies, rollout and recovery prerequisites, and a deploy/no-deploy recommendation. Do not execute deployment, migration, backfill, repair, or rollback.

