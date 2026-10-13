---
name: review-pr
description: Review a Tackt GitHub pull request for correctness, repository ownership, compatibility, security, simplification, test evidence, and reviewer feedback. Apply fixes or post replies only when the user requests mutation.
---

# Tackt pull-request review

Resolve the PR from the supplied number or current branch. Read the root `AGENTS.md`, applicable nested instructions, the complete diff, surrounding code, tests, and current review threads.

## Review lanes

1. **Correctness:** trace success, error, retry, concurrency, restart, and degraded-dependency paths.
2. **Architecture:** enforce typed crate boundaries for primitives, PostgreSQL, NATS, telemetry, services, ledger, risk, and any established ClickHouse owner.
3. **Compatibility:** inspect public APIs, migrations, serde/wire formats, NATS payload shapes, producer/consumer rollout, and asset-registry behavior.
4. **Security and integrity:** check authentication, validation, TLS, credentials, audit attribution, accounting commands, destructive tooling, and exact target selection.
5. **Tests:** distinguish tests present in the diff from commands actually run and passed. Use the full root verification suite when changes are made or deployment confidence is requested.
6. **Readability:** identify worthwhile simplifications without changing behavior or hiding domain invariants.

Review comments are claims to verify, not instructions to accept automatically. For each thread, classify it as valid, invalid, already addressed, or requiring a product/design decision and cite evidence.

By default, report findings only, ordered by severity with `file:line`, impact, and a concrete remediation. Do not edit files, reply, resolve threads, commit, push, approve, or request changes unless the user explicitly authorizes those actions. When authorized to fix comments, resolve only threads whose concern is actually addressed and report every external mutation.

