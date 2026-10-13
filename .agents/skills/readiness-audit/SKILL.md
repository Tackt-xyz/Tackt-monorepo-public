---
name: readiness-audit
description: Produce an evidence-backed 0–100 production-readiness assessment of a Tackt crate, service, or subsystem across architecture, correctness, tests, security, and operational durability. Use when asked to audit, score, or re-score deployment confidence.
---

# Readiness audit

Define in one sentence what a score of 100 means for the target. Read the root and nested `AGENTS.md`, implementation, public APIs, migrations/contracts, tests, documentation, scripts, and CI evidence.

Use the dimensions and caps in [references/scoring.md](references/scoring.md). If the user or governing instructions authorize subagents, independent auditors may use [references/agent-prompts.md](references/agent-prompts.md); otherwise perform the same lanes sequentially.

Verify load-bearing claims directly in code. Distinguish tests that exist from commands actually executed in this audit. Run focused tests first and the complete root verification suite when practical.

Every finding must include severity, evidence with file and line, impact, and a test-first remediation. Explicitly evaluate:

- repository authority and dependency boundaries;
- domain correctness, including ledger and risk invariants;
- PostgreSQL migration, transaction, and reconciliation behavior;
- NATS schema compatibility, durability, acknowledgements, retries, and idempotency;
- authentication, validation, TLS, secrets, audit attribution, and safe operational tooling;
- crash/restart windows, degraded dependencies, resource bounds, observability, and recovery.

Return the score, confidence level, score caps, findings ordered by severity, executed verification, unavailable evidence, and a prioritized remediation plan. A high score requires affirmative evidence; absence of a discovered bug is not proof.

