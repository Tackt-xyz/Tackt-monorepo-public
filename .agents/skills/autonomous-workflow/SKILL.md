---
name: autonomous-workflow
description: Deliver a well-scoped Tackt change end to end with repository research, an explicit plan, isolated parallel work when authorized, review, and full verification. Use for hands-off requests such as “ship this”; use active-workflow for user-steered pairing.
---

# Autonomous workflow

Own the requested outcome through implementation and verification. A ticket, branch, commit, push, or pull request is required only when the user requests it.

## Workflow

1. Read the root `AGENTS.md`, nested target instructions, and the relevant manifests and APIs.
2. Resolve ambiguities from repository evidence. Ask only when a missing choice would materially change the result.
3. Write a dependency-aware plan. Keep storage work in `tackt-postgres`, NATS behavior and contracts in `tackt-nats`, universal vocabulary in `tackt-primitives`, telemetry setup in `tackt-telemetry`, and orchestration in services.
4. If the user or governing instructions authorize delegation, parallelize only independent, bounded tasks with non-overlapping file ownership. The lead retains architecture, integration, and final verification.
5. Implement the smallest coherent change, preserving unrelated working-tree edits.
6. Review the complete diff for authority violations, backward compatibility, security boundaries, failure semantics, and test coverage.
7. Run every verification command required by the root `AGENTS.md`. Report exact blockers rather than weakening checks.

Do not turn acceleration-channel events into authoritative state, weaken durable JetStream flows, expose raw SQL or arbitrary ledger postings, duplicate asset metadata, or bypass typed infrastructure APIs.

