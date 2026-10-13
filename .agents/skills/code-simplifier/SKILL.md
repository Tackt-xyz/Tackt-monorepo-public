---
name: code-simplifier
description: Simplify recently changed code for clarity, consistency, and maintainability while preserving exact behavior. Use when asked to simplify, clean up, reduce complexity, or perform a readability refactor; do not activate for feature work or broad architectural redesign.
license: Apache-2.0
metadata:
  source: Adapted from Anthropic's code-simplifier agent
---

# Code simplifier

Improve the code a reviewer must understand without changing what the program does.

## Scope

Default to code changed in the current working-tree diff or current task. Expand beyond it only when the user specifies a broader target or a directly adjacent change is required to complete a safe simplification.

Before editing:

1. Read the root `AGENTS.md` and every nested instruction governing the target.
2. Inspect `git status` and the relevant diff so existing user changes remain intact.
3. Identify the observable behavior, public contracts, tests, and invariants that must not change.

## Simplify toward clarity

- Flatten unnecessary nesting with guard clauses, early returns, `?`, or equivalent straightforward control flow.
- Replace dense expressions, nested conditionals, and clever one-liners with explicit named steps when that reduces mental load.
- Improve names so values, predicates, ownership, units, and domain meaning are evident.
- Remove duplication when one clear local abstraction represents the same concept.
- Remove redundant wrappers, indirection, temporary variables, and abstractions that do not carry domain meaning.
- Consolidate logic that must change together while keeping distinct concerns separate.
- Remove comments that merely narrate syntax; preserve comments explaining invariants, compatibility, safety, accounting, delivery, or non-obvious tradeoffs.
- Follow established repository and local language conventions rather than imposing generic style preferences.

Prefer readable and explicit code over the fewest lines. Do not replace an understandable implementation with a more abstract, generic, macro-heavy, or compressed one.

## Behavior-preservation boundary

Do not change:

- public APIs, accepted inputs, returned values, errors, logs, or externally observable ordering;
- arithmetic, rounding, overflow, ledger, balance, reservation, or risk semantics;
- serialization, field order, NATS subjects, delivery, acknowledgements, retry, or idempotency;
- SQL behavior, migrations, transactions, authoritative data ownership, or reconciliation;
- authentication, authorization, audit attribution, TLS, validation, or secret handling;
- performance characteristics when the change could affect a hot path, allocation bound, blocking behavior, or concurrency without evidence.

Do not remove a useful abstraction merely because inlining reduces line count. Do not combine unrelated responsibilities. If a promising cleanup crosses one of these boundaries, report it separately instead of applying it.

For Rust with substantial cognitive complexity, also apply the repository's `cognitive-rust` guidance when available.

## Verification

Review the final diff specifically for accidental behavior changes. Run the smallest focused checks that cover the touched behavior, followed by the complete verification required by the governing `AGENTS.md` when the simplification changes repository code.

Report:

- the scope simplified;
- the meaningful clarity improvements;
- higher-risk opportunities deliberately skipped;
- commands run and their outcomes.

Do not create commits, push, open a pull request, or mutate external systems unless the user explicitly requests it.
