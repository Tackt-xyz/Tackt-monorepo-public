---
description: Perform a behavior-preserving Rust cleanup of one Tackt crate or service, respecting workspace ownership boundaries and running the repository verification suite.
argument-hint: "[crate-or-path — defaults to services/core-api]"
---

# Rust-standard cleanup

Target: **$ARGUMENTS**. If empty, use `services/core-api`. Accept either a workspace package name or a path under `crates/` or `services/`, and resolve it to exactly one workspace member before editing.

Review every Rust file in the target, including `src/`, `tests/`, `examples/`, benches, and build scripts when present. Apply only localized, high-confidence, behavior-preserving cleanup. Do not edit unrelated workspace members merely to broaden the cleanup.

Before changing files, read the root `AGENTS.md` and every nested `AGENTS.md` that governs the target. In particular, changes under `services/risk-engine/` must follow `services/risk-engine/AGENTS.md`.

## Safety boundary

Do not change runtime behavior, public APIs, arithmetic, persistence behavior, database schemas, NATS subjects or wire contracts, serde representations, initialization order, security boundaries, or dependency direction. Do not add dependencies. When a cleanup cannot be justified from the surrounding code with high confidence, skip it.

Preserve Tackt's ownership boundaries:

- `tackt-primitives` remains independent of storage, transport, and services. Asset facts continue to come only from its embedded root `asset-registry.json`.
- `tackt-postgres` remains the only home for SQL, SQLx, migrations, transactions, row mapping, and PostgreSQL persistence.
- `tackt-nats` remains the only home for NATS behavior, subjects, streams, wire contracts, acknowledgements, and retry conventions.
- `tackt-telemetry` continues to own shared tracing and logging initialization.
- Services orchestrate typed shared-crate APIs and do not duplicate infrastructure ownership.

Never edit an existing migration as part of cleanup.

## Allowed cleanup

1. **Imports.** Move function-local or mid-module `use` declarations to the top of their enclosing module when doing so remains correctly scoped and warning-free. Imports used only by tests belong at the top of the relevant test module, not at file scope. Preserve `#[cfg(...)]` gates. Deduplicate imports and merge brace groups where the result is clearer.
2. **Import style.** Replace repeated fully qualified paths with an unambiguous import and short name when that improves readability. Follow the existing local grouping style; do not churn a file solely to impose a preferred ordering.
3. **Comments and documentation.** Remove stale implementation-history narration, ticket references, dates, and comments that merely restate the following line. Preserve explanations of invariants, safety requirements, accounting rules, delivery guarantees, and non-obvious constraints. Add concise documentation to public items only when its meaning is unambiguous from the code and surrounding API.
4. **Mechanical simplification.** Apply rustfmt-compatible, idiomatic simplifications that cannot affect evaluation order, ownership behavior, errors, logging, serialization, or observable output.
5. **Dead code.** Remove only private code proven unused by compiler diagnostics or direct local inspection. Do not remove public items or compatibility surfaces during a cleanup pass.

## Explicitly out of scope

- Type, signature, trait-bound, visibility, or error-contract changes.
- Numeric representation or rounding changes, especially in ledger and risk code.
- Changes to serde attributes, field order, schema versions, database mappings, SQL, migrations, NATS envelopes, subjects, streams, or acknowledgement behavior.
- Asset registry changes or duplicated asset metadata.
- Dependency additions, feature changes, crate moves, or architectural refactors.
- Production configuration, credentials, TLS, authorization, audit attribution, backup, restore, or migration-repair behavior.

Report worthwhile out-of-scope findings separately; do not fold them into this cleanup.

## Working-tree discipline

Inspect `git status` before editing. Preserve all pre-existing user changes and avoid overlapping them unless the requested target requires it. Do not create a branch, commit, push, or open a pull request unless the user explicitly asks.

## Verification

During iteration, run focused checks for the resolved package as useful:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p <package> --all-targets --all-features -- -D warnings
cargo test --locked -p <package> --all-targets --all-features
```

Before handoff, run every repository-required check from the workspace root:

```sh
./scripts/check-boundaries.sh
cargo metadata --locked --format-version 1 > /dev/null
cargo fmt --all -- --check
cargo deny check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
```

Do not dismiss failures as unrelated without evidence. If a check cannot run because required infrastructure or tooling is unavailable, report the exact command, failure, and what remains unverified.

Finally, inspect the diff for accidental behavior changes, ownership-boundary violations, generated files, and unrelated edits. Summarize the files changed, the cleanup categories applied, skipped higher-risk findings, and verification results.
