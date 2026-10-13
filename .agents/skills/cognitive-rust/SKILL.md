---
name: cognitive-rust
description: Write or review Tackt Rust for low cognitive load using explicit domain types, shallow control flow, small functions, and visible invariants without weakening correctness or repository ownership boundaries.
---

# Cognitive Rust

Optimize for code a reviewer can understand without reconstructing hidden state.

- Prefer guard clauses, `?`, and `let ... else` over nested happy paths.
- Name meaningful predicates and intermediate domain values; avoid naming one-use syntax that is already obvious.
- Use enums and typed IDs to make valid states explicit. Do not replace meaningful Tackt domain types with strings, booleans, or generic maps.
- Keep functions focused on one level of abstraction. Extract logic when the extracted name communicates a real concept, not merely to reduce line count.
- Prefer direct data flow and ordinary control structures over clever traits, macros, indirection, or generic abstractions.
- Keep error context close to the failed operation and preserve typed error contracts.
- Make accounting, risk, delivery, idempotency, and authorization invariants visible in names, types, validation, and focused tests.
- Remove comments that narrate syntax; retain comments explaining non-obvious invariants, failure modes, compatibility constraints, or safety boundaries.

Read the root and applicable nested `AGENTS.md` before suggesting structural changes. Readability work must not alter arithmetic, ordering, serialization, SQL semantics, NATS contracts, acknowledgement behavior, or public APIs unless the user explicitly requests behavioral refactoring.

