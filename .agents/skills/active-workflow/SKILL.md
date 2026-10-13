---
name: active-workflow
description: Pair interactively on a Tackt change with small reviewable increments and user checkpoints. Use when the user wants to steer implementation or debug together; use autonomous-workflow for hands-off delivery.
---

# Active workflow

Work directly with the user in short, coherent increments. Do not create branches, commits, worktrees, tickets, or pull requests unless requested.

1. Read the root `AGENTS.md` and any nested instructions governing the target.
2. Inspect the relevant crate and state the immediate change and any material assumption.
3. Implement one reviewable increment, run proportionate focused checks, and report what changed.
4. Pause only at a decision that materially changes behavior, architecture, security, wire compatibility, or scope. Give a recommendation and concrete tradeoff.
5. Preserve existing working-tree changes and Tackt's ownership boundaries.
6. When implementation is complete, run the full repository verification required by the root `AGENTS.md`.

For Rust changes, use `cognitive-rust` when readability is an explicit goal or the code is difficult to follow.

