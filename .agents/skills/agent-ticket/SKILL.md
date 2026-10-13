---
name: agent-ticket
description: Create or enrich a Tackt engineering ticket in Linear with verified repository context, clear acceptance criteria, constraints, and executable verification. Use when asked to file, prepare, or improve a ticket.
---

# Tackt engineering ticket

Use the configured Linear integration or CLI. If neither is authenticated, stop before mutation and explain the required setup. Creating or modifying a ticket requires the user's request; repository research is read-only.

## Capture

1. Turn the reported problem or desired outcome into a concise title and current-state description.
2. Search for likely duplicates using the important domain nouns.
3. Infer team, project, priority, assignee, labels, estimate, and milestone only from available Linear context. Ask when a required choice is genuinely ambiguous.
4. Preview the title, description, acceptance criteria, and scheduling fields before creating the issue unless the user explicitly requested immediate creation.
5. Never fabricate identifiers, impact, reproduction evidence, ownership, or deadlines.

## Enrich for implementation

Read the root `AGENTS.md`, applicable nested instructions, and the relevant implementation, tests, and public APIs. Add only verified context:

- observable behavior and evidence;
- affected crate, service, or subsystem with `file:line` anchors;
- authority boundaries for primitives, PostgreSQL, NATS, telemetry, ledger, risk, and ClickHouse;
- compatibility, security, or data-integrity constraints;
- acceptance criteria stated as observable outcomes;
- a focused verification command plus the repository-required gate;
- unknowns explicitly labeled as unknown, and hypotheses labeled as hypotheses.

Split work when independent parts have different owners, rollout order, or verification. Use real Linear dependency relations when available rather than describing blockers only in prose.

Return the created or updated issue identifier and URL, the fields selected, and any unresolved decisions.

