---
name: linear-cli
description: Manages Linear from the command line with the `linear` CLI, falling back to the GraphQL API via curl for anything the CLI does not cover. Use when creating, searching, listing, updating, commenting on, assigning, or starting Linear issues; managing teams, projects, project updates, milestones, initiatives, labels, or documents; or scripting any Linear workflow from a terminal — including when the user says things like "file a Linear ticket", "move LIN-123 to in progress", "list my Linear issues", "add a label", or "create a project" without naming the skill.
---

# Linear CLI

A CLI to manage Linear issues from the command line, with git and jj integration.

Generated from linear CLI v1.8.1

## Prerequisites

The `linear` command must be available on PATH. To check:

```bash
linear --version
```

If not installed, follow the instructions at:\
https://github.com/schpet/linear-cli?tab=readme-ov-file#install

## Quick auth + workspace sanity checks (recommended)

Use these commands before creating/updating issues:

```bash
# Show configured workspaces (may be empty even when LINEAR_API_KEY is set)
linear auth list

# Verify active identity
linear auth whoami

# Verify API access quickly
linear team list
```

### Important CLI gotchas

1. `linear auth status` is **not** a valid command in current CLI versions.
   Use `linear auth list` and/or `linear auth whoami` instead.

2. If `LINEAR_API_KEY` is set, do **not** pass `--workspace`.
   The CLI will error with:
   `Cannot use --workspace flag when LINEAR_API_KEY environment variable is set`.

   Use one mode consistently:
   - **Env token mode**: use `LINEAR_API_KEY` and no `--workspace`
   - **Credential mode**: unset `LINEAR_API_KEY`, then use `--workspace <slug>` if needed

## Commands and reference docs

One reference file per command — read the one matching your task. Each file lists its subcommands and flags; full flag detail is also one `--help` away (see below).

- [issue](references/issue.md) — create, list, view, start, update, comment on, and attach issues
- [team](references/team.md) — list teams and members, manage autolinks
- [project](references/project.md) — manage projects
- [project-update](references/project-update.md) — post project status updates
- [milestone](references/milestone.md) — manage project milestones
- [initiative](references/initiative.md) — manage initiatives and the projects under them
- [initiative-update](references/initiative-update.md) — post initiative timeline updates
- [label](references/label.md) — manage issue labels
- [document](references/document.md) — manage Linear documents
- [auth](references/auth.md) — log in, check identity, print the API token
- [config](references/config.md) — generate `.linear.toml`
- [schema](references/schema.md) — print the GraphQL schema to stdout
- [commands](references/commands.md) — compact index of every per-command reference
- [organization-features](references/organization-features.md) — curated recipes for initiatives, labels, projects, and bulk operations

## Discovering Options

To see available subcommands and flags, run `--help` on any command:

```bash
linear --help
linear issue --help
linear issue list --help
linear issue create --help
```

Each command has detailed help output describing all available flags and options.

## Safe command patterns for agents

Prefer deterministic, non-interactive commands:

```bash
# List teams
linear team list

# Create issue (non-interactive)
linear issue create \
  --no-interactive \
  --team TRA \
  --assignee self \
  --title "Task title" \
  --description "Task details"
```

Tips:
- Pass `--no-interactive` in automation — without it the CLI prompts and hangs an agent that can't answer.
- Pass `--team` unless your default team is guaranteed; otherwise the issue may land in the wrong team.
- Use explicit `--assignee self` when ownership should be clear.

## Using the Linear GraphQL API Directly

**Prefer the CLI for all supported operations.** Direct API calls via curl are slower and should only be used as a fallback for advanced queries not covered by the CLI. For complex queries involving multiple calls, write and execute a script.

To make direct API calls, use `linear schema` and `linear auth token`:

### 1. Check the schema for available types and fields

Write the schema to a tempfile, then search it:

```bash
# Write schema to a tempfile (cross-platform)
linear schema -o "${TMPDIR:-/tmp}/linear-schema.graphql"

# Search for specific types or fields
grep -i "cycle" "${TMPDIR:-/tmp}/linear-schema.graphql"
grep -A 30 "^type Issue " "${TMPDIR:-/tmp}/linear-schema.graphql"

# View filter options
grep -A 50 "^input IssueFilter" "${TMPDIR:-/tmp}/linear-schema.graphql"
```

### 2. Get the auth token

```bash
linear auth token
```

### 3. Make a curl request

```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: $(linear auth token)" \
  -d '{"query": "{ issues(filter: { team: { key: { eq: \"CLI\" } } }, first: 5) { nodes { identifier title state { name } } } }"}'
```

### Example queries

```bash
# Get issues assigned to current user
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: $(linear auth token)" \
  -d '{"query": "{ viewer { assignedIssues(first: 10) { nodes { identifier title state { name } } } } }"}'
```
