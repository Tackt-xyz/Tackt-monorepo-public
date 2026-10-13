#!/bin/bash
# SessionEnd hook: if the session ran inside a linked git worktree whose branch's
# PR is merged, remove the worktree. Refuses (silently) when the worktree is
# dirty, locked, or detached — nothing uncommitted is ever destroyed.
set -u

input=$(cat)
cwd=$(printf '%s' "$input" | jq -r '.cwd // empty' 2>/dev/null)
[ -n "$cwd" ] && [ -d "$cwd" ] || exit 0

git_dir=$(git -C "$cwd" rev-parse --absolute-git-dir 2>/dev/null) || exit 0
common_dir=$(git -C "$cwd" rev-parse --path-format=absolute --git-common-dir 2>/dev/null) || exit 0
# Linked worktrees have git-dir != git-common-dir; the main checkout is left alone.
[ "$git_dir" != "$common_dir" ] || exit 0

# Detached HEAD → no branch to look up, keep it.
branch=$(git -C "$cwd" symbolic-ref --short -q HEAD) || exit 0
top=$(git -C "$cwd" rev-parse --show-toplevel 2>/dev/null) || exit 0
main_root=$(dirname "$common_dir")

merged=$(cd "$cwd" && gh pr list --head "$branch" --state merged --json number --jq 'length' 2>/dev/null) || exit 0
[ "${merged:-0}" -gt 0 ] || exit 0

# Worktrees are often reused for sequential PRs: keep it while any open PR
# still has this branch as head (follow-up PR) or base (stacked child PR).
open_head=$(cd "$cwd" && gh pr list --head "$branch" --state open --json number --jq 'length' 2>/dev/null) || exit 0
[ "${open_head:-1}" -eq 0 ] || exit 0
open_base=$(cd "$cwd" && gh pr list --base "$branch" --state open --json number --jq 'length' 2>/dev/null) || exit 0
[ "${open_base:-1}" -eq 0 ] || exit 0

# Non-force remove: git refuses if the tree is dirty or locked.
git -C "$main_root" worktree remove "$top" >/dev/null 2>&1
exit 0
