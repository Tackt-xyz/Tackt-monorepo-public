#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <postgres-data-directory> <database-url>" >&2
  exit 2
fi
postgres_data=$1
database_url=$2
if [[ ! -f "$postgres_data/PG_VERSION" || ! -f "$postgres_data/postmaster.pid" ]]; then
  echo "error: target must be a running PostgreSQL data directory" >&2
  exit 1
fi
if [[ -n "${PG_CTL_BIN:-}" ]]; then
  pg_ctl_bin=$PG_CTL_BIN
elif command -v pg_ctl >/dev/null 2>&1; then
  pg_ctl_bin=$(command -v pg_ctl)
else
  pg_ctl_bin="$(pg_config --bindir)/pg_ctl"
fi
[[ -x "$pg_ctl_bin" ]] || { echo "error: pg_ctl not found" >&2; exit 1; }

selected_data=$(realpath -e "$postgres_data")
connected_data=$(psql "$database_url" -Atqc 'SHOW data_directory')
connected_data=$(realpath -e "$connected_data")
if [[ "$selected_data" != "$connected_data" ]]; then
  echo "error: database URL targets $connected_data, not $selected_data" >&2
  exit 1
fi

journal_hash() {
  psql "$database_url" -Atqc "SELECT COALESCE(md5(string_agg(id::text||command_hash,'|' ORDER BY recorded_at,id)),'empty') FROM ledger.transactions"
}

before=$(journal_hash)
"$pg_ctl_bin" -D "$postgres_data" restart -m fast
after=$(journal_hash)
if [[ "$before" != "$after" ]]; then
  echo "error: journal hash changed across clean PostgreSQL restart" >&2
  exit 1
fi
# Simulated crash: immediate shutdown skips the checkpoint, so the following
# start must replay WAL. This is the case that catches real durability bugs.
"$pg_ctl_bin" -D "$postgres_data" restart -m immediate
after_crash=$(journal_hash)
if [[ "$before" != "$after_crash" ]]; then
  echo "error: journal hash changed across simulated PostgreSQL crash (WAL recovery)" >&2
  exit 1
fi
"$(dirname "$0")/verify-ledger-reconstruction.sh" "$database_url"
echo "PostgreSQL clean restart and simulated crash both preserved ledger journal hash: $after_crash"
