#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <backup.dump> <empty-target-database-url>" >&2
  exit 2
fi
backup=$1
target_url=$2
[[ -f "$backup" ]] || { echo "error: backup does not exist: $backup" >&2; exit 1; }
if [[ ! -f "$backup.sha256" ]]; then
  echo "error: checksum file missing: $backup.sha256 (refusing to restore an unverifiable backup)" >&2
  exit 1
fi
(cd "$(dirname "$backup")" && sha256sum --check "$(basename "$backup").sha256")
objects=$(psql "$target_url" -Atqc "SELECT
  (SELECT count(*) FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema','public') AND nspname NOT LIKE 'pg_toast%')
  + (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e'))
  + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e'))
  + (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typtype IN ('d','e','r') AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_type'::regclass AND d.objid=t.oid AND d.deptype='e'))")
if [[ "$objects" != "0" ]]; then
  echo "error: restore target is not empty" >&2
  exit 1
fi
# Restore objects under the target login without recreating source roles or ACLs.
pg_restore --dbname="$target_url" --no-owner --no-acl --exit-on-error --single-transaction "$backup"
"$(dirname "$0")/verify-ledger-reconstruction.sh" "$target_url"
echo "ledger backup restored into empty target under the target login and reconstruction verified"
