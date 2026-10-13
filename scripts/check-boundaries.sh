#!/usr/bin/env bash
set -euo pipefail

# Runs grep and exits with an error unless the pattern matched nothing. Before
# scanning, each pattern is proven against a known-violating sample so a broken
# pattern (or grep flavor drift) fails the check instead of passing it vacuously.
# grep exit codes: 0 = matches found (a violation), 1 = no matches, >=2 = failed.
forbid_in() {
  local root=$1
  shift
  local message=$1 pattern=$2 sample=$3
  shift 3
  if ! printf '%s\n' "$sample" | grep -qE "$pattern"; then
    echo "error: self-test failed; pattern did not match its known violation: $message" >&2
    exit 1
  fi
  local status=0
  grep -rEn "$@" -e "$pattern" "$root" || status=$?
  if [[ $status -eq 0 ]]; then
    echo "error: $message" >&2
    exit 1
  elif [[ $status -ge 2 ]]; then
    echo "error: grep failed with status $status while checking: $message" >&2
    exit "$status"
  fi
}

forbid() { forbid_in services "$@"; }

forbid 'services must read the canonical embedded asset registry through tackt-primitives' \
  'include_str![[:space:]]*\([^)]*asset-registry[.]json' \
  'include_str!("../asset-registry.json")' \
  --include='*.rs'

if [[ -n "$(find services crates -type f -name asset-registry.json -print -quit)" ]]; then
  echo 'error: asset-registry.json must exist only at the workspace root' >&2
  exit 1
fi

forbid_in crates/postgres/src 'accounting command execution belongs in Ledger, not PostgreSQL' \
  'LedgerCommand::|impl LedgerRepository|fn validate_(command|balanced)|fn hash_command' \
  'LedgerCommand::DrawCredit { quantity, .. }' \
  --include='*.rs'

forbid 'services may not use SQLx' \
  '(^|[^[:alnum:]_])sqlx([^[:alnum:]_]|$)' \
  'use sqlx::PgPool;' \
  --include='*.rs' --include='Cargo.toml'

forbid 'service startup must not provision streams, resume KV, or Ledger durables; use the NATS provisioning command' \
  '(\.|NatsClient::)(ensure_stream|ensure_hyperliquid_resume|provision_ledger_consumer)[[:space:]]*\(' \
  'client.ensure_stream(spec)' \
  --include='*.rs' --exclude-dir='tests'

forbid 'services may not depend on async-nats directly; use tackt-nats' \
  'async[_-]nats' \
  'async-nats.workspace = true' \
  --include='*.rs' --include='Cargo.toml'

indexer_clickhouse_pattern='(^|[^[:alnum:]_])(tackt[_-])?clickhouse(::|[.]workspace|[[:space:]]*=)'
if ! printf '%s\n' 'clickhouse.workspace = true' | grep -qE "$indexer_clickhouse_pattern"; then
  echo 'error: Indexer ClickHouse boundary self-test failed' >&2
  exit 1
fi
indexer_clickhouse_status=0
grep -rEn --include='*.rs' --include='Cargo.toml' \
  -e "$indexer_clickhouse_pattern" services/indexer || indexer_clickhouse_status=$?
if [[ $indexer_clickhouse_status -eq 0 ]]; then
  echo 'error: Indexer may not connect to ClickHouse; archive price evidence downstream from NATS' >&2
  exit 1
elif [[ $indexer_clickhouse_status -ge 2 ]]; then
  echo 'error: grep failed while checking the Indexer ClickHouse boundary' >&2
  exit "$indexer_clickhouse_status"
fi

price_postgres_pattern='tackt_postgres|PgRepository|upsert_observations|insert_observations'
if ! printf '%s\n' 'use tackt_postgres::PgRepository;' | grep -qE "$price_postgres_pattern"; then
  echo 'error: Indexer price/PostgreSQL boundary self-test failed' >&2
  exit 1
fi
price_postgres_status=0
grep -rEn --include='*.rs' -e "$price_postgres_pattern" \
  services/indexer/src/source/price_feed || price_postgres_status=$?
if [[ $price_postgres_status -eq 0 ]]; then
  echo 'error: Indexer price adapters may only publish through NATS, never PostgreSQL' >&2
  exit 1
elif [[ $price_postgres_status -ge 2 ]]; then
  echo 'error: grep failed while checking the Indexer price/PostgreSQL boundary' >&2
  exit "$price_postgres_status"
fi

generic_publish_pattern='(\.|NatsClient::)publish_durable[[:space:]]*\('
if ! printf '%s\n' 'client.publish_durable(subject, None, &payload)' \
    | grep -qE "$generic_publish_pattern"; then
  echo 'error: generic durable-publish boundary self-test failed' >&2
  exit 1
fi
# Integration tests deliberately exercise the low-level transport; production
# service code may publish only through typed tackt-nats contract methods.
forbid 'service source may not bypass typed NATS contracts with generic durable publishes' \
  "$generic_publish_pattern" \
  'NatsClient::publish_durable(client, subject, None, &payload)' \
  --include='*.rs' --exclude-dir='tests'

# Price evidence has no second durable copy, so it may travel only as an
# acknowledged JetStream publish through the typed contract method. A service
# naming the subject or stream itself is either building a core-NATS path to it
# or provisioning the stream behind tackt-nats' back.
forbid 'services may not name the price-evidence subject or stream; publish through publish_price_evidence' \
  'tackt[.]prices[.]evidence|TACKT_PRICE_EVIDENCE' \
  'client.publish("tackt.prices.evidence", payload)' \
  --include='*.rs' --include='Cargo.toml'

# Deliberately case-sensitive: lowercase matching would false-positive on
# tokio::select! and ordinary prose, and any real SQL execution path already
# trips the SQLx dependency ban above. This check is defense in depth.
sql_pattern='(SELECT|INSERT[[:space:]]+INTO|UPDATE[[:space:]]+[[:alnum:]_]+[[:space:]]+SET|DELETE[[:space:]]+FROM|(CREATE|ALTER|DROP)[[:space:]]+TABLE)'
for sample in 'SELECT id FROM users' 'INSERT INTO users VALUES (1)' 'UPDATE users SET x = 1' \
  'DELETE FROM users' 'CREATE TABLE users (id int)' 'ALTER TABLE users ADD COLUMN x int' \
  'DROP TABLE users'; do
  forbid 'services may not contain SQL or migrations' \
    "$sql_pattern" \
    "$sample" \
    --include='*.rs' --include='*.sql'
done

if find services -type d -name migrations -print -quit | grep -q .; then
  echo 'error: migrations are owned exclusively by crates/postgres' >&2
  exit 1
fi

dependency_violations=$(
  awk '
    /^\[.*dependencies(\.[^]]+)?\]$/ { in_dependencies = 1; next }
    /^\[/ { in_dependencies = 0 }
    in_dependencies && $0 !~ /^[[:space:]]*(#|$)/ && $0 !~ /workspace[[:space:]]*=[[:space:]]*true/ {
      print FILENAME ":" FNR ":" $0
    }
  ' crates/*/Cargo.toml services/*/Cargo.toml
)
if [[ -n "$dependency_violations" ]]; then
  printf '%s\n' "$dependency_violations"
  echo 'error: member dependencies must be declared through [workspace.dependencies]' >&2
  exit 1
fi

member_lockfile=$(find crates services -name Cargo.lock -print -quit)
if [[ -n "$member_lockfile" ]]; then
  echo "error: member lockfile found at $member_lockfile; use only the root Cargo.lock" >&2
  exit 1
fi

echo 'architecture boundaries verified'
