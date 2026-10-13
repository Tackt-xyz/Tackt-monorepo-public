#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <database-url> <output.dump>" >&2
  exit 2
fi
database_url=$1
output=$2
if [[ -e "$output" ]]; then
  echo "error: refusing to overwrite existing backup: $output" >&2
  exit 1
fi
tmp_output="$output.tmp"
trap 'rm -f "$tmp_output"' ERR INT TERM
pg_dump --dbname="$database_url" --format=custom --serializable-deferrable --file="$tmp_output"
output_name=$(basename "$output")
checksum=$(sha256sum "$tmp_output" | awk '{print $1}')
printf '%s  %s\n' "$checksum" "$output_name" > "$output.sha256"
mv "$tmp_output" "$output"
trap - ERR INT TERM
echo "ledger backup written to $output (checksum: $output.sha256)"
