#!/usr/bin/env bash
# Registers one schema per table (Pinot requires the schema name to match the table name) and the
# three real-time table configs. Safe to re-run: existing schemas are overwritten, existing tables kept.
set -euo pipefail

CONTROLLER="${CONTROLLER:-http://localhost:19000}"
cd "$(dirname "$0")/.."

for config in pinot/tables/*.json; do
  table="$(basename "$config" .json)"
  schema="$(sed "s/__TABLE__/$table/" pinot/schema.template.json)"

  code=$(curl -s -o /tmp/pinot-rt-resp.$$ -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    --data "$schema" "$CONTROLLER/schemas?override=true&force=true")
  [[ "$code" == 200 ]] || { echo "schema $table: HTTP $code: $(cat /tmp/pinot-rt-resp.$$)"; exit 1; }

  code=$(curl -s -o /tmp/pinot-rt-resp.$$ -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    --data @"$config" "$CONTROLLER/tables")
  if [[ "$code" == 200 ]]; then
    echo "created table $table"
  elif [[ "$code" == 409 ]]; then
    echo "table $table already exists"
  else
    echo "table $table: HTTP $code: $(cat /tmp/pinot-rt-resp.$$)"; exit 1
  fi
done
rm -f /tmp/pinot-rt-resp.$$
