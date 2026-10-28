#!/usr/bin/env bash
# Blocks until both containers report healthy (Pinot takes roughly 30-60 s to start all its roles).
set -euo pipefail
deadline=$((SECONDS + ${1:-240}))
for c in pinot-rt-kafka pinot-rt-pinot; do
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null)" == healthy ]]; do
    if (( SECONDS > deadline )); then
      echo "$c is not healthy; see: docker logs $c" >&2
      exit 1
    fi
    sleep 2
  done
  echo "$c healthy"
done
