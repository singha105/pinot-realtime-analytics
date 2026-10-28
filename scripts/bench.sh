#!/usr/bin/env bash
# End-to-end benchmark on a fresh stack (run `make demo`, which resets the containers first):
#   create topic + tables -> stream ROWS events -> wait until every table has them -> seal segments
#   -> query load on each table -> rollup capacity sweep -> produce-to-queryable probes
#   -> results/RESULTS.md, copied into the README between the results markers.
# Every number in the README results section is written by this script.
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS="${ROWS:-5000000}"
RATE="${RATE:-50000}"
PARTITIONS="${PARTITIONS:-4}"
QPS="${QPS:-20}"
DURATION="${DURATION:-30}"
WARMUP="${WARMUP:-5}"
SWEEP_QPS="${SWEEP_QPS:-50,100,200}"
SWEEP_DURATION="${SWEEP_DURATION:-20}"
PROBES="${PROBES:-20}"
JAR=target/pinot-rt.jar
OUT=results

run() { java -jar "$JAR" "$@"; }

java_major=$(java -version 2>&1 | awk -F'"' '/version/ {split($2, v, "."); print v[1]}')
if (( java_major < 17 )); then
  echo "Java 17+ is required (found $java_major)" >&2
  exit 1
fi
[[ -f "$JAR" ]] || { echo "missing $JAR; run make build" >&2; exit 1; }
mkdir -p "$OUT"

echo "== topic and tables"
run topic --partitions "$PARTITIONS"
scripts/create-tables.sh

echo "== streaming $ROWS events at $RATE/s"
run produce --count "$ROWS" --rate "$RATE" --partitions "$PARTITIONS" --summary "$OUT/produce.json"

echo "== waiting for every table to ingest $ROWS rows"
run await --expected "$ROWS" --summary "$OUT/ingest.json"

echo "== sealing consuming segments (star-tree indexes are built on commit)"
run commit

echo "== query load: $QPS QPS, ${DURATION}s per query and table"
run loadgen --qps "$QPS" --duration-sec "$DURATION" --warmup-sec "$WARMUP" \
  --csv "$OUT/latency.csv" --markdown "$OUT/latency.md"

echo "== capacity sweep: rollup query on the scan and star-tree tables at $SWEEP_QPS QPS"
# The scan table is expected to saturate here, so drops and timeouts are reported, not fatal.
run loadgen --only q1_rollup_region_status --tables deliveries_noidx,deliveries_startree \
  --qps "$SWEEP_QPS" --duration-sec "$SWEEP_DURATION" --warmup-sec "$WARMUP" --fail-on-errors false \
  --csv "$OUT/sweep.csv" --markdown "$OUT/sweep.md"

echo "== produce-to-queryable probes"
run freshness --probes "$PROBES" --markdown "$OUT/freshness.md"

echo "== writing $OUT/RESULTS.md"
json() { python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$@"; }
num() { python3 -c "import sys; print(f'{round(float(sys.argv[1])):,}')" "$1"; }
if [[ "$(uname)" == Darwin ]]; then
  host="$(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GB RAM, macOS $(sw_vers -productVersion)"
else
  host="$(nproc) cores, $(awk '/MemTotal/ {printf "%d", $2/1048576}' /proc/meminfo) GB RAM, $(uname -sr)"
fi
docker_mem=$(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f", $1/1073741824}')
images=$(docker compose config --images | sort | paste -sd ',' - | sed 's/,/, /g')

{
  echo "Run on $(date -u '+%Y-%m-%d %H:%M UTC') at commit \`$(git rev-parse --short HEAD 2>/dev/null || echo unknown)\`."
  echo "Host: $host. Docker VM memory: ${docker_mem} GiB. Images: $images."
  echo "Client JVM: $(java -version 2>&1 | head -1)."
  echo
  echo "### Ingestion"
  echo
  echo "| Events | Kafka partitions | Producer target rate | Producer achieved rate | Rows queryable in all 3 tables after last send |"
  echo "|---:|---:|---:|---:|---:|"
  echo "| $(num "$(json $OUT/produce.json acked)") | $PARTITIONS | $(num "$RATE")/s |" \
    "$(num "$(json $OUT/produce.json achieved_rate)")/s | $(json $OUT/ingest.json seconds) s |"
  echo
  echo "### Query latency by index"
  echo
  cat "$OUT/latency.md"
  echo "### Capacity: rollup query as offered load rises"
  echo
  cat "$OUT/sweep.md"
  echo "### Freshness"
  echo
  cat "$OUT/freshness.md"
} > "$OUT/RESULTS.md"

# Swap the README's results section for the new run.
awk -v f="$OUT/RESULTS.md" '
  /<!-- results:start -->/ { print; while ((getline line < f) > 0) print line; skip = 1; next }
  /<!-- results:end -->/ { skip = 0 }
  !skip
' README.md > README.md.tmp && mv README.md.tmp README.md

echo "done: see $OUT/RESULTS.md"
