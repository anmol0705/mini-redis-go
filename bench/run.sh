#!/usr/bin/env bash
# Throughput benchmark with redis-benchmark: miniredis and, if installed, a real
# redis-server on another port; each with and without pipelining. Raw CSV output
# and a summary are saved under bench/results/<timestamp>/.
#
# Usage: bench/run.sh [-n REQUESTS] [-c CLIENTS] [-d VALUE_BYTES]
# Environment: MINIREDIS_BIN (default bin/miniredis, built if missing),
#              MINI_PORT (6391), REDIS_PORT (6392)
#
# redis-benchmark's default test list starts with PING_INLINE (inline commands),
# which miniredis does not support, so the tests are selected explicitly.
set -euo pipefail

N=100000; C=50; D=3
while getopts "n:c:d:" opt; do
  case "$opt" in
    n) N="$OPTARG" ;; c) C="$OPTARG" ;; d) D="$OPTARG" ;;
    *) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${MINIREDIS_BIN:-$ROOT/bin/miniredis}"
MINI_PORT="${MINI_PORT:-6391}"
REDIS_PORT="${REDIS_PORT:-6392}"
TESTS="ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,lrange_100,mset"
OUT="$ROOT/bench/results/$(date +%Y%m%d-%H%M%S)"
PID=""

command -v redis-benchmark >/dev/null || { echo "redis-benchmark not found (apt install redis-tools)"; exit 2; }
if [[ ! -x "$BIN" ]]; then (cd "$ROOT" && go build -o "$BIN" ./cmd/miniredis); fi
mkdir -p "$OUT"
trap '[[ -n "$PID" ]] && kill "$PID" 2>/dev/null; wait 2>/dev/null || true' EXIT

wait_ready() {
  for _ in $(seq 1 100); do
    [[ "$(redis-cli -p "$1" PING 2>/dev/null || true)" == "PONG" ]] && return 0
    sleep 0.1
  done
  echo "server on port $1 did not start"; exit 1
}

bench() { # name port pipeline
  local file="$OUT/$1-P$3.csv"
  echo "== $1, pipeline $3"
  redis-benchmark -h 127.0.0.1 -p "$2" -n "$N" -c "$C" -d "$D" -P "$3" -t "$TESTS" --csv >"$file"
}

run_server() { # name port pipelines...
  local name="$1" port="$2"; shift 2
  for p in "$@"; do bench "$name" "$port" "$p"; done
}

{
  echo "# Benchmark run $(date -u +%FT%TZ)"
  echo "- host: $(uname -srm), $(nproc) CPUs"
  echo "- requests per test: $N, clients: $C, value bytes: $D"
  echo "- redis-benchmark: $(redis-benchmark --version 2>&1 | head -1)"
  echo "- Go: $(go version 2>/dev/null || echo unknown)"
} >"$OUT/summary.md"

# --- miniredis ---
"$BIN" -addr "127.0.0.1:$MINI_PORT" -log-level warn >"$OUT/miniredis.log" 2>&1 &
PID=$!
wait_ready "$MINI_PORT"
run_server miniredis "$MINI_PORT" 1 16
kill "$PID"; wait "$PID" 2>/dev/null || true; PID=""

# --- real redis, optional ---
HAVE_REDIS=0
if command -v redis-server >/dev/null; then
  HAVE_REDIS=1
  echo "- redis-server: $(redis-server --version | head -1)" >>"$OUT/summary.md"
  redis-server --port "$REDIS_PORT" --bind 127.0.0.1 --save "" --appendonly no --daemonize no >"$OUT/redis.log" 2>&1 &
  PID=$!
  wait_ready "$REDIS_PORT"
  run_server redis "$REDIS_PORT" 1 16
  kill "$PID"; wait "$PID" 2>/dev/null || true; PID=""
else
  echo "- redis-server: not installed (comparison skipped)" >>"$OUT/summary.md"
fi

# --- summary table (requests per second) ---
rps() { awk -F, -v t="\"$2\"" 'tolower($1) ~ tolower(t) {gsub(/"/, "", $2); print $2; exit}' "$1" 2>/dev/null || true; }
{
  echo
  echo "| test | miniredis P=1 | miniredis P=16 | redis P=1 | redis P=16 |"
  echo "|---|---|---|---|---|"
  for t in $(tr ',' ' ' <<<"$TESTS"); do
    pat="$t"; [[ "$t" == ping_mbulk ]] && pat="PING_MBULK"; [[ "$t" == lrange_100 ]] && pat="LRANGE_100"
    a="$(rps "$OUT/miniredis-P1.csv" "$pat")"; b="$(rps "$OUT/miniredis-P16.csv" "$pat")"
    c="n/a"; d="n/a"
    if [[ $HAVE_REDIS -eq 1 ]]; then c="$(rps "$OUT/redis-P1.csv" "$pat")"; d="$(rps "$OUT/redis-P16.csv" "$pat")"; fi
    echo "| $t | ${a:-?} | ${b:-?} | ${c:-?} | ${d:-?} |"
  done
} >>"$OUT/summary.md"

echo
cat "$OUT/summary.md"
echo
echo "results saved in $OUT"
