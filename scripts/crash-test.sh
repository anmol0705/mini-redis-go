#!/usr/bin/env bash
# Crash-recovery test for the append-only file (run on Linux/Debian).
#
#   1. start miniredis with -appendonly yes -appendfsync always
#   2. write N keys with redis-cli
#   3. kill -9 the server (no chance to flush or clean up)
#   4. restart on the same directory and verify every key
#   5. simulate a crash in the middle of a write (append half a command), restart
#      again and verify the keys are still all there and the server logged the repair
#
# Usage: scripts/crash-test.sh [N] [PORT]
# Needs: redis-cli, and either a built binary (MINIREDIS_BIN) or a Go toolchain.
set -euo pipefail

N="${1:-1000}"
PORT="${2:-6390}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${MINIREDIS_BIN:-$ROOT/bin/miniredis}"
WORK="$(mktemp -d)"
PID=""

cleanup() {
  [[ -n "$PID" ]] && kill -9 "$PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

command -v redis-cli >/dev/null || { echo "redis-cli not found (apt install redis-tools)"; exit 2; }
if [[ ! -x "$BIN" ]]; then
  echo "building $BIN"
  (cd "$ROOT" && go build -o "$BIN" ./cmd/miniredis)
fi

start_server() {
  "$BIN" -addr "127.0.0.1:$PORT" -dir "$WORK/data" -appendonly yes -appendfsync always \
    -log-level info >>"$WORK/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    if [[ "$(redis-cli -p "$PORT" PING 2>/dev/null || true)" == "PONG" ]]; then return 0; fi
    sleep 0.1
  done
  echo "server did not start; log:"; cat "$WORK/server.log"; exit 1
}

crash_server() {
  kill -9 "$PID"
  wait "$PID" 2>/dev/null || true
  PID=""
}

verify_all() {
  local label="$1"
  local size
  size="$(redis-cli -p "$PORT" DBSIZE)"
  if [[ "$size" != "$N" ]]; then
    echo "FAIL ($label): DBSIZE is $size, expected $N"; exit 1
  fi
  for i in $(seq 1 "$N"); do echo "GET crash:$i"; done | redis-cli -p "$PORT" >"$WORK/got.txt"
  for i in $(seq 1 "$N"); do echo "value-$i"; done >"$WORK/want.txt"
  if ! diff -q "$WORK/want.txt" "$WORK/got.txt" >/dev/null; then
    echo "FAIL ($label): some keys are missing or wrong:"
    diff "$WORK/want.txt" "$WORK/got.txt" | head
    exit 1
  fi
  echo "ok ($label): all $N keys present"
}

mkdir -p "$WORK/data"

echo "== phase 1: write $N keys, then kill -9"
start_server
for i in $(seq 1 "$N"); do echo "SET crash:$i value-$i"; done | redis-cli -p "$PORT" >"$WORK/set.txt"
acks="$(grep -c '^OK$' "$WORK/set.txt" || true)"
[[ "$acks" == "$N" ]] || { echo "FAIL: only $acks of $N writes were acknowledged"; exit 1; }
crash_server

echo "== phase 2: restart and verify"
start_server
verify_all "after kill -9"
crash_server

echo "== phase 3: torn write (half a command appended), restart and verify"
printf '*3\r\n$3\r\nSET\r\n$10\r\ncrash:torn' >>"$WORK/data/appendonly.aof"
start_server
verify_all "after a torn tail"
grep -q "incomplete command" "$WORK/server.log" || { echo "FAIL: the repair was not logged"; exit 1; }
echo "ok: the server logged the truncation"
if [[ "$(redis-cli -p "$PORT" EXISTS crash:torn)" != "0" ]]; then
  echo "FAIL: the half-written command was applied"; exit 1
fi
crash_server

echo "PASS"
