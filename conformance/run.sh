#!/usr/bin/env bash
# Conformance suite: runs command files through redis-cli and compares the
# output with expected files. It only needs a host, a port and redis-cli, so it
# works against ANY server that speaks RESP - miniredis (Go), a future C port,
# or real Redis (which is how the expected files were meant to be validated).
#
# Usage:
#   conformance/run.sh HOST PORT            run every case
#   conformance/run.sh HOST PORT --record   write the server's actual output over
#                                           the .out files (use against real Redis,
#                                           then review the diff in git)
#
# Environment: REDIS_CLI (default: redis-cli)
#
# How outputs are compared: redis-cli, when its output is not a terminal, prints
# raw values one per line. Blank lines are DROPPED from both sides, so a nil
# reply and an empty array (both print as blank lines) are not distinguished, and
# the exact placement of blank lines after errors does not matter. Commands whose
# reply order is unspecified (HGETALL, SMEMBERS, KEYS on several keys) are only
# used with a single element.
set -uo pipefail

if [[ $# -lt 2 ]]; then
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
HOST="$1"; PORT="$2"; MODE="${3:-check}"
CLI="${REDIS_CLI:-redis-cli}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
command -v "$CLI" >/dev/null || { echo "$CLI not found (apt install redis-tools)"; exit 2; }

cli() { "$CLI" -h "$HOST" -p "$PORT" "$@"; }

if [[ "$(cli PING 2>/dev/null || true)" != "PONG" ]]; then
  echo "no server answering PING at $HOST:$PORT"; exit 2
fi

pass=0; fail=0
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

ok()  { echo "ok   $1"; pass=$((pass + 1)); }
bad() { echo "FAIL $1"; fail=$((fail + 1)); }

# --- 1. command files with expected output ---
for in in "$DIR"/cases/*.in; do
  name="$(basename "${in%.in}")"
  out="${in%.in}.out"
  cli <"$in" 2>&1 | grep -v '^$' >"$TMP/actual" || true
  if [[ "$MODE" == "--record" ]]; then
    cp "$TMP/actual" "$out"; echo "recorded $name"; continue
  fi
  if grep -v '^$' "$out" | diff -u - "$TMP/actual" >"$TMP/diff"; then
    ok "$name"
  else
    bad "$name"; sed 's/^/     /' "$TMP/diff" | head -20
  fi
done
[[ "$MODE" == "--record" ]] && exit 0

# --- 2. checks that cannot be a fixed expected file ---

# Time passing: a key with a short TTL disappears by itself.
cli FLUSHALL >/dev/null
cli SET shortlived v PX 150 >/dev/null
sleep 0.5
if [[ "$(cli EXISTS shortlived)" == "0" && -z "$(cli GET shortlived)" ]]; then ok "expiry elapses"; else bad "expiry elapses"; fi

# Active expiry: keys nobody reads are removed in the background.
for i in $(seq 1 50); do echo "SET bulk:$i v PX 100"; done | cli >/dev/null
sleep 1.5
if [[ "$(cli DBSIZE)" == "0" ]]; then ok "active expiry removes unread keys"; else bad "active expiry (DBSIZE=$(cli DBSIZE))"; fi

# The reply to an unknown command starts like Redis's (the rest of the text differs between servers).
if [[ "$(cli NOSUCHCOMMAND 2>&1 | head -1)" == "ERR unknown command"* ]]; then ok "unknown command error"; else bad "unknown command error"; fi

# INFO has the fields clients rely on.
info="$(cli INFO | tr -d '\r')"
missing=""
for f in redis_version tcp_port uptime_in_seconds connected_clients; do
  grep -q "^$f:" <<<"$info" || missing="$missing $f"
done
if [[ -z "$missing" ]]; then ok "INFO fields"; else bad "INFO is missing:$missing"; fi

# A big value survives a round trip byte for byte.
head -c 300000 /dev/urandom | base64 -w0 >"$TMP/big"
cli -x SET bigvalue <"$TMP/big" >/dev/null
if [[ "$(cli STRLEN bigvalue)" == "$(wc -c <"$TMP/big")" ]]; then ok "large value length"; else bad "large value length"; fi

# Pipelining with many commands in one write (what --pipe does) but through a
# plain stdin script, which every server must support.
for i in $(seq 1 1000); do echo "SET pipe:$i $i"; done | cli >"$TMP/pipe" 2>&1
if [[ "$(grep -c '^OK$' "$TMP/pipe")" == "1000" && "$(cli GET pipe:1000)" == "1000" ]]; then ok "1000 pipelined commands"; else bad "1000 pipelined commands"; fi

# redis-cli --pipe is informational: some redis-cli versions end the stream with
# an INLINE command (ECHO ...) to detect completion, and miniredis does not
# support inline commands (see docs/known-limitations.md).
for i in $(seq 1 100); do
  k="pipe2:$i"; v="$i"
  printf '*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' "${#k}" "$k" "${#v}" "$v"
done | timeout 15 "$CLI" -h "$HOST" -p "$PORT" --pipe >"$TMP/pipe2" 2>&1
if grep -q 'errors: 0' "$TMP/pipe2"; then echo "ok   redis-cli --pipe (informational)"; else echo "note redis-cli --pipe is not supported by this server/redis-cli combination (needs inline commands)"; fi

cli FLUSHALL >/dev/null
echo
echo "passed: $pass   failed: $fail"
[[ "$fail" -eq 0 ]]
