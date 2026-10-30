# 005 - One goroutine owns the keyspace (instead of a mutex)

Status: accepted (Phase 2)

## Context

Phase 1 shared the store between connection goroutines with a lock. Phase 2
adds multi-step commands (INCR, LPOP count, EXPIRE with rewrites), a background
expiry sampler, and later MULTI/EXEC, an append-only file, snapshots and
replication. All of them need the same two things:

1. a command (or a whole transaction) must be atomic;
2. the order in which changes are *applied* must be the order in which they are
   *logged and replicated*.

## Options considered

1. **A mutex around each command** (`lockedHandler` in
   `internal/server/bench_test.go`). Simple. Atomic per command. But a
   transaction needs a lock that spans several commands, the expiry sampler and
   the AOF writer need the same lock, and the log order has to be guaranteed by
   holding the lock while appending to the log. A per-method `RWMutex` like
   Phase 1's cannot even make INCR atomic, so the lock must cover the whole
   command.
2. **Sharded locks.** Better scaling, but multi-key commands and transactions
   need lock ordering, and the log order problem remains.
3. **A single owner goroutine** (`internal/engine`). Connection goroutines
   queue requests on a bounded channel and wait for a plain `resp.Value`. The
   expiry sampler runs inside the same `select` loop. This is how Redis itself
   works.

## Measurements

Machine: Intel i5-1230U, 12 logical CPUs, Windows 11, Go 1.25.3, loopback TCP.
Workload: 50% GET / 50% SET, 10,000 keys, 32-byte values. TCP benchmark: 32
connections in the same process as the server. Handler benchmark (no network):
`b.SetParallelism(32)`, which is 32 x GOMAXPROCS = 384 goroutines.
Command (identical both times):
`go test -run '^$' -bench 'Handler|TCP' -benchtime 3s -count 3 ./internal/server`.
The "mutex" side is `lockedHandler` in `internal/server/bench_test.go`: one
global mutex around the typed DB (not the original Phase 1 store).

Medians of 3 runs, commands per second:

| Benchmark | mutex, 2026-10-15 | engine, 2026-10-15 | mutex, 2026-10-30 | engine, 2026-10-30 |
|---|---|---|---|---|
| Handler only, no network | 1.97 M | 1.16 M | 2.69 M | 1.19 M |
| TCP, no pipelining | 87.6 k | 133.6 k | 130.9 k | 126.2 k |
| TCP, pipeline depth 16 | 531 k | 594 k | 665 k | 577 k |

Raw output of the first run is in `docs/bench/phase2-lock-vs-engine.txt`.
Six alternating runs on 2026-10-30 for the TCP no-pipelining row gave engine
115 k to 179 k (median 127 k) and mutex 127 k to 151 k (median 132 k).

How to read it:

- **Over TCP there is no reliable difference.** The first run showed the engine
  ahead without pipelining (133.6 k vs 87.6 k), but that gap did not reproduce
  on a later run, so it must not be quoted as a result. The two designs are
  within run-to-run noise, which on this laptop is large.
- **With no network the mutex handler is clearly faster** (about 2.3 times in
  the later run). The engine pays for a hand-off to another goroutine on every
  command, roughly 0.4 to 0.5 us.
- Debian numbers: TBD - measure on Debian.

## Decision

Option 3. The measurements do not show a throughput win. The cost of the queue
hop is small next to a network round trip, and the design gives us, for free:
atomic transactions (an EXEC is one request), a single place where commands are applied *and* propagated (so AOF and replica order equal
execution order), an expiry sampler with no locking, and trivially race-free
data.

## Consequences

- One slow command (KEYS on a huge keyspace, a big LRANGE, SAVE) blocks every
  client. Redis has the same property. KEYS and SAVE are documented as such.
- Throughput of the data path is capped by one core; the network goroutines
  still use all cores for parsing and writing.
- The engine must never block: replies are plain values, the channel for each
  reply has room for it, the queue is bounded, and a submitted request is
  always answered (even at shutdown). Tests cover a full queue with a cancelled
  caller and `Close` with queued or blocked submitters. A client that vanishes
  while its request is pending was only checked by hand (no automated test).
- A panic in one command is recovered and answered with an error instead of
  killing the process; the DB might be left half-modified by that command,
  which is why a recovered panic is logged at error level.
- The server does not batch pipelined commands into one engine request: each
  command is its own round trip through the queue. A future optimisation could
  batch whatever is already buffered; it was not measured.
