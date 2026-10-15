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

## Measurements (taken before switching)

Machine: Intel i5-1230U, 12 logical CPUs, Windows 11, Go 1.25.3, loopback TCP.
Workload: 50% GET / 50% SET, 10,000 keys, 32-byte values, 32 parallel clients.
Median of 3 runs of 3 s each; raw output in
`docs/bench/phase2-lock-vs-engine.txt`.

| Benchmark | mutex | engine |
|---|---|---|
| Handler only, no network (cmd/s) | 1.97 M | 1.16 M |
| TCP, no pipelining (cmd/s) | 87.6 k | 133.6 k |
| TCP, pipeline depth 16 (cmd/s) | 531 k | 594 k |

Run-to-run spread was large (for example the mutex at depth 16 ranged from
483 k to 671 k), so only the first two rows show a clear difference. Debian
numbers: TBD - measure on Debian.

How to read it:

- The queue hop costs roughly **0.4 us per command** (about 850 ns versus
  500 ns per command at handler level, medians). That is the real price of
  the design.
- End to end it is invisible: a TCP round trip costs 7 to 12 us per command
  here, so the 0.4 us is lost in the noise, and the engine was ahead without
  pipelining. A plausible reason is that with 32 goroutines contending for one
  mutex the scheduler wastes time parking and waking them, while the engine
  turns contention into a queue. I did not investigate further, so treat that
  as a hypothesis.
- At depth 16 the two are within noise of each other.

## Decision

Option 3. The measured cost is small next to the network, and the design gives
us, for free: atomic transactions (an EXEC is one request), a single place
where commands are applied *and* propagated (so AOF and replica order equal
execution order), an expiry sampler with no locking, and trivially race-free
data.

## Consequences

- One slow command (KEYS on a huge keyspace, a big LRANGE, SAVE) blocks every
  client. Redis has the same property. KEYS and SAVE are documented as such.
- Throughput of the data path is capped by one core; the network goroutines
  still use all cores for parsing and writing.
- The engine must never block: replies are plain values, the channel for each
  reply has room for it, the queue is bounded, and a submitted request is
  always answered (even at shutdown). Tests cover a client that vanishes while
  its request is pending, a full queue with a cancelled caller, and `Close`
  with blocked submitters.
- A panic in one command is recovered and answered with an error instead of
  killing the process; the DB might be left half-modified by that command,
  which is why a recovered panic is logged at error level.
- The server does not batch pipelined commands into one engine request: each
  command is its own round trip through the queue. A future optimisation could
  batch whatever is already buffered; it was not measured.
