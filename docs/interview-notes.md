# Interview notes

Running list: "I chose X over Y because Z; I measured W". Where nothing has
been measured it says so.

## Phase 1

- I chose a **per-connection goroutine** over an event loop because Go's
  scheduler makes blocking reads cheap and the code stays sequential.
  Measured: nothing yet (TBD - measure on Debian with `redis-benchmark`).
- I chose **flush-before-blocking-read** over flush-per-command and over
  "flush when nothing is buffered" because the first wastes syscalls and the
  second can deadlock on a half-received next command (decision 001).
  Measured: the deadlock case has a regression test; syscall savings TBD.
- I chose a **session object** for connection-scoped commands over a big
  `if` in the read loop, because MULTI/EXEC and SUBSCRIBE need per-connection
  state (decision 002).
- I chose **`Listen` then `Serve`** over a single `Run` so that the real
  port is known before serving and INFO can use the server without a setter.
- I chose **size caps and chunked reads** because the protocol lets the client
  announce sizes; I tested allocation with `runtime.MemStats` against a
  declared 512 MiB value (decision 003). The bound used in the test is a
  generous limit, not a benchmark.
- I chose **`SetReadDeadline(now)` for shutdown** over closing connections,
  so running commands still reply (decision 004).
- I chose an **injected `Handler` interface** over a test-only `PANIC`
  command, so production has no debug backdoor.
- I chose **`RWMutex` first**, then benchmarked it against a single-owner goroutine
  in Phase 2 before switching (see Phase 2 below for the numbers).
- I chose **standard library only** (`flag`, `log/slog`, `net`) to keep the
  project explainable end to end.

## Phase 2

- I chose a **single goroutine that owns the data** over a mutex because it makes
  transactions, propagation order and expiry trivially correct. I measured
  both: the queue hop costs about 0.4 us per command at handler level (0.85 vs
  0.5 us, medians of 3 runs), and over TCP the engine was even or ahead
  (133.6k vs 87.6k cmd/s without pipelining, 594k vs 531k at depth 16) on a
  Windows laptop. Debian numbers: TBD - measure on Debian. (Decision 005.)
- I chose **my own skip list with span counts** over a library or a sorted
  slice, so rank and range-by-rank are O(log n). I verified it against a
  sorted-slice reference with randomized operations, plus a structural
  invariant checker (spans, backward pointers, tail).
- I chose **lazy plus sampled active expiry** over a timer per key or a heap,
  because it has bounded cost per cycle and costs nothing extra per key. The
  sampler repeats while more than ~25% of a sample was expired.
- I chose to **rewrite relative commands into absolute ones** (`EXPIRE` to
  `PEXPIREAT`, `SET ... EX` to `SET ... PXAT`) at the point where the command
  executes, so the log and replicas are replay-safe.
- I chose a **ring-buffer deque** for lists so both ends are O(1).
- I chose to put **memory accounting inside the store** (incremental
  deltas, verified against a recount after random operations), so the
  `maxmemory` check is O(1).
- I chose **injected time and randomness** (`SetNow`, seeded `rand.Rand`) so
  expiry and eviction tests are deterministic.

## Phase 3

- I chose **drop the subscriber** over drop-the-message or block-the-publisher
  when a pub/sub queue overflows, because it keeps publishers non-blocking,
  bounds memory, and makes the loss visible to the client (decision 006).
  Measured: a test with 300 x 256 KiB messages shows the healthy subscriber
  gets all 300 while the stalled one is cut off.
- I chose to run **`EXEC` as a single engine request** over locking: atomicity
  comes from the single owner, with zero extra machinery.
- I chose to **validate at queue time** (unknown command, wrong arity) so
  `EXEC` can return `EXECABORT`, and **not** to roll back runtime errors,
  matching Redis semantics.

## Phase 4

- I chose **logging after success, in absolute form** over logging the raw
  command, so replay is deterministic regardless of when it happens (tested by
  replaying with a different clock).
- I chose **the engine as the only AOF writer** over a separate writer goroutine,
  so log order equals execution order and `appendfsync always` can fsync before
  the reply is released. (Decision 007.)
- I chose **copy-inside-the-engine for BGSAVE** because Go cannot fork. The pause
  is measured and logged per save and exported as `rdb_last_bgsave_copy_usec`;
  large-keyspace numbers: TBD - measure on Debian.
- I chose **capture + swap** for BGREWRITEAOF so concurrent writes are never
  lost; verified by a test where 83 increments landed during one rewrite and all
  survived a restart (a single run; the count varies run to run).
- I chose to **treat EOF inside a record as a torn tail and anything else as
  corruption**, and tested torn tails at every byte offset of a multi-command log.
- I chose a **hand-written snapshot format with a CRC32 trailer** over `gob` or
  JSON, so the format is documented, stable, size-bounded when decoding and
  independent of Go versions. Every single-byte flip and every truncation of a
  sample file is detected (tests iterate all offsets).
- I chose **strict replay** (a failing command stops startup) over Redis's
  tolerance, so replay bugs show up immediately.

## Phase 5

- I chose an **incremental memory estimate** over reading the Go heap: O(1),
  deterministic, verified against a full recount after random operations.
  It is not the real RSS (stated in known-limitations).
- I chose **sampled LRU (5 keys)** over an exact list: no per-read cost and no
  extra pointers. I have not measured hit-rate quality against true LRU:
  TBD - measure on Debian.
- I chose **a private replication protocol** over implementing PSYNC/RDB, and
  say so in the README and decision 008.
- I chose to **attach a replica inside the engine** so snapshot and stream have no
  gap; a test with a writer racing the full sync (757 increments in one run, all
  present afterwards) checks it.
- I chose **drop the replica** over blocking when its queue fills, so the primary
  never stalls; tested with a replica that stops reading.
- I chose **`DEL` on expiry** over letting replicas expire by their own clock;
  tested with two independent fake clocks.
- I chose to **cancel the follower's context on the engine goroutine** at
  promotion, so a late command cannot be applied after `REPLICAOF NO ONE`.

## Phase 6

- I chose to **state properties and fuzz** the four parsers (RESP, command
  execution, AOF, snapshot) plus glob matching. Result: no failures found in 20 s
  per target on the first run; `make fuzz-short` repeats it. Property examples:
  accepted input round-trips; a write's logged form is itself a valid command.
- I chose a **conformance suite driven only by `redis-cli`** so it can be reused for
  another implementation. I ran it against real Redis 7.2.12 (Memurai) and against
  this server: 16 of 16 checks on both. It found one real thing: this `redis-cli`'s
  `--pipe` needs inline commands, which this server does not support.
- I chose **not to optimise parsing**: `SET` parses in about 430 ns with 14
  allocations, while a TCP round trip costs several microseconds. Measure first.
- I chose **loopback-only pprof, off by default** because profiling endpoints leak
  internals and can burn CPU.
- I ran **`staticcheck`, `errcheck` and `ineffassign`** separately because the
  combined `golangci-lint` could not be installed here; they found five small
  issues (a dead assignment, three capitalised error strings, an ignored return),
  fixed with the clean runs recorded.
- I verified the **README command table with a test**, so it cannot drift from the
  real command table.
