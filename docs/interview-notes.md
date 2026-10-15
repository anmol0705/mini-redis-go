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
- I chose **`RWMutex` first** and will benchmark it against a single-owner
  goroutine in Phase 2 before replacing it. Measured: not yet.
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
