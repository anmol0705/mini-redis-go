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
