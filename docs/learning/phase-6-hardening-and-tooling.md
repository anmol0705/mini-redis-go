# Phase 6 - Hardening, tooling, documentation

## What was built

- **Fuzz tests** for the RESP reader, command execution, the AOF loader, the
  snapshot loader and glob matching.
- **Benchmarks** for parsing, store operations, the engine, the skip list, and the
  mutex-vs-engine comparison from Phase 2.
- **`-pprof-addr`**: Go's profiler on a loopback-only port, off by default.
- **A `redis-cli` conformance suite** that works against any server.
- **Scripts**: `bench/run.sh` (redis-benchmark vs real Redis), `scripts/crash-test.sh`
  (`kill -9` recovery), `Makefile`, GitHub Actions CI, `.golangci.yml`.
- **Docs**: README, Debian guide, formats, decisions, this learning series.

## How the pieces work, with one example

*Fuzzing the snapshot loader.* `go test -fuzz=FuzzDecode` starts from a few valid
and broken snapshots (the **seed corpus**), mutates their bytes millions of times,
and keeps any mutation that reaches new code. For every input the test checks:
no panic, no hang, and if the loader accepts the bytes then encoding the result
and decoding again gives identical data. The fuzzer found no failure in 20 seconds
per target; the point is that the property is stated and re-checkable
(`make fuzz-short`).

## Go concepts used

- **`testing.F` (fuzzing)** - `f.Add` gives seeds, `f.Fuzz` takes a function of the
  inputs. The seeds also run as normal tests, so a plain `go test` re-checks them.
- **`b.Loop()` benchmarks** - the Go 1.24+ loop that keeps the compiler from
  optimising work away; `b.ReportAllocs()` shows allocations per operation.
- **`b.RunParallel`** - runs a benchmark body on many goroutines to model many
  clients.
- **`net/http/pprof` on a private mux** - importing the package registers handlers
  on the default mux; using our own mux and a loopback check means nothing leaks.
- **Build-independent code** - nothing OS-specific is used, so the same code is
  built for Linux, Windows and macOS in CI (`cross-build` job).
- **Black-box testing** - the conformance suite only knows host, port and
  `redis-cli`, so it can test a different implementation of the same protocol.

## Interview questions

1. **What does fuzzing find that unit tests do not?**
   Inputs nobody thought of: odd lengths, truncations, huge counts. Unit tests check
   the cases I imagined; fuzzing explores the rest. I state properties (never
   panic, round trip) so the fuzzer can judge any input.

2. **How did you protect the loaders from malicious files?**
   Lengths and counts from the file are never used to allocate: strings are read
   in 64 KiB chunks, slices start small and grow as elements arrive, and the CRC
   is verified before anything is returned. Tests assert the bytes allocated for a
   file that lies about its size.

3. **Why a conformance suite that only uses `redis-cli`?**
   It is implementation-agnostic: the same files can test a future C
   implementation or a real Redis. I ran it against real Redis 7.2 (through
   Memurai) and against this server and both pass, which validates the expected
   outputs themselves.

4. **What did you measure and what are the limits of those numbers?**
   Go micro-benchmarks and a mutex-vs-engine comparison on one Windows laptop.
   The redis-benchmark comparison is a script waiting for a Linux run; the README
   says TBD until then.

5. **Why is the pprof endpoint loopback-only?**
   Profiling endpoints reveal internals and can burn CPU on demand. The flag
   refuses any non-loopback address and the endpoint is off by default.

6. **What would you do first to make it faster?**
   Measure with pprof under `redis-benchmark`. Candidates: batching pipelined
   commands into one engine request, fewer allocations in `ReadCommand` (14 per
   `SET`), and avoiding per-reply `fmt.Fprintf`. I did not optimise them because
   parsing costs a few hundred ns against several microseconds of network round
   trip.

7. **How does CI cover both operating systems?**
   Linux runs `go test -race -cover`, Windows runs `go test`, and a matrix builds
   and vets for four OS/architecture pairs. A lint job checks formatting, `vet` and
   `golangci-lint`.

8. **What is still unverified?**
   Real `SIGTERM`, `kill -9` recovery, `redis-benchmark` compatibility and the
   Debian `redis-cli`, listed with exact commands in `docs/DEBIAN_TESTING.md`.
