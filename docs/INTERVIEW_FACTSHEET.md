# Interview factsheet: mini-redis-go

Everything here was checked against the code or against a command I ran.
Anything I could not check is marked **UNVERIFIED**.

- **Code state checked:** commit `a8e718c` ("chore: version 1.0.0", tag `v1.0`). Later commits (listed in section 13) fixed the problems this document found; the findings below are marked FIXED where that happened. Everything is now pushed.
- **Evidence date:** 2026-10-30 (local time +05:30), unless a line says otherwise.
- **Machine:** Intel Core i5-1230U (10 cores, 12 threads), 15.7 GB RAM, Windows 11 Home 10.0.26300, Go 1.25.3 windows/amd64.
- **Line numbers** like `engine.go:285` refer to that commit.
- **Evidence tags** like [E5] point to the log in Appendix A.

Status words used below:

| Word | Meaning |
|---|---|
| VERIFIED | I ran a command or test and saw the result. |
| CODE | I read the code. No test or run proves it. |
| UNVERIFIED | I did not check it. Do not claim it. |

---

## 0. Read this first: the findings that matter most

These are the surprises. Know them before the interview.

1. **FIXED: nothing was on GitHub except the old placeholder.** When I first checked, remote `main` was `3d97dc4` ("add placeholder main") and your 25 newer commits and 3 tags were only on this laptop. They were pushed afterwards (section 13). The repo is public. [E17]
2. **FIXED: a fresh clone failed `go test ./...`.** The 10 expected-output files `conformance/cases/*.out` had never been committed, because `.gitignore` line 15 (`*.out`) hid them. `TestConformanceCases` (10 subtests) failed on a clone. After committing the files, a fresh clone passes all 14 packages. [E18]
3. **The resume claim "133.6k vs 87.6k commands/s" did not reproduce.** The saved run does show those numbers. A re-run today (and 3 more interleaved rounds) shows no reliable difference. The engine median was 127k and the mutex median was 132k commands/s. Remove the "~50% higher" claim. Details in section 1. [E5] [E6]
4. **"client libraries work" was never tested.** Only `redis-cli` (Memurai's build, `memurai-cli` 4.1.8) was used. No Go, Python or Node client library was run. [E13]
5. **"LRU eviction" is approximate and off by default.** It samples 5 random keys and drops the oldest. The default is `-maxmemory 0` and policy `noeviction`.
6. **Persistence is off by default.** `-appendonly` defaults to `no`. With defaults, nothing is written to disk.
7. **The memory limit is an estimate.** For 500,000 small keys it said 52 MB while the process used 139 MB. [E10]
8. **One slow command stalls everyone.** `KEYS *` over 500,000 keys made another client's `PING` wait up to 64 ms (idle worst case: 0.7 ms). [E8]
9. **Replication is private to this project.** It cannot talk to real Redis. There is no failover. A primary restart forces every replica to do a full resync.
10. **Still not run:** Debian by hand, `redis-benchmark`, real Ctrl+C or SIGTERM handling, the `Makefile`. **GitHub Actions has now run and is green** (lint, Linux race tests, Windows tests, 4 cross-builds), which also covers Linux `-race` and `golangci-lint`. The `kill -9` crash test passed on Windows only. [E12] [E22]

### 0.1 Small glossary for a C++ reader

| Go word | Nearest C++ idea | Where it appears here |
|---|---|---|
| goroutine | a very cheap thread, scheduled by the Go runtime | one per client connection, one engine goroutine |
| channel (`chan T`) | a thread-safe queue; a buffered one has a fixed capacity and blocks when full | `engine.reqs`, pub/sub queues |
| `select` | wait on several queues at once (like `poll`) | `engine.run`, `stream` |
| `context.Context` | a cancellation token (like `std::stop_token`) | passed to `Serve`, `Execute` |
| `defer` | scope guard / RAII | `defer s.release(c)` |
| interface | structural: any type with the methods fits (like a C++ concept) | `server.Handler`, `engine.Propagator` |
| `sync.Mutex`, `RWMutex` | `std::mutex`, `std::shared_mutex` | listed in section 3.3 |
| `sync/atomic` | `std::atomic` | listed in section 3.3 |
| `sync.WaitGroup` | a countdown latch | waiting for goroutines to exit |
| slice, map | `vector`, `unordered_map` (Go map order is random on purpose) | everywhere |
| `error` return | return value instead of exceptions; `panic`/`recover` is the exception-like escape hatch, used only to contain bugs | `serveConn`, `engine.handle` |

| Redis word | Meaning |
|---|---|
| RESP | The wire format. A command is an array of bulk strings: `*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nanmol\r\n` |
| TTL | Time to live: a deadline after which a key disappears |
| AOF | Append-only file: a log of every write command, replayed at startup |
| RDB / snapshot | A point-in-time dump of all data. **This project uses its own format (`MRDB`), not Redis's RDB** |
| primary / replica | A server and a read-only copy that follows it |
| pipelining | Sending many commands without waiting for each reply |

---

## 1. Resume claims check

Your two bullets, checked phrase by phrase.

> • Built a Redis-compatible in-memory database in Go (standard library only) that speaks RESP2 (redis-cli and client libraries work), with 50+ commands, pub/sub, transactions, LRU eviction, append-only-log persistence and replication.
> • Replaced a mutex-guarded store with a single goroutine that owns the keyspace behind a bounded channel after benchmarking both: 133.6k vs 87.6k commands/s over TCP without pipelining (Windows laptop). Verified with race-detector runs, 5 fuzz targets and a 16-of-16 redis-cli conformance suite.

### 1.1 "in-memory database in Go (standard library only)": TRUE

- `go.mod` has 3 lines: the module name, a blank line and `go 1.25.3`. There is no `require` line. There is no `go.sum` and no `vendor/` folder. [E17]
- `go list -deps` over all packages shows zero packages outside the standard library and this project. [E17]
- Standard-library packages used by non-test code: `bufio bytes context errors flag fmt hash io math net os slices strconv strings sync syscall time`, plus `crypto/rand encoding/binary encoding/hex hash/crc32 log/slog math/rand/v2 net/http net/http/pprof os/signal path/filepath runtime/debug sync/atomic` (and `testing`, only through the test-helper package `internal/testclient`).
- Wording: "data store" is the usual word for Redis. "Database" is fine.

### 1.2 "speaks RESP2 (redis-cli and client libraries work)": PARTLY TRUE

- **TRUE:** `redis-cli` works. The conformance suite ran with `memurai-cli` 4.1.8 (a `redis-cli` build) and passed 16 of 16 checks against this server. [E13]
- **UNVERIFIED:** "client libraries work". No Go, Python or Node client library was ever pointed at it.
- **UNVERIFIED:** the Debian `redis-cli` and `redis-benchmark`.
- **Known gaps, VERIFIED live** [E7]:
  - Inline commands (`PING\r\n`) get `-ERR Protocol error: expected array header`, and the connection is closed.
  - `HELLO 3` (RESP3) returns `unknown command`.
  - `AUTH`, `SELECT`, `WATCH`, `SCAN` and `PSUBSCRIBE` return `unknown command`.
- **Smallest honest rewording:** "so `redis-cli` connects to it". Drop "and client libraries work".

### 1.3 "50+ commands": TRUE (exact number: 65 user commands)

- The command table in `command.go:213` (`buildTable`) has **59** entries. [E17]
- The session layer (`session.go:59-88`) adds **6** more: `QUIT`, `MULTI`, `EXEC`, `DISCARD`, `SUBSCRIBE`, `UNSUBSCRIBE`.
- A 7th session name, `MRSYNC`, is the private replication handshake, not a user command.
- So: 59 + 6 = **65 user commands** (66 names with `MRSYNC`).
- The README table matches the code in both directions. [E21]
- **Three of the 59 are stubs:** `COMMAND` returns an empty array. `CONFIG` accepts only `GET` and returns an empty array. `CLIENT` accepts only `SETNAME` and `SETINFO` and does nothing with them.

All 66 names by group:

| Group | Commands |
|---|---|
| Connection and server (11 in table) | `PING` `ECHO` `COMMAND`(stub) `CLIENT`(stub) `CONFIG`(stub) `INFO` `PUBLISH` `SAVE` `BGSAVE` `BGREWRITEAOF` `REPLICAOF` |
| Strings (10) | `SET` `GET` `MGET` `MSET` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `STRLEN` |
| Keys (14) | `DEL` `EXISTS` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` `TYPE` `KEYS` `DBSIZE` `FLUSHALL` `FLUSHDB` |
| Lists (7) | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` |
| Hashes (6) | `HSET` `HGET` `HDEL` `HGETALL` `HEXISTS` `HLEN` |
| Sets (5) | `SADD` `SREM` `SISMEMBER` `SMEMBERS` `SCARD` |
| Sorted sets (6) | `ZADD` `ZREM` `ZSCORE` `ZRANK` `ZRANGE` `ZCARD` |
| Session layer (6 + 1) | `QUIT` `MULTI` `EXEC` `DISCARD` `SUBSCRIBE` `UNSUBSCRIBE` and the internal `MRSYNC` |

Command options that exist: `SET` takes `EX PX EXAT PXAT NX XX KEEPTTL`. `ZRANGE` takes `WITHSCORES`. `LPOP`/`RPOP` take a count. `FLUSHALL`/`FLUSHDB` take `ASYNC` or `SYNC` (both are ignored: the flush is always synchronous).

### 1.4 "pub/sub, transactions, LRU eviction, append-only-log persistence and replication": mostly TRUE, with caveats

| Feature | Verdict | What to say precisely |
|---|---|---|
| Pub/sub | TRUE | `SUBSCRIBE`/`UNSUBSCRIBE`/`PUBLISH`, exact channel names only. No `PSUBSCRIBE`. Messages are not logged or replicated. |
| Transactions | TRUE | `MULTI`/`EXEC`/`DISCARD`. Runs as one atomic engine request. No `WATCH`. Errors inside `EXEC` do not roll back (same as Redis). |
| Key expiry | TRUE | Lazy plus a sampling sweeper (section 6). |
| "LRU eviction" | PARTLY TRUE | It is **sampled** LRU: look at 5 random keys, evict the least recently used of them. Also available: `allkeys-random` and `volatile-ttl`. **Off by default** (`-maxmemory 0`, `noeviction`). Hit-rate vs real LRU was never measured. |
| AOF persistence | TRUE, but **off by default** | Flag `-appendonly yes` turns it on. |
| Replication | TRUE | Full and partial resync, read-only replicas. **Private protocol, not compatible with Redis. No failover.** |

### 1.5 "Replaced a mutex-guarded store with a single goroutine...": TRUE as history

- **VERIFIED in git:** tag `v0.1` has `internal/store/store.go` with a `sync.RWMutex` around each method. Commits `2ab9bcf` (typed store, no locks) and `8ea705a` (engine goroutine) replaced it. HEAD has no `RWMutex` in `internal/store`. [E17]
- **Important detail:** the "mutex version" in the benchmark is **not** the original `v0.1` store. It is `lockedHandler` in `internal/server/bench_test.go:35`. It wraps the *new* typed `store.DB` in one global `sync.Mutex`. The original `v0.1` store cannot be benchmarked with this harness (different API). If an interviewer asks "did you benchmark your original store?", the honest answer is no: you benchmarked a global-mutex design against the engine design.

### 1.6 "133.6k vs 87.6k commands/s over TCP without pipelining": FALSE as a claim about the design

The numbers are real: they are the medians of one saved run. But they do **not** reproduce.

**What I ran** (identical command both times):

```
go test -run '^$' -bench 'Handler|TCP' -benchtime 3s -count 3 ./internal/server
```

**Setup:**

- **Date of the re-run:** 2026-10-30, 20:25 to 20:26 (+05:30). The original run was 2026-10-15 (saved in `docs/bench/phase2-lock-vs-engine.txt`).
- **Machine:** the one in the header. Windows power plan is a custom one named "More Powerrrrrrr baby" (its settings were not inspected). A Memurai Windows service (44 MB) was idle in the background.
- **Workload:** 50% `GET` / 50% `SET`, 10,000 keys, 32-byte values (`bench_test.go:65-84`).
- **Client count, TCP benchmark:** 32 TCP connections (`benchClient = 32`), in the **same process** as the server, over 127.0.0.1. Each connection sends one command at a time (pipeline 1) or 16 commands per write (pipeline 16).
- **Client count, "Handler" benchmark (no network):** `b.SetParallelism(32)` means 32 times GOMAXPROCS (12) = **384 goroutines**.
- **Server settings in the benchmark:** defaults, engine queue size 4096, no AOF, no replication.

**Results** (commands per second):

| Benchmark | Original run 2026-10-15 (median of 3) | Re-run 2026-10-30 (median of 3) |
|---|---|---|
| TCP, pipeline 1, mutex | 87,551 | 130,938 |
| TCP, pipeline 1, engine | 133,586 | 126,202 |
| TCP, pipeline 16, mutex | 530,722 | 664,557 |
| TCP, pipeline 16, engine | 593,672 | 576,958 |
| Handler only, mutex | 1,974,977 | 2,686,465 |
| Handler only, engine | 1,162,730 | 1,188,843 |

**Extra check for the key row** (TCP, pipeline 1): I ran engine and mutex alternately, 3 rounds, same day [E6].

- Engine runs: 179,030 / 115,390 / 137,074.
- Mutex runs: 150,491 / 141,609 / 126,820.
- Combined with the 3 re-run values, 6 runs each: **engine median 126,725 (range 115,390 to 179,030); mutex median 131,772 (range 126,820 to 150,491).** Engine is 3.8% below the mutex median, which is inside the noise.
- The original gap was +52.6% for the engine (133,586 / 87,551). It did not appear again.

**What is solid:**

- With **no network**, the mutex handler is faster: 2.69M vs 1.19M commands/s today, and 1.97M vs 1.16M in the original run. The engine pays for a channel hand-off to another goroutine on every command.
- Over TCP, the network cost hides that difference. Neither design is reliably faster.

**Smallest honest rewording** for the resume line:
"Replaced a mutex-guarded store with a single goroutine that owns all data behind a bounded channel (atomic transactions, ordered logging); benchmarked both: comparable throughput over TCP on a Windows laptop."
Or drop the benchmark sentence entirely and keep only the design sentence.

### 1.7 "Verified with race-detector runs": TRUE (Windows only)

- `go test -race -count=1 ./...` passes for all 14 test packages. [E3]
- **Linux:** the GitHub Actions job `test-linux` runs `go test -race -cover ./...` on Ubuntu and passed on the final run (section 13, [E22]).
- Caveat: the race detector only sees races on code paths the tests actually run.

### 1.8 "5 fuzz targets": TRUE

`FuzzReadCommand` (resp), `FuzzExecute` (command), `FuzzReplay` (aof), `FuzzDecode` (snapshot), `FuzzMatch` (glob). [E2]

- Run today for 10 s each: all PASS, 1.0M to 2.3M executions each, no failures. [E14]
- Run earlier in this project for 20 s each: no failures.
- These are short runs. They were never run for hours.
- Plain `go test` also runs each target's seed inputs.

### 1.9 "16-of-16 redis-cli conformance suite": TRUE, with a precise meaning, but see finding 2

- **16 checks** = 10 command-script cases (184 commands in total across the 10 `.in` files) + 6 extra checks (expiry elapses, active expiry, unknown-command error prefix, INFO fields, 300 KB value length, 1000 pipelined commands). [E13]
- Run today against this server: 16 passed, 0 failed. Run today against my own separate Memurai instance (Redis 7.2.12 compatible): 16 passed, 0 failed. [E13]
- The CLI used was `memurai-cli` 4.1.8, not the Debian `redis-cli` (**UNVERIFIED** on Debian).
- A 17th check (`redis-cli --pipe`) prints "note: not supported" against this server, because it needs inline commands. It is informational and does not count.
- **Catch (fixed):** the suite needs the 10 `.out` files, which had not been committed (finding 2). They are committed now.

### 1.10 "unit, fuzz and redis-cli conformance tests": TRUE (exact counts)

| Item | Count | How I counted |
|---|---|---|
| Test functions (`Test...`) | **229** | `go test -list '^Test'` per package [E2] |
| Fuzz targets | **5** | `go test -list '^Fuzz'` |
| Benchmarks | **23** | `go test -list '^Benchmark'` |
| Tests reported by `go test -v` | 234 top-level (229 + 5 fuzz seed runs), 364 subtests, **598 PASS**, 0 FAIL, 0 SKIP | counted lines in the verbose output (working tree) [E2] |
| Conformance command scripts | 10 files, 184 commands | `conformance/cases/*.in` |
| Conformance checks | 16 | `conformance/run.sh` |

Test functions per package: cmd/miniredis 5, aof 22, app 47, command 13, engine 24, glob 2, persist 8, pubsub 7, repl 4, resp 16, server 34, skiplist 4, snapshot 17, store 26. (`internal/repl` has few tests of its own; its code is exercised by the 16 replication tests in `internal/app/repl_test.go`.)

`go test ./...` summary (working tree, 2026-10-30 20:23) [E1]:

```
ok  	cmd/miniredis        ok  internal/aof      ok  internal/app       ok  internal/command
ok  	internal/engine      ok  internal/glob     ok  internal/persist   ok  internal/pubsub
ok  	internal/repl        ok  internal/resp     ok  internal/server    ok  internal/skiplist
ok  	internal/snapshot    ok  internal/store    ?   internal/testclient [no test files]
```

**On a fresh clone, the same command first failed** in `internal/app` (finding 2, since fixed). After the fix, a fresh clone passed all 14 packages. [E18]

Coverage measured today [E19]. Own-package: aof 94.0%, command 96.6%, glob 92.5%, pubsub 100%, resp 95.8%, skiplist 94.4%, snapshot 96.7%, store 96.1%, app 89.5%, engine 87.4%, cmd 86.3%, server 81.9%, persist 58.3%, repl 9.4%. Counting every package's tests against `repl`, `persist`, `engine`, `server`, `app`: repl 87.8%, persist 88.6%, engine 90.2%, server 91.4%, app 90.2%.

### 1.11 Suggested resume text (smallest honest changes)

- Bullet 1: "Built an in-memory key-value store in Go (standard library only) that speaks the Redis protocol (RESP2), so `redis-cli` connects to it, with 65 commands, pub/sub, transactions, key expiry, LRU-style eviction, append-only-file persistence and replication between instances."
- Bullet 2: "Replaced a mutex-guarded store with a single goroutine that owns all data behind a bounded channel (atomic transactions, ordered log); benchmarked both, with comparable throughput over TCP. Tested with the race detector, 5 fuzz targets and a `redis-cli` conformance suite that passes on this server and on a Redis 7.2-compatible server."

---

## 2. Project map

### 2.1 Every tracked file

110 tracked files. The line counts below were generated from the files at the time this document was written. 67 Go files: 7,521 lines of non-test code and 7,951 lines of test code (counted at `a8e718c`).

| File | Lines | What it does |
|---|---|---|
| `.gitattributes` | 0 | Forces LF line endings in git. |
| `.github/workflows/ci.yml` | 60 | GitHub Actions: lint, Linux race tests, Windows tests, 4-target cross build. Ran on GitHub: all 7 jobs green after two lint fixes. |
| `.gitignore` | 40 | Ignore rules. Line 15 (`*.out`) used to hide the conformance expected-output files; the last 2 lines un-ignore them (finding 2, fixed). |
| `.golangci.yml` | 33 | golangci-lint config (5 linters). Accepted by golangci-lint v2.15.0 in CI. |
| `LICENSE` | 21 | MIT licence text. |
| `Makefile` | 50 | Targets build, test, race, lint, bench, fuzz-short, conformance, cover, clean. Never run (no make here). |
| `README.md` | 185 | Overview, quick start, command table, diagram, benchmarks, limitations. |
| `bench/results/.gitkeep` | 0 | Keeps the results folder in git. |
| `bench/run.sh` | 101 | redis-benchmark runner (miniredis and real redis-server, P=1 and P=16). Never run. |
| `cmd/miniredis/main.go` | 158 | Program entry: flags, Ctrl+C/SIGTERM handling, calls app.Run. |
| `cmd/miniredis/main_test.go` | 150 | Tests flag errors, -h, shutdown on context cancel, parseSize, parseYesNo. |
| `conformance/README.md` | 59 | Explains the redis-cli conformance suite and its verified status. |
| `conformance/cases/01-basic.in` | 11 | Conformance script: PING, ECHO, SET/GET, DEL, EXISTS, DBSIZE. |
| `conformance/cases/02-strings.in` | 34 | Conformance script: string commands and SET options. |
| `conformance/cases/03-keys.in` | 33 | Conformance script: keys, TTL, EXPIRE family, KEYS, TYPE. |
| `conformance/cases/04-lists.in` | 19 | Conformance script: list commands. |
| `conformance/cases/05-hashes.in` | 13 | Conformance script: hash commands. |
| `conformance/cases/06-sets.in` | 10 | Conformance script: set commands. |
| `conformance/cases/07-zsets.in` | 21 | Conformance script: sorted-set commands. |
| `conformance/cases/08-errors.in` | 13 | Conformance script: WRONGTYPE, arity and syntax errors. |
| `conformance/cases/09-transactions.in` | 27 | Conformance script: MULTI/EXEC/DISCARD/EXECABORT. |
| `conformance/cases/10-pubsub-and-server.in` | 3 | Conformance script: PUBLISH, CLIENT SETNAME. |
| `conformance/run.sh` | 106 | Runs every case through redis-cli against HOST PORT (16 checks). Needs the .out files (committed now). |
| `docs/DEBIAN_TESTING.md` | 208 | Step-by-step Debian test guide (none of it run on Debian yet). |
| `docs/INTERVIEW_FACTSHEET.md` | 1222 | This file. |
| `docs/architecture.md` | 108 | Package map, request lifecycle, concurrency model, goroutine table. |
| `docs/bench/phase2-lock-vs-engine.txt` | 27 | Raw output of the 2026-10-15 mutex-vs-engine benchmark. |
| `docs/decisions/001-flush-strategy.md` | 43 | Decision: flush replies just before the reader would block. |
| `docs/decisions/002-session-layer.md` | 58 | Decision: per-connection session layer (still mentions the removed Dispatcher). |
| `docs/decisions/003-network-input-limits.md` | 36 | Decision: caps and chunked reads for untrusted sizes. |
| `docs/decisions/004-shutdown.md` | 51 | Decision: graceful shutdown via read deadlines and a grace period. |
| `docs/decisions/005-single-owner-engine.md` | 86 | Decision: single-owner engine (its TCP numbers did not reproduce). |
| `docs/decisions/006-slow-subscribers.md` | 56 | Decision: disconnect slow subscribers. |
| `docs/decisions/007-persistence-design.md` | 90 | Decision: AOF, snapshot and rewrite design. |
| `docs/decisions/008-eviction-and-replication.md` | 92 | Decision: memory estimate, sampled LRU, private replication protocol. |
| `docs/formats.md` | 82 | On-disk formats of the AOF and the snapshot. |
| `docs/interview-notes.md` | 133 | 'I chose X over Y' list (lines 38-39 hold the old benchmark claim). |
| `docs/known-limitations.md` | 100 | The project's own limitations list. |
| `docs/learning/phase-1-runnable-server.md` | 120 | Phase 1 learning notes. |
| `docs/learning/phase-2-data-model-and-engine.md` | 100 | Phase 2 learning notes. |
| `docs/learning/phase-3-pubsub-and-transactions.md` | 99 | Phase 3 learning notes. |
| `docs/learning/phase-4-persistence.md` | 100 | Phase 4 learning notes. |
| `docs/learning/phase-5-eviction-and-replication.md` | 100 | Phase 5 learning notes. |
| `docs/learning/phase-6-hardening-and-tooling.md` | 82 | Phase 6 learning notes. |
| `go.mod` | 3 | Module path and Go version. No dependencies. |
| `internal/aof/aof.go` | 516 | Append-only log: buffer, fsync policies, everysec goroutine, Replay, rewrite capture and swap, WriteItems. |
| `internal/aof/aof_test.go` | 628 | Fsync policies with a fake file, torn tail at every byte, corruption, rewrite. |
| `internal/aof/fuzz_test.go` | 57 | FuzzReplay. |
| `internal/app/app.go` | 275 | Wires everything: Config, Start, Run, Node, shutdown order. |
| `internal/app/app_test.go` | 268 | End-to-end tests over TCP: all types, expiry, maxmemory, eviction, pprof. |
| `internal/app/conformance_test.go` | 115 | Replays conformance/cases against a node (needs the .out files). |
| `internal/app/hooks.go` | 98 | Adapters: Hooks/SectionSource, multiPropagator (AOF then replication), replApplier. |
| `internal/app/persist_test.go` | 599 | Restart, crash copy, torn tail, corruption, rewrite under load, scheduler. |
| `internal/app/pprof.go` | 81 | Optional loopback-only pprof HTTP server. |
| `internal/app/repl_test.go` | 724 | Two-node replication tests over loopback TCP. |
| `internal/command/collections.go` | 252 | Handlers for lists, hashes, sets, sorted sets. |
| `internal/command/command.go` | 317 | Command table (59), Spec/Ctx/Env, Resolve, propagation rules, Hooks/Control. |
| `internal/command/command_test.go` | 631 | Table-driven tests of every command, propagation, INFO, README check. |
| `internal/command/fuzz_test.go` | 76 | FuzzExecute. |
| `internal/command/keys.go` | 138 | DEL, EXISTS, EXPIRE family, TTL/PTTL, PERSIST, TYPE, KEYS, DBSIZE, FLUSH*. |
| `internal/command/server.go` | 207 | PING, ECHO, COMMAND, CLIENT, CONFIG, PUBLISH, INFO, SAVE, BGSAVE, BGREWRITEAOF, REPLICAOF. |
| `internal/command/strings.go` | 185 | SET (options), GET, MGET, MSET, INCR family, APPEND, STRLEN. |
| `internal/engine/bench_test.go` | 79 | Engine benchmarks. |
| `internal/engine/engine.go` | 521 | The single data-owning goroutine: queue, exec, expiry tick, maxmemory, propagation. |
| `internal/engine/engine_test.go` | 585 | Atomicity, shutdown, full queue, read-only, maxmemory, expiry propagation. |
| `internal/glob/fuzz_test.go` | 27 | FuzzMatch. |
| `internal/glob/glob.go` | 98 | Redis-style glob matcher, linear time. |
| `internal/glob/glob_test.go` | 72 | Glob cases and a pathological-pattern test. |
| `internal/persist/persist.go` | 490 | Persistence manager: startup load, SAVE, BGSAVE, BGREWRITEAOF, scheduler. |
| `internal/persist/persist_test.go` | 251 | Unit tests for persistence failure paths. |
| `internal/pubsub/pubsub.go` | 162 | Hub and bounded Subscriber queues; non-blocking Publish. |
| `internal/pubsub/pubsub_test.go` | 180 | Pub/sub unit tests, slow subscriber, concurrency. |
| `internal/repl/manager.go` | 140 | Role switching (REPLICAOF / NO ONE); owns the follower goroutine. |
| `internal/repl/master.go` | 316 | Primary side: backlog, replica queues, attach (full/partial), stream. |
| `internal/repl/replica.go` | 211 | Replica side: connect, handshake, snapshot receive, apply stream, retry. |
| `internal/repl/ring.go` | 57 | Backlog ring buffer with absolute offsets. |
| `internal/repl/ring_test.go` | 103 | Ring buffer property test against a naive slice. |
| `internal/resp/bench_test.go` | 74 | Parse and encode benchmarks. |
| `internal/resp/fuzz_test.go` | 60 | FuzzReadCommand. |
| `internal/resp/reader.go` | 160 | RESP command reader with size caps and chunked reads. |
| `internal/resp/reader_limits_test.go` | 148 | Tests of line cap, chunked reads, no big preallocation. |
| `internal/resp/reader_test.go` | 66 | Basic reader cases (EOF, torn, malformed, pipelined). |
| `internal/resp/value.go` | 74 | Value and Kind reply types, WriteValue. |
| `internal/resp/value_test.go` | 97 | Value encoding, CRLF injection, AppendCommand round trip. |
| `internal/resp/writer.go` | 111 | Buffered RESP writer, AppendCommand, EncodedLen. |
| `internal/resp/writer_test.go` | 48 | Writer cases. |
| `internal/server/bench_test.go` | 184 | Mutex vs engine benchmarks (lockedHandler lives here). |
| `internal/server/conn.go` | 296 | Per-connection wrapper (flush before read, stop, pusher) and serveConn loop. |
| `internal/server/helpers_test.go` | 328 | Test client and server helpers. |
| `internal/server/pubsub_test.go` | 400 | Pub/sub and MULTI/EXEC tests over TCP. |
| `internal/server/server.go` | 268 | Listener, accept loop, max clients, shutdown. |
| `internal/server/server_test.go` | 466 | TCP tests: pipelining, byte-at-a-time, large value, errors, shutdown, panic. |
| `internal/server/session.go` | 317 | Per-connection state machine: MULTI/EXEC, SUBSCRIBE, QUIT, MRSYNC. |
| `internal/skiplist/skiplist.go` | 280 | Skip list with span counts. |
| `internal/skiplist/skiplist_test.go` | 200 | Randomized test against a naive sorted slice. |
| `internal/snapshot/fuzz_test.go` | 56 | FuzzDecode. |
| `internal/snapshot/snapshot.go` | 423 | Snapshot encode/decode (MRDB, CRC32), atomic file write. |
| `internal/snapshot/snapshot_test.go` | 323 | Round trips, every-byte corruption, truncation, lying lengths. |
| `internal/store/bench_test.go` | 117 | Store benchmarks. |
| `internal/store/dump.go` | 134 | Deep-copy Dump, Load, Canonical. |
| `internal/store/evict.go` | 74 | Eviction policies and sampled Evict. |
| `internal/store/hashes.go` | 87 | Hash operations. |
| `internal/store/lists.go` | 161 | Ring-buffer deque and list operations. |
| `internal/store/memory.go` | 22 | Memory-estimate constants. |
| `internal/store/sets.go` | 77 | Set operations. |
| `internal/store/store.go` | 371 | DB core: entries, lookup, lazy expiry, ActiveExpire, indexes. |
| `internal/store/store_test.go` | 839 | Store tests including random invariants. |
| `internal/store/strings.go` | 100 | String operations including IncrBy overflow. |
| `internal/store/zsets.go` | 133 | Sorted set: map plus skip list. |
| `internal/testclient/testclient.go` | 211 | Tiny RESP client for tests. |
| `scripts/crash-test.sh` | 93 | kill -9 crash test (needs redis-cli). |
| `conformance/cases/01-basic.out` | 10 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/02-strings.out` | 34 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/03-keys.out` | 32 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/04-lists.out` | 21 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/05-hashes.out` | 12 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/06-sets.out` | 9 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/07-zsets.out` | 27 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/08-errors.out` | 13 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/09-transactions.out` | 32 | Expected output for the matching `.in` (committed in `bef7377`). |
| `conformance/cases/10-pubsub-and-server.out` | 3 | Expected output for the matching `.in` (committed in `bef7377`). |

### 2.2 Which package imports which

Generated with `go list` (non-test imports, project packages only) [E17]:

```
cmd/miniredis    -> app, server
app              -> aof, command, engine, persist, pubsub, repl, resp, server, store
persist          -> aof, engine, resp, snapshot, store
repl             -> engine, resp, snapshot, store
engine           -> command, resp, store
server           -> command, pubsub, resp
aof              -> resp, store
snapshot         -> store
command          -> resp, store
store            -> glob, skiplist
resp, glob, skiplist, pubsub, testclient  -> (nothing in the project)
```

Layers, top to bottom. An arrow means "may import":

```
Layer 6   cmd/miniredis
Layer 5   app                      (the only package that imports almost everything)
Layer 4   persist     repl
Layer 3   engine      server
Layer 2   command     aof     snapshot
Layer 1   store
Layer 0   resp   glob   skiplist   pubsub
```

**Rules that hold in the code:**

- Imports only go downward. No cycles (the compiler forbids them).
- `command` never imports `server` or `engine`. It declares small interfaces (`InfoSource`, `Publisher`, `Hooks`, `Control`) that others implement.
- `server` never imports `engine`, `store`, `persist`, `aof` or `repl`. The engine is passed in through the `server.Handler` interface. Replication is passed in through `server.ReplicaServer`.
- `server` imports `command` for two things only: the `command.Info` type and `command.Resolve` (to validate commands queued after `MULTI`).
- `store` never imports `command` or `resp`.
- `resp` and `pubsub` import nothing from the project.
- Only `app` wires everything together. `cmd/miniredis` also imports `server`, only for two default constants.

### 2.3 Names to know, per package

| Package | Types and functions to know by name |
|---|---|
| `resp` | `ReadCommand` (parser), `readLine`, `readBulkString`, `readLarge`; `Value`, `Kind`, `Array`/`BulkString`/`Error`/`Integer`/`Null`/`SimpleString`; `Writer` with `WriteValue`, `Flush`; `AppendCommand`, `EncodedLen`; `ErrProtocol` |
| `store` | `DB`; `New`; `lookup` (lazy expiry), `typed`, `put`, `remove`, `setExpireAt`; `Set`/`Get`/`IncrBy`/`Append`; `Push`/`Pop`; `HSet`; `SAdd`; `ZAdd`/`ZRank`/`ZRange`; `ActiveExpire`; `Evict`; `Dump`/`Load`; `UsedMemory`; `Keys`; types `entry`, `deque`, `zset`, `Item`, `ZMember`, `Policy`, `Kind`; errors `ErrWrongType`, `ErrNotInteger`, `ErrOverflow`; `ParseInt` |
| `skiplist` | `List` with `Insert`, `Delete`, `Rank`, `At`, `Range`; `node`, `level` (the `span` field) |
| `glob` | `Match`, `matchClass` |
| `command` | `Spec` (name, arity, `Write`, `DenyOOM`, handler); `Ctx`; `Env`; `buildTable`; `Resolve`; `Execute`; `Ctx.Propagation`, `emit`, `skipPropagation`; interfaces `Hooks`, `Control`, `Publisher`, `InfoSource`, `SectionSource`; `cmdSet`, `expire`, `cmdInfo` |
| `engine` | `Engine`; `New`; `Execute`, `ExecuteAtomic`, `Exec`, `ExecuteFromMaster`; `run` (the loop), `handle`, `exec`, `reclaim`, `commit`, `tick`, `onRemove`; `State` (`Apply`, `ApplyFromMaster`, `DB`); `Propagator` interface; `Options`; `request` |
| `server` | `Server`; `Listen`, `Serve`, `admit`, `release`, `shutdown`; `connection` (`Read`, `send`, `flush`, `stop`, `forceClose`, `startPusher`); `serveConn`, `serveReplica`, `readFailed`; `session` (`execute`, `queue`, `exec`, `subscribe`); `Handler`, `ReplicaServer`, `Config` |
| `pubsub` | `Hub` (`Subscribe`, `Unsubscribe`, `UnsubscribeAll`, `Publish`, `Channels`); `Subscriber` (`Messages`, `Dropped`); `Message` |
| `aof` | `Log` (`Propagate`, `Commit`, `syncNow`, `syncLoop`, `BeginRewrite`, `FinishRewrite`, `AbortRewrite`, `Close`); `Replay`; `TruncateTo`; `WriteItems`; `Policy` (`FsyncAlways`, `FsyncEverySec`, `FsyncNo`); `CorruptError`; `File` interface |
| `snapshot` | `Encode`, `Decode`, `WriteFile`, `ReadFile`; `ErrCorrupt` |
| `persist` | `Manager` (`Load`, `loadAOF`, `loadSnapshot`, `Save`, `BGSave`, `RewriteAOF`, `finishRewrite`, `ResetAOF`, `StartScheduler`, `Close`, `InfoSection`) |
| `repl` | `Master` (`Propagate`, `attach`, `ServeReplica`, `stream`, `drop`, `setActive`); `ring`; `Replica` (`Run`, `syncOnce`, `receiveSnapshot`); `Manager` (`ReplicaOf`, `ReplicaOfNoOne`); `Applier` interface |
| `app` | `Config`; `Start`; `Run`; `Node`; `hooks`; `multiPropagator`; `replApplier`; `pprofServer` |
| `cmd/miniredis` | `main`, `run`, `parseSize`, `parseYesNo` |
| `testclient` | `Client`, `Reply`, `Dial`, `Encode` (tests only) |

---

## 3. Goroutine and data-ownership map

### 3.1 Every goroutine at runtime

| # | Goroutine | Started by | What it loops on | Stops when |
|---|---|---|---|---|
| G1 | main | the Go runtime | `main` -> `run` -> `app.Run` -> `Node.Wait()` blocks on the channel `n.done` | the node has shut down, then `main` exits |
| G2 | signal watcher | the standard library, inside `signal.NotifyContext` (`main.go:25`) | waits for Ctrl+C / SIGTERM, then cancels the context | process exit. **Real signal delivery is UNVERIFIED** (tests cancel the context directly) |
| G3 | **accept loop and shutdown driver** | `go n.run(...)` at `app.go:211` | `Server.Serve`: `ln.Accept()` in a loop (`server.go:172`). After the context is cancelled it runs the whole shutdown (below) | context cancelled; then `Serve` returns and the goroutine ends after closing everything |
| G4 | **one per client connection** | `s.wg.Go(...)` at `server.go:216` | `serveConn` (`conn.go:195`): `ReadCommand` -> `session.execute` -> write replies. Blocks on the socket read and on the engine reply (`<-r.reply`) | client leaves, protocol error, `QUIT`, idle timeout, panic (recovered), shutdown (`stop()` wakes its read), or forced close |
| G5 | **engine (the data owner)** | `go e.run()` at `engine.go:171` | `select` on `e.reqs` (requests), `ticker.C` (every 100 ms: active expiry) and `e.stopCh` (`engine.go:290-308`) | `Engine.Close()`; queued requests get an error reply first |
| G6 | pusher, one per **subscribed** connection | `c.pushWG.Go` at `conn.go:141`, started by the first `SUBSCRIBE` | `select` on the subscriber queue and `pushQuit`; writes pushed messages (up to 64 per flush) | `stopPusher` (called when the connection goroutine exits) or a write error |
| G7 | shutdown waiter | `go func()` at `server.go:243` | `s.wg.Wait()` then `close(done)` | all connection goroutines have exited |
| G8 | listener closer | `context.AfterFunc` at `server.go:163` | runs once if the context is cancelled: closes the listener so `Accept` returns | right after running once (or removed by `stop()` if `Serve` returns first) |
| G9 | replica-stream watcher | `watcher.Go` at `conn.go:256` | waits for `c.stopCh` (server shutdown) or the stream context ending | when either happens |
| G10 | replica disconnect detector | `readerWG.Go` at `master.go:260` | `io.Copy(io.Discard, conn)` so it notices when the replica closes | the connection closes |
| G11 | AOF fsync (only with AOF on and `everysec`) | `go l.syncLoop(...)` at `aof.go:165` | `select` on a 1-second ticker and `l.quit`; fsyncs if data was written since the last sync | `Log.Close()` |
| G12 | BGSAVE writer | `m.wg.Go` at `persist.go:322` | none: writes the snapshot file once | the write finishes |
| G13 | AOF rewrite writer | `m.wg.Go` at `persist.go:355` | none: writes the compact file, then asks the engine to swap it in | the rewrite finishes or fails |
| G14 | snapshot scheduler (only with `-snapshot-interval`) | `m.wg.Go` at `persist.go:450` | ticker; each tick asks the engine to run `BGSave` | `Manager.Close()` cancels its context |
| G15 | replica follower (only after `REPLICAOF`) | `m.wg.Go` at `manager.go:103` | `Replica.Run`: connect, handshake, apply stream; retry after 1 s | `REPLICAOF NO ONE`, a new `REPLICAOF`, or `Manager.Close()` |
| G16 | pprof HTTP server (only with `-pprof-addr`) | `go func()` at `pprof.go:64` | `http.Server.Serve` | `pprofServer.stop` |

**What does not exist** (you might be asked):

- **No separate expiry sweeper goroutine.** Active expiry is the `ticker.C` case inside G5.
- **No separate AOF writer goroutine.** G5 writes the AOF itself, in `Commit`. The only AOF goroutine is the `everysec` fsyncer G11.
- **No central pub/sub delivery goroutine.** `PUBLISH` runs on G5 and does non-blocking sends into each subscriber's queue. Each subscribed connection has its own pusher G6.
- **No separate replication-sender goroutine.** The connection goroutine G4 of the replica's connection becomes the sender (`serveReplica` -> `ServeReplica` -> `stream`).

**Shutdown order** (`app.Node.run`, `app.go:235-248`): `Serve` returns after it has stopped accepting, woken every connection and waited up to 5 s (the `-shutdown-grace` default) -> `engine.Close()` -> `repl.Manager.Close()` -> `persist.Manager.Close()` (flush, final snapshot if `-snapshot-interval` is set, close AOF) -> pprof stop.

### 3.2 Every channel in the production code

| Channel | Type and size | Who sends | Who receives | When full |
|---|---|---|---|---|
| `Engine.reqs` (`engine.go:117`) | `chan *request`, **buffered, 1024** by default (`-queue-size`) | any goroutine calling `Execute`, `ExecuteAtomic` or `Exec`: connection goroutines G4, the replica follower G15, persistence goroutines G12-G14, startup code | G5 only | The sender **blocks** inside `submit`'s `select` until there is room or its context is cancelled. Nothing is dropped. This is back-pressure on the connection goroutine. |
| `request.reply` (`engine.go:107`) | `chan response`, **buffered, 1** | G5, exactly once per request | the one goroutine that submitted the request | Never full: one send per request, one slot. This is why the engine never blocks on a client. |
| `Engine.stopCh`, `Engine.doneCh` | `chan struct{}`, closed once | `Close()` closes `stopCh`; `run` closes `doneCh` on exit | G5 reads `stopCh`; `Close` and `Done()` read `doneCh` | n/a (closed, never sent on) |
| `Subscriber.queue` (`pubsub.go:28`) | `chan Message`, **buffered, 256** by default (`-subscriber-queue`; minimum 1) | `Hub.Publish`, i.e. G5 | that connection's pusher G6 | `Publish` uses a non-blocking send. If full, the subscriber is **dropped**: removed from all channels, and its connection is force-closed. |
| `replica.queue` (`master.go:76`) | `chan []byte`, **buffered, 8192** by default (`-repl-queue-size`) | `Master.Propagate`, i.e. G5 | the replica's stream loop (on G4 of that connection) | Non-blocking send. If full, the replica is **dropped**: connection closed, it must resync. |
| `replica.dropped`, `gone` (`master.go:77`, `:258`) | `chan struct{}`, closed once | `drop` (G5) closes `dropped`; G10 closes `gone` | the stream loop | n/a |
| `connection.stopCh` (`conn.go:54`) | `chan struct{}`, closed once | `stop()` during shutdown | G9 | n/a |
| `connection.pushQuit` (`conn.go:57`) | `chan struct{}`, closed once | `stopPusher` | G6 | n/a |
| `Log.quit`, `Log.syncDone` (`aof.go:125-126`) | `chan struct{}`, closed once | `Close` / the fsyncer's exit | G11 / `Close` | n/a |
| AOF ticker (`aof.go:158`) | `time.Ticker` channel, 1 s | the Go runtime | G11 | ticks are dropped if G11 is slow (Go ticker behaviour) |
| Engine ticker (`engine.go:287`) | `time.Ticker` channel, 100 ms | the Go runtime | G5 | same |
| `persist` scheduler ticker (`persist.go:442`) | `time.Ticker` channel, `-snapshot-interval` | the runtime | G14 | same |
| `Node.done` (`app.go:89`), `pprofServer.done`, `follow.done`, `server.shutdown` local `done` | `chan struct{}`, closed once | the goroutine that finishes | waiters | n/a |

### 3.3 Every mutex and atomic left in the production code

| What | File:line | Why it is still there |
|---|---|---|
| `Engine.mu` (`RWMutex`) | `engine.go:122` | Guards the `closed` flag. `submit` takes the read side, `Close` the write side, so nobody can enqueue after `Close`. The request channel is never closed, so this flag is needed. |
| `Engine.pool` (`sync.Pool`) | `engine.go:118` | Reuses request objects and their reply channels to avoid an allocation per command. |
| `Server.mu` (`Mutex`) + `Server.wg` | `server.go:95`, `:99` | The set of live connections is touched by the accept goroutine, every connection goroutine (`release`), `Info()` and `shutdown`. `wg` counts connection goroutines. |
| `connection.wmu` (`Mutex`) | `conn.go:45` | The reply writer is used by the connection goroutine **and** the pusher (G6). A `bufio.Writer` is not safe for two goroutines. |
| `connection.mu` (`Mutex`) | `conn.go:52` | Guards the `stopping` flag and the read deadline, so an idle-timeout deadline can never overwrite the "stop now" deadline set at shutdown. |
| `connection.pushWG` | `conn.go:58` | Waits for the pusher to exit. |
| `Hub.mu` (`RWMutex`) | `pubsub.go:55` | The channel-to-subscribers map is changed by connection goroutines (subscribe) and read by G5 (publish). |
| `Subscriber.dropped` (`atomic.Bool`) | `pubsub.go:30` | Compare-and-swap so a subscriber is dropped exactly once. |
| `Log.fmu` (`Mutex`) | `aof.go:111` | The AOF file handle is used by G5 (writes), G11 (fsync) and the rewrite swap. |
| `Log.errMu` (`Mutex`), `Log.rewriting`, `Log.dirty` (`atomic.Bool`), `Log.closeOnce` | `aof.go:122`, `:119-120`, `:127` | The fsyncer reports errors to the engine; the rewrite and dirty flags are read across goroutines. |
| `Manager.aofLog` (`atomic.Pointer`), `bgSaving`, `lastSaveUnix`, `lastCopyUS`, `statusMu`, `wg` (persist) | `persist.go:68-79` | Counters read by INFO and the background save goroutines; the AOF pointer is set once during load and read by G5. |
| `Replica.replID` (`atomic.Pointer`), `offset` (`atomic.Int64`), `linkUp` (`atomic.Bool`) | `replica.go:64-66` | Written by the follower goroutine G15, read by `INFO` on G5. |
| `repl.Manager.wg` | `manager.go:41` | Waits for the follower goroutine. |

**No locks at all** in `store`, `command`, `skiplist`, `glob`, `resp`, `snapshot` and `engine`'s data paths (grep for `sync.` found none in those production files). [E17]

### 3.4 Who touches which data, and why there are no races

| Data | Touched by | How others get at it |
|---|---|---|
| `store.DB` (map, key list, TTL list, entries, skip lists, memory counter) | **G5 only** | By sending a request: `Execute`, `ExecuteAtomic` or `Exec`. Background jobs use `Dump()`, which returns a **deep copy**. |
| `command.Ctx`, `engine.pending`, `engine.readOnly`, `engine.mode` | G5 only | n/a |
| `repl.Master` state (offset, backlog ring, replica set, replication id) | G5 only | Attach and detach run inside `Engine.Exec`. |
| AOF buffer (`bw`, `enc`, `capture`) | G5 only | n/a. The file handle itself is shared with G11 under `fmu`. |
| One connection's `session` (queued commands, subscription, `MULTI` state) and its `bufio.Reader` | that connection's G4 only | n/a |
| One connection's reply writer | G4 and its pusher G6 | under `wmu` |
| `Hub.channels` | G4s (subscribe), G5 (publish) | under `Hub.mu` |
| `Server.conns` | G3, G4s, any caller of `Info` | under `Server.mu` |
| Replica offset and link status | G15 writes, G5 reads | atomics |

**Why there are no data races:**

1. **One owner.** All keyspace data lives in G5. Other goroutines never hold a pointer into it.
2. **Handing over values safely.** A reply (`resp.Value`) is built by G5, then passed through the reply channel to G4. A channel send happens-before the matching receive, so G4 sees the finished value. Strings in Go are immutable. `Dump()` copies slices. Replication messages are cloned once (`bytes.Clone`, `master.go:146`) and then only read.
3. **Everything else shared is behind a mutex or an atomic** (section 3.3).
4. **After G5 stops**, other code may read the DB directly: `Engine.Close` waits on `doneCh`, and the goroutine's exit happens-before that. Example: the final snapshot in `persist.Manager.Close`.
5. **Evidence:** `go test -race ./...` passes for every package [E3], including tests with 32 concurrent incrementers, concurrent transactions, a replica stalled mid-stream, and a rewrite running during writes. Limit: the detector only finds races on paths the tests execute.

---

## 4. Request walkthroughs

Notation: G4 = the client's connection goroutine, G5 = the engine goroutine. Wire bytes use `\r\n` for CRLF.

### a) `SET name anmol`

Wire in (34 bytes): `*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nanmol\r\n`. Wire out (5 bytes): `+OK\r\n`.

1. **(once per connection)** `Server.Serve` (`server.go:159`) calls `Accept`, `admit` (`:202`) checks the 10,000-client cap and starts G4 with `serveConn` (`conn.go:195`). It builds a `session` and a `bufio.Reader` (default 4096-byte buffer) over the connection.
2. G4 calls `resp.ReadCommand` (`reader.go:43`). `readLine` reads `*3`. `readBulkString` runs 3 times. Result: `["SET","name","anmol"]`. When the buffer is empty, `bufio` calls `connection.Read` (`conn.go:87`), which **first flushes any pending replies**, then does the socket read.
3. `session.execute` (`session.go:171`): `classify` says "not a session command", not in a transaction, not subscribed, so it calls `s.h.Execute(ctx, args)`.
4. `Engine.Execute` -> `executeMode` -> `roundTrip` -> `submit` (`engine.go:196-281`) puts a pooled `request` on `e.reqs`, then G4 waits on `<-r.reply`.
5. G5 (`run`, `engine.go:290`) takes it: `handle` (`:333`) -> `begin` (sets the DB's "now" from the clock) -> `exec` (`:395`) -> `command.Resolve` (`command.go:136`: table lookup plus argument-count check).
6. Policy checks (`engine.go:400-409`): is it a write on a read-only replica? Is it a memory-growing command, and is maxmemory exceeded? (`reclaim`, which does nothing when `-maxmemory 0`.)
7. `cmdSet` (`strings.go:25`) -> `DB.Set` (`store/strings.go:21`) -> `put` (`store.go:179`): adds the key to the map and the key list, updates the memory counter. Reply: `+OK`.
8. `Ctx.Propagation` (`command.go:99`): `SET` is a write command, the reply is not an error, no rewrite was requested, so the original command is queued for logging (`e.pending`).
9. `commit` (`engine.go:447`): for each pending command it calls `multiPropagator.Propagate` (`hooks.go:43`): first `persist.Manager.Propagate` (encodes it into the AOF buffer, only if AOF is on), then `repl.Manager.Propagate` (adds to the replication backlog and every replica's queue). Then `Commit`: the AOF buffer is written to the file and, under `always`, fsynced.
10. G5 sends the reply into `r.reply` (`engine.go:376`). G4 wakes, recycles the request.
11. G4 writes `+OK\r\n` into the `bufio.Writer` (`conn.go:223`). The next `ReadCommand` has no buffered data, so `connection.Read` flushes: **one socket write**. Then it blocks reading.

### b) `GET name`

Same as (a) up to step 5. Then `cmdGet` -> `DB.Get` -> `typed` -> `lookup` (`store.go:147`): finds the entry, checks its expiry time, records the access time. Reply `$5\r\nanmol\r\n`. `GET` is not a write command, so `Propagation` returns nothing. `commit` sees an empty list and does nothing: no AOF or replication work. [E7]

### c) `SET k v EX 5`, and what happens at second 5

1. `cmdSet` parses the option `EX 5` with `ParseInt` and computes `expireAt = now + 5000` ms (`addMs`). `DB.Set` stores the key with that deadline and adds it to `db.volatile`, the list of keys that have a TTL (`setExpireAt`, `store.go:212`).
2. **What is logged is rewritten:** `SET k v PXAT <absolute ms>`, not `EX 5` (`strings.go`, the `x.emit(...)` call). So replaying later gives the same deadline. [E11] shows this in the real AOF file.
3. **At second 5, two things can remove the key:**
   - **Lazy:** the next command that touches `k` runs `lookup`. It sees `expireAt <= now`, removes the key, counts it as expired and calls `notify`. `engine.onRemove` (`engine.go:439`) adds a `DEL k` to `pending`, so the AOF and replicas learn about it. The command then sees the key as missing.
   - **Active:** every 100 ms the engine's `ticker.C` case runs `tick()` (`engine.go:463`) -> `DB.ActiveExpire(20, 5ms)` (`store.go:342`). Details in section 6.
4. **Observed** [E7]: `SET t v PX 1500`; `DBSIZE` was 1 at 1400 ms and 0 at 1600 ms, **without anyone reading the key** (`DBSIZE` does not touch it). So the background sweep removed it within about 100 ms after expiry.

### d) `MULTI` / `SET` / `EXEC`

1. `MULTI`: handled by the session, not the engine (`session.go:203`). It sets `inTx`. Reply `+OK`.
2. `SET a 1`: `session.queue` (`:241`) calls `command.Resolve`. This only checks that the command exists and has the right argument count. Nothing runs. The args are appended to `s.queued` (cap 100,000). Reply `+QUEUED`. If the command is unknown or has the wrong argument count, the session is marked `dirty`.
3. `EXEC`: `session.exec` (`:257`). If `dirty`: reply `-EXECABORT Transaction discarded because of previous errors.` and nothing runs. Otherwise it calls `Handler.ExecuteAtomic(ctx, queued)`.
4. In the engine this is **one request** of kind `kindBatch`. G5 runs `e.exec` for every queued command back to back, with the same "now". Nothing from another client can run in between. There is **one** `commit()` at the end, so under `always` there is **one fsync** for the whole batch.
5. Reply: an array with one reply per command, e.g. `*1\r\n+OK\r\n`. A command that fails at run time (for example `LPUSH` on a string) gives an error element, and the others still run. **No rollback.**
6. In the AOF and replication stream the commands appear one after another with **no `MULTI`/`EXEC` frame**.
7. Tests: `TestExecIsOneAtomicBatch`, `TestTransactionsOverTheRealEngine`, `TestInvalidQueuedCommandAbortsExec`, `TestConcurrentTransactionsAreIsolated`, and conformance case `09-transactions`. [E1]

### e) `SUBSCRIBE news` plus another client's `PUBLISH news hi`

Client A (subscriber):

1. `session.subscribe` (`session.go:273`): on the first subscribe it creates a `pubsub.Subscriber` (queue of 256) and calls `onSub`, which starts the **pusher goroutine** G6 (`conn.go:139`). Then `Hub.Subscribe(sub, "news")`.
2. Reply: `*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n`.
3. From now on A is in subscribed mode: only `SUBSCRIBE`, `UNSUBSCRIBE`, `PING` and `QUIT` are accepted (`session.go:186-200`). Anything else gets `-ERR only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context`.

Client B (publisher):

4. `PUBLISH news hi` is an ordinary command: G4(B) -> engine -> `cmdPublish` (`server.go:59`) -> `Hub.Publish` (`pubsub.go:128`). For each subscriber it does a **non-blocking** send into the subscriber's queue. It returns how many got it. B receives `:1`.
5. `PUBLISH` is not a write command, so it is **not** logged and **not** replicated.

Back to A:

6. G6 receives the message from the queue and `pushMessages` (`conn.go:159`) writes `*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n` under `wmu`, batching up to 64 messages per flush.

**If A is too slow:** its queue (256) fills. The next `Publish` finds it full, marks it dropped (once), removes it from every channel, and calls `c.forceClose`, closing A's socket. The publisher is never delayed. Test: `TestSlowSubscriberIsDisconnected` (300 messages of 256 KiB). Observed live in the demo: subscriber output `subscribe news 1 message news hello subscribers`, and `PUBLISH` returned 1. [E15]

### f) A client sends half a command, then the rest

1. G4 is inside `ReadCommand`, blocked in `connection.Read` -> socket read. **Nothing executes** until the whole array has arrived.
2. **Observed** [E7]: I sent `*3\r\n$3\r\nSET\r\n$4\r\nhalf\r\n` (2 of 3 arguments), waited 700 ms, and asked from another connection: `EXISTS half` returned 0. I then sent `$5\r\nvalue\r\n`: reply `+OK`, and `GET half` returned `value`.
3. If the client disconnects mid-command: `ReadCommand` returns `io.ErrUnexpectedEOF`. `readFailed` (`conn.go:278`) logs "client disconnected mid-command" at debug level. Nothing was executed. Test: `TestClientDisconnectMidCommand`.
4. A command sent one byte per TCP segment also works (`TestCommandSentOneByteAtATime`).
5. **Weak point:** with `-idle-timeout` set, the deadline is renewed on every socket read, so a client that trickles 1 byte just under the timeout can hold a connection (and a goroutine) forever. **CODE, not tested.** The default is no idle timeout at all.

### g) A client disconnects in the middle of a request

Two cases:

- **Mid-command (before the full command arrived):** see (f). Clean.
- **After the command was queued to the engine:** G4 is blocked on `<-r.reply`, so it cannot notice the disconnect yet. G5 still runs the command, for example a `SET` really happens. G5 replies into the buffered channel (never blocks). G4 wakes, writes the reply into its buffer (no error yet), and on the next `connection.Read` the flush fails. The loop exits, the deferred `release` removes the connection. **Nothing leaks and the engine is not affected.**
- **Observed** [E8]: with 500,000 keys loaded, client C sent `KEYS *` and closed its socket at once. Within 250 ms `INFO clients` showed `connected_clients:2` (my two other connections; C was gone), the server answered `PING`, and the server log had no panic or error-level line. The debug log showed "write failed ... connection aborted" for C.
- **No automated test** covers "disconnect while the request is pending" (the engine tests cover a full queue, a cancelled caller and `Close` with queued requests). This one I verified by hand only.

### h) 10,000 pipelined commands in one write

1. The 10,000 commands arrive as a stream of bytes. G4's `bufio.Reader` refills in chunks (4096 bytes at a time). `ReadCommand` parses one command at a time.
2. **Each command is its own engine round trip**, one after another on that connection: submit, wait for reply, next. Commands from one connection are never batched into one engine request. (Other clients' commands interleave in the queue.)
3. Replies are appended to the connection's `bufio.Writer` (4096 bytes). It is flushed when it fills up **and** whenever the reader has to wait for more input (`connection.Read` flushes first). So replies go out in groups, not one `write` per command. The exact number of `write` calls was not measured (**UNVERIFIED**).
4. **Observed** [E7]: 10,000 `SET` commands (318,890 bytes) in one `Write` call. All 10,000 replies were `+OK`. The client wrote in 0.5 ms and had read every reply 47.6 ms after starting (single run). A bigger run: 500,000 `SET` in one write, all replies read in 1.56 s (about 320k commands/s on one connection, single run) [E10].
5. **Risk, CODE only:** if a client pipelines faster than it reads, the server's reply write blocks, G4 stops reading, and the client's own writes can block too. Real Redis avoids this with an unbounded reply buffer. On this Windows machine I could **not** reproduce a stall: 3,000,000 pipelined `GET`s of a 1 KiB value (66 MB sent, about 3 GB of replies, never read until the write finished) completed [E9]. Windows buffers a lot in the kernel. **The Linux behaviour is UNVERIFIED.**

---

## 5. Persistence

### 5.1 AOF format on disk

The AOF is the commands in RESP, exactly what a client would send. Default file name `appendonly.aof` inside `-dir`. Only **successful write commands** are logged, **after** they ran in memory.

I ran 11 commands against a server started with `-appendonly yes -appendfsync always` [E11]. Two were not logged (the `GET` and `DEL nosuchkey`, which changed nothing). The file was 340 bytes with 9 commands. First bytes (hex, then ASCII):

```
00000000: 2a33 0d0a 2433 0d0a 5345 540d 0a24 340d  *3..$3..SET..$4.
00000010: 0a6e 616d 650d 0a24 350d 0a61 6e6d 6f6c  .name..$5..anmol
00000020: 0d0a 2a35 0d0a 2433 0d0a 5345 540d 0a24  ..*5..$3..SET..$
00000030: 370d 0a73 6573 7369 6f6e 0d0a 2433 0d0a  7..session..$3..
00000040: 6162 630d 0a24 340d 0a50 5841 540d 0a24  abc..$4..PXAT..$
```

The first command as text (`cat -A`, where `^M$` means CRLF and end of line):

```
*3^M$  $3^M$  SET^M$  $4^M$  name^M$  $5^M$  anmol^M$
```

All 9 logged commands, decoded:

```
1  SET name anmol
2  SET session abc PXAT 1793372673267      <- I sent: SET session abc EX 100
3  RPUSH l a b
4  INCR counter
5  PEXPIREAT name 1793372673460            <- I sent: EXPIRE name 100
6  ZADD z 1.5 alice
7  HSET h f v
8  SET gone x
9  DEL gone
```

Relative times are rewritten to absolute times so that replaying the log later gives the same result. `docs/formats.md` documents the rule.

### 5.2 When is fsync called? The three policies

Code: `aof.Log.Commit` (`aof.go:241`). The engine calls it once per **request** that changed data (an `EXEC` batch is one request). The call happens **before the reply is released** to the client.

| `-appendfsync` | What happens | What you can lose |
|---|---|---|
| `always` | `Commit` writes the buffer to the file, then `f.Sync()` (`syncNow`, `:255`) | Nothing that was acknowledged, if the disk honours fsync. It costs one fsync per request. **Throughput under `always` was never measured (UNVERIFIED).** |
| `everysec` (**default**) | `Commit` writes to the file (OS cache) and marks "dirty". Goroutine G11 fsyncs once per second if dirty (`syncLoop`, `:186`) | Up to about 1 second of acknowledged writes if the **whole OS** dies. If only the process is killed, the OS cache survives (see 5.5). |
| `no` | Writes to the file, never fsyncs (except at `Close`) | Whatever the OS had not flushed |

If a background fsync fails, the error is stored and returned by the next `Commit`, so the next write gets `-MISCONF`. When fsync works again, writes work again (`TestEverySecBackgroundErrorSurfacesAndClears`).

**Code-reading risk (not tested):** if `Commit` fails, `engine.handle` (`engine.go:357-374`) replaces the reply with `-MISCONF`, but the change **already happened in memory** and is not rolled back. So the client is told "failed" while the key exists.

### 5.3 A crash in the middle of a write

- A crash can leave the file ending in half a command.
- On startup `aof.Replay` (`aof.go:412`) returns `torn = true` when the data ends inside a command, together with `valid`, the byte count of complete commands. `persist.Manager.loadAOF` (`persist.go:177`) then **truncates the file to `valid`** and logs a warning ("the append-only file ends with an incomplete command ...", with the bytes kept and dropped).
- **Tested:** `TestReplayTornTailAtEveryOffset` (cuts a 4-command log at every byte offset), `TestTornTailIsTruncatedAndLogged` (whole app), and `scripts/crash-test.sh` phase 3, which appended half a command and restarted: PASS [E12].
- **Corruption in the middle** (bytes that are not valid RESP): `Replay` returns `CorruptError` with the byte offset, and startup **fails** with "refusing to start: the append-only file ... is damaged ..." and leaves the file untouched (`TestCorruptionInTheMiddleFailsStartup`).
- **Limit:** a damaged length field near the end of the file looks like a torn tail and would be truncated (with a warning). The log has **no per-record checksum**.

### 5.4 Startup replay

`persist.Manager.Load` (`persist.go:140`):

| `-appendonly` | AOF file | Snapshot file | What is loaded |
|---|---|---|---|
| yes | non-empty | any | **AOF** (it wins over the snapshot) |
| yes | missing or empty | exists | snapshot, then a new AOF is written from it |
| yes | none | none | nothing |
| no | any | exists | snapshot |
| no | exists | none | nothing (a warning says the AOF was ignored) |

Replay goes through the **same command code** (`engine.State.Apply`), without sending replies and without logging again. If any command **fails** during replay, startup stops (strict replay). Observed: 9 commands replayed in 364 microseconds [E11]; an `EXPIRE name 100` key showed `TTL 88` about 12 seconds later, which proves the absolute time survived the restart.

### 5.5 Rewrite, snapshots, and what is NOT durable

- **Compaction: yes**, only on demand with `BGREWRITEAOF` (no automatic trigger). Flow: `BeginRewrite` starts capturing newly logged commands and `Dump()` copies the data (both on the engine). A goroutine writes the minimal commands to `appendonly.aof.rewrite`. Then, back on the engine, `FinishRewrite` (`aof.go:295`) appends the captured commands, fsyncs, closes the old file, renames the new one over it, and reopens. Tests: `TestRewriteUnderLoadLosesNothing` (83 increments landed during one rewrite and all survived a restart; the count varies per run) and `TestRewriteShrinksTheFile`. **The engine does disk I/O and an fsync during `FinishRewrite`.** That pause was not measured.
- **Snapshots: yes.** `SAVE` writes on the engine goroutine (**blocks every client**). `BGSAVE` copies the data on the engine (`Dump()`), then a goroutine writes `dump.mrdb` via a temp file, fsync and rename. `-snapshot-interval` makes the scheduler run `BGSAVE` periodically and also saves once at shutdown. The format is this project's own (`MRDB`, big-endian, CRC32 trailer), described in `docs/formats.md`. It is **not** Redis's RDB. Measured copy cost: `Dump` of a 100,000-key store takes about 7 ms (14.4 MB allocated) [E20].
- **What is NOT durable:**
  - **With defaults, nothing**: AOF is off and no snapshot interval is set.
  - With `everysec`, up to about 1 s of writes if the OS crashes or power fails.
  - Writes after the last snapshot, if only snapshots are used.
  - Pub/sub messages (never stored).
  - A half-applied `EXEC` after a crash (no transaction frame in the log).
  - Snapshot or AOF integrity beyond the checks above: the AOF has no checksum.
  - Directory fsync after a rename is skipped on Windows (best effort elsewhere).

### 5.6 The `kill -9` crash test

- **Run:** `scripts/crash-test.sh 1000 6412` [E12], unmodified, on 2026-10-30. It starts the server with `-appendonly yes -appendfsync always`, writes 1000 keys, `kill -9`s the process, restarts, and checks every key. Then it appends half a command, restarts, and checks again.
- **Result: PASS** (exit status 0): all 1000 keys present after the kill; all 1000 present after the torn tail; the truncation was logged.
- **Not the Debian flow.** I ran it on Windows in Git Bash. The script needs `redis-cli`, so I put a tiny wrapper script on the PATH that calls `memurai-cli.exe` and strips carriage returns. (The first attempt failed only because that CLI prints Windows line endings; the `DBSIZE == 1000` check before it had passed, so no data was lost.) `kill -9` on a Windows program ends it abruptly, like `TerminateProcess`.
- **What it proves:** the data survives a **process** kill. **What it does not prove:** survival of an OS crash or power cut, because the OS file cache survives a process kill. **A Linux run is UNVERIFIED.**

---

## 6. Expiry and eviction

### 6.1 How TTLs are stored

- Each key's entry (`store.go`, `type entry`) has `expireAt`, an `int64` of **Unix milliseconds** (0 means no TTL).
- The DB also keeps `db.volatile`, a slice of the key names that have a TTL. Each entry remembers its position (`vidx`). Removing a key swaps the last element into the gap, so adding and removing are O(1) and **picking a random key with a TTL is O(1)**. That is what sampling needs.
- "Now" comes from the engine's clock (`time.Now` in production; tests inject a fake clock). The engine sets it once per request, so all commands in an `EXEC` see the same time.

### 6.2 Lazy expiry

`DB.lookup` (`store.go:147`): every command that reads a key goes through it. If `expireAt <= now`, it removes the key, increments the expired counter, and emits a `DEL` to the log and replicas. Exactly at the deadline counts as expired. On a **replica** it only hides the key and never deletes it.

### 6.3 Active expiry (the sweep)

`DB.ActiveExpire(sample, budget)` (`store.go:342`), called from `engine.tick` every `TickInterval` = **100 ms**:

1. Take `n = min(20, number of keys with a TTL)`.
2. Pick `n` random keys from `db.volatile` (with replacement). Delete each one that is expired.
3. If **more than 25%** of the sample was expired (`expired*4 > n`) **and** less than **5 ms** of real time has been spent, repeat immediately. Otherwise stop until the next tick.

This copies Redis's idea. Consequence: if only a tiny fraction of the TTL keys are expired, they can sit in memory until a read touches them or a later sample hits them. Replicas skip the sweep and wait for the primary's `DEL`.
Tests: `TestActiveExpire`, `TestActiveExpireStopsWhenFewAreExpired`, `TestActiveExpireRespectsBudget`, `TestActiveExpiryRunsOnTicker`, `TestActiveExpiryRemovesUntouchedKeys`. Observed live [E7] (section 4c).

### 6.4 Eviction

- **Trigger:** before each command flagged `DenyOOM` (memory-growing writes such as `SET`, `LPUSH`, `HSET`, `ZADD`), if `-maxmemory` is set and `DB.UsedMemory() > limit`, the engine calls `DB.Evict` in a loop until it is under the limit or nothing can be evicted (`engine.reclaim`, `engine.go:420`). If it cannot, the write gets `-OOM command not allowed when used memory > 'maxmemory'.` Reads, `DEL`, `EXPIRE` and the like are never refused.
- **Policies** (`-maxmemory-policy`): `noeviction` (default; always OOM at the limit), `allkeys-lru`, `allkeys-random`, `volatile-ttl`.
- **How a key is picked** (`evict.go:45`):
  - `allkeys-lru`: sample `-maxmemory-samples` (default **5**) random keys from all keys; evict the one with the oldest last-access time (`entry.access`, in ms).
  - `allkeys-random`: sample 1 key.
  - `volatile-ttl`: sample from the keys that have a TTL; evict the one that expires soonest. If no key has a TTL, nothing is evictable and the write fails with OOM.
- Each eviction is also logged and replicated as `DEL`.
- **Approximation:** a 5-key sample is not exact LRU, and last-access has 1 ms resolution. **Hit-rate quality versus real LRU was not measured (UNVERIFIED).** The check happens *before* the command, so one big write can overshoot the limit by its own size.

### 6.5 How "memory" is measured, honestly

- It is an **estimate that the code keeps up to date on every change**, not what the OS reports. Per key: `96 + len(key) + value bytes`, plus per element `16` (list), `48` (hash field), `32` (set member), `96` (sorted-set member) (`memory.go:14-18`). Tests recount the whole keyspace from scratch after random operations and compare.
- **Measured against reality** [E10]: after loading 500,000 keys with a one-byte value, `INFO memory` said `used_memory:52388890` (about 50 MB), and the process working set was 139 MB (7.9 MB when empty). The estimate was about 38% of the real size (single run). It ignores Go runtime overhead, garbage not yet collected, the 4 KB buffers per connection and the copy made by `BGSAVE`.
- So `-maxmemory 100mb` limits the **estimate**, and the real process can be much larger.

---

## 7. Replication

### 7.1 How it works, step by step

1. `REPLICAOF host port` (or `-replicaof "host port"` at startup) runs on the engine: `Manager.ReplicaOf` (`manager.go:82`) stops the primary role, turns on read-only mode and starts the follower goroutine G15.
2. G15 runs `Replica.Run` -> `syncOnce` (`replica.go:119`): connects (5 s dial timeout) and sends the private command `MRSYNC <replid> <offset>`. A fresh replica sends `MRSYNC ? -1`.
3. On the primary, the connection's session sees `MRSYNC`, and `serveReplica` hands the socket to `Master.ServeReplica` (`master.go:232`). It runs `attach` **inside the engine** (`Engine.Exec`):
   - If the replication id matches **and** the replica's offset is still inside the backlog ring buffer: **partial sync**. The primary copies the missing bytes.
   - Otherwise: **full sync**. The engine deep-copies the data (`Dump()`) and registers the replica's queue **in the same engine step**, so the snapshot and the live stream meet with no gap and no overlap.
4. The primary replies with one line, then data:
   - `+FULLRESYNC <replid> <offset>\r\n` then the snapshot as one bulk string (`$len\r\n<bytes>\r\n`), in the project's own snapshot format.
   - or `+CONTINUE <replid>\r\n` then the missing stream bytes.
5. Afterwards every command that changed data is sent in RESP, byte for byte what the AOF gets. Expiry and eviction are sent as explicit `DEL`.
6. The replica applies each command inside the engine through `ApplyFromMaster` (skips the read-only and memory checks) and adds the command's encoded length to its **offset**.
7. Identity: the replication id is 40 random hex characters (20 bytes from `crypto/rand`), created in `NewMaster` each time the process starts or when promoted. The offset counts the bytes of the stream. The backlog is a ring buffer (default **1 MiB**, `-repl-backlog-size`).
8. A replica applies the snapshot with `DB.Load`, and if it has its own AOF it rewrites it (`ResetAOF`) so a later restart recovers the replicated data.

**Observed with two real processes** [E16] (primary on 6406, replica started with `-replicaof "127.0.0.1 6406"`): data that existed before the replica started arrived (full sync, `keys=2 offset=68` in the primary log); live `SET`/`INCR` appeared on the replica within 0.2 s; a write on the replica returned `READONLY You can't write against a read only replica.`; a key with `PX 300` was gone from the replica after 0.6 s and `DBSIZE` matched on both (4 and 4); `INFO` showed `master_repl_offset:204` on the primary and `slave_repl_offset:204` on the replica, `connected_slaves:1`, `master_link_status:up`.

### 7.2 Compatible with real Redis?

**No.** `MRSYNC`, the stream framing and the snapshot format are private. A miniredis replica cannot follow real Redis and the reverse. This is stated in the package comment of `repl/master.go`, decision 008 and the README. A real Redis client can still talk to a miniredis node, including `INFO replication`, but cannot make it a replica.

### 7.3 Slow replica, disconnect, failover

- **Slow replica:** each replica has a queue of 8192 commands. If the queue is full when the primary has a new command, the replica is **dropped** (connection closed, counter `replicas_dropped` goes up). The primary never waits. Test: `TestSlowReplicaIsDroppedAndPrimaryKeepsWorking`.
- **Disconnect:** the replica retries every 1 s (`RetryDelay` default). It sends its last replication id and offset. Partial resync if the bytes are still in the 1 MiB backlog, otherwise full sync. Tests: `TestPartialResyncAfterDisconnect`, `TestFullResyncWhenBacklogTooSmall`. These two cut the link inside one test process. **A real two-process partial resync was not run (UNVERIFIED).**
- **Failover: none.** There is no Sentinel, no election and no automatic promotion. A person runs `REPLICAOF NO ONE` on the replica. That makes it a writable primary with a **new** replication id, so old replicas cannot confuse histories.

### 7.4 What is not handled

- No failover or election; no split-brain protection.
- No chained replication: a replica refuses `MRSYNC`.
- No authentication: **any client that can connect can send `MRSYNC` and download the whole dataset** (CODE).
- No application-level heartbeat or `WAIT` command.
- After a **primary restart** the replication id changes and the backlog is gone, so every replica does a full resync. After a **replica restart** its offset is lost, so it also does a full sync.
- Pub/sub is not replicated.
- A full sync copies the data and builds the whole snapshot in memory on the primary, and receives it fully in memory on the replica (limit constant 8 GiB). Under heavy writes during a long transfer the queue could overflow and the replica could loop through full syncs.
- A command from the primary that fails on the replica is logged as a warning and skipped.

---

## 8. Protocol and safety

### 8.1 RESP parsing rules (`resp/reader.go`)

- Only one input shape is accepted: an **array of bulk strings** (`*N\r\n` then N times `$len\r\n<bytes>\r\n`). Inline commands are not supported.
- **Limits** (constants at `reader.go:25-35`):

| Limit | Value | What happens past it |
|---|---|---|
| Header line (`*3`, `$1234`) | 64 KiB including CRLF | `-ERR Protocol error: line too long`, connection closed |
| Arguments per command | 1 to 1,048,576 | `invalid array length` (also for `*0`) |
| One bulk string | 0 to 536,870,912 bytes (512 MiB) | `invalid bulk string length` |
| Bulk strings above 64 KiB | read in 64 KiB chunks | memory grows with bytes actually received |
| Argument slice | starts at min(N, 64) entries and grows | a lying `*1048576` costs almost nothing |
| Queued commands in one `MULTI` | 100,000 | the transaction is marked bad |
| Channels per subscriber | 10,000 | error reply |
| Clients | 10,000 (`-max-clients`) | new client gets `-ERR max number of clients reached` and is closed |

- **Observed replies** [E7] (each followed by the server closing the connection): inline `PING` -> `-ERR Protocol error: expected array header`; garbage line -> same; 70,000 bytes with no CRLF -> `line too long`; `*1048577` -> `invalid array length`; `*0` -> `invalid array length`; `$536870913` -> `invalid bulk string length`; `$-5` -> `invalid bulk string length`; `$4\r\nPINGxx` -> `bulk string not terminated by CRLF`.
- The error text comes from fixed strings, never from client bytes. Client text put into an `unknown command` error is escaped with `%q` and capped at 64 bytes (a command named `BAD\r\n+OK` produced `unknown command "BAD\r\n+OK"` as visible escapes). The writer also replaces CR and LF inside simple-string and error replies with spaces.
- **Missing limit:** there is no cap on the **total bytes of one command** or on the total input of a connection. A client can send up to 1,048,576 arguments, each up to 512 MiB, as long as it really sends the bytes. Memory is only bounded by what clients send and `-maxmemory` does not cover it. (CODE)

### 8.2 Slow clients, huge values, idle connections

- **Slow reader of replies:** its own goroutine blocks in the socket write. There is **no write timeout** (only the final error reply to a client being dropped has a 1 s deadline). It does not block the engine or other clients. At shutdown, forced close after the 5 s grace period frees it (`TestShutdownForcesStuckConnectionsAfterGrace`).
- **Slow subscriber:** disconnected (section 4e).
- **Slow replica:** dropped (section 7.3).
- **Huge value:** accepted up to 512 MiB per argument. It is held in memory. A declared 512 MiB value with no data costs one 64 KiB chunk (test `TestReadCommandBulkDoesNotPreallocate`). `-maxmemory` only checks **before** a command, so one big `SET` can exceed the limit by its own size.
- **Idle connections:** `-idle-timeout` defaults to 0 = **never**. At the default, idle connections stay forever, capped by 10,000 clients (each has a goroutine and two 4 KB buffers).
- **Panics:** a panic in one connection's code is recovered, logged with a stack trace, and only that connection closes (`TestPanicInHandlerIsContained`). A panic inside a command is recovered by the engine, which replies `ERR internal error ...`. The command may have left its key half-modified.

### 8.3 What the 5 fuzz targets test

| Target | What it feeds | What it checks |
|---|---|---|
| `FuzzReadCommand` | random bytes, a few pipelined commands | never panics; only returns EOF, unexpected-EOF or protocol errors; anything accepted survives an encode-then-decode round trip unchanged |
| `FuzzExecute` | random commands built from NUL-separated bytes | never panics; every reply has a valid type; every command a write asks to log is itself a valid command |
| `FuzzReplay` (AOF loader) | random bytes | never panics; `valid` never exceeds the input length; a torn tail never comes with an error; a clean replay consumes every byte |
| `FuzzDecode` (snapshot loader) | random bytes | never panics or hangs; damaged input gives an error; anything accepted re-encodes and decodes to the same items |
| `FuzzMatch` (glob) | random pattern and string | never panics or hangs; `*` matches everything; a pattern with no special characters matches only itself |

**Bugs the fuzzers found: none.** 10 s per target today, 20 s per target earlier, all PASS [E14]. These are short runs.

**Real bugs found by ordinary tests during development, for honesty:**

1. A replica's `DEL` for a key that was already logically expired did not remove it (the key was hidden, not stored-out). Found by the engine test `TestReplicaDoesNotExpireByItself`; fixed with a regression test (`TestReplicaDeleteRemovesHiddenExpiredKey`).
2. Five lint findings (a dead assignment, three capitalised error strings, an ignored return value), fixed.
3. The conformance expected-output files were never committed (finding 2). **Not fixed.**

---

## 9. Design decisions

### 9.1 The 8 decision records, summarised

| # | File | Problem | Options considered | Choice and why | Trade-off |
|---|---|---|---|---|---|
| 001 | `001-flush-strategy.md` | Answer pipelined commands cheaply but a lone command at once | flush after every command; flush when no input is buffered; flush right before the reader would block | The third. `connection.Read` flushes first, then reads. It cannot deadlock on a half-received next command | A flush error shows up as a read error. No write timeout. The syscall saving was never measured |
| 002 | `002-session-layer.md` | `QUIT`, `MULTI`, `SUBSCRIBE` need per-connection state | state in the command package; `if`s in the read loop; a session object per connection | Session object owned by the connection goroutine. `Handler` interface declared in `server`. `Listen` split from `Serve` | `server` imports `command` for `Info` and `Resolve`. **The record still describes `command.Dispatcher`, which no longer exists** (the engine replaced it) |
| 003 | `003-network-input-limits.md` | Sizes in the protocol come from an untrusted client | trust the numbers; cap them; read in chunks | Caps, chunked reads, small initial slices, `%q`-escaped errors, max clients | A legitimate 512 MiB value still costs memory. Limits are constants, not flags |
| 004 | `004-shutdown.md` | Stop cleanly on Windows and Linux, finish running commands | close everything at once; a flag; per-connection stop via a read deadline | `SetReadDeadline(now)` wakes a blocked read; wait for a grace period; then force close | A half-received command is dropped. A stuck handler cannot be interrupted |
| 005 | `005-single-owner-engine.md` | Atomic transactions and ordered logging with many connections | a mutex per command; sharded locks; one owner goroutine | One owner goroutine, as Redis does | Data path is single-threaded; slow commands block everyone. **Corrected: the record now shows both benchmark runs and says there is no reliable TCP difference** |
| 006 | `006-slow-subscribers.md` | A slow subscriber must not block publishers | block; unbounded queue; drop the message; drop the subscriber | Bounded queue (256), disconnect on overflow | A bursty subscriber can be dropped. Messages are not durable |
| 007 | `007-persistence-design.md` | Persistence without `fork`, without losing writes | separate writer goroutine; iterate the live data; copy | Engine is the only AOF writer; `BGSAVE` copies inside the engine, writes outside; rewrite = capture + copy + swap; log absolute times | Memory for the copy; engine pause during copy and swap; no log checksum |
| 008 | `008-eviction-and-replication.md` | Memory limit; replication without implementing Redis's protocol | read the Go heap; exact LRU list; PSYNC and RDB | Incremental memory estimate; sampled LRU; private `MRSYNC` protocol | Estimate is far from real memory; replication is not Redis-compatible |

### 9.2 Top 10 trade-offs a reviewer could challenge

1. **Single goroutine vs locks.** *Challenge:* "You only use one core for data." *Answer:* Yes, same as Redis. Parsing and socket I/O still use all cores. I chose it for atomic `EXEC` and an ordered log. My benchmark shows no reliable TCP throughput win (the mutex was 2.3 times faster with no network). If the data path became the bottleneck I would shard by key, which I did **not** build.
2. **No inline commands.** *Answer:* Smaller parser, nothing needed for `redis-cli`. Cost: `telnet` and `redis-cli --pipe` do not work, and the default `redis-benchmark` list (starts with `PING_INLINE`) fails unless I select tests with `-t`.
3. **Private replication protocol.** *Answer:* Real PSYNC and RDB were out of scope. The ideas are the same (id, offset, backlog, snapshot plus stream). Cost: it cannot join a real Redis setup.
4. **Memory limit counts an estimate.** *Answer:* An O(1) incremental count is predictable and testable. It showed 38% of real memory in one run. A real limit would need `runtime.MemStats` or per-allocation accounting.
5. **Log after executing, not before.** *Answer:* The reply is only released after `Commit`, so an acknowledged write is in the file (under `always`). The risk is the reverse: a failed `Commit` leaves the change in memory (section 5.2).
6. **`BGSAVE` copies instead of forking.** *Answer:* Go cannot fork safely. The cost is the copy pause (7 ms per 100,000 keys, growing with size) and extra memory.
7. **Sampled LRU with 5 keys.** *Answer:* No per-read bookkeeping. The quality was not measured against true LRU.
8. **Disconnect slow subscribers and drop slow replicas.** *Answer:* Never block the engine. The loss is visible (a closed connection). Cost: a client that is only briefly slow can be cut off.
9. **Strict AOF replay.** *Answer:* Any command that fails on replay stops startup, so bugs surface instead of hiding. Cost: a command that becomes invalid after an upgrade blocks startup.
10. **Own snapshot format with a CRC32.** *Answer:* Documented, bounded when decoding, no dependency. Cost: not readable by Redis tools. The AOF itself has no checksum.

---

## 10. Known weaknesses, ranked by how likely an interviewer is to find them

**Likelihood:** H = they will probably find it, M = a careful reviewer might, L = only if they dig.

| # | H/M/L | Weakness | Evidence |
|---|---|---|---|
| 1 | ~~H~~ | ~~The GitHub repo shows only the old placeholder.~~ **FIXED**: pushed, tags included. | [E17] |
| 2 | ~~H~~ | ~~A fresh clone fails `go test ./...`.~~ **FIXED**: `.out` files committed; fresh clone passes. | [E18] |
| 3 | **H** | The throughput comparison is not a clean win (and the resume claim did not reproduce). The "mutex" baseline is not the original store. | [E5] [E6] |
| 4 | **H** | Persistence is off by default. `everysec` can lose about 1 s. No AOF checksums. No transaction frame in the log. | code, `-h` |
| 5 | **H** | Replication is a private protocol with no failover. | section 7 |
| 6 | **H** | One slow command (`KEYS`, huge `HGETALL`, `SMEMBERS`, `LRANGE 0 -1`, `SAVE`, a big `EXEC`, deleting a huge collection) blocks every client. Measured: `KEYS *` on 500,000 keys, worst `PING` latency elsewhere 53 to 64 ms vs 0.6 to 0.7 ms idle. | [E8] |
| 7 | **M-H** | No inline commands, no `AUTH`, no TLS, no `HELLO`/RESP3, no `WATCH`, no `SCAN`, no `SELECT`, no pattern pub/sub. | [E7] |
| 8 | **M** | The memory limit is an estimate: 52 MB reported vs 139 MB real (500,000 keys). `-maxmemory` defaults to 0 (no limit). | [E10] |
| 9 | **M** | Slow-client handling: no write timeout, no idle timeout by default, a trickling client can hold a connection. No cap on total bytes per command, or per connection, or per `MULTI` queue (100,000 commands with up to 512 MiB each). | CODE |
| 10 | **M** | A pipelining client that never reads can stall itself on Linux (blocked reply write). Not reproduced on Windows at 66 MB. | [E9], Linux UNVERIFIED |
| 11 | **M** | A failed AOF write leaves the change applied in memory while the client gets `MISCONF`. | CODE (`engine.go:357`), untested |
| 12 | **M** | Benchmarks are Windows-only, one laptop, same process for client and server, and vary by ±40% between runs and sessions. `redis-benchmark` and Debian numbers do not exist. | [E5] [E20] |
| 13 | **M** | `Makefile` and `bench/run.sh` were never run. (The CI workflow and `golangci-lint` config have now run: green. It needed two small lint fixes first.) | [E22] |
| 14 | **M** | Docs drift, partly fixed: decision 005, `docs/interview-notes.md` and the Phase-2 note no longer quote the old benchmark as a result. **Still stale:** decision 002 and the Phase-1 learning note describe `command.Dispatcher`, which no longer exists. | `git grep` |
| 15 | **M** | `BGSAVE`, full resync and `BGREWRITEAOF` copy all data (about 2 times memory during the copy). `FinishRewrite` does an fsync and rename on the engine goroutine. | CODE, [E20] |
| 16 | **L-M** | Every command is its own engine round trip, even pipelined ones. No batching. Replicas also apply one command per engine round trip. | CODE |
| 17 | **L-M** | Eviction quality (5-key sampled LRU) never measured. | section 6.4 |
| 18 | **L** | `INFO` reports `redis_version:1.0.0`. A client that reads that field could think it talks to Redis 1.0. Impact **UNVERIFIED**. | `INFO` output [E13] |
| 19 | **L** | `COMMAND` and `CONFIG GET` return empty arrays. Whether `redis-benchmark` accepts that is **UNVERIFIED**. | code |
| 20 | **L** | Hash, set and `KEYS` replies come in random order. `FLUSHALL ASYNC` is synchronous. Only one database. | code |
| 21 | **L** | For a connection that has ever subscribed, `session.subscribed()` builds and sorts the channel list on every command (`session.go:152`). Minor overhead. | CODE |
| 22 | **L** | `internal/repl` has 9.4% coverage from its own tests (87.8% when all tests count). | [E19] |
| 23 | **L** | A TCP peer that dies silently is only noticed when a write fails: no heartbeat. | CODE |
| 24 | **L** | **TODO/FIXME comments in the code: none** (searched). | [E17] |

---

## 11. Interview Q&A seeds (40)

"**NOT BUILT**" marks questions where the honest answer is that I did not build that.

### Concurrency (12)

**1. Why one goroutine owns the data instead of a mutex?**
Commands become atomic for free. `EXEC` is just one request. The order commands run is the order they are logged and sent to replicas. There are no locks on the data. The cost is that the data path uses one core. I benchmarked a global-mutex handler against it: with no network the mutex was faster, over TCP neither was reliably faster [E5].

**2. A single goroutine caps you at one core. What do you do about it?**
Nothing: this is the same limit Redis has. Network parsing and writing use all cores, because each client has its own goroutine. **Sharding by key across several engines is NOT BUILT.** I would add it only after profiling showed the engine as the bottleneck.

**3. How do clients talk to the engine, and what happens when the queue is full?**
Each client goroutine puts a request on a buffered channel (1024 slots) and waits on a private reply channel. If the queue is full the client goroutine blocks until there is room. Nothing is dropped. That is back-pressure.

**4. Can the engine block on a slow client?**
No. The reply is a plain value dropped into a channel with exactly one free slot. The client goroutine does the socket write. A slow client only slows itself.

**5. What if a client disconnects while its command is queued?**
The command still runs. The reply lands in the buffered channel. The goroutine wakes up, its next read or flush fails, and it exits. I observed this live with a `KEYS *` followed by an instant disconnect [E8]. There is no automated test for this exact case.

**6. How do you prevent goroutine leaks, and how is that tested?**
Every goroutine has an owner and a stop condition (table in section 3.1). Tests count goroutines before and after shutdown, with clients, subscribers and replicas attached (`TestGracefulShutdownLeavesNoGoroutines`, `TestShutdownWithSubscribersLeavesNoGoroutines`, `TestShutdownWithReplicationLeavesNoGoroutines`, `TestEngineGoroutineExitsOnClose`).

**7. Explain graceful shutdown.**
The context is cancelled. The listener is closed. Every connection gets a read deadline of "now", which wakes blocked reads. Commands already received still finish and reply. After a 5 s grace period leftover connections are force-closed. Then the engine closes, then replication, then persistence (final flush, optional snapshot, close AOF). **Real Ctrl+C or SIGTERM delivery is UNVERIFIED.**

**8. Where is the data-race risk, and how do you check it?**
At the hand-offs: replies, `Dump()` copies, and the few shared structures. I use `go test -race`, which passes today on Windows [E3]. It only covers paths the tests run. CI also runs it on Ubuntu, and that passed.

**9. What does the `sync.Pool` in the engine do? Is reuse safe?**
It recycles request objects and their reply channels. It is safe because a request is used once: submitted, answered once, received by its owner, then recycled. The reply channel has capacity 1 and exactly one send.

**10. How is `EXEC` atomic? Does it roll back?**
It is one engine request, so no other client's command runs in between. It does not roll back: a failing command yields an error in the reply array and the rest still run, as in Redis. Commands that are unknown or have wrong argument counts at queue time abort the whole `EXEC`. **`WATCH` is NOT BUILT.**

**11. How does pub/sub avoid blocking publishers?**
Each subscriber has a bounded queue (256). `Publish` does a non-blocking send. If the queue is full the subscriber is dropped and its connection closed. Test: 300 messages of 256 KiB with one stalled subscriber: the healthy one received all 300 and the publisher was never delayed.

**12. Why does `connection.Read` flush the writer?**
So pipelined commands get their replies in few writes, and a lone command gets its reply immediately. The wrapper flushes right before the buffered reader would block on the socket. The alternative "flush when nothing is buffered" can deadlock if a packet holds one full command plus the start of the next.

### Correctness (8)

**13. How does expiry work, and why both lazy and active?**
Each key has an absolute expiry time. A read of an expired key deletes it (lazy). Lazy alone would leave never-read keys forever, so every 100 ms the engine samples 20 random keys that have a TTL, deletes expired ones, and repeats while more than 25% were expired, within 5 ms.

**14. Why do replicas not expire keys themselves?**
Clocks differ between machines. The primary sends an explicit `DEL`. A replica only hides keys that are past their time. Tested with two different fake clocks (`TestExpiryIsPropagatedAsDelete`).

**15. Why is `EXPIRE` logged as `PEXPIREAT`?**
Replaying `EXPIRE k 10` an hour later would give a fresh 10 seconds. The absolute form gives the same result any time. Verified in the real file [E11]: `EXPIRE name 100` appeared as `PEXPIREAT name 1793372673460`, and after a restart the TTL was 88.

**16. How do you ensure a reply is not sent before the log write?**
`handle` calls `commit` (log write and, under `always`, fsync) before it sends the reply into the channel (`engine.go:357` then `:376`). Under `everysec` and `no`, "written" means in the OS cache, not on disk.

**17. How does the skip list give O(log n) rank?**
Each forward pointer stores a span: how many bottom-level nodes it skips. Adding spans along the search path gives the position. Levels are random with probability 1/4, up to 32. Checked against a sorted-slice reference with randomized operations plus a structural invariant checker.

**18. How are `WRONGTYPE` errors and empty collections handled?**
Every typed operation calls `DB.typed`, which returns `WRONGTYPE Operation against a key holding the wrong kind of value` if the key holds another type. When a list, hash, set or sorted set becomes empty the key is removed, like Redis.

**19. How do you protect against malformed or oversized input?**
Caps on line length (64 KiB), argument count (1,048,576) and bulk size (512 MiB). Bulk data over 64 KiB is read in chunks, so memory follows bytes received. Errors close the connection. Verified live [E7]. **There is no cap on the total size of one command (weakness 9).**

**20. What does `INCR` do on bad or huge numbers?**
`ParseInt` accepts only canonical decimal integers (no spaces, no `+`, no leading zeros). Otherwise `ERR value is not an integer or out of range`. Overflow gives `ERR increment or decrement would overflow`.

### Persistence (6)

**21. What are the fsync policies, and what can you lose?**
See section 5.2. `always`: nothing acknowledged. `everysec`: about 1 s if the OS dies. `no`: whatever the OS had not flushed. Default is `everysec`, and AOF is off by default.

**22. What happens on a crash in the middle of a write?**
Torn tail: the file ends inside a command, so startup truncates to the last complete command and logs a warning. Anything that is not valid RESP in the middle stops startup with the byte offset. The AOF has no checksum, so a damaged length near the end can look like a torn tail.

**23. How does `BGREWRITEAOF` avoid losing writes?**
When it starts, the engine begins capturing every newly logged command and copies the data in the same step. A goroutine writes the compact file. Then the engine appends the captured commands and swaps the file. A test ran increments during a rewrite and all survived a restart.

**24. How do you do `BGSAVE` without `fork`?**
The engine deep-copies the data (about 7 ms per 100,000 keys, measured [E20]) and a goroutine writes the copy. The price is the pause and about double the memory while the copy lives.

**25. What exactly is NOT durable?**
With defaults, everything (AOF is off). See the list in section 5.5.

**26. Why does the AOF win over the snapshot at startup?**
It is updated on every write, so it is normally newer than a periodic snapshot. If AOF is off, the snapshot is used.

### Replication (4)

**27. How does a replica sync, full versus partial?**
Section 7.1. The replica sends `MRSYNC <id> <offset>`. If the id matches and the offset is still in the 1 MiB backlog, it gets only the missing bytes. Otherwise it gets a snapshot, then the live stream.

**28. How do you get a consistent snapshot while writes continue?**
Attaching the replica runs inside the engine: copy the data and register the replica's queue in one step. Everything after goes into the queue. A test with a writer racing the sync (757 increments in one run) found none lost.

**29. What if the replica is slow or disconnects?**
Slow: its 8192-command queue fills, it is dropped and must resync. Disconnect: it retries every second and tries a partial resync first.

**30. What about failover?**
**NOT BUILT.** A person runs `REPLICAOF NO ONE`. No election, no Sentinel, no split-brain handling.

### Performance (5)

**31. What did you measure, and what did it show?**
Honestly: the mutex-versus-engine TCP comparison is within noise (section 1.6). No-network: the mutex is 2.3 times faster. Parse `SET` about 290 ns with 14 allocations. Store `SET`/`GET` about 20 ns. A 100,000-key `Dump` about 7 ms. `redis-benchmark`: **UNVERIFIED, not run**.

**32. Where would you look first to speed it up?**
Profile with the built-in `-pprof-addr`. Likely candidates: batch pipelined commands into one engine request, cut the 14 allocations per parse, avoid `fmt` in reply writing. I did not optimise because the network round trip (microseconds) dominates a 290 ns parse.

**33. How does pipelining work here?**
Section 4h. Each command is still its own engine round trip, but replies are batched in the connection's write buffer and flushed when it fills or the reader would block. I measured 10,000 pipelined `SET`s answered within 48 ms and 500,000 in 1.56 s on one connection (single runs).

**34. What does a large command cost the rest of the server?**
It blocks the engine for its whole duration. Measured: `KEYS *` with 500,000 keys delayed another client's `PING` by up to 64 ms [E8]. `SAVE` blocks completely. There is no `SCAN`.

**35. How accurate is `maxmemory`?**
It limits an estimate. For 500,000 small keys the estimate was about 38% of the process's real size [E10]. It also checks before a write, not after.

### Design alternatives and scaling (5)

**36. Why not use an epoll event loop like Redis?**
Go's runtime already uses epoll underneath. A goroutine blocked on a socket costs a few KB, so one goroutine per connection keeps the code straightforward. The data path is still single-threaded like Redis.

**37. How would you scale beyond one core or one machine?**
**NOT BUILT.** Options: shard keys across several engine goroutines (multi-key commands and `EXEC` get harder), or run many nodes with key-based routing (cluster).

**38. How would you add `AUTH` and TLS?**
**NOT BUILT.** `AUTH` would be a session-layer command with per-connection state; the listener could be wrapped with `crypto/tls`. `MRSYNC` would need authentication too (today anyone can request a full dataset).

**39. How would you add `WATCH`?**
**NOT BUILT.** Keep a version counter per key, record watched versions in the session, and have `ExecuteAtomic` check them first and abort if changed. Because the engine is single-threaded, the check and the batch are atomic.

**40. What would you do differently, and what are the biggest weaknesses?**
(Already done: commit the conformance `.out` files and push to GitHub.) Run everything on Linux by hand. Fix the benchmark story: benchmark the same workload longer and on Linux. Add a total-size cap per command, a write timeout and an output buffer limit. Add a checksum to AOF records. Write a client-library compatibility test.

---

## 12. Run-it-yourself cheat sheet

**Note for this Windows machine:** a Memurai Windows service already listens on port 6379 here, so use another port such as **6390** and do **not** run `FLUSHALL` against 6379. On Windows the CLI is `"C:\Program Files\Memurai\memurai-cli.exe"`. On Debian use `redis-cli`. Every command below was run by me, on port 6404 or similar [E15].

### Build, start, connect

```bash
# Debian / Git Bash
go build -o bin/miniredis ./cmd/miniredis
./bin/miniredis -addr 127.0.0.1:6390 -dir ./data -appendonly yes -appendfsync everysec
# second terminal:
redis-cli -p 6390
```

```powershell
# Windows PowerShell
go build -o bin\miniredis.exe .\cmd\miniredis
.\bin\miniredis.exe -addr 127.0.0.1:6390 -dir .\data -appendonly yes -appendfsync everysec
# second terminal:
& "C:\Program Files\Memurai\memurai-cli.exe" -p 6390
```

`./bin/miniredis -h` lists every flag. Stop with Ctrl+C. (Real signal handling is UNVERIFIED.)

### 5-minute demo (each line's expected result was seen)

```
SET name anmol            -> OK          GET name   -> anmol
INCR visits               -> 1           INCRBY visits 10 -> 11
APPEND name " jain"       -> 10          GET name   -> anmol jain

SET token abc EX 3        -> OK          TTL token  -> 3
(wait 3.3 s)  GET token   -> (nil)       TTL token  -> -2

RPUSH jobs a b c          -> 3           LPUSH jobs z -> 4
LRANGE jobs 0 -1          -> z a b c     LPOP jobs  -> z

HSET user:1 name anmol city pune -> 2    HGETALL user:1 -> name anmol city pune

ZADD board 50 alice 70 bob 60 carol -> 3
ZRANGE board 0 -1 WITHSCORES -> alice 50 carol 60 bob 70
ZRANK board carol         -> 1
```

**MULTI/EXEC** (interactive `redis-cli` shows it directly; or pipe one connection):

```bash
printf 'MULTI\nSET a 1\nINCR a\nGET a\nEXEC\n' | redis-cli -p 6390
# prints: OK QUEUED QUEUED QUEUED OK 2 2   (the last three lines are EXEC's replies)
```

**Pub/sub with two terminals:**

```
terminal 1:  redis-cli -p 6390 SUBSCRIBE news      # prints: subscribe news 1, then waits
terminal 2:  redis-cli -p 6390 PUBLISH news "hello subscribers"   # prints 1
terminal 1 then shows:  message news hello subscribers
```

**Restart and see the data come back:**

```bash
# server was started with -appendonly yes -dir ./data. Write something, stop it, start it again:
redis-cli -p 6390 SET name anmol
# stop the server (Ctrl+C), then start it with the same flags
redis-cli -p 6390 GET name         # -> "anmol"
ls -l ./data                       # appendonly.aof
```

(I tested the hard-kill version of this: kill the process abruptly, restart, data present [E11].)

**Replication demo (two terminals each):**

```bash
./bin/miniredis -addr 127.0.0.1:6406 &                                     # primary
./bin/miniredis -addr 127.0.0.1:6407 -replicaof "127.0.0.1 6406" &         # replica
redis-cli -p 6406 SET k v ;  redis-cli -p 6407 GET k        # -> v
redis-cli -p 6407 SET x y                                   # -> READONLY ...
redis-cli -p 6406 INFO replication ; redis-cli -p 6407 INFO replication
```

### Tests and benchmarks

```bash
go test ./...                       # all tests (a fresh clone passes)
go test -race -count=1 ./...        # race detector (needs gcc; works on this Windows machine)
go test -count=1 -v ./... | grep -c -- '--- PASS'      # -> 598 today
# fuzz one target for 10 s:
go test -run '^$' -fuzz '^FuzzReadCommand$' -fuzztime 10s ./internal/resp
# the exact benchmark behind section 1.6:
go test -run '^$' -bench 'Handler|TCP' -benchtime 3s -count 3 ./internal/server
# conformance suite (it runs FLUSHALL on the target! use a scratch server):
conformance/run.sh 127.0.0.1 6390
# crash test on Debian:
scripts/crash-test.sh 1000
```

On Windows, `conformance/run.sh` and `scripts/crash-test.sh` need Git Bash and a `redis-cli` on the PATH. I ran them with a tiny wrapper that calls `memurai-cli.exe` and strips carriage returns.

---

## 13. Git status

State after the fixes (checked 2026-10-30):

| Question | Answer | Evidence |
|---|---|---|
| Is everything committed? | Yes. `git status` is clean. | [E17] |
| Is it pushed to `github.com/anmol0705/mini-redis-go`? | **Yes.** `main` and the tags `v0.1`, `v0.2`, `v1.0` are on the remote. | `git ls-remote`, [E22] |
| Is the repo public? | **Yes** (`gh repo view`: `PUBLIC`). | [E17] |
| Does a fresh clone pass `go test ./...`? | **Yes**, all 14 packages `ok`. | [E18] |
| Is CI green? | **Yes**, all 7 jobs: lint, test-linux (`-race -cover`), test-windows, 4 cross-builds. | [E22] |
| Does the README match the code? | Yes: links resolve, every command in the table exists and every command in the code is in the table, quick-start flags are valid per `-h`. | [E21] |

### What changed after this document first found problems

| Commit | Change |
|---|---|
| `6b1dd4c` | README: Go version, decision-005 row, benchmark figures (README only) |
| `8140cd7` | This factsheet added |
| `bef7377` | `.gitignore`: `!conformance/cases/*.out`; the 10 `.out` files committed (finding 2) |
| `5128b58` | Decision 005, `docs/interview-notes.md` and the Phase-2 note: the TCP benchmark showed no reliable difference; the "133.6k vs 87.6k" figure kept only as a historical data point. Also corrected a sentence that wrongly said a test covers a vanishing client |
| `cd0a088`, `5549f31` | Lint fixes after the first CI run (see below) |

### CI history (GitHub Actions workflow `ci`)

1. First run (`5128b58`): test-linux, test-windows and all 4 cross-builds **passed**; `lint` **failed** with 4 staticcheck style findings (QF1008 "could remove embedded field `Conn` from selector" at `internal/server/conn.go` lines 69, 101, 117; QF1001 De Morgan's law at `internal/skiplist/skiplist_test.go:172`). My local `staticcheck` does not enable those rules, which is why I had not seen them.
2. Second run (`cd0a088`): `lint` failed again with 2 more of the same QF1008 kind (`conn.go` lines 130 and 133). The tool reports a few at a time.
3. Third run (`5549f31`): **all 7 jobs green.** The fixes were mechanical (`c.Conn.Close()` became `c.Close()`, and so on). `c.Conn.Read` had to stay because `connection` defines its own `Read`.

Warnings in the CI log that are not failures: Node.js 20 deprecation for `actions/checkout@v4` and `actions/setup-go@v5`; "Restore cache failed: Dependencies file is not found ... go.sum" (the project has no `go.sum`, so Go module caching is skipped); a notice that `ubuntu-latest` will move to a newer Ubuntu.

### README changes (made earlier)

1. **Quick start:** "Requires Go 1.25 or newer" became "Go 1.25.3 or newer (the version in `go.mod`)".
2. **Decision table row 005:** says the engine was chosen for atomic transactions and ordered logging, and that the benchmark showed no reliable throughput difference over TCP.
3. **Benchmarks intro:** dated 2026-10-30, and says results vary a lot between runs and sessions.
4. **Benchmarks table, TCP rows:** replaced "133.6 k vs 87.6 k" with re-run medians, ranges and a note that the earlier gap did not reproduce.
5. **Benchmarks table, micro-benchmarks:** today's medians; noted that the store benchmark cycles through 1,024 of the 100,000 keys.

---

## Appendix A: Evidence log

All on 2026-10-30 unless marked, in the working tree at `a8e718c` (+ README edit), Windows, Go 1.25.3.

| Tag | What I ran | Result |
|---|---|---|
| E1 | `go test -count=1 ./...` (20:23) | 14 packages `ok`, `internal/testclient` has no tests |
| E2 | `go test -list` per package; `go test -count=1 -v ./...` (counts of `--- PASS`) | 229 Test functions, 5 Fuzz, 23 Benchmark; 234 top-level + 364 subtests = 598 PASS, 0 FAIL, 0 SKIP |
| E3 | `go test -race -count=1 ./...` | all `ok` |
| E4 | `go vet ./...`, `gofmt -l .`, `GOOS=linux` and `GOOS=windows go vet`, `staticcheck`, `errcheck -ignoretests`, `ineffassign` | all clean |
| E5 | `go test -run '^$' -bench 'Handler\|TCP' -benchtime 3s -count 3 ./internal/server` (20:25) | numbers in section 1.6 |
| E6 | 3 rounds alternating engine and mutex TCP pipeline-1 runs (20:26) | engine 179,030 / 115,390 / 137,074; mutex 150,491 / 141,609 / 126,820 |
| E7 | scratch RESP client against the real binary: half command, 10,000 pipelined `SET`, `SET PX 1500` + `DBSIZE`, malformed inputs, unsupported commands | section 4 and 8.1 |
| E8 | 500,000 keys; `KEYS *` while another client pings; a client that sends `KEYS *` and closes | `KEYS *` 165 to 196 ms end to end; worst concurrent `PING` 53 to 64 ms (idle max 0.6 to 0.7 ms); server healthy; clients back to 2 |
| E9 | 200,000 / 1,000,000 / 3,000,000 pipelined `GET` of a 1 KiB value in one write without reading | all completed, all replies read; no stall on Windows |
| E10 | fresh server, 500,000 `SET` pipelined, `INFO memory`, process working set | `used_memory:52388890`; working set 139 MB (7.9 MB empty); load took 1.56 s |
| E11 | server with `-appendonly yes -appendfsync always`; 11 commands; hex dump; `taskkill /F`; restart | 9 commands logged, 340 bytes; all data back; replay 364 us; TTL 88 after 12 s |
| E12 | `scripts/crash-test.sh 1000 6412` (unmodified; wrapper for `redis-cli`) | **PASS**, exit 0. First attempt FAIL from CRLF output only |
| E13 | `conformance/run.sh` against this server and my own separate Memurai instance (Redis 7.2.12) with `memurai-cli` 4.1.8 | 16 passed, 0 failed on both |
| E14 | 10 s fuzz per target | all PASS; 2,135,937 / 1,378,377 / 992,196 / 1,208,619 / 2,339,966 executions |
| E15 | every demo command in the cheat sheet against a live server | outputs as listed |
| E16 | two real processes, primary and `-replicaof` replica | section 7.1 |
| E17 | `git status`, `git log`, `git fetch`, `git rev-list origin/main..main`, `gh repo view`, `gh api` contents checks, `go list -deps`, `grep` for TODO and `sync.` | section 13; 0 non-std deps; 25 commits ahead; remote has only the placeholder |
| E18 | `git clone` of HEAD into a scratch folder; `go test -count=1 ./...` there | 13 packages ok; `internal/app` FAIL: `TestConformanceCases` (10 subtests), missing `.out` files |
| E19 | `go test -cover`; merged `-coverpkg` runs for repl, persist, engine, server, app | numbers in section 1.10 |
| E20 | micro-benchmarks, 3 runs each | parse 289 ns (14 allocs); `Set` 23 ns; `Get` 19 ns; `ZRank+ZRange` 1.44 us; `Dump` 6.99 ms (14.4 MB) |
| E21 | README links, README-vs-table check in both directions | all links resolve; no missing or invented commands |
| E22 | `git push origin main --tags`; `gh run list`, `gh run view` | run 1 (`5128b58`): lint failed, 6 other jobs passed; run 2 (`cd0a088`): lint failed; run 3 (`5549f31`): **all 7 jobs success** |

## Appendix B: Everything I could not verify (UNVERIFIED)

- Real Ctrl+C and SIGTERM handling and the exit code.
- Anything done by hand on Debian or Linux: `redis-cli`, `redis-benchmark`, the crash test, two-process replication. (CI did run `go test -race -cover` and the build on Ubuntu.)
- `redis-benchmark` compatibility, including whether it accepts the empty `CONFIG GET` reply.
- Any client library (Go, Python, Node).
- The `Makefile` targets and `bench/run.sh`.
- Throughput and latency under `appendfsync always`.
- Durability across an OS crash or power loss.
- Reply-buffer stalls with huge unread pipelines on Linux.
- Hit-rate quality of the sampled LRU.
- The behaviour of a failed AOF write beyond what the code says.
- A real two-process partial resync.
- Long fuzz runs (hours) and long soak runs.
