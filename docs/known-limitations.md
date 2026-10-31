# Known limitations

An honest list. State: v1.0.

## Protocol and compatibility

- **No inline commands.** Only RESP arrays are accepted, so typing `PING` into
  `telnet` gets a protocol error. Consequences seen in practice: the default
  `redis-benchmark` test list starts with `PING_INLINE` and fails (select tests
  with `-t`), and `redis-cli --pipe` (this version ends its stream with an inline
  `ECHO`) does not complete.
- **RESP2 only.** `HELLO` / RESP3 are not supported.
- **The unknown-command error text differs from Redis.** It quotes names with Go
  escaping (`"NOSUCH"`) where Redis uses single quotes. Client text is escaped and
  capped at 64 bytes on purpose (reply-injection defence).
- **`COMMAND` and `CONFIG GET` return empty arrays.** Clients that need real data
  from them will not get it. Whether the real `redis-benchmark` accepts an empty
  `CONFIG GET` reply has **not been verified** (no `redis-benchmark` was available
  on the Windows development machine); it is a Debian check.
- **`INFO`** has the sections server, clients, memory, stats, persistence,
  replication and keyspace, with a subset of the usual fields, and reports
  `redis_version` as this project's own version.
- **One database.** No `SELECT`; `FLUSHDB` equals `FLUSHALL`.
- **Missing features:** `WATCH`, `PSUBSCRIBE`, Lua, streams, `SCAN`, `CLIENT KILL`,
  authentication, TLS, `MONITOR`, `OBJECT`, `DEBUG`, cluster.
- **Partial command options.** `SET` supports `EX PX EXAT PXAT NX XX KEEPTTL`
  (no `GET`); `ZADD` has no `NX/XX/GT/LT/CH/INCR`; `ZRANGE` is by index only.
- **Order of `HGETALL`, `SMEMBERS` and `KEYS`** is unspecified (Go map order).

## Concurrency and performance

- **The data path is single-threaded.** One slow command (`KEYS` on a large
  keyspace, a huge `LRANGE`, `SAVE`) delays every client. Network parsing and
  writing still use all cores.
- **Each command is its own queue round trip.** The server does not batch
  pipelined commands into one engine request (not measured).
- **No write timeout** on client connections: a client that stops reading holds
  its goroutine until it disconnects or shutdown's grace period force-closes it.
- **Size limits are constants**, not flags: 64 KiB per protocol line, 1,048,576
  arguments per command, 512 MiB per value, 100,000 queued commands per
  transaction, 10,000 channels per subscriber.
- **A panic inside a command is recovered** and answered with an error, but the
  command may have left its own key half-modified.
- **Performance numbers** in this repository come from one Windows laptop.
  `redis-benchmark` and Debian numbers are marked TBD until measured.

## Memory

- **The memory limit is an estimate, not the process's real memory.** Per key it
  counts `96 + len(key) + value bytes`, plus per element `16` (list), `48` (hash
  field), `32` (set member), `96` (sorted-set member). It ignores the Go
  runtime's overhead, garbage not yet collected, and the copy made by `BGSAVE`
  and full resyncs. Actual RSS can be noticeably higher.
- **Eviction is approximate** (5 sampled keys by default) and the check runs
  before a write, so memory can exceed the limit by one command.
- `BGSAVE` and full resync **copy the whole keyspace** (a pause proportional to
  its size, about 11 ms for 100,000 small keys on the development machine) and
  hold that copy in memory while it is written.

## Persistence

- **A torn tail cannot be told apart from some corruption.** A damaged length
  field near the end of the AOF looks like a cut-off record and is truncated (with
  a warning that says how many bytes were dropped).
- **Strict replay.** Any command that fails while replaying the AOF stops startup.
- **Transactions are not framed in the AOF.** The commands of an `EXEC` are logged
  one after another without `MULTI`/`EXEC`, so a crash in the middle of writing one
  can leave it half-applied after recovery.
- **AOF rewrites are triggered by size only.** The automatic rewrite
  (`-aof-rewrite-percentage`, `-aof-rewrite-min-size`) looks at the file once a
  second. Like `BGREWRITEAOF`, a rewrite pauses the engine briefly to copy the
  keyspace and again to append the captured commands and swap the file; those
  pauses were not measured. The size it counts is the bytes written to the file,
  not what is still in the write buffer. After a failed rewrite the automatic
  trigger waits one minute before trying again.
- **`appendfsync everysec`** can lose about a second of acknowledged writes; a
  fsync running long can delay the engine's next write (as in Redis).
- **`SAVE` blocks the engine.** `-snapshot-interval` also takes a final snapshot
  at shutdown.
- **Windows:** directory fsync after a rename is skipped (not supported there).
- Pub/Sub messages are never persisted.

## Replication

- **Not compatible with Redis.** The handshake (`MRSYNC`), stream framing and
  snapshot format are private to this project. A miniredis replica cannot follow a
  real Redis and vice versa.
- No chained replication (a replica refuses to serve replicas), no heartbeat or
  `WAIT`, no authentication.
- A replica that falls behind by more than `-repl-queue-size` commands is
  dropped; under sustained heavy writes with a huge dataset it can loop through
  full syncs.
- A replica keeps its offset only in memory: restarting it forces a full sync.
- Pub/Sub is not replicated.
- After a primary restarts it has a new replication id, so every replica does a
  full sync.

## Testing

- Verified only on Windows so far: real `SIGTERM` delivery, `kill -9` crash
  recovery, `redis-benchmark` compatibility and the Debian `redis-cli` (all in
  `docs/DEBIAN_TESTING.md`). The conformance suite was run with `memurai-cli` 4.1.8
  against this server and against Memurai (Redis 7.2.12 compatible): 16 of 16 checks
  passed on both.
- `golangci-lint` itself could not be installed here; `.golangci.yml` is untested
  as a file, but `go vet`, `staticcheck`, `errcheck` (production code) and
  `ineffassign` were each run and are clean.
