# Known limitations

An honest list, updated every phase. State: end of Phase 1.

- **No inline commands.** Only RESP arrays are accepted, so `telnet` typing
  `PING` gets a protocol error. `redis-benchmark`'s default test list starts
  with `PING_INLINE`, which fails; select tests with `-t` (see the Debian
  commands in the Phase 1 report).
- **Strings only.** No lists, hashes, sets, sorted sets or expiry yet
  (Phase 2). `SET` takes no options; `SET k v EX 10` returns `ERR syntax error`.
- **One database.** `FLUSHDB` and `FLUSHALL` are the same; there is no `SELECT`.
- **`COMMAND` and `CONFIG GET` return empty arrays.** Clients that expect
  real data from them will not get it. Whether the real `redis-benchmark`
  accepts an empty `CONFIG GET` reply has not been verified yet (no Redis
  tools were available on the Windows dev machine); it is a check on Debian.
- **No write timeout.** A client that stops reading can park its goroutine in
  a write until shutdown's grace period force-closes it.
- **Size limits are constants**, not flags: 64 KiB per protocol line, 1 Mi
  arguments, 512 MiB per value.
- **Shutdown drops a half-received command.** Only fully received commands
  are guaranteed to run.
- **INFO is minimal** (server, clients, keyspace sections only) and reports
  `redis_version` as this project's own version, not a Redis version.
- **Store is lock-based** until the Phase 2 engine.
- **Not tested here:** real signal delivery (Ctrl+C / SIGTERM) and
  `redis-cli`/`redis-benchmark` compatibility; both are on the Debian checklist.

## Added in Phases 2 and 3

- **Single-threaded data path.** One slow command (`KEYS` on a large keyspace,
  `LRANGE` of a huge list, `SAVE`) delays every client.
- **`SET` options:** EX, PX, EXAT, PXAT, NX, XX, KEEPTTL are supported; `GET`,
  `SETEX`, `GETEX` and friends are not.
- **`ZADD` has no options** (NX, XX, GT, LT, CH, INCR). `ZRANGE` supports only
  index ranges and `WITHSCORES`.
- **No `WATCH`.** Optimistic locking is not available.
- **No pattern subscriptions** (`PSUBSCRIBE`); channel names are exact.
- **Pub/sub is not logged or replicated.** Messages published on a primary are
  not seen by subscribers connected to a replica.
- **Transactions are not wrapped for the log:** inside the append-only file the
  commands of an `EXEC` appear one after another without a `MULTI`/`EXEC` frame,
  so a crash in the middle of writing a transaction can leave it half-applied
  after recovery.
- **A panic inside a command is recovered** and answered with an error, but the
  command may have left its key half-modified.
- **Order of `HGETALL`, `SMEMBERS`, `KEYS`** is unspecified (Go map order).
- **Memory accounting is an estimate**: per key `96 + len(key) + value`, with
  per-element constants (16 bytes per list element, 48 per hash field, 32 per
  set member, 96 per sorted-set member). It does not measure the Go heap, so
  `maxmemory` limits the estimate, not the process's real memory use.
- **No `SELECT`**; there is one database.
