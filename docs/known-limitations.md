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
