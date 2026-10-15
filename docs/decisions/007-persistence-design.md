# 007 - Persistence around the single-owner engine

Status: accepted (Phase 4)

## Context

We want an append-only log (durable, replayable), snapshots (compact,
fast to load), and background work that does not freeze clients. Redis does the
background work by `fork()`ing and letting the child see a frozen copy of
memory. Go cannot safely fork a multi-threaded runtime, and the project must
also run on Windows.

## Decisions

### 1. The engine is the only writer of the log

After each request the engine calls `Propagate` for every command that changed
data, then `Commit`. With `appendfsync always`, `Commit` fsyncs before the
reply is released, so an acknowledged write is on disk. Several commands in one
request (an `EXEC`) share one fsync. Log order is execution order by
construction.

*Rejected:* a separate writer goroutine fed by a channel. It would need its own
ordering and back-pressure story, and `always` would need an acknowledgement
round trip back to the engine.

### 2. Log successes in a replay-safe form

Commands are logged *after* they succeed, and relative or conditional ones are
rewritten (see `docs/formats.md`). `EXPIRE k 10` replayed an hour later would
otherwise give the key another 10 seconds. Expiry and eviction deletions are
logged as `DEL` so that replay (and replicas) never need a clock to decide.

### 3. BGSAVE: copy inside the engine, write outside

`store.DB.Dump` deep-copies the keyspace on the engine goroutine (a pause
proportional to the data, measured and logged as `engine_pause` and reported
as `rdb_last_bgsave_copy_usec` in INFO). A goroutine encodes and writes the
copy. This replaces `fork`'s copy-on-write. The cost is real memory for the
copy while the save runs. `SAVE` simply does everything on the engine and
blocks it.

*Rejected:* iterating the live DB from another goroutine (needs a lock on every
operation), and copy-on-write structures (a large complexity jump).
Measured pause: TBD - measure on Debian with a large keyspace.

### 4. BGREWRITEAOF without losing writes

1. On the engine: start *capturing* every command that is logged from now on,
   and `Dump` the keyspace. Snapshot plus captured commands equals the full
   state, because both happen in the same engine step.
2. In a goroutine: write the minimal commands for the dump to a temp file and
   fsync it.
3. Back on the engine (`Engine.Exec`): append the captured commands to the temp
   file, fsync, close the old log, `rename` the temp file over it, reopen for
   appending. Nothing is written during step 3 because the engine is the only
   writer.

If anything fails the old log keeps working and the temp file is deleted.
Windows cannot rename over an open file, which is why the old handle is closed
first; if the rename fails, the old file is reopened.

### 5. `everysec` uses a goroutine, and failures are reported

A goroutine fsyncs once per second if there is unsynced data. A failure is
remembered and returned by the next `Commit` (so clients see `MISCONF` errors)
until a later fsync succeeds. The goroutine shares the file handle with the
engine, so every use of the handle takes one mutex; a long fsync can therefore
delay a write, which is also true in Redis.

### 6. Startup rules

AOF wins over the snapshot when enabled and non-empty. A torn last record is
truncated with a warning (this is the normal result of a crash mid-write).
Damage anywhere else, a command that fails on replay, or a bad snapshot
checksum all stop startup with a clear message and leave the files untouched.

## Consequences

- A truncated record that happens to look like "end of file" is treated as a
  torn tail, so a corrupted length field near the end could silently drop the
  records after it. Redis has the same trade-off. The warning prints how many
  bytes were dropped.
- Strict replay (any failing command aborts startup) catches bugs early but
  means a command that stops being valid in a later version blocks startup.
- Transactions are logged as plain commands without a `MULTI/EXEC` frame (see
  known-limitations).
- Tests: every fsync policy with a fake file counting `Sync`; a torn tail at
  every byte offset; corruption in the middle; a rewrite with concurrent
  writes; restart round trips of every type with TTLs on an injected clock.
