# Phase 4 - Persistence

## What was built

- **Append-only file (AOF)**: every successful write command is appended to a
  file in the same RESP form clients use. At startup the file is replayed to
  rebuild the data.
- **Snapshots**: the whole keyspace written to one binary file with a checksum.
  `SAVE` (blocking) and `BGSAVE` (background), plus an optional timer.
- **`BGREWRITEAOF`**: shrinks the log by replacing it with the minimal commands
  that produce the current data, without losing writes that arrive meanwhile.
- **Flags**: `-dir`, `-appendonly`, `-appendfsync`, `-dbfilename`,
  `-snapshot-interval`.

## Data flow, with one example

`SET name Anmol EX 60` with `-appendonly yes -appendfsync always`:

1. The engine runs `cmdSet`. The key is stored with an expiry of `now + 60000`.
2. `cmdSet` tells the engine what to log: not the original command, but
   `SET name Anmol PXAT 1700000060000`, an absolute time.
3. After the request, the engine calls `Manager.Propagate` (which encodes the
   command into the AOF's buffer) and `Manager.Commit`.
4. `Commit` writes the buffer to the file and, because the policy is `always`,
   calls `fsync`. Only now is the reply `+OK` released to the client.
5. After a crash, startup reads the file, parses each command and runs it
   through the same command code. The key comes back with its expiry intact. If
   the time has passed, it is simply gone.

## Go concepts used

- **`io.Reader` / `io.Writer` / `bufio`** - the AOF writes through a
  `bufio.Writer`; replay reads through a `bufio.Reader` wrapped in a
  byte-counting reader (`aof.Replay`) so we know exactly where the last good
  command ended.
- **Interfaces for testing** - `aof.File` is the three methods we need from a
  file. Tests pass a fake that counts `Sync` calls to prove each fsync policy.
- **Atomic file replacement** - write `*.tmp`, `fsync`, then `os.Rename` over
  the target (`snapshot.WriteFile`). A crash leaves the old or the new file,
  never half.
- **`defer` with a named return** - `WriteFile` uses `defer` plus the named
  `err` to remove the temp file only on failure.
- **`sync.Mutex` around a handle that can be swapped** - the background
  fsync goroutine and the engine share the file; the rewrite replaces it, so
  all use goes through `Log.fmu`.
- **`atomic.Pointer`** - `Manager.aofLog` is set once during load and read by the
  engine goroutine; an atomic pointer avoids a data race without a lock.
- **`encoding/binary` and `hash/crc32`** - the snapshot format is
  hand-encoded big-endian integers, and a CRC32 over everything before the
  trailer.
- **Errors as values with `errors.As`** - `aof.CorruptError` carries the byte
  offset and is found with `errors.As`.
- **Timers you can inject** - `Options.Ticks` returns a channel; tests send on
  it to trigger the once-a-second fsync, so there is no sleeping.

## Interview questions

1. **What is the difference between the three fsync policies, and what can you
   lose?**
   `always`: fsync before replying; nothing acknowledged is lost, slowest.
   `everysec`: a goroutine fsyncs each second; up to about a second of
   acknowledged writes can be lost. `no`: the operating system decides; the most
   you can lose is whatever it had not flushed. Tested with a fake file that
   counts `Sync`.

2. **Why not just log the command the client sent?**
   `EXPIRE k 10` replayed later would restart the 10 seconds. So commands are
   rewritten to absolute form (`PEXPIREAT`, `SET ... PXAT`) at execution time.

3. **How do you survive a crash in the middle of writing a record?**
   The file may end with half a command. On replay, hitting end-of-file inside
   a command means "torn tail": we truncate to the last complete command and
   warn. I tested this by cutting a log at every possible byte offset.

4. **How do you tell a torn tail from corruption?**
   Torn: the data simply ends inside a command. Corrupt: bytes that are not
   valid protocol earlier in the file. Corruption stops startup with the byte
   offset, because guessing could silently drop data. (Caveat: a damaged length
   field near the end looks like a torn tail.)

5. **How does `BGSAVE` work without `fork`?**
   Go cannot fork safely. The engine deep-copies the keyspace (a short pause that
   I measure and log) and a goroutine writes the copy. The price is the memory of
   the copy while it runs.

6. **How can `BGREWRITEAOF` lose no writes?**
   When it starts, the engine begins capturing every logged command and takes
   a copy of the data in the same step. A goroutine writes the compacted file.
   Then, on the engine, the captured commands are appended and the file is
   swapped in. Because the engine is the only writer, nothing can slip in
   between. A test runs INCRs concurrently with a rewrite and checks the final
   count after restart.

7. **How do you make a snapshot write crash-safe?**
   Temp file, fsync, rename. The loader also verifies a CRC32 before accepting
   anything, so a damaged file loads nothing.

8. **If both an AOF and a snapshot exist, which wins and why?**
   The AOF, because it is updated on every write and so is normally more recent
   than a periodic snapshot.
