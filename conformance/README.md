# Conformance suite

Black-box tests that talk to a server only through `redis-cli`, so they can be
reused unchanged for any implementation of the protocol (for example a C port
of this project), or run against real Redis to validate the expectations.

```bash
conformance/run.sh 127.0.0.1 6379            # check
conformance/run.sh 127.0.0.1 6379 --record   # overwrite expectations with this server's output
```

## Layout

- `cases/NN-name.in` - one `redis-cli` command per line, run over one connection
  (so `MULTI`/`EXEC` work).
- `cases/NN-name.out` - the expected output, one value per line.
- `run.sh` - runs every case, then a few checks that cannot be fixed files:
  expiry over real time, active expiry, the unknown-command error prefix, the
  fields of `INFO`, a 400 KB value, and `redis-cli --pipe`.

## What is covered

Every command the server supports except the ones that need a second
connection or change server state: strings (`SET` with `NX/XX/EX/PX/KEEPTTL`,
`GET`, `MGET`, `MSET`, `INCR` family, `APPEND`, `STRLEN`), keys (`DEL`, `EXISTS`,
`EXPIRE` family, `TTL`/`PTTL`, `PERSIST`, `TYPE`, `KEYS`, `DBSIZE`, `FLUSHALL`),
lists, hashes, sets, sorted sets, `WRONGTYPE`/syntax/arity/overflow errors,
`MULTI`/`EXEC`/`DISCARD` (including `EXECABORT`), `PUBLISH`, `CLIENT SETNAME`.
`SUBSCRIBE`, `SAVE`/`BGSAVE`/`BGREWRITEAOF`, `REPLICAOF` and `MRSYNC` are
covered by the Go integration tests (they need several connections, files or a
second server).

## How output is compared

In non-interactive mode `redis-cli` prints raw values, one per line. Blank lines
are dropped on both sides, so nil and empty-array replies (blank lines) are not
distinguished, and the blank line `redis-cli` may add after an error does not
matter. Commands with unspecified result order are only used with one element.

## Verified status

Run on the development machine (Windows) with `memurai-cli 4.1.8` (a
`redis-cli` build) and with Memurai Developer 4.1.8, which reports
`redis_version:7.2.12`:

| Server | Result |
|---|---|
| miniredis (this project) | 16 checks passed, 0 failed |
| Memurai (Redis 7.2.12 compatible) | 16 checks passed, 0 failed |

So the expected files agree with a real Redis, including the exact error texts,
and with this server. `internal/app/conformance_test.go` also replays every
`.in` file over TCP in `go test`, so a regression is caught without `redis-cli`.

`redis-cli --pipe` is reported as informational: this `redis-cli` build ends the
stream with an inline `ECHO`, which miniredis does not support (a documented
limitation), so `--pipe` against miniredis prints a note instead of passing.
Still to do on Debian: run the suite with the Debian `redis-cli`
(`apt install redis-tools`), whose output format should be identical.
