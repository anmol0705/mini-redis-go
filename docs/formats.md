# On-disk formats

Both files live in the directory given by `-dir`.

## Append-only file (`appendonly.aof`)

A plain concatenation of commands in RESP, exactly what a client would send:

```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n
```

- Only **successful write commands** are logged, after the command ran.
- Commands that depend on the clock or on a condition are logged in a replay-safe
  form: `EXPIRE/PEXPIRE/EXPIREAT` become `PEXPIREAT key <unix ms>`, `SET k v EX n`
  becomes `SET k v PXAT <unix ms>`, `SET ... NX/XX` is logged without the
  condition, an `EXPIRE` that deleted the key is logged as `DEL`.
- Keys removed by expiry or eviction are logged as `DEL key`.
- On startup the file is replayed through the same command code, with no
  replies sent. A **torn final record** (the file ends in the middle of a
  command) is cut off and a warning is logged. Anything unparseable earlier in
  the file stops startup with an error that names the byte offset.
- `BGREWRITEAOF` replaces the file by one that contains the minimal commands
  for the current data: `SET`, and chunked `RPUSH` / `HSET` / `SADD` / `ZADD`
  (256 elements per command), each followed by `PEXPIREAT` when the key has an
  expiry (strings carry `PXAT` in the `SET`).

## Snapshot (`dump.mrdb`, name set by `-dbfilename`)

All integers are big-endian. The file is covered by a CRC32 (IEEE) trailer.

```
header
  4 bytes   magic            "MRDB"
  2 bytes   version          1
  2 bytes   flags            0 (reserved)

entries, repeated:
  1 byte    type tag         1 string, 2 list, 3 hash, 4 set, 5 sorted set
  8 bytes   expire-at        unix milliseconds, 0 = no expiry (never negative)
  4 + n     key              uint32 length, then bytes
  value, by type:
    string  : 4 + n         uint32 length, bytes
    list    : 4 + ...       uint32 count (>= 1), then count x string, head first
    hash    : 4 + ...       uint32 count (>= 1), then count x (field string, value string)
    set     : 4 + ...       uint32 count (>= 1), then count x member string
    zset    : 4 + ...       uint32 count (>= 1), then count x (8 byte IEEE-754 score, member string)

end marker:
  1 byte    0xFF

trailer:
  4 bytes   CRC32 (IEEE) of every byte from the magic up to and including 0xFF
```

where "string" means a `uint32` length followed by that many bytes.

Rules enforced when loading (`snapshot.Decode`), each failing with an error
that wraps `snapshot.ErrCorrupt`:

- magic and version must match; unknown type tags are rejected;
- a string longer than 512 MiB, an empty collection, a NaN score or a negative
  expiry is rejected;
- lengths and counts are never trusted for allocation: strings are read in
  64 KiB chunks and slices grow only as elements arrive;
- the file must end exactly after the checksum and the checksum must match
  *before* any item is returned, so a damaged file loads nothing.

Writing is atomic: the data goes to `dump.mrdb.tmp`, is fsynced, and is then
renamed over `dump.mrdb`.

## Which file is used at startup

| `-appendonly` | AOF exists | Snapshot exists | Loaded |
|---|---|---|---|
| yes | yes (non-empty) | yes | AOF (it wins) |
| yes | yes | no | AOF |
| yes | no | yes | snapshot, then a new AOF is written from it |
| yes | no | no | nothing |
| no | any | yes | snapshot |
| no | yes | no | nothing (a warning says the AOF was ignored) |
