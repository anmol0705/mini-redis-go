# 008 - Eviction and replication

Status: accepted (Phase 5)

## Part 1: memory limit and eviction

### Context
`-maxmemory` should keep the process bounded and `-maxmemory-policy` should say
what to do at the limit.

### Decisions
- **Estimate, don't measure.** The store keeps a running total, updated by every
  operation: per key `96 + len(key) + value bytes`, plus per element
  `16` (list), `48` (hash field), `32` (set member), `96` (sorted-set member).
  Measuring the Go heap (`runtime.MemStats`) is slow, global and includes
  garbage; an estimate is O(1) and monotone with the data. A property test
  recounts the whole keyspace from scratch after random operations and compares.
  The number is NOT what the OS reports (see known-limitations).
- **Check before write commands that can grow memory** (`DenyOOM` flag on the
  command): if over the limit, evict until under it or fail with the Redis text
  `OOM command not allowed when used memory > 'maxmemory'.` Reads, `DEL`,
  `EXPIRE` and friends are never refused, so a full server can always free
  itself.
- **Approximate LRU by sampling.** Each key stores a last-access time; eviction
  looks at `-maxmemory-samples` (default 5) random keys and removes the oldest.
  No linked list to maintain on every read, no per-key pointers. To pick random
  keys cheaply the store keeps an index slice next to the map (swap-remove on
  delete), and a second slice for keys with an expiry (used by `volatile-ttl` and
  by the expiry sampler).
- **Evictions are propagated as `DEL`** so the AOF and replicas stay identical.
  Replicas never evict: they follow the primary.

### Consequences
`allkeys-lru` quality depends on the sample size (larger = closer to true LRU,
slower). The check happens before the command, so memory can exceed the limit by
one write.

## Part 2: replication

### Context
The brief asks for REPLICAOF, full and partial resync, a read-only replica,
expiry propagation and protection from slow replicas. Real Redis uses PSYNC and
the RDB format, and speaking it exactly is a project of its own.

### Decision: a simplified miniredis protocol - NOT wire compatible
**This server cannot replicate to or from real Redis.** A miniredis replica
connects to a miniredis primary with a private command, `MRSYNC <replid>
<offset>`, and the primary answers `+FULLRESYNC <replid> <offset>` plus a
snapshot in this project's own format (`docs/formats.md`), or `+CONTINUE
<replid>` plus the missing part of the stream. After that the primary sends
every changing command in RESP, byte for byte what its AOF receives.

### How the pieces fit
- **Identity and offset.** A primary has a random 40-hex `replid` and counts the
  bytes of the stream it has produced (`master_repl_offset`). A replica counts
  the bytes it has applied (`slave_repl_offset`); they match when it is caught up.
- **Full sync, consistently.** Attaching a replica runs *inside the engine*: it
  deep-copies the keyspace and registers the replica's queue in the same engine
  step, so the snapshot and the live stream meet exactly, with no gap and no
  overlap. The snapshot is encoded and sent outside the engine. Commands that
  arrive while it is being sent wait in the replica's bounded queue and are
  sent right after.
- **Partial sync.** The primary keeps the last `-repl-backlog-size` bytes of the
  stream in a ring buffer. A replica that reconnects with the same `replid` and
  an offset still inside the window gets only the missing bytes; otherwise a
  full sync.
- **Replicas are read-only** for normal clients (`READONLY` error). They apply
  the stream through the same command code, in a mode that skips the read-only and
  maxmemory checks.
- **Expiry.** Only the primary expires keys. When it does (lazily or by the
  sampler) it sends `DEL`, so replicas never need to agree about the time. A
  replica hides keys that are past their expiry from reads but does not delete
  them (tested with two different clocks).
- **Slow replicas.** Each replica has a bounded queue (`-repl-queue-size`). If it
  is full when the primary has a new command, the replica is dropped and its
  connection closed (it will resync). The engine therefore never waits for a
  replica (tested with a replica that stops reading).
- **Promotion.** `REPLICAOF NO ONE` cancels the follower, becomes writable and
  starts a new `replid`, so other nodes cannot confuse its history with the old
  one. The follower's apply step checks "am I still following?" inside the same
  engine step as the command, so no late command can slip in after promotion.
- **A replica with an AOF** rewrites it after a full sync, so restarting it
  alone recovers the replicated data.

### Consequences and limits
- A very large dataset under heavy writes can overflow the queue during the
  snapshot transfer and loop through full syncs; raise `-repl-queue-size`.
- No chained replication (a replica refuses to serve replicas), no heartbeats
  (a silently dead peer is noticed on the next write or by TCP), no `WAIT`,
  no authentication.
- Pub/Sub is not replicated.
- Primary and replica must run the same version of the snapshot format.
