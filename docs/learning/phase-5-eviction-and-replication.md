# Phase 5 - Eviction and replication

## What was built

- **`-maxmemory` / `-maxmemory-policy`**: `noeviction`, `allkeys-lru`,
  `allkeys-random`, `volatile-ttl`. Over the limit, writes either evict keys or
  fail with the Redis `OOM` error.
- **Replication**: `REPLICAOF host port` and `REPLICAOF NO ONE`. Full sync with a
  consistent snapshot, partial sync from a backlog, read-only replicas, expiry
  sent as `DEL`, slow replicas dropped. A **simplified private protocol, not
  compatible with real Redis** (decision 008).

## Data flow, with one example

A replica connects to a primary and syncs; then a client runs `SET a 1` on the
primary.

1. The replica's goroutine dials the primary and sends
   `MRSYNC ? -1` ("I know no history").
2. The primary's connection goroutine sees `MRSYNC`, and hands the socket to
   `Master.ServeReplica`. That asks the engine (via `Exec`) to attach the replica:
   inside one engine step it copies the keyspace and registers the replica's
   queue.
3. The primary sends `+FULLRESYNC <id> <offset>` and the snapshot. The replica
   loads it and records the offset.
4. A client runs `SET a 1` on the primary. After the engine runs it,
   `Master.Propagate` appends `*3 $3 SET $1 a $1 1` to the backlog and puts the
   same bytes in the replica's queue (a non-blocking send).
5. The replica's connection goroutine writes it to the socket. The replica reads
   the command, applies it inside its engine, and adds the command's byte length
   to its offset. Both offsets are now equal.
6. If the link breaks and later returns, the replica sends
   `MRSYNC <id> <offset>`; if those bytes are still in the primary's ring buffer
   it gets only what it missed.

## Go concepts used

- **Ring buffer** - a fixed array with absolute offsets (`repl/ring.go`);
  property-tested against a naive slice.
- **`context.AfterFunc`** - closes the replica's socket when its context is
  cancelled, which unblocks the read (`Replica.syncOnce`).
- **`context.WithoutCancel`** - the replica's cleanup (`detach`) must still run
  when its own context is already cancelled.
- **`io.CopyN` into a `bytes.Buffer`** - reads the snapshot; memory grows with
  the bytes that arrive, not with the announced length.
- **`atomic.Pointer` and `atomic.Int64`** - the replica goroutine writes its
  offset and link state; INFO (another goroutine) reads them.
- **Interfaces to cut dependencies** - `repl.Applier` is what the replica needs
  from the node; `server.ReplicaServer` is what the server needs from
  replication. `app` supplies both.
- **Dial function injection** - tests pass a `DialFunc` that records and cuts
  connections, and can hold reconnection until the test says so.
- **Goroutine ownership** - the replica goroutine belongs to the `Manager`'s
  wait group; the previous follower is waited for by the next one's goroutine,
  never by the engine goroutine (which would deadlock).

## Interview questions

1. **How do you get a consistent snapshot without stopping the world?**
   Attaching a replica runs inside the engine: it copies the data and registers
   the replica's queue in one step. Anything after that goes to the queue, so the
   snapshot and the stream meet exactly. The copy is a short pause; encoding and
   sending happen outside the engine.

2. **How does a partial resync work, and when does it fall back to a full one?**
   The primary keeps the last N bytes of the command stream. The replica
   reports `(replid, offset)`. If the id matches and the offset is inside the
   window, the primary sends the bytes from there. A different id (the primary
   changed) or an offset that fell out of the window means a full sync.

3. **Why does the replica never expire keys itself?**
   Clocks differ between machines; if replicas expired by their own clock they
   could disagree with the primary. The primary sends an explicit `DEL`. The
   replica only hides keys that are past their time. Tested with two different
   fake clocks.

4. **What stops a slow replica from stalling the primary?**
   A bounded queue per replica and a non-blocking send. A full queue means the
   replica is dropped and must resync. The engine never waits.

5. **How do you stop a replica applying a late command after `REPLICAOF NO ONE`?**
   Its apply step runs inside the engine and first checks its own context. The
   command that promotes the node cancels that context on the same engine
   goroutine, so any later apply sees it cancelled.

6. **Why isn't this compatible with real Redis, and would you change that?**
   Redis's PSYNC and RDB formats are large; I built a smaller protocol that
   demonstrates the same ideas (ids, offsets, backlog, snapshot plus stream). It
   is clearly documented as private. Compatibility would mean implementing RDB
   encoding and the PSYNC handshake.

7. **How does approximate LRU work and why not an exact one?**
   Each key remembers when it was last used; to evict, sample 5 random keys and
   remove the oldest. An exact LRU needs a linked list updated on every read,
   which costs memory and time. Sampling gives nearly the same result.

8. **Why check memory before a write instead of after?**
   Refusing is cheap and the failing command has done nothing yet. The cost is
   that memory can exceed the limit by one command. Commands that free memory
   (`DEL`) are never refused.
