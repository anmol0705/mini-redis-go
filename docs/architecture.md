# Architecture

Status: v1.0.

## Package map

```
cmd/miniredis        flags, signals (run(ctx, args, stderr))
internal/app         wires everything; Start/Run; used by main and by integration tests
internal/server      TCP listener, per-connection goroutines, session, shutdown
internal/engine      the single goroutine that owns the keyspace; request queue; expiry tick
internal/command     command table, arity, handlers, propagation rewrites, INFO
internal/store       the keyspace: typed values, expiry, memory estimate, eviction, Dump/Load
internal/skiplist    span-counted skip list behind sorted sets
internal/glob        KEYS pattern matching
internal/resp        RESP reader, writer and Value (reply as data)
internal/testclient  tiny RESP client for tests only
```

Dependencies point downwards: `app -> server, engine -> command -> store -> skiplist, glob`.
`server` and `command` both use `resp`. `server` imports `command` only for the
`Info` type; the engine implements `server.Handler`.

## Request lifecycle

1. `Server.Serve` calls `Accept`; `admit` checks `MaxClients` and starts
   `serveConn` in a new goroutine.
2. `serveConn` loops: `resp.ReadCommand` -> `session.execute` ->
   `Handler.Execute` (the engine) -> `Writer.WriteValue`.
3. `Engine.Execute` queues a request; the engine goroutine runs
   `command.Resolve`, checks read-only/maxmemory policy, runs the handler on
   the `store.DB`, collects the commands to propagate, and replies.
4. Replies are flushed just before the reader would block (decision 001).
5. The loop ends on EOF, a protocol error (reply then close), `QUIT`, idle
   timeout, a panic (recovered) or shutdown.

## Concurrency model

- One accept loop (the goroutine that called `Serve`).
- One goroutine per client. It exclusively owns that client's reader, writer
  and session, so those need no locking.
- **One engine goroutine owns the `store.DB`.** No other goroutine touches it.
  It receives requests on a bounded channel and also runs the expiry sampler
  on a ticker. Replies travel back through a per-request channel of capacity 1,
  so the engine never blocks on a client (decision 005).
- Shared state elsewhere: the server's connection set (`Server.mu`), and
  `Engine.mu` (a gate that stops submissions after `Close`).
- Shutdown order (`app.Node.run`): the server stops first (no new commands),
  then the engine is closed. Every goroutine has an owner and an exit path.

## Phase 3 additions

- `internal/pubsub`: the `Hub` (channel to subscribers map, `RWMutex`) and
  bounded per-connection `Subscriber` queues. `PUBLISH` runs on the engine and
  calls `Hub.Publish` through the `command.Publisher` interface; it never
  blocks.
- `internal/server/session.go`: per-connection state machine. It answers QUIT,
  MULTI/EXEC/DISCARD and SUBSCRIBE/UNSUBSCRIBE itself. `EXEC` calls
  `Handler.ExecuteAtomic`, which the engine runs as one request.
- A subscribed connection has a second goroutine (the pusher) that writes
  messages. It is owned by the connection goroutine; both write through
  `connection.wmu`. Teardown order: pusher stopped, subscriptions removed,
  connection released.

## Phase 4 additions (persistence)

- `internal/aof`: the log. `Propagate` encodes a command into a buffer, `Commit`
  writes it and applies the fsync policy. Under `everysec` one goroutine fsyncs
  once a second; the file handle is shared with the engine, so it is guarded by one
  mutex. Rewrites capture commands, swap the file atomically and reopen it.
- `internal/snapshot`: the binary snapshot format with a CRC32 trailer.
- `internal/persist`: startup loading (AOF wins), `SAVE`, `BGSAVE` (copy inside the
  engine, write in a goroutine), `BGREWRITEAOF`, the snapshot scheduler. Its
  goroutines are owned by one `WaitGroup` and stopped by `Close`.
- The engine calls a `Propagator` after each request: first the AOF, then the
  replication feed (`app.multiPropagator`).

## Phase 5 additions (eviction and replication)

- The engine checks `maxmemory` before commands flagged `DenyOOM` and evicts by
  sampling; evictions and expiries are propagated as `DEL`.
- `internal/repl`: `Master` (engine-owned state: offset, backlog ring buffer,
  replica queues) and `Replica` (its own goroutine: connect, resync, apply the
  stream through `engine.Exec`). `Manager` switches roles on `REPLICAOF`.
- A replica connection is taken over by the server (`MRSYNC` in the session ->
  `Server.serveReplica` -> `Master.ServeReplica`) and ends on disconnect, on being
  dropped for lag, or at shutdown.

## Phase 6 additions

- `internal/app/pprof.go`: optional loopback-only profiler, owned by the node.
- Shutdown order in `app.Node.run`: server (no new commands) -> engine ->
  replication -> persistence (flush, final snapshot, close AOF) -> profiler.

## Goroutine inventory (who starts it, who stops it)

| Goroutine | Started by | Stops when |
|---|---|---|
| accept loop | `Server.Serve` caller | context cancelled |
| per-connection | `Server.admit` | client leaves, error, QUIT, shutdown |
| pusher (subscribed connection) | `connection.startPusher` | `stopPusher` in `serveConn` |
| engine | `engine.New` | `Engine.Close` |
| AOF syncer (`everysec`) | `aof.New` | `Log.Close` |
| BGSAVE / rewrite writers | `persist.Manager` | they finish; `Close` waits |
| snapshot scheduler | `persist.StartScheduler` | `Manager.Close` |
| replica follower | `repl.Manager.ReplicaOf` | context cancelled (`NO ONE`, new `REPLICAOF`, `Close`) |
| replica serving reader | `Master.stream` | connection closes |
| pprof | `startPprof` | `pprofServer.stop` |
