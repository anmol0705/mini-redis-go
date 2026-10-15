# Architecture

Status: end of Phase 3. This file grows each phase.

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
