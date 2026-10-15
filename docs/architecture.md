# Architecture

Status: end of Phase 1. This file grows each phase.

## Package map

```
cmd/miniredis        flags, signals, wiring (run(ctx, args, stderr))
internal/server      TCP listener, per-connection goroutines, session, shutdown
internal/command     command table, arity checks, handlers, INFO
internal/store       the keyspace (map + RWMutex in Phase 1)
internal/resp        RESP reader, writer and Value (reply as data)
```

Dependencies point downwards: `cmd -> server -> command -> store, resp`.
`server` also uses `resp`. `command` does not import `server`; it declares the
`InfoSource` interface that the server satisfies.

## Request lifecycle

1. `Server.Serve` calls `Accept`; `admit` checks `MaxClients` and starts
   `serveConn` in a new goroutine.
2. `serveConn` loops: `resp.ReadCommand` -> `session.execute` ->
   `Handler.Execute` (the `command.Dispatcher`) -> `Writer.WriteValue`.
3. Replies are flushed just before the reader would block (decision 001).
4. The loop ends on EOF, a protocol error (reply then close), `QUIT`, idle
   timeout, a panic (recovered), or shutdown.

## Concurrency model (Phase 1)

- One accept loop (the goroutine that called `Serve`).
- One goroutine per client. It exclusively owns that client's reader, writer
  and session, so those need no locking.
- Shared state: the `Store` (an `RWMutex` inside) and the server's connection
  set (`Server.mu`). The lock order is `Server.mu` then `connection.mu`; the
  reverse never happens.
- Shutdown: see decision 004. Every goroutine has an owner (`Server.wg`) and
  an exit path (the stop deadline, or the forced close).
- Phase 2 replaces the store lock with a single owner goroutine.
