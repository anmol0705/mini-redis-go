# Phase 1 - A runnable server

## What was built

A TCP server that speaks the Redis protocol (RESP). You can point `redis-cli`
at it and run `PING`, `SET`, `GET`, `DEL`, `EXISTS`, `DBSIZE`, `FLUSHALL`,
`INFO` and a few housekeeping commands.

| Piece | Folder | Job |
|---|---|---|
| Protocol | `internal/resp` | Turn bytes into a command (`[]string`) and replies into bytes. |
| Data | `internal/store` | A map from key to value, safe to share between goroutines. |
| Commands | `internal/command` | Look up the command name, check argument count, run it. |
| Network | `internal/server` | Accept connections, one goroutine each, shutdown. |
| Program | `cmd/miniredis` | Read flags, wire the pieces together, handle Ctrl+C. |

## Data flow, with one example

The client types `SET name Anmol`. `redis-cli` sends:

```
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nAnmol\r\n
```

`*3` = "an array of 3 items", `$3` = "the next item is 3 bytes long".

1. `serveConn` (one goroutine for this client) calls `resp.ReadCommand`,
   which returns `["SET", "name", "Anmol"]`.
2. `session.execute` checks for connection-level commands (`QUIT`). Not one,
   so it calls the `Handler`.
3. `Dispatcher.Execute` upper-cases `SET`, finds it in the table, checks the
   argument count, and calls `cmdSet`, which calls `store.Set`.
4. The reply `resp.SimpleString("OK")` goes into the connection's buffered
   writer as `+OK\r\n`.
5. The next time the buffered reader has to wait for the network, it first
   flushes the writer. The client gets `+OK`.

If the client had sent 100 commands in one packet, steps 1-4 repeat 100 times
and step 5 happens once.

## Go concepts used

- **Goroutine** - a very cheap thread managed by Go. `Server.admit` starts one
  per client with `s.wg.Go(...)`. Each goroutine owns its connection, so
  nothing about the connection needs a lock.
- **sync.Mutex / RWMutex** - lets only one goroutine change shared data at a
  time. `store.Store` uses an `RWMutex`: many readers or one writer.
  `Server.mu` guards the connection set.
- **sync.WaitGroup** - counts running goroutines so you can wait for all of
  them. `Server.wg`; `shutdown` waits on it. `wg.Go` is the Go 1.25 helper.
- **context.Context** - a cancellation signal passed down the call chain.
  `Serve(ctx, ...)` stops when `ctx` is cancelled; `main` creates it from
  Ctrl+C/SIGTERM with `signal.NotifyContext`. `context.AfterFunc` runs a
  function on cancellation (closes the listener).
- **Channels and select** - `shutdown` uses a `done` channel and `select` to
  wait for "all connections finished" or "grace period over", whichever
  first. Tests use channels to wait for events instead of sleeping.
- **Interfaces** - a set of methods a type must have. `server.Handler`
  (declared where it is used) lets tests plug in a fake. `command.InfoSource`
  lets INFO read server numbers without importing the server.
  `connection` implements `io.Reader` so `bufio.Reader` can wrap it.
- **Embedding** - `connection` embeds `net.Conn`, so it has all its methods
  and overrides only `Read`.
- **Errors** - `fmt.Errorf("...%w", err)` wraps; `errors.Is(err, target)`
  checks the chain. `resp.ErrProtocol` is checked this way in `readFailed`.
- **defer / recover** - `defer` runs a function when the surrounding function
  returns; `recover` inside a deferred function turns a panic into a value
  (`serveConn`), so one bad client cannot crash the server.
- **Table-driven tests** - a slice of cases run in a loop with `t.Run`
  (`command_test.go`).
- **Race detector** - `go test -race` reports unsynchronised access to shared
  memory.

## Interview questions

1. **Why does the server use one goroutine per connection?**
   Goroutines are cheap (a few KB), so a goroutine that blocks in `Read` costs
   almost nothing. It keeps the code straight-line: read, execute, reply. The
   alternative, an event loop (what Redis does), needs state machines but
   avoids per-connection stacks. In Phase 2 I still keep one goroutine per
   connection for I/O but move all data access to one owner goroutine.

2. **How do you answer pipelined commands efficiently?**
   Replies go into a buffered writer. A small wrapper around the socket
   flushes the buffer just before the buffered reader would block on the
   network. N commands that arrived together produce one `write`.
   (`connection.Read`, decision 001.)

3. **Why not "flush when the reader has nothing buffered"?**
   If a packet holds one whole command plus half of the next, data is still
   buffered so you do not flush, then block waiting for the rest of the
   second command while the client waits for the first reply: a deadlock.
   `TestReplyNotHeldBackByPartialNextCommand` guards against it.

4. **What stops a malicious client from exhausting memory?**
   Lines are capped at 64 KiB, big values are read in chunks so memory follows
   received bytes, argument slices start small, connection count is capped,
   and error replies never echo more than 64 bytes of client text.
   (Decision 003.)

5. **How does graceful shutdown work?**
   Cancel the context, which closes the listener. Each connection gets a
   "stop" that sets a read deadline of now, waking blocked reads. Running
   commands finish and reply. We wait up to a grace period, force-close what
   is left, and `Serve` returns after all goroutines exit. (Decision 004.)

6. **What happens if a command handler panics?**
   `serveConn` has a deferred `recover`. It logs the panic with a stack
   trace and closes only that connection; the server keeps running. Tested
   with an injected handler that panics.

7. **Why is the store protected by an RWMutex, and why is that temporary?**
   Many goroutines share the map, and a Go map is not safe for concurrent
   writes. `RWMutex` is the simplest correct fix. Phase 2 replaces it with a
   single owner goroutine after benchmarking both.

8. **Why does the command package take an `InfoSource` interface?**
   INFO needs the connection count, port and uptime, which only the server
   knows. Declaring a small interface in `command` keeps the dependency
   pointing one way and lets tests supply fixed numbers.
