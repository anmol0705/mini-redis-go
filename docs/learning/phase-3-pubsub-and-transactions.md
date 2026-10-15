# Phase 3 - Pub/Sub and transactions

## What was built

- **Pub/Sub**: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` with exact channel
  names. Pushed messages look like Redis's: `["message", channel, payload]`.
  While subscribed, a client may only send `SUBSCRIBE`, `UNSUBSCRIBE`, `PING`
  and `QUIT`.
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`. Commands between `MULTI` and
  `EXEC` are queued per connection, then run as one unit inside the engine.
- A **bounded queue per subscriber**: a subscriber that falls too far behind
  is disconnected (decision 006).

## Data flow, with examples

**Pub/Sub.** Client A sends `SUBSCRIBE news`; client B sends `PUBLISH news hi`.

1. A's session creates a `Subscriber` (a channel with room for 256 messages)
   and registers it in the `Hub` under `news`. The server starts a small
   "pusher" goroutine for A's connection.
2. B's `PUBLISH` goes through the engine like any command. `cmdPublish` calls
   `Hub.Publish("news", "hi")`, which tries a non-blocking send into A's queue
   and returns 1.
3. A's pusher goroutine takes the message out of the queue and writes
   `*3 $7 message $4 news $2 hi` to A's socket.

**Transaction.** `MULTI`, `SET a 1`, `INCR a`, `EXEC`.

1. `MULTI` flips a flag in the session. `SET a 1` and `INCR a` are checked
   (known command? right argument count?) and stored in a list; the client gets
   `+QUEUED`.
2. `EXEC` sends the whole list to the engine as ONE request (`ExecuteAtomic`).
   The engine runs both commands back to back; no other client can run in
   between. The replies come back as one array: `[+OK, :2]`.

## Go concepts used

- **Goroutine ownership** - the pusher is started by the connection goroutine
  and stopped by it too (`connection.startPusher`, `stopPusher`), so it cannot
  outlive the connection.
- **Non-blocking channel send** - `select { case ch <- v: default: }` tries to
  send and gives up immediately if full (`Hub.Publish`). This is the core of
  "never block the publisher".
- **`sync.Mutex` for a shared writer** - two goroutines may write to the same
  socket buffer, so `connection.send`/`flush` take a mutex (`wmu`).
- **`sync.WaitGroup.Go`** - starts a goroutine tracked by a wait group; used for
  the pusher so we can wait for it.
- **`atomic.Bool` + `CompareAndSwap`** - makes "drop this subscriber" happen
  exactly once even if two publishers notice the overflow together
  (`Subscriber.dropped`).
- **Defer ordering** - deferred calls run last-in first-out. `serveConn`
  registers `stopPusher` after `sess.close` so that the pusher stops first.
- **State machine in a struct** - `session` has `inTx`, `dirty` and `queued`
  fields and `execute` behaves differently depending on them.
- **Reusing a slice for results** - `session.out` is reused for every command
  to avoid allocating, and the comment says it is valid only until the next call.

## Interview questions

1. **How do you make sure a slow subscriber cannot block publishers?**
   Each subscriber has a bounded queue and `Publish` uses a non-blocking send.
   If the queue is full the subscriber is dropped and its connection closed. So
   publish cost does not depend on the speed of any client, and memory per
   client is bounded. (Decision 006.)

2. **Why disconnect instead of dropping the message?**
   Silent loss leaves the client with a wrong picture and no way to know.
   A closed connection is visible, and the client can reconnect and
   resynchronise. Redis does the same.

3. **How is `EXEC` atomic?**
   The engine runs one request at a time, and `EXEC` is a single request that
   contains all queued commands, so nothing from another client can run in the
   middle. There are no locks involved (decision 005).

4. **What happens when a command inside `EXEC` fails?**
   Nothing is rolled back; the failing command's reply in the result array is
   an error and the others run normally. This matches Redis. A command that is
   *invalid at queue time* (unknown, wrong argument count) poisons the
   transaction and `EXEC` returns `EXECABORT` without running anything.

5. **Why does the pusher goroutine need a mutex when the connection goroutine
   writes too?**
   Both write into the same buffered writer. A `bufio.Writer` is not safe for
   concurrent use, so every write and flush goes through `wmu`.

6. **How do you avoid a goroutine leak when a subscribed client disconnects?**
   `serveConn` defers `stopPusher`: it closes the quit channel, force-closes
   the socket (which unblocks a push stuck in `Write`) and waits on the wait
   group. A test counts goroutines after shutdown with subscribers present.

7. **Why are session-level commands not in the command table?**
   `MULTI`, `SUBSCRIBE` and friends need per-connection state (a queue, a
   subscription), which the table-driven data commands do not have. The session
   intercepts them first (decision 002).

8. **What are the limits you set and why?**
   100,000 queued commands per transaction and 10,000 subscribed channels per
   connection, because both are numbers the client controls.
