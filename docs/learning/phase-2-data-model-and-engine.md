# Phase 2 - Data model and the single-owner engine

## What was built

- **Five value types** in `internal/store`: strings, lists, hashes, sets and
  sorted sets, each with the exact Redis error texts.
- **A skip list** (`internal/skiplist`) that I wrote myself: it keeps sorted-set
  members in score order and answers "what is the rank of X" and "give me
  items 10 to 20" in O(log n).
- **Expiry**: keys can have a deadline. They are removed when touched (lazy)
  and also by a background sampler that checks a few random expiring keys
  many times per second.
- **Glob matching** (`internal/glob`) for `KEYS`.
- **The engine** (`internal/engine`): one goroutine owns all the data. Client
  goroutines send it requests through a channel.
- **Memory estimate and eviction hooks** (used fully in Phase 5).

## Data flow, with one example

The client sends `ZADD board 5 alice`, then `ZRANK board alice`.

1. The connection goroutine parses the command and calls
   `engine.Execute(ctx, args)`.
2. `Execute` takes a request object from a pool, puts it on the engine's
   channel, and waits on the request's private reply channel.
3. The engine goroutine receives it, sets "now" from the clock, looks the
   command up (`command.Resolve`), checks policy (read-only? out of memory?),
   and runs `cmdZAdd`, which calls `store.ZAdd`.
4. `ZAdd` puts `alice -> 5` in a map (fast lookup by name) **and** in the skip
   list (ordered by score). Both are updated together.
5. The reply (`:1`) goes into the request's reply channel, and the connection
   goroutine writes it to the socket.
6. `ZRANK` asks the map for alice's score, then the skip list for her
   position by adding up "span" numbers along the search path.

## Go concepts used

- **Channels as a queue** - `Engine.reqs` is a buffered channel (`engine.go`,
  `submit`). When it is full, senders wait: this is back-pressure.
- **`select` with several cases** - the engine loop waits for a request, a
  timer tick, or a stop signal (`Engine.run`).
- **`sync.Pool`** - recycles request objects so each command does not allocate
  one (`Engine.pool`).
- **`sync.RWMutex` used as a gate** - `submit` takes the read side, `Close`
  takes the write side, so nobody can queue work after shutdown (`Engine.mu`).
- **Closures** - the function passed to `Engine.Exec` runs on the engine
  goroutine and can use local variables of the caller.
- **`recover`** - the engine recovers from a panic in a command so one bug
  cannot kill the server (`Engine.handle`).
- **Generics-free containers** - the ring-buffer deque (`store/lists.go`) uses
  a power-of-two size so wrap-around is a bit mask.
- **Dependency inversion** - `engine.Propagator`, `command.InfoSource`,
  `command.Hooks` are interfaces declared by the code that uses them.
- **Property tests** - random operations compared against a trivial slice
  (`skiplist_test.go`), and invariants checked after every step
  (`store_test.go`).
- **Injected time and randomness** - the clock is a function and the random
  generator is a parameter, so tests are deterministic.

## Interview questions

1. **Why one goroutine instead of locks?**
   Commands become atomic for free, transactions are just a batch, the order
   changes are applied equals the order they are logged, and there are no data
   races. The measured cost was about 0.4 us per command at handler level and
   nothing visible over TCP (decision 005).

2. **How can the engine never block on a slow client?**
   It only produces a `resp.Value` and drops it in a channel that has room for
   exactly that one reply. The connection goroutine does the socket write. A
   slow client only slows its own goroutine.

3. **What if a client disconnects while its command is queued?**
   The command still runs, the reply lands in the buffered reply channel, the
   goroutine receives it, notices the dead socket on write and exits. Nothing
   leaks and the engine is never involved in the disconnect.

4. **Why a skip list and not a balanced tree?**
   It is far simpler to get right, supports rank queries with "span" counters,
   and gives O(log n) on average. Redis uses the same structure. The trade-off
   is randomised rather than guaranteed depth.

5. **How does a rank query work in O(log n)?**
   Every forward pointer remembers how many bottom-level nodes it skips. While
   searching from the top level down, adding those spans gives the position.

6. **How does expiry work, and why both lazy and active?**
   Lazy: a key past its deadline is deleted when accessed. That alone would
   leave never-read keys forever, so the sampler picks 20 random expiring keys
   per round, deletes the dead ones, and repeats at once if more than 25% were
   dead, within a time budget. This is Redis's approach and keeps the cost
   bounded.

7. **Why does `EXPIRE` get logged as `PEXPIREAT`?**
   Replaying `EXPIRE k 10` an hour later would give the key a fresh 10 seconds.
   Logging the absolute time makes replay give the same result.

8. **What does `EXEC` do inside the engine?**
   (Phase 3 uses it.) A batch of commands is a single request, so nothing else
   can run in between.
