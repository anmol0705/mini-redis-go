# 002 - Session layer, dispatcher and how INFO gets its data

Status: accepted (Phase 1)

## Context

Most commands are functions of the data: `GET key` needs only the store.
Some commands are about the *connection*:

- `QUIT` closes this connection.
- Later: `MULTI/EXEC/DISCARD` need a per-connection queue; `SUBSCRIBE` needs a
  per-connection subscription list and changes which commands are allowed.

A pure `Execute(store, args)` function has nowhere to keep that state.

## Options considered

1. Put connection state in the command package and pass a `*Conn` to every
   handler. Mixes network concerns into data commands and makes the command
   package hard to test alone.
2. Special-case connection commands inline in the server read loop. Works
   for QUIT, but becomes an `if` ladder once MULTI and SUBSCRIBE arrive.
3. A **session** object per connection, owned by that connection's goroutine
   (so it needs no lock). It intercepts connection-level commands and passes
   everything else to a `Handler`.

## Decision

Option 3 (`internal/server/session.go`). `Handler` is a one-method interface
(`Execute(args) resp.Value`) declared in `server`, where it is used. The
production implementation is `command.Dispatcher`; tests inject their own,
for example one that panics, to prove panic recovery without adding a
`PANIC` command to the product.

Related choices:

- **`Dispatcher` object instead of `Execute(store, args)`.** The original
  free function could not reach live server numbers. A `Dispatcher` holds the
  store plus an `InfoSource`. This changes the old API; its only callers were
  tests, which were rewritten. (The brief described an existing
  `internal/command`; the directory actually held only a `.gitkeep`, so this
  was a first implementation, not a refactor.)
- **INFO data flows through an interface.** Uptime, client count and port
  belong to the server. `command` declares `InfoSource` (`Info() Info`); the
  `*server.Server` implements it. `command` never imports `server`.
- **`Listen` and `Serve` are separate.** `server.Listen` binds the socket and
  returns the `Server`; `main` then builds the dispatcher with the server as
  its `InfoSource` and calls `Serve`. That breaks the construction cycle
  without a setter, and gives callers the real port when they ask for `:0`.

## Consequences

- Phase 3 adds fields to `session` (transaction queue, subscriptions) without
  touching the command table.
- `QUIT` is not in the command table, so it is invisible to `COMMAND`. That is
  harmless because `COMMAND` returns an empty array.
- `server` imports `command` for the `Info` type. Accepted: a three-field
  struct is not worth a shared package.
