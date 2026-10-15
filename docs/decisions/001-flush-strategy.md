# 001 - When to flush replies

Status: accepted (Phase 1)

## Context

Each connection has a buffered `resp.Writer`. A client may send one command
and wait, or send hundreds at once (pipelining). We want:

- a lone command answered immediately;
- N pipelined commands answered with as few `write` system calls as possible.

## Options considered

1. **Flush after every command.** Simple, correct, but N pipelined commands
   cost N syscalls. That throws away the main benefit of pipelining.
2. **Flush when `bufio.Reader.Buffered() == 0`** (no unread input left). This
   is the usual first idea. It has a trap: if one TCP segment holds a complete
   command plus the *start* of the next, `Buffered() > 0`, so we skip the
   flush, then block reading the rest of the second command. The client is
   waiting for the first reply, so both sides wait for each other.
3. **Flush right before the reader would block.** `bufio.Reader` only calls
   `Read` on the socket when its buffer has no more data. Wrap the socket in
   a tiny `io.Reader` (`connection.Read`) that flushes the writer and then
   reads. Every reply for input we already consumed has been queued by then.

## Decision

Option 3 (`internal/server/conn.go`, `connection.Read`).

## Consequences

- Pipelined commands are answered in one write; a lone command is answered
  immediately; the deadlock in option 2 cannot happen.
  `TestReplyNotHeldBackByPartialNextCommand` is the regression test.
- Replies are also flushed when the reader needs more bytes in the middle of
  a large command. That is correct (earlier commands deserve their replies)
  and costs at most one extra write.
- A flush error surfaces as a read error, which closes the connection.
- A client that never reads can block the flush; shutdown handles it with the
  grace period (see 004). No write timeout exists yet (known limitation).
- Not measured yet: the syscall saving is expected, not benchmarked.
  TBD - measure on Debian with `redis-benchmark -P 16`.
