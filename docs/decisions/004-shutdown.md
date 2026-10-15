# 004 - Graceful shutdown that works on Windows and Linux

Status: accepted (Phase 1)

## Context

On Ctrl+C or SIGTERM we must stop accepting, let commands that are already
running finish, close everything, and exit 0. It must behave the same on
Windows (development) and Debian (testing). No fork, no POSIX-only calls.

## Options considered

1. **Close every connection immediately.** Simple; kills in-flight commands
   and loses replies.
2. **Set a shared "stopping" flag and let each goroutine notice it.** A
   goroutine blocked in `Read` never notices.
3. **Per-connection `stop()`**: set a flag under a per-connection mutex and
   call `SetReadDeadline(time.Now())`. A blocked `Read` returns at once; a
   goroutine in the middle of a command is not interrupted and replies before
   it next reads. Then wait with a grace period and force-close stragglers.

## Decision

Option 3 (`server.shutdown`, `connection.stop`, `connection.Read`).

Order of events when the context is cancelled:

1. `context.AfterFunc` closes the listener, so `Accept` returns.
2. `stopping = true`; every connection gets `stop()`.
3. Commands already in the reader's buffer are still executed and answered;
   the next `Read` flushes the replies and then fails with `errServerStopping`.
4. `wg.Wait` in a helper goroutine versus a `ShutdownGrace` timer (default 5 s).
   If the timer wins, all connections are closed forcibly.
5. `Serve` returns only after every connection goroutine has exited.

The idle-timeout deadline and the stop deadline are both set while holding the
connection mutex, so an idle deadline can never overwrite "return now".

## Consequences

- A half-received command is dropped at shutdown (the peer sees EOF). Only
  commands fully received are guaranteed to finish.
- A handler stuck inside `Execute` cannot be interrupted, so `Serve` would
  wait for it. Phase 1 handlers never block; Phase 2's engine will bound this.
- A client that never reads a huge reply blocks its goroutine in `write`.
  The forced close after the grace period fixes that
  (`TestShutdownForcesStuckConnectionsAfterGrace`).
- Tested by `TestGracefulShutdownLeavesNoGoroutines`,
  `TestShutdownLetsInflightCommandFinish`, and `TestRunServesAndShutsDownOnCancel`.
  Actual Ctrl+C/SIGTERM delivery is not covered by an automated test; it is
  a manual check in the Debian steps.
