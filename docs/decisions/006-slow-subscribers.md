# 006 - What happens to a slow subscriber

Status: accepted (Phase 3)

## Context

`PUBLISH` hands a message to every subscriber of a channel. A subscriber is a
network connection and some of them are slow or stuck. Whatever we do, three
things must stay true:

1. a publisher (and the engine goroutine that runs `PUBLISH`) never waits for
   a subscriber;
2. one slow subscriber must not delay the others;
3. memory per subscriber must be bounded.

## Options considered

1. **Block the publisher until the subscriber takes the message.** Violates (1)
   and (2); a stuck client would freeze the whole engine.
2. **Unbounded queue per subscriber.** Violates (3); a client that never reads
   would eat all memory.
3. **Bounded queue, drop the new message when full.** Satisfies 1 to 3 but
   silently loses messages: the subscriber cannot tell and its view of the
   stream is wrong.
4. **Bounded queue, disconnect the subscriber when it overflows.** Satisfies 1
   to 3 and the loss is visible: the client sees its connection close and can
   reconnect and resynchronise. This is what Redis does with its
   `client-output-buffer-limit pubsub` setting.

## Decision

Option 4 (`internal/pubsub`). Each connection that subscribes gets a
`Subscriber` with a queue of `-subscriber-queue` messages (default 256).
`Hub.Publish` tries a non-blocking send to each subscriber. If the queue is
full, the subscriber is removed from all its channels, marked dropped, and its
overflow callback runs; the server's callback force-closes the connection.

The messages already queued are still delivered if the socket can take them,
but nothing more is added.

## Consequences

- `Publish` is O(subscribers of the channel) and never blocks. Test:
  `TestSlowSubscriberIsDroppedWithoutBlocking`.
- End to end, a client that subscribes and never reads while 300 messages of
  256 KiB are published is disconnected, and a second, healthy subscriber
  receives all 300: `TestSlowSubscriberIsDisconnected`.
- A subscriber that is merely bursty with a small queue may be dropped
  unnecessarily. The queue size is a flag for that reason.
- The overflow callback runs on the publishing goroutine (the engine). It only
  closes a socket, which does not block.
- Pub/sub messages are not persisted and not replicated (a limitation, listed
  in `known-limitations.md`).
- While a client is subscribed it may only send SUBSCRIBE, UNSUBSCRIBE, PING
  and QUIT. Its connection has a second goroutine, the pusher, which owns
  pushing messages; both goroutines write through a per-connection mutex.
