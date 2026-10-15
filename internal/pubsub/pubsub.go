// Package pubsub routes published messages to subscribers by exact channel
// name.
//
// Concurrency and the slow-subscriber policy (decision 006): a Hub is safe for
// concurrent use. Publish never blocks. Every Subscriber owns a bounded queue;
// Publish tries to put the message in each subscriber's queue without waiting,
// and a subscriber whose queue is full is DISCONNECTED: it is removed from
// every channel and its overflow callback runs (the server closes the
// connection). So one slow reader can neither delay publishers nor starve other
// subscribers, and memory per subscriber is bounded.
package pubsub

import (
	"slices"
	"sync"
	"sync/atomic"
)

// Message is one published message as delivered to a subscriber.
type Message struct {
	Channel string
	Payload string
}

// Subscriber receives messages for the channels it is subscribed to. Create
// it with NewSubscriber and register channels through a Hub.
type Subscriber struct {
	queue      chan Message
	onOverflow func()
	dropped    atomic.Bool

	channels map[string]struct{} // guarded by the Hub's lock
}

// NewSubscriber returns a Subscriber whose queue holds up to queueSize
// messages. onOverflow, if not nil, is called (once, from the publishing
// goroutine) when the queue overflows and the subscriber is dropped; it must
// not block.
func NewSubscriber(queueSize int, onOverflow func()) *Subscriber {
	return &Subscriber{
		queue:      make(chan Message, max(queueSize, 1)),
		onOverflow: onOverflow,
		channels:   make(map[string]struct{}),
	}
}

// Messages returns the channel on which published messages arrive.
func (s *Subscriber) Messages() <-chan Message { return s.queue }

// Dropped reports whether the subscriber was disconnected for being too slow.
func (s *Subscriber) Dropped() bool { return s.dropped.Load() }

// Hub holds the channel -> subscribers map.
type Hub struct {
	mu       sync.RWMutex
	channels map[string]map[*Subscriber]struct{}
}

// NewHub returns an empty Hub.
func NewHub() *Hub {
	return &Hub{channels: make(map[string]map[*Subscriber]struct{})}
}

// Subscribe adds s to channel and returns how many channels s is now on.
func (h *Hub) Subscribe(s *Subscriber, channel string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := h.channels[channel]
	if set == nil {
		set = make(map[*Subscriber]struct{})
		h.channels[channel] = set
	}
	set[s] = struct{}{}
	s.channels[channel] = struct{}{}
	return len(s.channels)
}

// Unsubscribe removes s from channel and returns how many channels s is
// still on.
func (h *Hub) Unsubscribe(s *Subscriber, channel string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(s, channel)
	return len(s.channels)
}

func (h *Hub) removeLocked(s *Subscriber, channel string) {
	if set := h.channels[channel]; set != nil {
		delete(set, s)
		if len(set) == 0 {
			delete(h.channels, channel)
		}
	}
	delete(s.channels, channel)
}

// UnsubscribeAll removes s from every channel and returns the channel names
// it was on, sorted.
func (h *Hub) UnsubscribeAll(s *Subscriber) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := h.namesLocked(s)
	for _, c := range names {
		h.removeLocked(s, c)
	}
	return names
}

// Channels returns the channels s is subscribed to, sorted.
func (h *Hub) Channels(s *Subscriber) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.namesLocked(s)
}

func (h *Hub) namesLocked(s *Subscriber) []string {
	names := make([]string, 0, len(s.channels))
	for c := range s.channels {
		names = append(names, c)
	}
	slices.Sort(names)
	return names
}

// Publish delivers payload to every subscriber of channel and returns how
// many received it. It never blocks: a subscriber whose queue is full is
// dropped (see the package comment).
func (h *Hub) Publish(channel, payload string) int {
	msg := Message{Channel: channel, Payload: payload}
	delivered := 0
	var overflowed []*Subscriber

	h.mu.RLock()
	for s := range h.channels[channel] {
		select {
		case s.queue <- msg:
			delivered++
		default:
			overflowed = append(overflowed, s)
		}
	}
	h.mu.RUnlock()

	// Dropping needs the write lock, so it happens after the read lock is
	// released.
	for _, s := range overflowed {
		if s.dropped.CompareAndSwap(false, true) {
			h.UnsubscribeAll(s)
			if s.onOverflow != nil {
				s.onOverflow()
			}
		}
	}
	return delivered
}

// NumSubscribers returns how many subscribers are on channel.
func (h *Hub) NumSubscribers(channel string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.channels[channel])
}
