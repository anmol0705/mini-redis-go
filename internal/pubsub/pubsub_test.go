package pubsub

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func drain(s *Subscriber) []Message {
	var out []Message
	for {
		select {
		case m := <-s.Messages():
			out = append(out, m)
		default:
			return out
		}
	}
}

func TestSubscribePublish(t *testing.T) {
	h := NewHub()
	a, b := NewSubscriber(8, nil), NewSubscriber(8, nil)
	if n := h.Subscribe(a, "news"); n != 1 {
		t.Errorf("count = %d", n)
	}
	h.Subscribe(a, "sport")
	if n := h.Subscribe(b, "news"); n != 1 {
		t.Errorf("count = %d", n)
	}
	if n := h.Subscribe(a, "news"); n != 2 { // subscribing twice changes nothing
		t.Errorf("duplicate subscribe count = %d; want 2", n)
	}

	if got := h.Publish("news", "hello"); got != 2 {
		t.Errorf("Publish(news) reached %d; want 2", got)
	}
	if got := h.Publish("sport", "goal"); got != 1 {
		t.Errorf("Publish(sport) reached %d; want 1", got)
	}
	if got := h.Publish("nobody", "x"); got != 0 {
		t.Errorf("Publish(nobody) reached %d", got)
	}

	if got := drain(a); !slices.Equal(got, []Message{{"news", "hello"}, {"sport", "goal"}}) {
		t.Errorf("a received %v", got)
	}
	if got := drain(b); !slices.Equal(got, []Message{{"news", "hello"}}) {
		t.Errorf("b received %v", got)
	}
}

func TestExactChannelNamesOnly(t *testing.T) {
	h := NewHub()
	s := NewSubscriber(8, nil)
	h.Subscribe(s, "news.*")
	h.Subscribe(s, "News")
	if got := h.Publish("news.sport", "x"); got != 0 {
		t.Errorf("pattern-like name matched %d subscribers", got)
	}
	if got := h.Publish("news", "x"); got != 0 {
		t.Errorf("prefix matched %d subscribers", got)
	}
}

func TestUnsubscribe(t *testing.T) {
	h := NewHub()
	s := NewSubscriber(8, nil)
	h.Subscribe(s, "a")
	h.Subscribe(s, "b")
	h.Subscribe(s, "c")

	if n := h.Unsubscribe(s, "a"); n != 2 {
		t.Errorf("count after Unsubscribe = %d", n)
	}
	if n := h.Unsubscribe(s, "never"); n != 2 {
		t.Errorf("Unsubscribe of unknown channel changed count to %d", n)
	}
	if h.Publish("a", "x") != 0 {
		t.Error("message delivered after Unsubscribe")
	}
	if got := h.Channels(s); !slices.Equal(got, []string{"b", "c"}) {
		t.Errorf("Channels = %v", got)
	}
	if got := h.UnsubscribeAll(s); !slices.Equal(got, []string{"b", "c"}) {
		t.Errorf("UnsubscribeAll = %v", got)
	}
	if h.NumSubscribers("b") != 0 {
		t.Error("channel entry not cleaned up")
	}
	if got := h.UnsubscribeAll(s); len(got) != 0 {
		t.Errorf("second UnsubscribeAll = %v", got)
	}
}

// The core promise: a subscriber that never reads can neither block a
// publisher nor hurt other subscribers.
func TestSlowSubscriberIsDroppedWithoutBlocking(t *testing.T) {
	h := NewHub()
	overflows := make(chan struct{}, 4)
	slow := NewSubscriber(3, func() { overflows <- struct{}{} })
	fast := NewSubscriber(1000, nil)
	h.Subscribe(slow, "ch")
	h.Subscribe(fast, "ch")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100 {
			h.Publish("ch", fmt.Sprint(i))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Publish blocked on a slow subscriber")
	}

	if !slow.Dropped() {
		t.Error("slow subscriber was not dropped")
	}
	if len(overflows) != 1 {
		t.Errorf("overflow callback ran %d times; want exactly once", len(overflows))
	}
	if h.NumSubscribers("ch") != 1 {
		t.Errorf("%d subscribers left; want only the fast one", h.NumSubscribers("ch"))
	}
	if got := len(drain(fast)); got != 100 {
		t.Errorf("fast subscriber got %d messages; want all 100", got)
	}
	// The dropped subscriber keeps what fit in its queue, and nothing more.
	if got := len(drain(slow)); got != 3 {
		t.Errorf("slow subscriber has %d queued messages; want 3", got)
	}
}

func TestPublishReturnsOnlySuccessfulDeliveries(t *testing.T) {
	h := NewHub()
	s := NewSubscriber(1, nil)
	h.Subscribe(s, "ch")
	if got := h.Publish("ch", "1"); got != 1 {
		t.Errorf("first = %d", got)
	}
	if got := h.Publish("ch", "2"); got != 0 { // queue full: dropped, not delivered
		t.Errorf("overflowing publish reported %d deliveries", got)
	}
}

func TestQueueSizeIsAtLeastOne(t *testing.T) {
	s := NewSubscriber(0, nil)
	h := NewHub()
	h.Subscribe(s, "c")
	if h.Publish("c", "x") != 1 {
		t.Error("a zero queue size must still hold one message")
	}
}

// Run with -race: concurrent subscribe, publish and unsubscribe.
func TestConcurrentUse(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			s := NewSubscriber(16, nil)
			ch := fmt.Sprint("c", g%3)
			for i := range 300 {
				h.Subscribe(s, ch)
				h.Publish(ch, fmt.Sprint(i))
				drain(s)
				if i%7 == 0 {
					h.Unsubscribe(s, ch)
				}
			}
			h.UnsubscribeAll(s)
		})
	}
	wg.Wait()
}
