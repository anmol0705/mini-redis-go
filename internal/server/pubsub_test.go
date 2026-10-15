package server

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/pubsub"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/testclient"
)

func TestSubscribeAndPublish(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	sub := testclient.MustDial(t, ts.addr)
	pub := testclient.MustDial(t, ts.addr)

	if err := sub.Send("SUBSCRIBE", "news", "sport"); err != nil {
		t.Fatal(err)
	}
	if got := sub.MustRead(t); got != "*[$subscribe $news :1]" {
		t.Errorf("first confirmation = %q", got)
	}
	if got := sub.MustRead(t); got != "*[$subscribe $sport :2]" {
		t.Errorf("second confirmation = %q", got)
	}

	if got := pub.Cmd(t, "PUBLISH", "news", "hello"); got != ":1" {
		t.Errorf("PUBLISH news = %q; want :1", got)
	}
	if got := pub.Cmd(t, "PUBLISH", "nobody", "x"); got != ":0" {
		t.Errorf("PUBLISH nobody = %q; want :0", got)
	}
	if got := sub.MustRead(t); got != "*[$message $news $hello]" {
		t.Errorf("message = %q", got)
	}

	// Several messages arrive in order.
	for i := range 20 {
		pub.Cmd(t, "PUBLISH", "sport", fmt.Sprint("m", i))
	}
	for i := range 20 {
		if got, want := sub.MustRead(t), fmt.Sprintf("*[$message $sport $m%d]", i); got != want {
			t.Fatalf("message %d = %q; want %q", i, got, want)
		}
	}
}

func TestSubscribedModeRestrictsCommands(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := testclient.MustDial(t, ts.addr)

	if got := c.Cmd(t, "SUBSCRIBE", "a", "b"); got != "*[$subscribe $a :1]" {
		t.Fatalf("SUBSCRIBE = %q", got)
	}
	c.MustRead(t) // confirmation for b

	if got := c.Cmd(t, "GET", "k"); !strings.HasPrefix(got, "-ERR only SUBSCRIBE") {
		t.Errorf("GET while subscribed = %q", got)
	}
	if got := c.Cmd(t, "SET", "k", "v"); !strings.HasPrefix(got, "-ERR only SUBSCRIBE") {
		t.Errorf("SET while subscribed = %q", got)
	}
	if got := c.Cmd(t, "MULTI"); !strings.HasPrefix(got, "-ERR only SUBSCRIBE") {
		t.Errorf("MULTI while subscribed = %q", got)
	}
	if got := c.Cmd(t, "PING"); got != "*[$pong $]" {
		t.Errorf("PING while subscribed = %q", got)
	}
	if got := c.Cmd(t, "PING", "hi"); got != "*[$pong $hi]" {
		t.Errorf("PING hi while subscribed = %q", got)
	}
	if got := c.Cmd(t, "UNSUBSCRIBE", "a"); got != "*[$unsubscribe $a :1]" {
		t.Errorf("UNSUBSCRIBE a = %q", got)
	}
	if got := c.Cmd(t, "GET", "k"); !strings.HasPrefix(got, "-ERR only SUBSCRIBE") {
		t.Errorf("still subscribed to b, but GET gave %q", got)
	}
	if got := c.Cmd(t, "UNSUBSCRIBE"); got != "*[$unsubscribe $b :0]" {
		t.Errorf("UNSUBSCRIBE (all) = %q", got)
	}
	// Back to normal mode.
	if got := c.Cmd(t, "SET", "k", "v"); got != "+OK" {
		t.Errorf("SET after leaving subscribed mode = %q", got)
	}
	if got := c.Cmd(t, "UNSUBSCRIBE"); got != "*[$unsubscribe $nil :0]" {
		t.Errorf("UNSUBSCRIBE with nothing subscribed = %q", got)
	}
}

func TestQuitWhileSubscribed(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := testclient.MustDial(t, ts.addr)
	c.Cmd(t, "SUBSCRIBE", "a")
	if got := c.Cmd(t, "QUIT"); got != "+OK" {
		t.Errorf("QUIT = %q", got)
	}
	c.ExpectClosed(t)
}

func TestSubscribeArityAndNoHub(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := testclient.MustDial(t, ts.addr)
	if got := c.Cmd(t, "SUBSCRIBE"); !strings.Contains(got, "wrong number of arguments") {
		t.Errorf("SUBSCRIBE with no channel = %q", got)
	}

	// A server without a hub refuses politely.
	s := newSession(funcHandler(func([]string) resp.Value { return resp.Null() }), nil, 0, nil, nil)
	replies, _ := s.execute(context.Background(), []string{"SUBSCRIBE", "x"})
	if len(replies) != 1 || replies[0].Kind != resp.KindError {
		t.Errorf("SUBSCRIBE without a hub = %+v", replies)
	}
}

func TestSubscriberCleanupOnDisconnect(t *testing.T) {
	hub := pubsub.NewHub()
	ts := startServer(t, Config{Hub: hub}, nil)
	c, err := testclient.Dial(ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	c.Cmd(t, "SUBSCRIBE", "a", "b")
	if hub.NumSubscribers("a") != 1 {
		t.Fatal("subscription not registered")
	}
	_ = c.Close()
	eventually(t, "subscriptions to be removed", func() bool {
		return hub.NumSubscribers("a") == 0 && hub.NumSubscribers("b") == 0
	})
}

// A subscriber that never reads must be disconnected without hurting
// publishers or other subscribers (decision 006).
func TestSlowSubscriberIsDisconnected(t *testing.T) {
	hub := pubsub.NewHub()
	ts := startServer(t, Config{Hub: hub, SubscriberQueue: 4}, nil)

	slow := testclient.MustDial(t, ts.addr)
	slow.Cmd(t, "SUBSCRIBE", "ch") // and then never read again

	fast := testclient.MustDial(t, ts.addr)
	fast.Cmd(t, "SUBSCRIBE", "ch")
	pub := testclient.MustDial(t, ts.addr)

	const messages = 300
	payload := strings.Repeat("x", 256<<10) // enough to fill the socket buffers

	received := make(chan int, 1)
	go func() {
		n := 0
		for range messages {
			if _, err := fast.Read(); err != nil {
				break
			}
			n++
		}
		received <- n
	}()

	for i := range messages {
		got, err := pub.Do("PUBLISH", "ch", payload)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if got.Kind != ':' {
			t.Fatalf("publish %d = %v", i, got)
		}
	}
	if n := <-received; n != messages {
		t.Errorf("fast subscriber got %d of %d messages", n, messages)
	}
	eventually(t, "the slow subscriber to be dropped", func() bool { return hub.NumSubscribers("ch") == 1 })

	// The slow client's connection is closed: reading drains what was
	// already sent and then fails.
	for {
		if _, err := slow.Read(); err != nil {
			break
		}
	}
}

func TestShutdownWithSubscribersLeavesNoGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	ts := startServer(t, Config{}, nil)
	var clients []*testclient.Client
	for i := range 5 {
		c := testclient.MustDial(t, ts.addr)
		c.Cmd(t, "SUBSCRIBE", fmt.Sprint("ch", i))
		clients = append(clients, c)
	}
	if err := ts.stop(t); err != nil {
		t.Fatal(err)
	}
	ts.eng.Close()
	for _, c := range clients {
		c.ExpectClosed(t)
		_ = c.Close()
	}
	waitForGoroutines(t, baseline)
}

// ---- transactions ----

// recordingHandler remembers how it was called so tests can check that EXEC
// reaches the engine as ONE atomic batch.
type recordingHandler struct {
	mu      sync.Mutex
	singles [][]string
	batches [][][]string
}

func (r *recordingHandler) Execute(_ context.Context, args []string) resp.Value {
	r.mu.Lock()
	r.singles = append(r.singles, args)
	r.mu.Unlock()
	return resp.SimpleString("OK")
}

func (r *recordingHandler) ExecuteAtomic(_ context.Context, cmds [][]string) []resp.Value {
	r.mu.Lock()
	r.batches = append(r.batches, cmds)
	r.mu.Unlock()
	out := make([]resp.Value, len(cmds))
	for i := range out {
		out[i] = resp.SimpleString("OK")
	}
	return out
}

func TestExecIsOneAtomicBatch(t *testing.T) {
	rec := &recordingHandler{}
	ts := startServer(t, Config{}, rec)
	c := testclient.MustDial(t, ts.addr)

	if got := c.Cmd(t, "MULTI"); got != "+OK" {
		t.Fatalf("MULTI = %q", got)
	}
	for _, cmd := range [][]string{{"SET", "a", "1"}, {"INCR", "a"}, {"GET", "a"}} {
		if got := c.Cmd(t, cmd...); got != "+QUEUED" {
			t.Fatalf("%v = %q; want +QUEUED", cmd, got)
		}
	}
	if len(rec.singles) != 0 {
		t.Fatalf("queued commands reached the handler early: %v", rec.singles)
	}
	if got := c.Cmd(t, "EXEC"); got != "*[+OK +OK +OK]" {
		t.Errorf("EXEC = %q", got)
	}
	if len(rec.batches) != 1 || len(rec.batches[0]) != 3 || rec.batches[0][1][0] != "INCR" {
		t.Errorf("batches = %v; want one batch of the three commands", rec.batches)
	}
	if len(rec.singles) != 0 {
		t.Errorf("commands ran individually: %v", rec.singles)
	}
}

func TestTransactionsOverTheRealEngine(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := testclient.MustDial(t, ts.addr)

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"EXEC"}, "-ERR EXEC without MULTI"},
		{[]string{"DISCARD"}, "-ERR DISCARD without MULTI"},
		{[]string{"MULTI"}, "+OK"},
		{[]string{"MULTI"}, "-ERR MULTI calls can not be nested"},
		{[]string{"SET", "n", "10"}, "+QUEUED"},
		{[]string{"INCR", "n"}, "+QUEUED"},
		{[]string{"GET", "n"}, "+QUEUED"},
		{[]string{"EXEC"}, "*[+OK :11 $11]"},
		{[]string{"GET", "n"}, "$11"},

		// Nothing is applied until EXEC; DISCARD throws the queue away.
		{[]string{"MULTI"}, "+OK"},
		{[]string{"SET", "n", "99"}, "+QUEUED"},
		{[]string{"DISCARD"}, "+OK"},
		{[]string{"GET", "n"}, "$11"},

		// An empty transaction is fine.
		{[]string{"MULTI"}, "+OK"},
		{[]string{"EXEC"}, "*[]"},

		// A runtime error does not roll back the other commands.
		{[]string{"SET", "s", "text"}, "+OK"},
		{[]string{"MULTI"}, "+OK"},
		{[]string{"SET", "before", "1"}, "+QUEUED"},
		{[]string{"LPUSH", "s", "x"}, "+QUEUED"},
		{[]string{"SET", "after", "2"}, "+QUEUED"},
		{[]string{"EXEC"}, "*[+OK -WRONGTYPE Operation against a key holding the wrong kind of value +OK]"},
		{[]string{"MGET", "before", "after"}, "*[$1 $2]"},

		// PUBLISH can be part of a transaction.
		{[]string{"MULTI"}, "+OK"},
		{[]string{"PUBLISH", "ch", "m"}, "+QUEUED"},
		{[]string{"EXEC"}, "*[:0]"},
	}
	for _, s := range steps {
		if got := c.Cmd(t, s.args...); got != s.want {
			t.Errorf("%v = %q; want %q", s.args, got, s.want)
		}
	}
}

func TestInvalidQueuedCommandAbortsExec(t *testing.T) {
	for name, bad := range map[string][]string{
		"unknown command": {"NOSUCH", "x"},
		"wrong arity":     {"GET"},
		"subscribe":       {"SUBSCRIBE", "ch"},
	} {
		t.Run(name, func(t *testing.T) {
			ts := startServer(t, Config{}, nil)
			c := testclient.MustDial(t, ts.addr)
			c.Cmd(t, "MULTI")
			if got := c.Cmd(t, "SET", "k", "v"); got != "+QUEUED" {
				t.Fatalf("SET = %q", got)
			}
			if got := c.Cmd(t, bad...); !strings.HasPrefix(got, "-ERR") {
				t.Fatalf("%v = %q; want an error at queue time", bad, got)
			}
			if got := c.Cmd(t, "EXEC"); got != "-EXECABORT Transaction discarded because of previous errors." {
				t.Errorf("EXEC = %q", got)
			}
			if got := c.Cmd(t, "GET", "k"); got != "$nil" {
				t.Errorf("SET from the aborted transaction was applied: %q", got)
			}
			// The connection is usable and a new transaction works.
			c.Cmd(t, "MULTI")
			c.Cmd(t, "SET", "k", "v")
			if got := c.Cmd(t, "EXEC"); got != "*[+OK]" {
				t.Errorf("second EXEC = %q", got)
			}
		})
	}
}

// Concurrent transactions never interleave: each EXEC sees its own
// read-modify-write as a unit.
func TestConcurrentTransactionsAreIsolated(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	setup := testclient.MustDial(t, ts.addr)
	setup.Cmd(t, "SET", "counter", "0")

	const clients, rounds = 8, 100
	var wg sync.WaitGroup
	for range clients {
		wg.Go(func() {
			c, err := testclient.Dial(ts.addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			for range rounds {
				for _, cmd := range [][]string{{"MULTI"}, {"GET", "counter"}, {"INCR", "counter"}, {"GET", "counter"}} {
					if _, err := c.Do(cmd...); err != nil {
						t.Error(err)
						return
					}
				}
				r, err := c.Do("EXEC")
				if err != nil || len(r.Elems) != 3 {
					t.Errorf("EXEC = %v, %v", r, err)
					return
				}
				var before, after int
				fmt.Sscan(r.Elems[0].Str, &before)
				fmt.Sscan(r.Elems[2].Str, &after)
				if after != before+1 {
					t.Errorf("transaction interleaved: before=%d after=%d", before, after)
					return
				}
			}
		})
	}
	wg.Wait()
	if got := setup.Cmd(t, "GET", "counter"); got != fmt.Sprintf("$%d", clients*rounds) {
		t.Errorf("counter = %s; want %d", got, clients*rounds)
	}
}

func TestTransactionTooLarge(t *testing.T) {
	s := newSession(&recordingHandler{}, nil, 0, nil, nil)
	s.execute(context.Background(), []string{"MULTI"})
	s.queued = make([][]string, maxQueuedCommands) // pretend the queue is full
	replies, _ := s.execute(context.Background(), []string{"SET", "k", "v"})
	if replies[0].Kind != resp.KindError || !s.dirty {
		t.Errorf("reply = %+v dirty = %v", replies[0], s.dirty)
	}
	replies, _ = s.execute(context.Background(), []string{"EXEC"})
	if !strings.HasPrefix(replies[0].Str, "EXECABORT") {
		t.Errorf("EXEC = %+v", replies[0])
	}
}
