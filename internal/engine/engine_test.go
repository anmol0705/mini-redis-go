package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

const testTimeout = 10 * time.Second

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeClock is a clock the test advances by hand.
type fakeClock struct{ ms atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return c
}
func (c *fakeClock) Now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// recorder is a Propagator that remembers what it was given.
type recorder struct {
	mu      sync.Mutex
	cmds    []string // each command joined by spaces
	commits int
	err     error
}

func (r *recorder) Propagate(cmd []string) {
	r.mu.Lock()
	r.cmds = append(r.cmds, strings.Join(cmd, " "))
	r.mu.Unlock()
}
func (r *recorder) Commit() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits++
	return r.err
}
func (r *recorder) snapshot() ([]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.cmds...), r.commits
}

func newEngine(t *testing.T, mod func(*Options)) (*Engine, *fakeClock) {
	t.Helper()
	clock := newClock()
	opts := Options{
		DB:           store.New(rand.New(rand.NewPCG(1, 2))),
		Clock:        clock.Now,
		TickInterval: time.Hour, // tests drive ticks explicitly unless they override
		Logger:       quiet,
	}
	if mod != nil {
		mod(&opts)
	}
	e := New(opts)
	t.Cleanup(e.Close)
	return e, clock
}

var bg = context.Background()

func TestExecuteBasics(t *testing.T) {
	e, _ := newEngine(t, nil)
	if got := e.Execute(bg, []string{"SET", "k", "v"}); got.Str != "OK" {
		t.Fatalf("SET = %+v", got)
	}
	if got := e.Execute(bg, []string{"GET", "k"}); got.Str != "v" || got.Kind != resp.KindBulkString {
		t.Fatalf("GET = %+v", got)
	}
	if got := e.Execute(bg, []string{"NOSUCH"}); got.Kind != resp.KindError {
		t.Fatalf("unknown command = %+v", got)
	}
	if got := e.Execute(bg, []string{"GET"}); !strings.Contains(got.Str, "wrong number of arguments") {
		t.Fatalf("arity error = %+v", got)
	}
}

func TestConcurrentIncrementsAreAtomic(t *testing.T) {
	e, _ := newEngine(t, nil)
	const goroutines, each = 32, 200
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range each {
				if got := e.Execute(bg, []string{"INCR", "n"}); got.Kind != resp.KindInteger {
					t.Errorf("INCR = %+v", got)
					return
				}
			}
		})
	}
	wg.Wait()
	if got := e.Execute(bg, []string{"GET", "n"}); got.Str != fmt.Sprint(goroutines*each) {
		t.Errorf("counter = %q; want %d", got.Str, goroutines*each)
	}
}

func TestExecuteAtomicRunsContiguously(t *testing.T) {
	e, _ := newEngine(t, nil)
	e.Execute(bg, []string{"SET", "a", "0"})

	// Writers hammer the same key while batches read-modify-write it. If
	// anything interleaved inside a batch the two GETs would differ.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				e.Execute(bg, []string{"INCR", "a"})
			}
		}
	})
	for range 300 {
		got := e.ExecuteAtomic(bg, [][]string{{"GET", "a"}, {"INCR", "a"}, {"GET", "a"}})
		before, after := got[0].Str, got[2].Str
		if before == "" || after == "" || got[1].Int < 1 {
			t.Fatalf("bad batch reply %+v", got)
		}
		var b, a int
		fmt.Sscan(before, &b)
		fmt.Sscan(after, &a)
		if a != b+1 {
			t.Fatalf("another client interleaved: before=%d after=%d", b, a)
		}
	}
	close(stop)
	wg.Wait()
}

func TestBatchContinuesPastErrors(t *testing.T) {
	e, _ := newEngine(t, nil)
	got := e.ExecuteAtomic(bg, [][]string{{"SET", "s", "x"}, {"LPUSH", "s", "a"}, {"GET", "s"}})
	if got[0].Str != "OK" || got[1].Kind != resp.KindError || got[2].Str != "x" {
		t.Errorf("replies = %+v", got)
	}
}

func TestSameInstantInsideABatch(t *testing.T) {
	e, clock := newEngine(t, nil)
	e.Execute(bg, []string{"SET", "k", "v", "PX", "1000"})
	clock.Advance(999 * time.Millisecond)
	got := e.ExecuteAtomic(bg, [][]string{{"PTTL", "k"}, {"PTTL", "k"}})
	if got[0].Int != 1 || got[1].Int != 1 {
		t.Errorf("PTTLs = %d, %d; want 1, 1", got[0].Int, got[1].Int)
	}
}

func TestExecRunsOnEngineGoroutine(t *testing.T) {
	e, _ := newEngine(t, nil)
	e.Execute(bg, []string{"SET", "k", "v"})
	var items []store.Item
	if err := e.Exec(bg, func(s *State) { items = s.DB().Dump() }); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Key != "k" {
		t.Errorf("Dump = %+v", items)
	}
}

func TestStateApplyDoesNotPropagate(t *testing.T) {
	rec := &recorder{}
	e, _ := newEngine(t, func(o *Options) { o.Propagator = rec })
	_ = e.Exec(bg, func(s *State) {
		if got := s.Apply([]string{"SET", "k", "v"}); got.Str != "OK" {
			t.Errorf("Apply = %+v", got)
		}
	})
	if cmds, _ := rec.snapshot(); len(cmds) != 0 {
		t.Errorf("loading propagated %v", cmds)
	}
	if got := e.Execute(bg, []string{"GET", "k"}); got.Str != "v" {
		t.Errorf("GET = %+v", got)
	}
}

// ---- propagation ----

func TestPropagationOrderAndCommitPerRequest(t *testing.T) {
	rec := &recorder{}
	e, clock := newEngine(t, func(o *Options) { o.Propagator = rec })

	e.Execute(bg, []string{"SET", "a", "1", "EX", "10"})
	e.Execute(bg, []string{"GET", "a"}) // read: nothing to log, no commit
	e.ExecuteAtomic(bg, [][]string{{"INCR", "a"}, {"LPUSH", "l", "x"}})
	e.Execute(bg, []string{"DEL", "missing"}) // changed nothing

	cmds, commits := rec.snapshot()
	want := []string{
		fmt.Sprintf("SET a 1 PXAT %d", clock.Now().UnixMilli()+10_000),
		"INCR a",
		"LPUSH l x",
	}
	if !reflect.DeepEqual(cmds, want) {
		t.Errorf("propagated %q; want %q", cmds, want)
	}
	if commits != 2 {
		t.Errorf("commits = %d; want 2 (one per request that wrote)", commits)
	}
}

func TestCommitFailureTurnsRepliesIntoErrors(t *testing.T) {
	rec := &recorder{err: errors.New("disk on fire")}
	e, _ := newEngine(t, func(o *Options) { o.Propagator = rec })

	got := e.Execute(bg, []string{"SET", "k", "v"})
	if got.Kind != resp.KindError || !strings.HasPrefix(got.Str, "MISCONF") {
		t.Errorf("SET = %+v; want MISCONF error", got)
	}
	batch := e.ExecuteAtomic(bg, [][]string{{"SET", "a", "1"}, {"NOSUCH"}})
	if !strings.HasPrefix(batch[0].Str, "MISCONF") || !strings.HasPrefix(batch[1].Str, "ERR unknown") {
		t.Errorf("batch = %+v", batch)
	}
	rec.mu.Lock()
	rec.err = nil
	rec.mu.Unlock()
	if got := e.Execute(bg, []string{"SET", "k", "v"}); got.Str != "OK" {
		t.Errorf("after recovery SET = %+v", got)
	}
}

// ---- expiry ----

func TestLazyExpiryPropagatesDel(t *testing.T) {
	rec := &recorder{}
	e, clock := newEngine(t, func(o *Options) { o.Propagator = rec })
	e.Execute(bg, []string{"SET", "k", "v", "PX", "100"})
	clock.Advance(200 * time.Millisecond)

	if got := e.Execute(bg, []string{"GET", "k"}); got.Kind != resp.KindNull {
		t.Fatalf("GET = %+v", got)
	}
	cmds, _ := rec.snapshot()
	if len(cmds) != 2 || cmds[1] != "DEL k" {
		t.Errorf("propagated %q; want the SET then DEL k", cmds)
	}
}

func TestActiveExpiryRunsOnTicker(t *testing.T) {
	rec := &recorder{}
	e, clock := newEngine(t, func(o *Options) {
		o.Propagator = rec
		o.TickInterval = time.Millisecond
	})
	for i := range 50 {
		e.Execute(bg, []string{"SET", fmt.Sprint("k", i), "v", "PX", "100"})
	}
	clock.Advance(time.Second)

	waitFor(t, "the sampler to delete the expired keys", func() bool {
		var size int
		_ = e.Exec(bg, func(s *State) { size = s.DB().Len() })
		return size == 0
	})
	cmds, _ := rec.snapshot()
	dels := 0
	for _, c := range cmds {
		if strings.HasPrefix(c, "DEL k") {
			dels++
		}
	}
	if dels != 50 {
		t.Errorf("propagated %d DELs; want 50", dels)
	}
}

// ---- replica mode ----

func TestReadOnlyMode(t *testing.T) {
	e, _ := newEngine(t, nil)
	e.Execute(bg, []string{"SET", "k", "v"})
	_ = e.Exec(bg, func(s *State) {
		s.SetReadOnly(true)
		if !s.ReadOnly() {
			t.Error("ReadOnly() = false")
		}
	})

	if got := e.Execute(bg, []string{"SET", "k", "x"}); !strings.HasPrefix(got.Str, "READONLY") {
		t.Errorf("client write on a replica = %+v", got)
	}
	if got := e.Execute(bg, []string{"GET", "k"}); got.Str != "v" {
		t.Errorf("client read on a replica = %+v", got)
	}
	if got := e.ExecuteFromMaster(bg, []string{"SET", "k", "x"}); got.Str != "OK" {
		t.Errorf("write from the master = %+v", got)
	}
	if got := e.Execute(bg, []string{"GET", "k"}); got.Str != "x" {
		t.Errorf("GET after master write = %+v", got)
	}

	// REPLICAOF NO ONE style: back to a writable primary.
	_ = e.Exec(bg, func(s *State) { s.SetReadOnly(false) })
	if got := e.Execute(bg, []string{"SET", "k", "y"}); got.Str != "OK" {
		t.Errorf("write after leaving replica mode = %+v", got)
	}
}

func TestReplicaDoesNotExpireByItself(t *testing.T) {
	rec := &recorder{}
	e, clock := newEngine(t, func(o *Options) {
		o.Propagator = rec
		o.TickInterval = time.Millisecond
	})
	e.Execute(bg, []string{"SET", "k", "v", "PX", "100"})
	_ = e.Exec(bg, func(s *State) { s.SetReadOnly(true) })
	before, _ := rec.snapshot()
	clock.Advance(time.Second)

	// Give the ticker plenty of chances, then check nothing was deleted
	// or propagated, yet reads already hide the key.
	for range 20 {
		_ = e.Exec(bg, func(*State) {})
	}
	var size int
	_ = e.Exec(bg, func(s *State) { size = s.DB().Len() })
	if after, _ := rec.snapshot(); size != 1 || len(after) != len(before) {
		t.Errorf("replica removed or logged a key by itself (len=%d, logged %d more)", size, len(after)-len(before))
	}
	if got := e.Execute(bg, []string{"GET", "k"}); got.Kind != resp.KindNull {
		t.Errorf("expired key visible on replica: %+v", got)
	}
	// The primary's DEL arrives and removes it.
	e.ExecuteFromMaster(bg, []string{"DEL", "k"})
	_ = e.Exec(bg, func(s *State) { size = s.DB().Len() })
	if size != 0 {
		t.Error("DEL from master did not remove the key")
	}
}

// ---- maxmemory ----

func fill(e *Engine, n int) {
	for i := range n {
		e.Execute(bg, []string{"SET", fmt.Sprint("key", i), strings.Repeat("x", 100)})
	}
}

func TestNoEvictionRefusesWrites(t *testing.T) {
	e, _ := newEngine(t, func(o *Options) { o.MaxMemory = 2000; o.Policy = store.NoEviction })
	fill(e, 30)
	got := e.Execute(bg, []string{"SET", "more", "v"})
	if got.Kind != resp.KindError || got.Str != "OOM command not allowed when used memory > 'maxmemory'." {
		t.Fatalf("SET over the limit = %+v", got)
	}
	// Reads and deletes still work, and deleting frees room.
	if got := e.Execute(bg, []string{"GET", "key0"}); got.Kind != resp.KindBulkString {
		t.Errorf("GET = %+v", got)
	}
	for i := range 30 {
		e.Execute(bg, []string{"DEL", fmt.Sprint("key", i)})
	}
	if got := e.Execute(bg, []string{"SET", "more", "v"}); got.Str != "OK" {
		t.Errorf("SET after freeing memory = %+v", got)
	}
}

func TestEvictionKeepsMemoryBounded(t *testing.T) {
	for _, policy := range []store.Policy{store.AllKeysLRU, store.AllKeysRandom} {
		t.Run(policy.String(), func(t *testing.T) {
			rec := &recorder{}
			e, _ := newEngine(t, func(o *Options) {
				o.MaxMemory = 5000
				o.Policy = policy
				o.Propagator = rec
			})
			fill(e, 500)
			var used int64
			var n int
			_ = e.Exec(bg, func(s *State) { used, n = s.DB().UsedMemory(), s.DB().Len() })
			if used > 5000+400 { // may exceed by about one write
				t.Errorf("memory %d is far above the 5000 limit", used)
			}
			if n >= 500 || n == 0 {
				t.Errorf("%d keys left; expected some eviction", n)
			}
			cmds, _ := rec.snapshot()
			dels := 0
			for _, c := range cmds {
				if strings.HasPrefix(c, "DEL ") {
					dels++
				}
			}
			if dels != 500-n {
				t.Errorf("propagated %d DELs for %d evictions", dels, 500-n)
			}
		})
	}
}

func TestVolatileTTLWithoutVolatileKeysIsOOM(t *testing.T) {
	e, _ := newEngine(t, func(o *Options) { o.MaxMemory = 1500; o.Policy = store.VolatileTTL })
	fill(e, 20) // none have an expiry
	if got := e.Execute(bg, []string{"SET", "x", "v"}); !strings.HasPrefix(got.Str, "OOM") {
		t.Errorf("SET = %+v; want OOM", got)
	}
}

func TestInfoReportsLimits(t *testing.T) {
	e, _ := newEngine(t, func(o *Options) { o.MaxMemory = 12345; o.Policy = store.AllKeysLRU })
	got := e.Execute(bg, []string{"INFO", "memory"}).Str
	if !strings.Contains(got, "maxmemory:12345") || !strings.Contains(got, "maxmemory_policy:allkeys-lru") {
		t.Errorf("INFO memory = %q", got)
	}
}

func TestREPLICAOFTogglesReadOnlyThroughControl(t *testing.T) {
	hooks := &stubHooks{}
	e, _ := newEngine(t, func(o *Options) { o.Env = &command.Env{Hooks: hooks} })
	if got := e.Execute(bg, []string{"REPLICAOF", "127.0.0.1", "7000"}); got.Str != "OK" {
		t.Fatalf("REPLICAOF = %+v", got)
	}
	if got := e.Execute(bg, []string{"SET", "k", "v"}); !strings.HasPrefix(got.Str, "READONLY") {
		t.Errorf("write on replica = %+v", got)
	}
	if got := e.Execute(bg, []string{"REPLICAOF", "NO", "ONE"}); got.Str != "OK" {
		t.Fatalf("REPLICAOF NO ONE = %+v", got)
	}
	if got := e.Execute(bg, []string{"SET", "k", "v"}); got.Str != "OK" {
		t.Errorf("write after NO ONE = %+v", got)
	}
}

type stubHooks struct{}

func (stubHooks) Save(*store.DB) error                 { return nil }
func (stubHooks) BGSave(*store.DB) (string, error)     { return "ok", nil }
func (stubHooks) RewriteAOF(*store.DB) (string, error) { return "ok", nil }
func (stubHooks) ReplicaOf(string, string) error       { return nil }
func (stubHooks) ReplicaOfNoOne() error                { return nil }

// ---- lifecycle ----

func TestCloseIsIdempotentAndRefusesNewWork(t *testing.T) {
	e, _ := newEngine(t, nil)
	e.Close()
	e.Close()
	if got := e.Execute(bg, []string{"PING"}); got.Kind != resp.KindError || !strings.Contains(got.Str, "shutting down") {
		t.Errorf("Execute after Close = %+v", got)
	}
	batch := e.ExecuteAtomic(bg, [][]string{{"PING"}, {"PING"}})
	if len(batch) != 2 || batch[0].Kind != resp.KindError {
		t.Errorf("ExecuteAtomic after Close = %+v", batch)
	}
	if err := e.Exec(bg, func(*State) {}); !errors.Is(err, ErrClosed) {
		t.Errorf("Exec after Close = %v", err)
	}
}

func TestEngineGoroutineExitsOnClose(t *testing.T) {
	before := runtime.NumGoroutine()
	e := New(Options{DB: store.New(nil), Logger: quiet})
	e.Execute(bg, []string{"PING"})
	e.Close()
	waitFor(t, "the engine goroutine to exit", func() bool { return runtime.NumGoroutine() <= before })
}

// Requests that are queued when Close happens must still be answered.
func TestQueuedRequestsAnsweredOnClose(t *testing.T) {
	e, _ := newEngine(t, func(o *Options) { o.QueueSize = 64 })
	started, release := make(chan struct{}), make(chan struct{})
	go func() { _ = e.Exec(bg, func(*State) { close(started); <-release }) }()
	<-started // the engine is now busy; later requests pile up in the queue

	const waiters = 20
	replies := make(chan resp.Value, waiters)
	for range waiters {
		go func() { replies <- e.Execute(bg, []string{"PING"}) }()
	}
	waitFor(t, "requests to queue up", func() bool { return e.QueueDepth() == waiters })

	closed := make(chan struct{})
	go func() { e.Close(); close(closed) }()
	close(release)

	for range waiters {
		select {
		case got := <-replies:
			if got.Kind != resp.KindSimpleString && !strings.Contains(got.Str, "shutting down") {
				t.Errorf("unexpected reply %+v", got)
			}
		case <-time.After(testTimeout):
			t.Fatal("a queued request was never answered")
		}
	}
	<-closed
}

// A caller that gives up while the queue is full must not block forever and
// must not wedge the engine.
func TestSubmitHonoursContextWhenQueueIsFull(t *testing.T) {
	e, _ := newEngine(t, func(o *Options) { o.QueueSize = 1 })
	started, release := make(chan struct{}), make(chan struct{})
	go func() { _ = e.Exec(bg, func(*State) { close(started); <-release }) }()
	<-started
	go func() { e.Execute(bg, []string{"PING"}) }() // fills the one queue slot
	waitFor(t, "the queue to fill", func() bool { return e.QueueDepth() == 1 })

	ctx, cancel := context.WithCancel(bg)
	done := make(chan resp.Value, 1)
	go func() { done <- e.Execute(ctx, []string{"PING"}) }()
	cancel()
	select {
	case got := <-done:
		if got.Kind != resp.KindError {
			t.Errorf("cancelled Execute = %+v; want an error", got)
		}
	case <-time.After(testTimeout):
		t.Fatal("Execute ignored context cancellation")
	}

	close(release)
	if got := e.Execute(bg, []string{"PING"}); got.Str != "PONG" {
		t.Errorf("engine wedged after a cancelled submit: %+v", got)
	}
}

func TestCloseWhileSubmittersAreBlocked(t *testing.T) {
	e, _ := newEngine(t, func(o *Options) { o.QueueSize = 1 })
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 20 {
				e.Execute(bg, []string{"INCR", "n"})
			}
		})
	}
	e.Close() // must neither deadlock nor lose track of a request
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(testTimeout):
		t.Fatal("submitters still blocked after Close")
	}
}

// A panic while running a command is contained.
func TestPanicInCommandIsContained(t *testing.T) {
	e, _ := newEngine(t, nil)
	_ = e.Exec(bg, func(*State) { panic("boom") }) // returns an error, engine survives
	if err := e.Exec(bg, func(*State) { panic("boom") }); err == nil {
		t.Error("Exec with a panicking function returned nil")
	}
	if got := e.Execute(bg, []string{"PING"}); got.Str != "PONG" {
		t.Errorf("engine died after a panic: %+v", got)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(testTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
