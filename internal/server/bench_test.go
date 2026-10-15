package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// These benchmarks compare two ways of sharing one keyspace between many
// connection goroutines (decision 005):
//
//   - lockedHandler: every command takes one mutex, runs on the caller's
//     goroutine, and releases it. This is the Phase 1 design carried over to
//     the typed DB. (A per-method RWMutex, as Phase 1's store had, cannot make
//     INCR or a transaction atomic, so the lock has to cover the whole
//     command.)
//   - the engine: commands are queued to one goroutine that owns the DB.
//
// Both are measured at handler level (no network) and end to end over real
// TCP on 127.0.0.1 with and without pipelining.

// lockedHandler is the mutex-based alternative, for comparison only.
type lockedHandler struct {
	mu sync.Mutex
	db *store.DB
	cx command.Ctx
}

func newLockedHandler() *lockedHandler {
	db := store.New(nil)
	return &lockedHandler{db: db, cx: command.Ctx{DB: db, Env: &command.Env{}}}
}

func (h *lockedHandler) Execute(_ context.Context, args []string) resp.Value {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.db.SetNow(time.Now().UnixMilli())
	return command.Execute(&h.cx, args)
}

func (h *lockedHandler) ExecuteAtomic(ctx context.Context, cmds [][]string) []resp.Value {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.db.SetNow(time.Now().UnixMilli())
	out := make([]resp.Value, len(cmds))
	for i, c := range cmds {
		out[i] = command.Execute(&h.cx, c)
	}
	return out
}

const (
	benchKeys   = 10_000
	benchClient = 32
)

var benchValue = fmt.Sprintf("%032d", 7)

func newEngineHandler(b testing.TB) *engine.Engine {
	e := engine.New(engine.Options{DB: store.New(nil), QueueSize: 4096})
	b.Cleanup(e.Close)
	return e
}

// mixedCommand returns a 50% GET / 50% SET command on a random key.
func mixedCommand(rng *rand.Rand) []string {
	key := "key:" + strconv.Itoa(rng.IntN(benchKeys))
	if rng.IntN(2) == 0 {
		return []string{"GET", key}
	}
	return []string{"SET", key, benchValue}
}

func BenchmarkHandler(b *testing.B) {
	for _, impl := range []struct {
		name string
		make func(testing.TB) Handler
	}{
		{"locked", func(testing.TB) Handler { return newLockedHandler() }},
		{"engine", func(tb testing.TB) Handler { return newEngineHandler(tb) }},
	} {
		b.Run(impl.name, func(b *testing.B) {
			h := impl.make(b)
			ctx := context.Background()
			b.SetParallelism(benchClient)
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewPCG(rand.Uint64(), 1))
				for pb.Next() {
					h.Execute(ctx, mixedCommand(rng))
				}
			})
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cmd/s")
		})
	}
}

// skipReply consumes one reply (+OK, bulk string or null) from r.
func skipReply(r *bufio.Reader) error {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if line[0] == '$' {
		n, _ := strconv.Atoi(string(line[1 : len(line)-2]))
		if n >= 0 {
			_, err = r.Discard(n + 2)
		}
	}
	return err
}

func BenchmarkTCP(b *testing.B) {
	for _, impl := range []struct {
		name string
		make func(testing.TB) Handler
	}{
		{"locked", func(testing.TB) Handler { return newLockedHandler() }},
		{"engine", func(tb testing.TB) Handler { return newEngineHandler(tb) }},
	} {
		for _, depth := range []int{1, 16} {
			b.Run(fmt.Sprintf("%s/pipeline=%d", impl.name, depth), func(b *testing.B) {
				ts := startServer(b, Config{}, impl.make(b))
				benchTCP(b, ts.addr, depth)
			})
		}
	}
}

// benchTCP runs benchClient clients; each sends batches of depth commands in
// one write and then reads depth replies. One benchmark iteration is one
// batch, so cmd/s is the headline number.
func benchTCP(b *testing.B, addr string, depth int) {
	conns := make([]net.Conn, benchClient)
	for i := range conns {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		conns[i] = c
		b.Cleanup(func() { _ = c.Close() })
	}

	batches := max(b.N/benchClient, 1)
	var wg sync.WaitGroup
	b.ResetTimer()
	for i, c := range conns {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 99))
			r := bufio.NewReader(c)
			var buf []byte
			for range batches {
				buf = buf[:0]
				for range depth {
					buf = append(buf, encode(mixedCommand(rng)...)...)
				}
				if _, err := c.Write(buf); err != nil {
					b.Error(err)
					return
				}
				for range depth {
					if err := skipReply(r); err != nil && err != io.EOF {
						b.Error(err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(batches*benchClient*depth)/b.Elapsed().Seconds(), "cmd/s")
}
