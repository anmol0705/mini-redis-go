package engine

import (
	"strconv"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/store"
)

func benchEngine(b *testing.B) *Engine {
	b.Helper()
	e := New(Options{DB: store.New(nil), Logger: quiet, QueueSize: 4096})
	b.Cleanup(e.Close)
	return e
}

// BenchmarkExecute measures one client doing a request/reply at a time: the
// full round trip through the queue.
func BenchmarkExecute(b *testing.B) {
	e := benchEngine(b)
	set := []string{"SET", "key", "value"}
	get := []string{"GET", "key"}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i&1 == 0 {
			e.Execute(bg, set)
		} else {
			e.Execute(bg, get)
		}
	}
}

// BenchmarkExecuteParallel measures many clients sharing the engine.
func BenchmarkExecuteParallel(b *testing.B) {
	e := benchEngine(b)
	b.SetParallelism(8)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key:" + strconv.Itoa(i&1023)
			if i&1 == 0 {
				e.Execute(bg, []string{"SET", key, "value"})
			} else {
				e.Execute(bg, []string{"GET", key})
			}
			i++
		}
	})
}

// BenchmarkExecuteAtomic measures a 16-command batch as one request, the shape
// of EXEC: one queue hop for sixteen commands.
func BenchmarkExecuteAtomic(b *testing.B) {
	e := benchEngine(b)
	batch := make([][]string, 16)
	for i := range batch {
		batch[i] = []string{"INCR", "counter"}
	}
	b.ReportAllocs()
	for b.Loop() {
		e.ExecuteAtomic(bg, batch)
	}
}

func BenchmarkExecuteWithPropagation(b *testing.B) {
	e := New(Options{DB: store.New(nil), Logger: quiet, QueueSize: 4096, Propagator: &countingProp{}})
	b.Cleanup(e.Close)
	set := []string{"SET", "key", "value"}
	b.ReportAllocs()
	for b.Loop() {
		e.Execute(bg, set)
	}
}

type countingProp struct{ n int }

func (c *countingProp) Propagate([]string) { c.n++ }
func (c *countingProp) Commit() error      { return nil }
