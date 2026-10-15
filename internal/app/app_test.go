package app

import (
	"context"
	"io"
	"log/slog"
	"net"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/testclient"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeClock struct{ ms atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return c
}
func (c *fakeClock) Now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// startNode runs a node on an ephemeral port and stops it when the test ends.
func startNode(t *testing.T, mod func(*Config)) (*Node, string, *fakeClock) {
	t.Helper()
	clock := newClock()
	cfg := Config{Addr: "127.0.0.1:0", Logger: quiet, Clock: clock.Now, Version: "test"}
	if mod != nil {
		mod(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n, err := Start(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		if err := n.Wait(); err != nil {
			t.Errorf("node stopped with %v", err)
		}
	})
	return n, n.Addr().String(), clock
}

func TestEveryTypeOverTCP(t *testing.T) {
	_, addr, _ := startNode(t, nil)
	c := testclient.MustDial(t, addr)

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"SET", "s", "1"}, "+OK"},
		{[]string{"INCRBY", "s", "41"}, ":42"},
		{[]string{"APPEND", "s", "!"}, ":3"},
		{[]string{"GET", "s"}, "$42!"},
		{[]string{"MSET", "a", "1", "b", "2"}, "+OK"},
		{[]string{"MGET", "a", "nope", "b"}, "*[$1 $nil $2]"},
		{[]string{"RPUSH", "l", "x", "y", "z"}, ":3"},
		{[]string{"LPUSH", "l", "w"}, ":4"},
		{[]string{"LRANGE", "l", "0", "-1"}, "*[$w $x $y $z]"},
		{[]string{"LPOP", "l", "2"}, "*[$w $x]"},
		{[]string{"LINDEX", "l", "0"}, "$y"},
		{[]string{"HSET", "h", "f1", "v1", "f2", "v2"}, ":2"},
		{[]string{"HGET", "h", "f2"}, "$v2"},
		{[]string{"HLEN", "h"}, ":2"},
		{[]string{"SADD", "set", "m1", "m2", "m1"}, ":2"},
		{[]string{"SISMEMBER", "set", "m2"}, ":1"},
		{[]string{"SCARD", "set"}, ":2"},
		{[]string{"ZADD", "z", "2", "two", "1", "one", "3", "three"}, ":3"},
		{[]string{"ZRANGE", "z", "0", "-1", "WITHSCORES"}, "*[$one $1 $two $2 $three $3]"},
		{[]string{"ZRANK", "z", "three"}, ":2"},
		{[]string{"ZSCORE", "z", "two"}, "$2"},
		{[]string{"ZCARD", "z"}, ":3"},
		{[]string{"TYPE", "z"}, "+zset"},
		{[]string{"GET", "l"}, "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{[]string{"LPUSH", "s", "x"}, "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{[]string{"INCR", "h"}, "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{[]string{"SET", "txt", "abc"}, "+OK"},
		{[]string{"INCR", "txt"}, "-ERR value is not an integer or out of range"},
		{[]string{"ZADD", "z", "bad", "m"}, "-ERR value is not a valid float"},
		{[]string{"HSET", "h", "odd"}, "-ERR wrong number of arguments for 'hset' command"},
		{[]string{"DEL", "s", "a", "nope"}, ":2"},
		{[]string{"EXISTS", "b", "b"}, ":2"},
		{[]string{"DBSIZE"}, ":6"},
	}
	for _, s := range steps {
		if got := c.Cmd(t, s.args...); got != s.want {
			t.Errorf("%v = %q; want %q", s.args, got, s.want)
		}
	}

	keys := c.Reply(t, "KEYS", "*").Strings()
	sort.Strings(keys)
	if want := []string{"b", "h", "l", "set", "txt", "z"}; !slices.Equal(keys, want) {
		t.Errorf("KEYS * = %v; want %v", keys, want)
	}
	if got := c.Reply(t, "KEYS", "[hl]").Strings(); len(got) != 2 {
		t.Errorf("KEYS [hl] = %v", got)
	}
}

func TestExpiryOverTCP(t *testing.T) {
	_, addr, clock := startNode(t, nil)
	c := testclient.MustDial(t, addr)

	c.Cmd(t, "SET", "session", "abc", "EX", "30")
	c.Cmd(t, "SET", "forever", "x")
	if got := c.Cmd(t, "TTL", "session"); got != ":30" {
		t.Errorf("TTL = %s", got)
	}
	if got := c.Cmd(t, "PERSIST", "forever"); got != ":0" {
		t.Errorf("PERSIST = %s", got)
	}

	clock.Advance(29 * time.Second)
	if got := c.Cmd(t, "GET", "session"); got != "$abc" {
		t.Errorf("GET before expiry = %s", got)
	}
	clock.Advance(time.Second)
	if got := c.Cmd(t, "GET", "session"); got != "$nil" {
		t.Errorf("GET after expiry = %s", got)
	}
	if got := c.Cmd(t, "TTL", "session"); got != ":-2" {
		t.Errorf("TTL after expiry = %s", got)
	}
	if got := c.Cmd(t, "TTL", "forever"); got != ":-1" {
		t.Errorf("TTL of a persistent key = %s", got)
	}
}

func TestActiveExpiryRemovesUntouchedKeys(t *testing.T) {
	_, addr, clock := startNode(t, nil)
	c := testclient.MustDial(t, addr)
	for i := range 100 {
		c.Cmd(t, "SET", "k"+string(rune('a'+i%26))+string(rune('a'+i/26)), "v", "PX", "50")
	}
	clock.Advance(time.Second)

	// Nobody reads these keys; only the sampler can remove them.
	deadline := time.After(testclient.Timeout)
	for {
		if c.Cmd(t, "DBSIZE") == ":0" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expired keys never removed; DBSIZE = %s", c.Cmd(t, "DBSIZE"))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestMaxMemoryOverTCP(t *testing.T) {
	_, addr, _ := startNode(t, func(c *Config) { c.MaxMemory = 4000; c.MaxMemoryPolicy = "noeviction" })
	c := testclient.MustDial(t, addr)
	value := strings.Repeat("x", 200)
	var oom bool
	for i := 0; i < 100 && !oom; i++ {
		oom = strings.HasPrefix(c.Cmd(t, "SET", "key"+string(rune('a'+i%26))+string(rune('a'+i/26)), value), "-OOM")
	}
	if !oom {
		t.Error("never got an OOM error")
	}
	info := c.Reply(t, "INFO", "memory").Str
	if !strings.Contains(info, "maxmemory:4000") || !strings.Contains(info, "maxmemory_policy:noeviction") {
		t.Errorf("INFO memory = %q", info)
	}
}

func TestEvictionOverTCP(t *testing.T) {
	_, addr, _ := startNode(t, func(c *Config) { c.MaxMemory = 6000; c.MaxMemoryPolicy = "allkeys-lru" })
	c := testclient.MustDial(t, addr)
	value := strings.Repeat("x", 100)
	for i := range 300 {
		if got := c.Cmd(t, "SET", "k"+string(rune('a'+i%26))+string(rune('a'+i/26)), value); got != "+OK" {
			t.Fatalf("SET %d = %s (eviction should make room)", i, got)
		}
	}
	size := c.Reply(t, "DBSIZE").Int
	if size <= 0 || size >= 300 {
		t.Errorf("DBSIZE = %d; expected some, but not all, keys to survive", size)
	}
	if !strings.Contains(c.Reply(t, "INFO", "stats").Str, "evicted_keys:") {
		t.Error("INFO stats lacks evicted_keys")
	}
}

func TestStartValidation(t *testing.T) {
	for _, cfg := range []Config{
		{Addr: "127.0.0.1:0", MaxMemoryPolicy: "lfu"},
		{Addr: "127.0.0.1:0", MaxMemory: -1},
		{Addr: "not an address"},
	} {
		cfg.Logger = quiet
		if n, err := Start(context.Background(), cfg); err == nil {
			t.Errorf("Start(%+v) succeeded", cfg)
			_ = n
		}
	}
}

func TestRunReturnsAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Config{Addr: "127.0.0.1:0", Logger: quiet}) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v", err)
		}
	case <-time.After(testclient.Timeout):
		t.Fatal("Run did not return")
	}
}

func TestAddr(t *testing.T) {
	n, addr, _ := startNode(t, nil)
	if _, port, err := net.SplitHostPort(addr); err != nil || port == "0" {
		t.Errorf("Addr = %q (%v)", addr, err)
	}
	_ = n
}
