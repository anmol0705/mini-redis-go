package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/resp"
)

func TestPing(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)
	if got := c.do(t, "PING"); got != "+PONG" {
		t.Errorf("PING = %q; want +PONG", got)
	}
	if got := c.do(t, "ping", "hello"); got != "$hello" {
		t.Errorf("ping hello = %q; want $hello", got)
	}
}

func TestSetGet(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)
	if got := c.do(t, "SET", "name", "Anmol"); got != "+OK" {
		t.Errorf("SET = %q", got)
	}
	if got := c.do(t, "GET", "name"); got != "$Anmol" {
		t.Errorf("GET = %q", got)
	}
	if got := c.do(t, "GET", "missing"); got != "$nil" {
		t.Errorf("GET missing = %q", got)
	}
	if got := c.do(t, "NOSUCH"); !strings.HasPrefix(got, "-ERR unknown command") {
		t.Errorf("unknown command = %q", got)
	}
}

// Many commands in one write must all be answered, in order.
func TestPipelinedCommands(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)

	const n = 1000
	var batch []byte
	for i := range n {
		batch = append(batch, encode("SET", "k"+strconv.Itoa(i), strconv.Itoa(i))...)
	}
	for i := range n {
		batch = append(batch, encode("GET", "k"+strconv.Itoa(i))...)
	}
	if err := c.write(batch); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if got := c.mustRead(t); got != "+OK" {
			t.Fatalf("reply %d = %q; want +OK", i, got)
		}
	}
	for i := range n {
		if got, want := c.mustRead(t), "$"+strconv.Itoa(i); got != want {
			t.Fatalf("GET reply %d = %q; want %q", i, got, want)
		}
	}
}

// A command that arrives one byte per TCP segment is reassembled.
func TestCommandSentOneByteAtATime(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)
	for _, b := range encode("SET", "slow", "value") {
		if err := c.write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.mustRead(t); got != "+OK" {
		t.Errorf("SET = %q", got)
	}
	if got := c.do(t, "GET", "slow"); got != "$value" {
		t.Errorf("GET = %q", got)
	}
}

// Regression guard for the flush strategy: a full command followed by only
// the START of the next one must still be answered immediately. A rule such
// as "flush only when no input is buffered" would hold the first reply back
// while the server waits for the rest of the second command.
func TestReplyNotHeldBackByPartialNextCommand(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)

	second := encode("PING")
	chunk := append(encode("ECHO", "first"), second[:5]...)
	if err := c.write(chunk); err != nil {
		t.Fatal(err)
	}
	if got := c.mustRead(t); got != "$first" {
		t.Fatalf("first reply = %q; want $first (server held it back?)", got)
	}
	if err := c.write(second[5:]); err != nil {
		t.Fatal(err)
	}
	if got := c.mustRead(t); got != "+PONG" {
		t.Errorf("second reply = %q", got)
	}
}

func TestLargeValue(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)

	value := strings.Repeat("0123456789abcdef", 1<<16) // 1 MiB
	if got := c.do(t, "SET", "big", value); got != "+OK" {
		t.Fatalf("SET = %q", got[:min(len(got), 50)])
	}
	if got := c.do(t, "GET", "big"); got != "$"+value {
		t.Errorf("GET returned %d bytes; value differs from the %d bytes stored", len(got)-1, len(value))
	}
}

func TestProtocolError(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)
	if err := c.write([]byte("hello world\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := c.mustRead(t); got != "-ERR Protocol error: expected array header" {
		t.Errorf("reply = %q", got)
	}
	c.expectClosed(t)

	// The server survives and serves others.
	if got := dial(t, ts.addr).do(t, "PING"); got != "+PONG" {
		t.Errorf("PING after protocol error = %q", got)
	}
}

func TestProtocolErrorAfterPipelinedCommands(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)
	if err := c.write(append(encode("PING"), "garbage\r\n"...)); err != nil {
		t.Fatal(err)
	}
	if got := c.mustRead(t); got != "+PONG" {
		t.Errorf("first reply = %q", got)
	}
	if got := c.mustRead(t); !strings.HasPrefix(got, "-ERR Protocol error") {
		t.Errorf("second reply = %q", got)
	}
	c.expectClosed(t)
}

func TestQuit(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c := dial(t, ts.addr)
	if got := c.do(t, "quit"); got != "+OK" {
		t.Errorf("QUIT = %q", got)
	}
	c.expectClosed(t)
}

func TestConcurrentClients(t *testing.T) {
	ts := startServer(t, Config{}, nil)

	const clients, rounds = 100, 50
	var wg sync.WaitGroup
	for id := range clients {
		wg.Go(func() {
			c, err := dialClient(ts.addr)
			if err != nil {
				t.Errorf("client %d: dial: %v", id, err)
				return
			}
			defer c.conn.Close()
			for i := range rounds {
				key, val := fmt.Sprintf("c%d-k%d", id, i), fmt.Sprintf("v%d-%d", id, i)
				for _, step := range []struct {
					args []string
					want string
				}{
					{[]string{"SET", key, val}, "+OK"},
					{[]string{"GET", key}, "$" + val},
				} {
					if err := c.send(step.args...); err != nil {
						t.Errorf("client %d: send: %v", id, err)
						return
					}
					r, err := c.readReply()
					if err != nil || r.String() != step.want {
						t.Errorf("client %d: %v = %v, %v; want %s", id, step.args, r, err, step.want)
						return
					}
				}
			}
		})
	}
	wg.Wait()

	c := dial(t, ts.addr)
	if got, want := c.do(t, "DBSIZE"), ":"+strconv.Itoa(clients*rounds); got != want {
		t.Errorf("DBSIZE = %q; want %q", got, want)
	}
}

func TestMaxClients(t *testing.T) {
	ts := startServer(t, Config{MaxClients: 2}, nil)

	// Complete a round trip on each so the server has surely counted them.
	c1 := dial(t, ts.addr)
	c2 := dial(t, ts.addr)
	c1.do(t, "PING")
	c2.do(t, "PING")

	c3 := dial(t, ts.addr)
	if got := c3.mustRead(t); got != "-ERR max number of clients reached" {
		t.Errorf("third client got %q", got)
	}
	c3.expectClosed(t)

	// Existing clients are unaffected, and a freed slot is reusable.
	if got := c1.do(t, "PING"); got != "+PONG" {
		t.Errorf("c1 PING = %q", got)
	}
	_ = c1.conn.Close()
	eventually(t, "a slot to free up", func() bool {
		c, err := dialClient(ts.addr)
		if err != nil {
			return false
		}
		defer c.conn.Close()
		if err := c.send("PING"); err != nil {
			return false
		}
		r, err := c.readReply()
		return err == nil && r.String() == "+PONG"
	})
}

func TestPanicInHandlerIsContained(t *testing.T) {
	logger, seen := newLogWatcher("panic while serving client; closing it")
	boom := funcHandler(func(args []string) resp.Value {
		if args[0] == "BOOM" {
			panic("handler exploded")
		}
		return resp.SimpleString("PONG")
	})
	ts := startServer(t, Config{Logger: logger}, boom)

	victim := dial(t, ts.addr)
	if err := victim.send("BOOM"); err != nil {
		t.Fatal(err)
	}
	victim.expectClosed(t)
	waitFor(t, seen["panic while serving client; closing it"], "the panic to be logged")

	if got := dial(t, ts.addr).do(t, "PING"); got != "+PONG" {
		t.Errorf("server did not survive the panic: PING = %q", got)
	}
	eventually(t, "the panicked connection to be released", func() bool {
		return ts.Info().ConnectedClients <= 1
	})
}

func TestIdleTimeout(t *testing.T) {
	ts := startServer(t, Config{IdleTimeout: 50 * time.Millisecond}, nil)
	c := dial(t, ts.addr)
	c.expectClosed(t) // never send anything
	if ts.Info().ConnectedClients != 0 {
		eventually(t, "idle client to be released", func() bool { return ts.Info().ConnectedClients == 0 })
	}
}

func TestIdleTimeoutResetsOnActivity(t *testing.T) {
	ts := startServer(t, Config{IdleTimeout: 2 * time.Second}, nil)
	c := dial(t, ts.addr)
	for range 3 {
		if got := c.do(t, "PING"); got != "+PONG" {
			t.Fatalf("PING = %q", got)
		}
	}
}

func TestClientDisconnectMidCommand(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	c, err := dialClient(ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the client to be counted", func() bool { return ts.Info().ConnectedClients == 1 })
	if err := c.write(encode("SET", "k", "value")[:12]); err != nil {
		t.Fatal(err)
	}
	_ = c.conn.Close()
	eventually(t, "the connection to be released", func() bool { return ts.Info().ConnectedClients == 0 })
}

func TestInfoOverTCP(t *testing.T) {
	now := time.Unix(1000, 0)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	ts := startServer(t, Config{Version: "1.2.3", Now: clock}, nil)
	c := dial(t, ts.addr)

	mu.Lock()
	now = now.Add(90 * time.Second)
	mu.Unlock()

	_, port, _ := net.SplitHostPort(ts.addr)
	got := c.do(t, "INFO")
	for _, want := range []string{
		"redis_version:1.2.3", "tcp_port:" + port, "uptime_in_seconds:90", "connected_clients:1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("INFO lacks %q:\n%s", want, got)
		}
	}
}

// ---- shutdown -----------------------------------------------------------

func TestGracefulShutdownLeavesNoGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()

	ts := startServer(t, Config{}, nil)
	var clients []*client
	for range 10 {
		c := dial(t, ts.addr)
		c.do(t, "PING")
		clients = append(clients, c)
	}
	idle := dial(t, ts.addr) // connected but silent: must not delay shutdown
	_ = idle

	if err := ts.stop(t); err != nil {
		t.Fatalf("Serve returned %v; want nil", err)
	}
	ts.eng.Close() // the engine is part of the stack under test: it must exit too
	for _, c := range clients {
		c.expectClosed(t)
	}
	idle.expectClosed(t)

	// New connections are refused once Serve has returned.
	if c, err := net.DialTimeout("tcp", ts.addr, time.Second); err == nil {
		_ = c.Close()
		t.Error("server still accepting after shutdown")
	}

	for _, c := range clients {
		_ = c.conn.Close()
	}
	_ = idle.conn.Close()
	waitForGoroutines(t, baseline)
}

func TestShutdownLetsInflightCommandFinish(t *testing.T) {
	logger, seen := newLogWatcher("shutting down")
	entered, release := make(chan struct{}), make(chan struct{})
	slow := funcHandler(func(args []string) resp.Value {
		if args[0] == "SLOW" {
			close(entered)
			<-release
			return resp.SimpleString("DONE")
		}
		return resp.SimpleString("PONG")
	})
	ts := startServer(t, Config{Logger: logger}, slow)

	c := dial(t, ts.addr)
	// SLOW is followed by a pipelined PING that was already received: it
	// must also be answered before the connection closes.
	if err := c.write(append(encode("SLOW"), encode("PING")...)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, entered, "the handler to start")

	ts.cancel()
	waitFor(t, seen["shutting down"], "shutdown to begin")
	select {
	case err := <-ts.done:
		t.Fatalf("Serve returned (%v) while a command was still running", err)
	default:
	}

	close(release)
	if got := c.mustRead(t); got != "+DONE" {
		t.Errorf("in-flight reply = %q; want +DONE", got)
	}
	if got := c.mustRead(t); got != "+PONG" {
		t.Errorf("pipelined reply = %q; want +PONG", got)
	}
	c.expectClosed(t)
	if err := ts.stop(t); err != nil {
		t.Errorf("Serve returned %v", err)
	}
}

func TestShutdownForcesStuckConnectionsAfterGrace(t *testing.T) {
	// A client that never reads a huge reply blocks the connection's
	// goroutine in a write; the grace period must still end the shutdown.
	big := strings.Repeat("x", 64<<20)
	h := funcHandler(func(args []string) resp.Value { return resp.BulkString(big) })
	ts := startServer(t, Config{ShutdownGrace: 100 * time.Millisecond}, h)

	c := dial(t, ts.addr)
	if err := c.send("GET", "anything"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the connection to be counted", func() bool { return ts.Info().ConnectedClients == 1 })

	if err := ts.stop(t); err != nil {
		t.Errorf("Serve returned %v", err)
	}
	if n := ts.Info().ConnectedClients; n != 0 {
		t.Errorf("%d connections left after shutdown", n)
	}
}

func TestListenValidation(t *testing.T) {
	for _, cfg := range []Config{{MaxClients: -1}, {IdleTimeout: -1}, {ShutdownGrace: -1}} {
		if _, err := Listen(cfg); err == nil {
			t.Errorf("Listen(%+v) succeeded; want an error", cfg)
		}
	}

	ts := startServer(t, Config{}, nil)
	if _, err := Listen(Config{Addr: ts.addr}); err == nil {
		t.Error("second Listen on a busy address succeeded")
	}
	if _, err := Listen(Config{Addr: "not an address"}); err == nil {
		t.Error("Listen on a malformed address succeeded")
	}
}

func TestServeReturnsErrorWhenListenerIsClosedUnderneath(t *testing.T) {
	ts := startServer(t, Config{}, nil)
	_ = ts.ln.Close()
	select {
	case err := <-ts.done:
		if err == nil {
			t.Error("Serve returned nil after its listener was closed")
		}
	case <-time.After(testTimeout):
		t.Fatal("Serve did not return")
	}
	ts.once.Do(func() {}) // already finished; nothing for cleanup to wait on
}

func TestSessionQuitIsCaseInsensitive(t *testing.T) {
	s := newSession(funcHandler(func([]string) resp.Value { return resp.Error("must not be called") }), nil, 0, nil, nil)
	for _, name := range []string{"QUIT", "quit", "Quit"} {
		replies, closeAfter := s.execute(context.Background(), []string{name})
		if !closeAfter || len(replies) != 1 || replies[0].Kind != resp.KindSimpleString || replies[0].Str != "OK" {
			t.Errorf("%s: replies=%+v closeAfter=%v", name, replies, closeAfter)
		}
	}
}

var _ io.Reader = (*connection)(nil) // the bufio.Reader depends on this
