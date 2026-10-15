package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// testTimeout bounds every blocking wait in these tests. It is only the
// "this is broken" limit; passing tests never wait for it.
const testTimeout = 10 * time.Second

// ---- a tiny RESP client -------------------------------------------------

// reply is a decoded server reply.
type reply struct {
	kind  byte // '+', '-', ':', '$', '*'
	str   string
	null  bool
	elems []reply
}

// String renders a reply compactly so tests can compare with one string.
func (r reply) String() string {
	switch r.kind {
	case '$':
		if r.null {
			return "$nil"
		}
		return "$" + r.str
	case '*':
		parts := make([]string, len(r.elems))
		for i, e := range r.elems {
			parts[i] = e.String()
		}
		return "*[" + strings.Join(parts, " ") + "]"
	default:
		return string(r.kind) + r.str
	}
}

type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialClient(addr string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, testTimeout)
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, r: bufio.NewReader(conn)}, nil
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := dialClient(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.conn.Close() })
	return c
}

func encode(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

func (c *client) write(p []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(testTimeout))
	_, err := c.conn.Write(p)
	return err
}

func (c *client) send(args ...string) error { return c.write(encode(args...)) }

func (c *client) readReply() (reply, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	line, err := c.r.ReadString('\n')
	if err != nil {
		return reply{}, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return reply{}, fmt.Errorf("empty reply line")
	}
	kind, rest := line[0], line[1:]
	switch kind {
	case '+', '-', ':':
		return reply{kind: kind, str: rest}, nil
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return reply{}, err
		}
		if n < 0 {
			return reply{kind: kind, null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return reply{}, err
		}
		return reply{kind: kind, str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return reply{}, err
		}
		out := reply{kind: kind}
		for range n {
			e, err := c.readReply()
			if err != nil {
				return reply{}, err
			}
			out.elems = append(out.elems, e)
		}
		return out, nil
	}
	return reply{}, fmt.Errorf("unknown reply type in %q", line)
}

// do sends one command and returns the reply as a string, failing the test
// on any I/O error. Use only from the test goroutine.
func (c *client) do(t *testing.T, args ...string) string {
	t.Helper()
	if err := c.send(args...); err != nil {
		t.Fatalf("send %v: %v", args, err)
	}
	return c.mustRead(t)
}

func (c *client) mustRead(t *testing.T) string {
	t.Helper()
	r, err := c.readReply()
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return r.String()
}

// expectClosed fails unless the server closes the connection (EOF or reset).
func (c *client) expectClosed(t *testing.T) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := c.r.ReadByte(); err == nil {
		t.Fatal("connection still open; want it closed by the server")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("timed out waiting for the server to close the connection")
	}
}

// ---- starting a server --------------------------------------------------

type testServer struct {
	*Server
	addr   string
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

// startServer listens on 127.0.0.1:0 and serves h (default: the real command
// dispatcher). The server is stopped by t.Cleanup if the test does not do so.
func startServer(t *testing.T, cfg Config, h Handler) *testServer {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	srv, err := Listen(cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if h == nil {
		h = command.New(store.New(), srv)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ts := &testServer{Server: srv, addr: srv.Addr().String(), cancel: cancel, done: make(chan error, 1)}
	go func() { ts.done <- srv.Serve(ctx, h) }()
	t.Cleanup(func() { _ = ts.stop(t) })
	return ts
}

// stop cancels the server and waits (with a deadline) for Serve to return.
func (ts *testServer) stop(t *testing.T) error {
	ts.once.Do(func() {
		ts.cancel()
		select {
		case ts.err = <-ts.done:
		case <-time.After(testTimeout):
			t.Error("Serve did not return after cancellation")
		}
	})
	return ts.err
}

// ---- waiting without sleeping ------------------------------------------

// eventually polls cond until it is true or the deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
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

// waitForGoroutines fails (with a goroutine dump) if the count does not fall to max.
func waitForGoroutines(t *testing.T, max int) {
	t.Helper()
	deadline := time.NewTimer(testTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for runtime.NumGoroutine() > max {
		select {
		case <-tick.C:
		case <-deadline.C:
			var b bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
			t.Fatalf("goroutine leak: %d running, want at most %d\n%s", runtime.NumGoroutine(), max, b.String())
		}
	}
}

// ---- observing logs -----------------------------------------------------

// logWatcher is an slog.Handler that closes a channel the first time each
// watched message is logged, so tests can wait for server events without
// sleeping.
type logWatcher struct {
	mu   sync.Mutex
	seen map[string]chan struct{}
}

func newLogWatcher(messages ...string) (*slog.Logger, map[string]<-chan struct{}) {
	w := &logWatcher{seen: make(map[string]chan struct{})}
	out := make(map[string]<-chan struct{})
	for _, m := range messages {
		ch := make(chan struct{})
		w.seen[m] = ch
		out[m] = ch
	}
	return slog.New(w), out
}

func (w *logWatcher) Enabled(context.Context, slog.Level) bool { return true }
func (w *logWatcher) WithAttrs([]slog.Attr) slog.Handler       { return w }
func (w *logWatcher) WithGroup(string) slog.Handler            { return w }
func (w *logWatcher) Handle(_ context.Context, r slog.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch, ok := w.seen[r.Message]; ok {
		select {
		case <-ch: // already closed
		default:
			close(ch)
		}
	}
	return nil
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// ---- handlers for specific tests ---------------------------------------

// funcHandler adapts a function to the Handler interface.
type funcHandler func(args []string) resp.Value

func (f funcHandler) Execute(args []string) resp.Value { return f(args) }
