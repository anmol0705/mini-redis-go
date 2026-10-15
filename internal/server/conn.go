package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/resp"
)

// errServerStopping is what a connection's Read returns once shutdown began.
var errServerStopping = errors.New("server: shutting down")

// finalReplyTimeout bounds the write of a last error message to a client
// we are about to hang up on, so a client that never reads cannot hold us.
const finalReplyTimeout = time.Second

// connection wraps a net.Conn with the two behaviours the command loop
// needs from its reader:
//
//  1. Flush before blocking. The bufio.Reader above us only calls Read when
//     its buffer is empty, which means every command received so far has been
//     answered. That is the right moment to flush the reply buffer, so
//     pipelined commands are answered in one write and a lone command is
//     answered right away. See docs/decisions/001-flush-strategy.md.
//  2. Cooperative stop. stop() makes the blocked Read return so the
//     goroutine can exit after finishing its current command.
type connection struct {
	net.Conn
	w    *resp.Writer
	idle time.Duration

	// ctx is cancelled when the connection is force-closed or released.
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex // guards stopping and deadline updates
	stopping bool
}

func newConnection(nc net.Conn, idle time.Duration) *connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &connection{Conn: nc, w: resp.NewWriter(nc), idle: idle, ctx: ctx, cancel: cancel}
}

// forceClose cancels the connection's context and closes the socket.
func (c *connection) forceClose() {
	c.cancel()
	_ = c.Conn.Close()
}

// Read implements io.Reader for the bufio.Reader of serveConn.
func (c *connection) Read(p []byte) (int, error) {
	if err := c.w.Flush(); err != nil {
		return 0, err
	}

	c.mu.Lock()
	if c.stopping {
		c.mu.Unlock()
		return 0, errServerStopping
	}
	if c.idle > 0 {
		// Holding mu makes this atomic with stop(), so the idle deadline
		// can never overwrite the "return now" deadline set by stop().
		// An error here means the socket is dead; Read below reports it.
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.idle))
	}
	c.mu.Unlock()

	return c.Conn.Read(p)
}

// stop asks the connection's goroutine to finish: a Read that is blocked
// returns immediately, and later Reads fail with errServerStopping. Commands
// already buffered are still executed.
func (c *connection) stop() {
	c.mu.Lock()
	c.stopping = true
	_ = c.Conn.SetReadDeadline(time.Now()) // wakes a blocked Read; errors mean it is already dead
	c.mu.Unlock()
}

func (c *connection) isStopping() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopping
}

// replyAndClose writes a final error and closes the connection. Write
// errors are ignored: the client is being dropped anyway.
func (c *connection) replyAndClose(msg string) {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(finalReplyTimeout))
	_ = c.w.WriteError(msg)
	_ = c.w.Flush()
	_ = c.Conn.Close()
}

// serveConn is the per-connection goroutine: read a command, run it, queue
// the reply. It returns when the client leaves, on a protocol error, on
// QUIT, or when the server shuts down.
func (s *Server) serveConn(c *connection, h Handler) {
	defer s.release(c)
	// A panic in one connection's handler must not take the server down.
	// recover only works inside a deferred function in the same goroutine.
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic while serving client; closing it",
				"remote", c.RemoteAddr(), "panic", r, "stack", string(debug.Stack()))
		}
	}()

	r := bufio.NewReader(c)
	sess := newSession(h)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			s.readFailed(c, err)
			return
		}

		reply, closeAfter := sess.execute(c.ctx, args)
		if err := c.w.WriteValue(reply); err != nil {
			s.log.Debug("write failed", "remote", c.RemoteAddr(), "err", err)
			return
		}
		if closeAfter {
			if err := c.w.Flush(); err != nil {
				s.log.Debug("flush failed", "remote", c.RemoteAddr(), "err", err)
			}
			return
		}
	}
}

// readFailed decides what to do with the error that ended the read loop.
func (s *Server) readFailed(c *connection, err error) {
	remote := c.RemoteAddr()
	switch {
	case c.isStopping(), errors.Is(err, io.EOF):
		// Server shutdown or the client hung up between commands: normal.
	case errors.Is(err, io.ErrUnexpectedEOF):
		s.log.Debug("client disconnected mid-command", "remote", remote)
	case errors.Is(err, resp.ErrProtocol):
		// The message is built from fixed strings in package resp, never
		// from client bytes, so it is safe to send back.
		msg := strings.TrimPrefix(err.Error(), resp.ErrProtocol.Error()+": ")
		s.log.Debug("protocol error", "remote", remote, "err", err)
		c.replyAndClose("ERR Protocol error: " + msg)
	case errors.Is(err, os.ErrDeadlineExceeded):
		s.log.Debug("closing idle client", "remote", remote)
	default:
		s.log.Debug("read failed", "remote", remote, "err", err)
	}
}
