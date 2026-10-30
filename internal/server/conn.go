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

	"github.com/anmol0705/mini-redis-go/internal/pubsub"
	"github.com/anmol0705/mini-redis-go/internal/resp"
)

// errServerStopping is what a connection's Read returns once shutdown began.
var errServerStopping = errors.New("server: shutting down")

// finalReplyTimeout bounds the write of a last error message to a client
// we are about to hang up on, so a client that never reads cannot hold us.
const finalReplyTimeout = time.Second

// pushBatch is how many queued pub/sub messages are written before flushing.
const pushBatch = 64

// connection wraps a net.Conn with the behaviours the command loop needs:
//
//  1. Flush before blocking. The bufio.Reader above us only calls Read when
//     its buffer is empty, which means every command received so far has been
//     answered. That is the right moment to flush the reply buffer, so
//     pipelined commands are answered in one write and a lone command is
//     answered right away. See docs/decisions/001-flush-strategy.md.
//  2. Cooperative stop. stop() makes the blocked Read return so the
//     goroutine can exit after finishing its current command.
//  3. Serialised writes. Normally only the connection goroutine writes, but a
//     subscribed connection also has a push goroutine, so every write and
//     flush takes wmu.
type connection struct {
	net.Conn
	idle time.Duration

	wmu sync.Mutex // guards w
	w   *resp.Writer

	// ctx is cancelled when the connection is force-closed or released.
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex // guards stopping and deadline updates
	stopping bool
	stopCh   chan struct{} // closed by stop(), for long-running modes

	// Push goroutine for subscribed connections (see startPusher).
	pushQuit chan struct{}
	pushWG   sync.WaitGroup
}

func newConnection(nc net.Conn, idle time.Duration) *connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &connection{Conn: nc, w: resp.NewWriter(nc), idle: idle, ctx: ctx, cancel: cancel, stopCh: make(chan struct{})}
}

// forceClose cancels the connection's context and closes the socket.
func (c *connection) forceClose() {
	c.cancel()
	_ = c.Close()
}

// send queues a reply in the write buffer.
func (c *connection) send(v resp.Value) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.w.WriteValue(v)
}

// flush writes out everything queued.
func (c *connection) flush() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.w.Flush()
}

// Read implements io.Reader for the bufio.Reader of serveConn.
func (c *connection) Read(p []byte) (int, error) {
	if err := c.flush(); err != nil {
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
		_ = c.SetReadDeadline(time.Now().Add(c.idle))
	}
	c.mu.Unlock()

	return c.Conn.Read(p)
}

// stop asks the connection's goroutine to finish: a Read that is blocked
// returns immediately, and later Reads fail with errServerStopping. Commands
// already buffered are still executed.
func (c *connection) stop() {
	c.mu.Lock()
	if !c.stopping {
		close(c.stopCh)
	}
	c.stopping = true
	_ = c.SetReadDeadline(time.Now()) // wakes a blocked Read; errors mean it is already dead
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
	_ = c.SetWriteDeadline(time.Now().Add(finalReplyTimeout))
	_ = c.send(resp.Error(msg))
	_ = c.flush()
	_ = c.Close()
}

// startPusher starts the goroutine that writes pub/sub messages for sub to
// the client. It is called once, by the connection goroutine, at the first
// SUBSCRIBE.
func (c *connection) startPusher(sub *pubsub.Subscriber, log func(msg string, err error)) {
	c.pushQuit = make(chan struct{})
	c.pushWG.Go(func() {
		for {
			select {
			case m := <-sub.Messages():
				if err := c.pushMessages(sub, m); err != nil {
					log("push to subscriber failed", err)
					c.forceClose() // the read loop will notice and exit
					return
				}
			case <-c.pushQuit:
				return
			}
		}
	})
}

// pushMessages writes m and whatever else is already queued (up to
// pushBatch), then flushes once.
func (c *connection) pushMessages(sub *pubsub.Subscriber, m pubsub.Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	for i := 0; ; i++ {
		push := resp.Array(resp.BulkString("message"), resp.BulkString(m.Channel), resp.BulkString(m.Payload))
		if err := c.w.WriteValue(push); err != nil {
			return err
		}
		if i+1 >= pushBatch {
			break
		}
		select {
		case m = <-sub.Messages():
			continue
		default:
		}
		break
	}
	return c.w.Flush()
}

// stopPusher ends the push goroutine, if one was started. The connection is
// being torn down anyway, so it is closed first: that unblocks a push that is
// stuck writing to a client which stopped reading.
func (c *connection) stopPusher() {
	if c.pushQuit == nil {
		return
	}
	close(c.pushQuit)
	c.forceClose()
	c.pushWG.Wait()
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

	sess := newSession(h, s.cfg.Hub, s.cfg.SubscriberQueue, func(sub *pubsub.Subscriber) {
		c.startPusher(sub, func(msg string, err error) {
			s.log.Debug(msg, "remote", c.RemoteAddr(), "err", err)
		})
	}, c.forceClose)
	defer sess.close()
	defer c.stopPusher() // registered last, so it runs first

	r := bufio.NewReader(c)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			s.readFailed(c, err)
			return
		}

		replies, closeAfter := sess.execute(c.ctx, args)
		for _, reply := range replies {
			if err := c.send(reply); err != nil {
				s.log.Debug("write failed", "remote", c.RemoteAddr(), "err", err)
				return
			}
		}
		if closeAfter {
			if err := c.flush(); err != nil {
				s.log.Debug("flush failed", "remote", c.RemoteAddr(), "err", err)
			}
			return
		}
		if req := sess.takeSync(); req != nil {
			s.serveReplica(c, req)
			return
		}
	}
}

// serveReplica hands a connection that sent MRSYNC to the replication code and
// returns when the replica is gone. The connection is closed by the caller.
func (s *Server) serveReplica(c *connection, req *syncRequest) {
	if s.cfg.Replication == nil {
		c.replyAndClose("ERR replication is not enabled on this server")
		return
	}
	if err := c.flush(); err != nil {
		return
	}
	// Graceful shutdown must end the stream: stop() closes stopCh, which
	// cancels the context given to the replication code.
	ctx, cancel := context.WithCancel(c.ctx)
	var watcher sync.WaitGroup
	watcher.Go(func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-ctx.Done():
		}
	})
	defer func() {
		cancel()
		watcher.Wait()
	}()

	s.log.Debug("replica attached", "remote", c.RemoteAddr())
	served, err := s.cfg.Replication.ServeReplica(ctx, c.Conn, req.replID, req.offset)
	if !served && err != nil {
		c.replyAndClose("ERR " + err.Error())
		return
	}
	s.log.Debug("replica detached", "remote", c.RemoteAddr(), "err", err)
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
