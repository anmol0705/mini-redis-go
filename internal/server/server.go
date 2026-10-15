// Package server accepts TCP connections and runs the command loop for each.
//
// Concurrency model:
//
//   - Serve runs the accept loop on the caller's goroutine.
//   - Each accepted connection gets exactly one goroutine (serveConn) that
//     owns that connection's bufio.Reader, resp.Writer and session. Nothing
//     else reads or writes the socket, so those need no locks.
//   - The Server's own state (the connection set and the stopping flag) is
//     guarded by one mutex, and a sync.WaitGroup counts live connection
//     goroutines so shutdown can wait for them.
//   - Shutdown is driven by cancelling the context passed to Serve. Serve
//     stops accepting, tells every connection to stop reading, waits for the
//     in-flight commands to finish (up to a grace period), force-closes any
//     stragglers and returns only after every goroutine it started has exited.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/pubsub"
	"github.com/anmol0705/mini-redis-go/internal/resp"
)

// Defaults applied by Listen to zero values in Config.
const (
	DefaultMaxClients    = 10000
	DefaultShutdownGrace = 5 * time.Second
)

// Handler executes parsed commands. It is declared here, where it is used,
// so tests can substitute their own (for example one that panics). Its
// methods are called from many connection goroutines at once. The engine is
// the production implementation.
type Handler interface {
	// Execute runs one command. ctx is cancelled only when the server gives
	// up on the connection (forced close), never during a graceful shutdown.
	Execute(ctx context.Context, args []string) resp.Value
	// ExecuteAtomic runs the commands in order with nothing interleaved and
	// returns one reply per command (used by EXEC).
	ExecuteAtomic(ctx context.Context, cmds [][]string) []resp.Value
}

// Config holds the server's tunable limits. The zero value of every field
// selects a safe default.
type Config struct {
	// Addr is the TCP address to listen on, for example ":6379".
	Addr string
	// MaxClients caps simultaneous connections (default DefaultMaxClients).
	MaxClients int
	// IdleTimeout closes connections that send nothing for this long.
	// Zero disables it.
	IdleTimeout time.Duration
	// ShutdownGrace is how long Serve waits for connections to finish
	// after the context is cancelled (default DefaultShutdownGrace).
	ShutdownGrace time.Duration
	// Hub enables SUBSCRIBE/UNSUBSCRIBE. Nil makes them reply with an error.
	Hub *pubsub.Hub
	// SubscriberQueue is how many pub/sub messages one connection may have
	// waiting before it is disconnected as too slow (default
	// DefaultSubscriberQueue).
	SubscriberQueue int
	// Version is reported by INFO.
	Version string
	// Logger receives structured logs (default slog.Default()).
	Logger *slog.Logger
	// Now supplies the time for uptime (default time.Now); tests inject one.
	Now func() time.Time
}

// Server is a RESP-over-TCP server. Create one with Listen.
type Server struct {
	cfg     Config
	ln      net.Listener
	log     *slog.Logger
	started time.Time

	mu       sync.Mutex // guards conns and stopping
	conns    map[*connection]struct{}
	stopping bool

	wg sync.WaitGroup // one count per running serveConn
}

// Listen binds the socket and returns a Server that is ready to Serve.
// Binding is separate from serving so that the caller learns about a busy
// port immediately and can read the real address (useful with port 0).
func Listen(cfg Config) (*Server, error) {
	if cfg.MaxClients < 0 || cfg.IdleTimeout < 0 || cfg.ShutdownGrace < 0 || cfg.SubscriberQueue < 0 {
		return nil, errors.New("server: MaxClients, IdleTimeout, ShutdownGrace and SubscriberQueue must not be negative")
	}
	if cfg.MaxClients == 0 {
		cfg.MaxClients = DefaultMaxClients
	}
	if cfg.ShutdownGrace == 0 {
		cfg.ShutdownGrace = DefaultShutdownGrace
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("server: listen on %q: %w", cfg.Addr, err)
	}
	return &Server{
		cfg:     cfg,
		ln:      ln,
		log:     cfg.Logger,
		started: cfg.Now(),
		conns:   make(map[*connection]struct{}),
	}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Info reports live server data for the INFO command.
func (s *Server) Info() command.Info {
	s.mu.Lock()
	clients := len(s.conns)
	s.mu.Unlock()

	var port int
	if a, ok := s.ln.Addr().(*net.TCPAddr); ok {
		port = a.Port
	}
	return command.Info{
		Version:          s.cfg.Version,
		TCPPort:          port,
		UptimeSeconds:    int64(s.cfg.Now().Sub(s.started) / time.Second),
		ConnectedClients: clients,
	}
}

// Serve accepts connections and runs h for each command until ctx is
// cancelled, then shuts down gracefully (see the package comment). It
// returns nil after a normal shutdown and must be called at most once.
func (s *Server) Serve(ctx context.Context, h Handler) error {
	// Accept blocks, so something must wake it when ctx ends. AfterFunc
	// runs the closure in its own short-lived goroutine only if ctx is
	// cancelled, and stop() removes it if Serve returns first.
	stop := context.AfterFunc(ctx, func() {
		_ = s.ln.Close() // the only effect we want is unblocking Accept
	})
	defer stop()

	var serveErr error
	var backoff time.Duration
accepting:
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(err, net.ErrClosed) {
				serveErr = err
				break
			}
			// Probably a temporary condition such as running out of file
			// descriptors. Back off instead of spinning, but stay alive.
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.log.Warn("accept failed; retrying", "err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
				continue
			case <-ctx.Done():
				break accepting
			}
		}
		backoff = 0
		s.admit(nc, h)
	}

	s.shutdown()
	return serveErr
}

// admit registers a new connection and starts its goroutine, or turns it
// away if the server is full.
func (s *Server) admit(nc net.Conn, h Handler) {
	c := newConnection(nc, s.cfg.IdleTimeout)

	s.mu.Lock()
	if len(s.conns) >= s.cfg.MaxClients {
		s.mu.Unlock()
		s.log.Warn("rejecting client: max clients reached", "remote", nc.RemoteAddr(), "max", s.cfg.MaxClients)
		c.replyAndClose("ERR max number of clients reached")
		return
	}
	s.conns[c] = struct{}{}
	s.mu.Unlock()

	s.log.Debug("client connected", "remote", nc.RemoteAddr())
	s.wg.Go(func() { s.serveConn(c, h) })
}

// release forgets a connection and closes it. It is called exactly once per
// connection, by that connection's goroutine.
func (s *Server) release(c *connection) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	c.forceClose() // the peer may already have closed it; nothing to do about an error
}

// shutdown stops the server: no new connections, let running commands
// finish, force-close whatever is left after the grace period.
func (s *Server) shutdown() {
	_ = s.ln.Close() // may already be closed by the context hook

	s.mu.Lock()
	s.stopping = true
	for c := range s.conns {
		c.stop()
	}
	n := len(s.conns)
	s.mu.Unlock()
	s.log.Info("shutting down", "connections", n, "grace", s.cfg.ShutdownGrace)

	done := make(chan struct{})
	go func() { // exits as soon as every connection goroutine has
		s.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(s.cfg.ShutdownGrace)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		s.log.Warn("grace period over; closing remaining connections")
		s.mu.Lock()
		for c := range s.conns {
			c.forceClose() // unblocks any goroutine stuck reading or writing
		}
		s.mu.Unlock()
		// A handler stuck inside Execute cannot be interrupted from here;
		// we still wait so that Serve never returns with goroutines alive.
		<-done
	}
}
