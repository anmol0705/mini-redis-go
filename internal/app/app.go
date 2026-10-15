// Package app wires the pieces together: keyspace, engine, server and (in
// later phases) persistence and replication. cmd/miniredis turns flags into a
// Config and calls Run; integration tests call Start to get whole nodes
// in-process on ephemeral ports.
//
// Concurrency: Start launches one goroutine that serves until the context is
// cancelled and then shuts the components down in dependency order (server
// first, so no new commands arrive, then the engine). Wait returns after that
// goroutine has finished, so a caller that waits has no goroutines of ours
// left running.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/aof"
	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/persist"
	"github.com/anmol0705/mini-redis-go/internal/pubsub"
	"github.com/anmol0705/mini-redis-go/internal/repl"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/server"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// Config holds every setting of a node. The zero value of a field selects
// that field's default.
type Config struct {
	Addr          string
	MaxClients    int
	IdleTimeout   time.Duration
	ShutdownGrace time.Duration
	Version       string
	Logger        *slog.Logger

	// QueueSize is the engine's pending-request capacity.
	QueueSize int
	// SubscriberQueue is the per-connection pub/sub backlog before a slow
	// subscriber is disconnected (default 256).
	SubscriberQueue int

	// MaxMemory is the estimated-bytes limit (0 = unlimited).
	MaxMemory        int64
	MaxMemoryPolicy  string // noeviction (default), allkeys-lru, allkeys-random, volatile-ttl
	MaxMemorySamples int    // keys sampled per eviction (default 5)

	// Persistence (see package persist). With AppendOnly false and no
	// snapshot file nothing is read or written.
	Dir              string        // data directory (default ".")
	AppendOnly       bool          // log every write to the append-only file
	AppendFsync      string        // always, everysec (default) or no
	DBFilename       string        // snapshot file name (default dump.mrdb)
	SnapshotInterval time.Duration // background snapshots this often (0 = off)

	// Replication. ReplicaOf ("host port" or "host:port") starts the node as
	// a replica of that primary.
	ReplicaOf       string
	ReplBacklogSize int           // bytes kept for partial resyncs (default 1 MiB)
	ReplQueueSize   int           // commands buffered per replica before it is dropped
	ReplRetry       time.Duration // pause between connection attempts (default 1s)
	ReplDial        repl.DialFunc // tests use it to cut the link

	// Clock supplies the time; tests inject a fake. Default time.Now.
	Clock func() time.Time

	// AOFTicks and SnapshotTicks replace the real timers in tests.
	AOFTicks      func() (<-chan time.Time, func())
	SnapshotTicks func() (<-chan time.Time, func())
}

// Node is a running server.
type Node struct {
	srv  *server.Server
	eng  *engine.Engine
	pers *persist.Manager
	repl *repl.Manager
	done chan struct{}
	err  error
}

// Start builds a node, starts listening and begins serving in the
// background. It returns as soon as the socket is bound. The node runs until
// ctx is cancelled; call Wait to block until it has shut down.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	policy, err := store.ParsePolicy(orDefault(cfg.MaxMemoryPolicy, "noeviction"))
	if err != nil {
		return nil, err
	}
	if cfg.MaxMemory < 0 || cfg.MaxMemorySamples < 0 {
		return nil, errors.New("app: maxmemory and maxmemory samples must not be negative")
	}

	hub := pubsub.NewHub()
	applier := &replApplier{log: cfg.Logger}
	rm := repl.NewManager(repl.Config{
		Master:     repl.MasterConfig{BacklogSize: cfg.ReplBacklogSize, QueueSize: cfg.ReplQueueSize, Logger: cfg.Logger},
		RetryDelay: cfg.ReplRetry,
		Dial:       cfg.ReplDial,
		Logger:     cfg.Logger,
	}, applier)
	srv, err := server.Listen(server.Config{
		Hub:             hub,
		Replication:     rm,
		SubscriberQueue: cfg.SubscriberQueue,
		Addr:            cfg.Addr,
		MaxClients:      cfg.MaxClients,
		IdleTimeout:     cfg.IdleTimeout,
		ShutdownGrace:   cfg.ShutdownGrace,
		Version:         cfg.Version,
		Logger:          cfg.Logger,
		Now:             cfg.Clock,
	})
	if err != nil {
		return nil, err
	}

	fsync := aof.FsyncEverySec
	if cfg.AppendFsync != "" {
		if fsync, err = aof.ParsePolicy(cfg.AppendFsync); err != nil {
			_ = srv.Close()
			return nil, err
		}
	}
	db := store.New(nil)
	pers, err := persist.New(persist.Config{
		Dir: cfg.Dir, AppendOnly: cfg.AppendOnly, Fsync: fsync, DBFilename: cfg.DBFilename,
		SnapshotInterval: cfg.SnapshotInterval, Logger: cfg.Logger,
		AOFTicks: cfg.AOFTicks, SnapshotTicks: cfg.SnapshotTicks,
	}, db)
	if err != nil {
		_ = srv.Close()
		return nil, err
	}

	h := &hooks{pers: pers, repl: rm}
	env := &command.Env{Info: srv, Publisher: hub, Hooks: h, Sections: h}
	eng := engine.New(engine.Options{
		DB:           db,
		Env:          env,
		Clock:        cfg.Clock,
		QueueSize:    cfg.QueueSize,
		MaxMemory:    cfg.MaxMemory,
		Policy:       policy,
		EvictSamples: cfg.MaxMemorySamples,
		Propagator:   multiPropagator{pers, rm}, // AOF first, then the replication feed
		Logger:       cfg.Logger,
	})
	pers.Attach(eng)
	rm.Attach(eng)
	applier.eng, applier.pers = eng, pers

	// Restore data before serving anyone. Clients that connect meanwhile wait
	// in the listen backlog.
	if err := pers.Load(ctx); err != nil {
		eng.Close()
		rm.Close()
		_ = pers.Close()
		_ = srv.Close()
		return nil, err
	}
	pers.StartScheduler()
	if cfg.ReplicaOf != "" {
		host, port, err := splitHostPort(cfg.ReplicaOf)
		if err == nil {
			if r := eng.Execute(ctx, []string{"REPLICAOF", host, port}); r.Kind == resp.KindError {
				err = errors.New(r.Str)
			}
		}
		if err != nil {
			eng.Close()
			rm.Close()
			_ = pers.Close()
			_ = srv.Close()
			return nil, fmt.Errorf("app: replicaof %q: %w", cfg.ReplicaOf, err)
		}
	}

	n := &Node{srv: srv, eng: eng, pers: pers, repl: rm, done: make(chan struct{})}
	cfg.Logger.Info("listening", "addr", srv.Addr().String(), "version", cfg.Version)
	go n.run(ctx, cfg.Logger)
	return n, nil
}

// splitHostPort accepts "host port" or "host:port".
func splitHostPort(s string) (host, port string, err error) {
	if f := strings.Fields(s); len(f) == 2 {
		return f[0], f[1], nil
	}
	h, p, err := net.SplitHostPort(s)
	if err != nil {
		return "", "", errors.New(`want "host port" or "host:port"`)
	}
	return h, p, nil
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// run serves until ctx ends, then tears everything down.
func (n *Node) run(ctx context.Context, log *slog.Logger) {
	defer close(n.done)
	n.err = n.srv.Serve(ctx, n.eng)
	n.eng.Close()
	n.repl.Close() // after the engine: the replica goroutine may be waiting on it
	if err := n.pers.Close(); err != nil {
		log.Error("closing persistence failed", "err", err)
		n.err = errors.Join(n.err, err)
	}
	log.Info("stopped")
}

// Addr returns the address the node is listening on.
func (n *Node) Addr() net.Addr { return n.srv.Addr() }

// Wait blocks until the node has shut down and returns the reason it
// stopped (nil after a normal, context-driven shutdown).
func (n *Node) Wait() error {
	<-n.done
	return n.err
}

// Run starts a node and waits for it to stop.
func Run(ctx context.Context, cfg Config) error {
	n, err := Start(ctx, cfg)
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}
	return n.Wait()
}
