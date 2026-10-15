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
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/pubsub"
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

	// Clock supplies the time; tests inject a fake. Default time.Now.
	Clock func() time.Time
}

// Node is a running server.
type Node struct {
	srv  *server.Server
	eng  *engine.Engine
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
	srv, err := server.Listen(server.Config{
		Hub:             hub,
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

	env := &command.Env{Info: srv, Publisher: hub}
	eng := engine.New(engine.Options{
		DB:           store.New(nil),
		Env:          env,
		Clock:        cfg.Clock,
		QueueSize:    cfg.QueueSize,
		MaxMemory:    cfg.MaxMemory,
		Policy:       policy,
		EvictSamples: cfg.MaxMemorySamples,
		Logger:       cfg.Logger,
	})

	n := &Node{srv: srv, eng: eng, done: make(chan struct{})}
	cfg.Logger.Info("listening", "addr", srv.Addr().String(), "version", cfg.Version)
	go n.run(ctx, cfg.Logger)
	return n, nil
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
