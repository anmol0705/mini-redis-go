package repl

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/engine"
)

// Config configures a Manager.
type Config struct {
	Master MasterConfig
	// RetryDelay and Dial are passed to replicas (see ReplicaConfig).
	RetryDelay time.Duration
	Dial       DialFunc
	Logger     *slog.Logger
}

// Manager is the node's replication role: it is a primary (serving replicas)
// until REPLICAOF turns it into a replica, and again after REPLICAOF NO ONE.
//
// Concurrency: ReplicaOf, ReplicaOfNoOne, InfoSection and Propagate run on the
// engine goroutine, which is why the role state needs no lock. The replica's
// own goroutine is started with the Manager's WaitGroup and stopped by
// cancelling its context. Stopping never WAITS for it on the engine goroutine
// (it might itself be waiting for the engine); the next goroutine waits for the
// previous one instead.
type Manager struct {
	cfg     Config
	log     *slog.Logger
	master  *Master
	applier Applier

	root   context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Engine goroutine only:
	cur *follow
}

type follow struct {
	replica *Replica
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewManager returns a Manager that starts as a primary.
func NewManager(cfg Config, applier Applier) *Manager {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Master.Logger == nil {
		cfg.Master.Logger = cfg.Logger
	}
	root, cancel := context.WithCancel(context.Background())
	return &Manager{cfg: cfg, log: cfg.Logger, master: NewMaster(cfg.Master), applier: applier, root: root, cancel: cancel}
}

// Attach gives the Manager the engine.
func (m *Manager) Attach(e *engine.Engine) { m.master.Attach(e) }

// Propagate and Commit make the Manager an engine.Propagator by delegating to
// the primary side.
func (m *Manager) Propagate(cmd []string) { m.master.Propagate(cmd) }

// Commit implements engine.Propagator.
func (m *Manager) Commit() error { return m.master.Commit() }

// ServeReplica serves a replica connection (see Master.ServeReplica).
func (m *Manager) ServeReplica(ctx context.Context, conn net.Conn, replID string, offset int64) (bool, error) {
	return m.master.ServeReplica(ctx, conn, replID, offset)
}

// ReplicaOf starts following host:port. It implements command.Hooks and runs
// on the engine goroutine.
func (m *Manager) ReplicaOf(host, port string) error {
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return errors.New("Invalid master port")
	}
	prev := m.cur
	if prev != nil {
		prev.cancel() // cancelled before the new follower starts, so it cannot apply anything more
	}
	m.master.setActive(false)

	ctx, cancel := context.WithCancel(m.root)
	f := &follow{
		cancel: cancel,
		done:   make(chan struct{}),
		replica: NewReplica(ReplicaConfig{
			Host: host, Port: port, Applier: m.applier, Dial: m.cfg.Dial,
			RetryDelay: m.cfg.RetryDelay, Logger: m.log,
		}),
	}
	m.cur = f
	m.log.Info("now a replica", "primary", net.JoinHostPort(host, port))
	m.wg.Go(func() {
		defer close(f.done)
		if prev != nil {
			<-prev.done // never run two followers at once
		}
		f.replica.Run(ctx)
	})
	return nil
}

// ReplicaOfNoOne stops following and becomes a primary with a new history.
func (m *Manager) ReplicaOfNoOne() error {
	if m.cur != nil {
		m.cur.cancel()
		m.cur = nil
	}
	m.master.setActive(true)
	m.log.Info("now a primary", "replid", m.master.replID)
	return nil
}

// InfoSection supplies the "replication" section of INFO.
func (m *Manager) InfoSection(name string) (string, bool) {
	if name != "replication" {
		return "", false
	}
	if m.cur != nil {
		return "# Replication\r\n" + m.cur.replica.Info(), true
	}
	return "# Replication\r\n" + m.master.info(), true
}

// Close stops following the primary and waits for the replica goroutine. Call
// it after the engine has stopped.
func (m *Manager) Close() {
	m.cancel()
	m.wg.Wait()
}
