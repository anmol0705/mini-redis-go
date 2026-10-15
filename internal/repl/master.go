// Package repl implements primary/replica replication.
//
// The protocol is a SIMPLIFIED miniredis protocol and is NOT wire-compatible
// with real Redis PSYNC or RDB (decision 008). A replica connects to the
// primary's normal port and sends
//
//	MRSYNC <replid> <offset>
//
// The primary answers with one line and then a stream:
//
//	+FULLRESYNC <replid> <offset>\r\n   followed by the snapshot as one RESP bulk
//	                                    string ($len\r\n<bytes>\r\n), then the
//	                                    live stream of write commands
//	+CONTINUE <replid>\r\n              followed directly by the live stream,
//	                                    starting at the replica's <offset>
//
// The stream is the RESP encoding of every command that changed data, exactly
// what the append-only file would contain. <offset> counts the bytes of that
// stream since the primary became a primary, so a replica that tracks it can
// resume after a disconnect if the missing bytes are still in the primary's
// backlog ring buffer.
//
// Concurrency model:
//
//   - A Master's state (offset, backlog, replica set) is owned by the ENGINE
//     goroutine: Propagate is called there, and attaching or detaching a
//     replica runs through Engine.Exec. No locks.
//   - Each replica has one connection goroutine (the server's, which calls
//     ServeReplica) plus a tiny reader goroutine that notices disconnection.
//     They communicate with the engine only through the replica's bounded queue.
//   - Propagate NEVER blocks: if a replica's queue is full the replica is
//     dropped (its connection closed) and it will have to resync. A slow
//     replica therefore cannot stall the primary.
package repl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/snapshot"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// Defaults for MasterConfig.
const (
	DefaultBacklogSize = 1 << 20
	DefaultQueueSize   = 8192
)

// ErrNotMaster is returned when a node that is itself a replica is asked to
// serve replicas.
var ErrNotMaster = errors.New("this node is a replica and cannot serve replicas")

// MasterConfig configures the primary side.
type MasterConfig struct {
	// BacklogSize is the size in bytes of the ring buffer used for partial resyncs.
	BacklogSize int
	// QueueSize is how many commands may wait for one replica before it is dropped.
	QueueSize int
	Logger    *slog.Logger
}

type replica struct {
	addr    string
	queue   chan []byte
	dropped chan struct{}
	kick    func() // closes the replica's connection
}

// Master is the primary side of replication.
type Master struct {
	cfg MasterConfig
	log *slog.Logger
	eng *engine.Engine

	// Engine goroutine only:
	active   bool
	replID   string
	offset   int64
	backlog  *ring
	replicas map[*replica]struct{}
	enc      []byte

	fullSyncs    int64
	partialSyncs int64
	dropCount    int64
}

// NewMaster returns a Master that is active (this node is a primary).
func NewMaster(cfg MasterConfig) *Master {
	if cfg.BacklogSize <= 0 {
		cfg.BacklogSize = DefaultBacklogSize
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Master{
		cfg:      cfg,
		log:      cfg.Logger,
		active:   true,
		replID:   newReplID(),
		backlog:  newRing(cfg.BacklogSize),
		replicas: make(map[*replica]struct{}),
	}
}

// Attach gives the Master the engine it needs to take consistent snapshots.
func (m *Master) Attach(e *engine.Engine) { m.eng = e }

// newReplID returns 40 random hex characters.
func newReplID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("repl: no randomness available: " + err.Error()) // crypto/rand failing is unrecoverable
	}
	return hex.EncodeToString(b[:])
}

// ---- engine.Propagator (engine goroutine) ----

// Propagate adds a command to the backlog and queues it for every replica.
func (m *Master) Propagate(cmd []string) {
	if !m.active {
		return
	}
	m.enc = resp.AppendCommand(m.enc[:0], cmd)
	m.backlog.write(m.enc)
	m.offset += int64(len(m.enc))
	if len(m.replicas) == 0 {
		return
	}
	msg := bytes.Clone(m.enc) // shared, read-only, by all replicas
	for r := range m.replicas {
		select {
		case r.queue <- msg:
		default:
			m.drop(r, "its queue is full (it is too slow)")
		}
	}
}

// Commit implements engine.Propagator; replicas need no durability step.
func (m *Master) Commit() error { return nil }

// drop removes a replica and closes its connection. Engine goroutine.
func (m *Master) drop(r *replica, why string) {
	if _, ok := m.replicas[r]; !ok {
		return
	}
	delete(m.replicas, r)
	m.dropCount++
	close(r.dropped)
	r.kick()
	m.log.Warn("dropping replica", "replica", r.addr, "reason", why)
}

// setActive switches between primary and replica roles. Engine goroutine.
// Becoming a replica drops our own replicas; becoming a primary starts a new
// stream identity, so old replicas cannot mistake it for the old history.
func (m *Master) setActive(on bool) {
	for r := range m.replicas {
		m.drop(r, "role changed")
	}
	m.active = on
	if on {
		m.replID = newReplID()
		m.offset = 0
		m.backlog.reset(0)
	}
}

// info returns the master-side fields. Engine goroutine.
func (m *Master) info() string {
	return fmt.Sprintf("role:master\r\nconnected_slaves:%d\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n"+
		"repl_backlog_size:%d\r\nrepl_backlog_histlen:%d\r\nsync_full:%d\r\nsync_partial_ok:%d\r\nreplicas_dropped:%d\r\n",
		len(m.replicas), m.replID, m.offset, m.cfg.BacklogSize, m.backlog.held(),
		m.fullSyncs, m.partialSyncs, m.dropCount)
}

// ---- serving a replica ----

type attachment struct {
	err     error
	full    bool
	items   []store.Item
	replID  string
	offset  int64 // offset the snapshot (or the continuation) corresponds to
	initial []byte
}

// attach registers r and decides between partial and full resync. It runs on
// the engine goroutine, so the snapshot (or backlog copy) and the point from
// which the live queue starts are exactly consistent: nothing can happen in
// between.
func (m *Master) attach(st *engine.State, r *replica, replID string, offset int64) attachment {
	if !m.active {
		return attachment{err: ErrNotMaster}
	}
	m.replicas[r] = struct{}{}
	if replID == m.replID {
		if tail, ok := m.backlog.from(offset); ok {
			m.partialSyncs++
			return attachment{replID: m.replID, offset: offset, initial: tail}
		}
	}
	m.fullSyncs++
	return attachment{full: true, items: st.DB().Dump(), replID: m.replID, offset: m.offset}
}

func (m *Master) detach(r *replica) {
	delete(m.replicas, r)
}

// ServeReplica takes over a connection that sent MRSYNC. It returns when the
// replica goes away, is dropped, or ctx ends. served is false if it failed
// before writing anything to conn (for example ErrNotMaster), in which case
// the caller should send the error to the client itself.
func (m *Master) ServeReplica(ctx context.Context, conn net.Conn, replID string, offset int64) (served bool, err error) {
	r := &replica{
		addr:    conn.RemoteAddr().String(),
		queue:   make(chan []byte, m.cfg.QueueSize),
		dropped: make(chan struct{}),
		kick:    func() { _ = conn.Close() }, // closing an already closed conn is harmless
	}
	var att attachment
	if err := m.eng.Exec(ctx, func(st *engine.State) { att = m.attach(st, r, replID, offset) }); err != nil {
		return false, err
	}
	if att.err != nil {
		return false, att.err
	}
	return true, m.stream(ctx, conn, r, att)
}

// stream sends the handshake and then the live commands to a registered replica.
func (m *Master) stream(ctx context.Context, conn net.Conn, r *replica, att attachment) error {
	// Whatever happens next, stop feeding this replica.
	defer func() {
		// ctx may already be cancelled; the detach must still happen.
		_ = m.eng.Exec(context.WithoutCancel(ctx), func(*engine.State) { m.detach(r) }) // fails only if the engine stopped, when nothing is left to detach from
	}()

	// Notice a replica that goes away even while the stream is idle.
	gone := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Go(func() {
		defer close(gone)
		_, _ = io.Copy(io.Discard, conn) // returns when the connection closes
	})
	defer func() {
		_ = conn.Close() // unblocks the reader goroutine
		readerWG.Wait()
	}()

	bw := bufio.NewWriterSize(conn, 64*1024)
	if att.full {
		m.log.Info("full resync", "replica", r.addr, "keys", len(att.items), "offset", att.offset)
		var buf bytes.Buffer
		if err := snapshot.Encode(&buf, att.items); err != nil {
			return fmt.Errorf("repl: encode snapshot: %w", err)
		}
		att.items = nil
		fmt.Fprintf(bw, "+FULLRESYNC %s %d\r\n$%d\r\n", att.replID, att.offset, buf.Len())
		bw.Write(buf.Bytes()) // bufio remembers write errors; Flush below reports them
		bw.WriteString("\r\n")
	} else {
		m.log.Info("partial resync", "replica", r.addr, "offset", att.offset, "bytes", len(att.initial))
		fmt.Fprintf(bw, "+CONTINUE %s\r\n", att.replID)
		bw.Write(att.initial)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("repl: send handshake: %w", err)
	}

	for {
		select {
		case msg := <-r.queue:
			bw.Write(msg)
			// Write whatever else is already waiting, then flush once.
		drain:
			for {
				select {
				case more := <-r.queue:
					bw.Write(more)
				default:
					break drain
				}
			}
			if err := bw.Flush(); err != nil {
				return fmt.Errorf("repl: write to replica: %w", err)
			}
		case <-r.dropped:
			return errors.New("repl: replica dropped (too slow)")
		case <-gone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
