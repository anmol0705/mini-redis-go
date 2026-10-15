package repl

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/snapshot"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// maxSnapshotBytes bounds the snapshot a primary may send (the length comes
// from the network). Memory still grows only with the bytes that arrive.
const maxSnapshotBytes = 8 << 30

// Applier is how a Replica changes the local node. Both methods must be
// atomic with respect to the engine and must refuse to do anything once ctx is
// cancelled (that is how a replica stops following a primary: after
// REPLICAOF NO ONE no late command may be applied).
type Applier interface {
	// LoadSnapshot replaces the whole dataset with items.
	LoadSnapshot(ctx context.Context, items []store.Item) error
	// Apply runs one command received from the primary.
	Apply(ctx context.Context, args []string) error
}

// DialFunc connects to the primary. Tests replace it to be able to cut the link.
type DialFunc func(ctx context.Context, addr string) (net.Conn, error)

func defaultDial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, "tcp", addr)
}

// ReplicaConfig configures the replica side.
type ReplicaConfig struct {
	Host, Port string
	Applier    Applier
	// Dial defaults to a TCP dial with a 5 second timeout.
	Dial DialFunc
	// RetryDelay is the pause between connection attempts (default 1s).
	RetryDelay time.Duration
	Logger     *slog.Logger
}

// Replica follows a primary: it connects, resynchronises (fully or from its
// offset) and then applies the live stream until its context is cancelled.
// Its state fields are atomics because INFO reads them from another goroutine.
type Replica struct {
	cfg  ReplicaConfig
	log  *slog.Logger
	addr string

	replID atomic.Pointer[string]
	offset atomic.Int64
	linkUp atomic.Bool
}

// NewReplica returns a Replica. Call Run (usually in its own goroutine).
func NewReplica(cfg ReplicaConfig) *Replica {
	if cfg.Dial == nil {
		cfg.Dial = defaultDial
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	r := &Replica{cfg: cfg, log: cfg.Logger, addr: net.JoinHostPort(cfg.Host, cfg.Port)}
	none := "?"
	r.replID.Store(&none)
	r.offset.Store(-1)
	return r
}

// Run follows the primary until ctx is cancelled, reconnecting after failures.
func (r *Replica) Run(ctx context.Context) {
	for {
		err := r.syncOnce(ctx)
		r.linkUp.Store(false)
		if ctx.Err() != nil {
			return
		}
		r.log.Warn("replication link down; will retry", "primary", r.addr, "err", err, "retry_in", r.cfg.RetryDelay)
		t := time.NewTimer(r.cfg.RetryDelay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return
		}
	}
}

// Info returns the replica-side fields for INFO.
func (r *Replica) Info() string {
	status := "down"
	if r.linkUp.Load() {
		status = "up"
	}
	off := max(r.offset.Load(), 0)
	return fmt.Sprintf("role:slave\r\nmaster_host:%s\r\nmaster_port:%s\r\nmaster_link_status:%s\r\n"+
		"master_replid:%s\r\nmaster_repl_offset:%d\r\nslave_repl_offset:%d\r\n",
		r.cfg.Host, r.cfg.Port, status, *r.replID.Load(), off, off)
}

// syncOnce is one connection's lifetime: handshake, then streaming.
func (r *Replica) syncOnce(ctx context.Context) error {
	conn, err := r.cfg.Dial(ctx, r.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close() }() // may already be closed by the context hook below
	// Cancelling ctx closes the socket, which unblocks any read below.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	req := resp.AppendCommand(nil, []string{"MRSYNC", *r.replID.Load(), strconv.FormatInt(r.offset.Load(), 10)})
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("send handshake: %w", err)
	}

	br := bufio.NewReaderSize(conn, 64*1024)
	line, err := br.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("read handshake reply: %w", err)
	}
	fields := strings.Fields(strings.TrimRight(string(line), "\r\n"))
	switch {
	case len(fields) == 3 && fields[0] == "+FULLRESYNC":
		off, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || off < 0 {
			return fmt.Errorf("bad FULLRESYNC offset %q", fields[2])
		}
		if err := r.receiveSnapshot(ctx, br); err != nil {
			return err
		}
		id := fields[1]
		r.replID.Store(&id)
		r.offset.Store(off)
		r.log.Info("full resync complete", "primary", r.addr, "offset", off)
	case len(fields) == 2 && fields[0] == "+CONTINUE":
		if fields[1] != *r.replID.Load() {
			return fmt.Errorf("primary continued with replication id %q, expected %q", fields[1], *r.replID.Load())
		}
		r.log.Info("partial resync accepted", "primary", r.addr, "offset", r.offset.Load())
	case len(fields) > 0 && strings.HasPrefix(fields[0], "-"):
		return fmt.Errorf("primary refused: %s", strings.Join(fields, " "))
	default:
		return fmt.Errorf("unexpected handshake reply %q", fields)
	}

	r.linkUp.Store(true)
	for {
		args, err := resp.ReadCommand(br)
		if err != nil {
			return fmt.Errorf("stream: %w", err)
		}
		if err := r.cfg.Applier.Apply(ctx, args); err != nil {
			return fmt.Errorf("apply %q: %w", args[0], err)
		}
		r.offset.Add(int64(resp.EncodedLen(args)))
	}
}

// receiveSnapshot reads "$<len>\r\n<snapshot>\r\n", validates it (the CRC is
// checked before anything is returned) and installs it.
func (r *Replica) receiveSnapshot(ctx context.Context, br *bufio.Reader) error {
	line, err := br.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("read snapshot header: %w", err)
	}
	hdr := strings.TrimRight(string(line), "\r\n")
	if !strings.HasPrefix(hdr, "$") {
		return fmt.Errorf("expected a snapshot, got %q", hdr)
	}
	n, err := strconv.ParseInt(hdr[1:], 10, 64)
	if err != nil || n < 0 || n > maxSnapshotBytes {
		return fmt.Errorf("invalid snapshot length %q", hdr[1:])
	}
	// bytes.Buffer grows as data arrives, so a lying length costs nothing.
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, br, n); err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	if _, err := br.Discard(2); err != nil { // trailing CRLF
		return fmt.Errorf("read snapshot terminator: %w", err)
	}
	items, err := snapshot.Decode(&buf)
	if err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if err := r.cfg.Applier.LoadSnapshot(ctx, items); err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	return nil
}

// ErrStopped is what an Applier returns when its context was cancelled.
var ErrStopped = errors.New("repl: replica stopped")
