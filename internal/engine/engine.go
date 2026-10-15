// Package engine owns the keyspace with a single goroutine.
//
// Concurrency model (see docs/decisions/005-single-owner-engine.md):
//
//   - One goroutine, started by New, is the ONLY code that touches the
//     store.DB. It runs commands, expires keys on a ticker and serves
//     "run this function with the DB" requests. No data needs a lock.
//   - Any number of connection goroutines submit requests over one bounded
//     channel and wait for the reply on a channel of capacity one that is
//     private to the request. The engine therefore never blocks on a client:
//     its reply is a plain resp.Value dropped into a channel with room for it,
//     and the connection goroutine does the network writing.
//   - A request that has been submitted is always answered, even during
//     shutdown, so a waiting goroutine can never be stranded. A client that
//     disconnects while its request is pending costs nothing: its goroutine
//     simply receives the reply, finds the socket closed and exits.
//   - Close stops the goroutine; requests still queued get an error reply.
//
// Propagation: after each request the engine hands the commands that changed
// data (already rewritten to a replay-safe form) to a Propagator, which feeds
// the append-only file and replicas, and then asks it to commit (fsync).
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// ErrClosed is returned by calls made after Close.
var ErrClosed = errors.New("engine: closed")

// Reply texts the engine itself produces.
var (
	errShuttingDown = resp.Error("ERR server is shutting down")
	errReadOnly     = resp.Error("READONLY You can't write against a read only replica.")
	errOOM          = resp.Error("OOM command not allowed when used memory > 'maxmemory'.")
	errInternal     = resp.Error("ERR internal error while running the command")
)

// Propagator receives every state change, in order, on the engine goroutine.
type Propagator interface {
	// Propagate is called with one command that changed data. cmd is only
	// valid during the call; copy or encode it before returning.
	Propagate(cmd []string)
	// Commit is called once after a request's commands were propagated. A
	// durable implementation flushes and fsyncs here. An error makes the
	// request's replies error replies.
	Commit() error
}

// Options configure an Engine. Only DB is required.
type Options struct {
	DB  *store.DB
	Env *command.Env // optional; the engine fills in MaxMemory, Policy and Control

	Clock        func() time.Time // default time.Now
	QueueSize    int              // pending-request capacity (default 1024)
	TickInterval time.Duration    // active-expiry period (default 100ms)
	ExpireSample int              // keys sampled per expiry round (default 20)
	ExpireBudget time.Duration    // max real time per expiry cycle (default 5ms)

	MaxMemory    int64        // bytes, 0 = unlimited
	Policy       store.Policy // what to evict when over MaxMemory
	EvictSamples int          // keys sampled per eviction (default 5)

	Propagator Propagator // optional
	Logger     *slog.Logger
}

type mode int

const (
	modeClient  mode = iota // an ordinary client: all policies apply
	modeMaster              // applying the primary's stream on a replica
	modeLoading             // replaying persisted data: no policy, no propagation
)

type kind int

const (
	kindSingle kind = iota
	kindBatch
	kindFunc
)

type response struct {
	v   resp.Value
	vs  []resp.Value
	err error
}

type request struct {
	kind  kind
	mode  mode
	args  []string
	batch [][]string
	fn    func(*State)
	reply chan response // capacity 1, owned by the request for its whole life
}

// Engine is the single-owner command executor.
type Engine struct {
	opts Options
	log  *slog.Logger
	db   *store.DB
	cx   command.Ctx

	reqs   chan *request
	pool   sync.Pool
	stopCh chan struct{}
	doneCh chan struct{}

	mu     sync.RWMutex // guards closed; readers are submitters
	closed bool

	// Everything below is touched only by the engine goroutine.
	readOnly bool
	mode     mode
	pending  [][]string
}

// New creates an Engine and starts its goroutine. Call Close to stop it.
func New(opts Options) *Engine {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 1024
	}
	if opts.TickInterval <= 0 {
		opts.TickInterval = 100 * time.Millisecond
	}
	if opts.ExpireSample <= 0 {
		opts.ExpireSample = 20
	}
	if opts.ExpireBudget <= 0 {
		opts.ExpireBudget = 5 * time.Millisecond
	}
	if opts.EvictSamples <= 0 {
		opts.EvictSamples = 5
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Env == nil {
		opts.Env = &command.Env{}
	}

	e := &Engine{
		opts:   opts,
		log:    opts.Logger,
		db:     opts.DB,
		reqs:   make(chan *request, opts.QueueSize),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	e.pool.New = func() any { return &request{reply: make(chan response, 1)} }
	opts.Env.MaxMemory, opts.Env.Policy, opts.Env.Control = opts.MaxMemory, opts.Policy, e
	e.cx = command.Ctx{DB: e.db, Env: opts.Env}
	e.db.SetOnRemove(e.onRemove)

	go e.run()
	return e
}

// Close stops the engine goroutine and waits for it. Requests that were
// queued but not started receive an error reply. Close is idempotent.
func (e *Engine) Close() {
	e.mu.Lock()
	already := e.closed
	e.closed = true
	e.mu.Unlock()
	if !already {
		close(e.stopCh)
	}
	<-e.doneCh
}

// QueueDepth returns how many requests are waiting (for diagnostics).
func (e *Engine) QueueDepth() int { return len(e.reqs) }

// ---- the public, goroutine-safe API ----

// Execute runs one command and returns its reply. ctx only bounds the wait
// to get into the queue; once queued the command is always run (or refused
// at shutdown) and answered.
func (e *Engine) Execute(ctx context.Context, args []string) resp.Value {
	return e.executeMode(ctx, args, modeClient)
}

// ExecuteFromMaster runs a command received from the primary on a replica:
// read-only and maxmemory checks are skipped.
func (e *Engine) ExecuteFromMaster(ctx context.Context, args []string) resp.Value {
	return e.executeMode(ctx, args, modeMaster)
}

func (e *Engine) executeMode(ctx context.Context, args []string, m mode) resp.Value {
	r := e.pool.Get().(*request)
	r.kind, r.mode, r.args = kindSingle, m, args
	res, err := e.roundTrip(ctx, r)
	if err != nil {
		return submitError(err)
	}
	return res.v
}

// ExecuteAtomic runs cmds one after another with nothing from other clients
// in between (the basis of MULTI/EXEC) and returns one reply per command.
func (e *Engine) ExecuteAtomic(ctx context.Context, cmds [][]string) []resp.Value {
	r := e.pool.Get().(*request)
	r.kind, r.mode, r.batch = kindBatch, modeClient, cmds
	res, err := e.roundTrip(ctx, r)
	if err != nil {
		out := make([]resp.Value, len(cmds))
		for i := range out {
			out[i] = submitError(err)
		}
		return out
	}
	return res.vs
}

// Exec runs fn on the engine goroutine, giving it direct access to the DB.
// It is how snapshots, AOF loading and replication get a consistent view.
// fn must be quick; the whole server waits while it runs.
func (e *Engine) Exec(ctx context.Context, fn func(*State)) error {
	r := e.pool.Get().(*request)
	r.kind, r.mode, r.fn = kindFunc, modeClient, fn
	res, err := e.roundTrip(ctx, r)
	if err != nil {
		return err
	}
	return res.err
}

func submitError(err error) resp.Value {
	if errors.Is(err, ErrClosed) {
		return errShuttingDown
	}
	return resp.Error("ERR " + err.Error())
}

// roundTrip submits r and waits for its answer, then recycles r.
func (e *Engine) roundTrip(ctx context.Context, r *request) (response, error) {
	if err := e.submit(ctx, r); err != nil {
		e.recycle(r)
		return response{}, err
	}
	// Always wait: the engine answers every request it accepted.
	res := <-r.reply
	e.recycle(r)
	return res, nil
}

func (e *Engine) recycle(r *request) {
	r.args, r.batch, r.fn = nil, nil, nil
	e.pool.Put(r)
}

func (e *Engine) submit(ctx context.Context, r *request) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	select {
	case e.reqs <- r:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ---- the engine goroutine ----

func (e *Engine) run() {
	defer close(e.doneCh)
	ticker := time.NewTicker(e.opts.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case r := <-e.reqs:
			e.handle(r)
		case <-ticker.C:
			e.tick()
		case <-e.stopCh:
			// Answer everything still queued; submit() refuses new work
			// once closed is set, which happened before stopCh closed.
			for {
				select {
				case r := <-e.reqs:
					e.reject(r)
				default:
					return
				}
			}
		}
	}
}

func (e *Engine) reject(r *request) {
	switch r.kind {
	case kindSingle:
		r.reply <- response{v: errShuttingDown}
	case kindBatch:
		vs := make([]resp.Value, len(r.batch))
		for i := range vs {
			vs[i] = errShuttingDown
		}
		r.reply <- response{vs: vs}
	default:
		r.reply <- response{err: ErrClosed}
	}
}

// begin fixes "now" for everything the engine does next, so all commands of
// an atomic batch see the same instant.
func (e *Engine) begin(m mode) {
	e.db.SetNow(e.opts.Clock().UnixMilli())
	e.mode = m
}

func (e *Engine) handle(r *request) {
	var res response
	func() {
		defer func() {
			// A bug in one command must not take the whole server down.
			if p := recover(); p != nil {
				e.log.Error("panic in engine", "panic", p, "stack", string(debug.Stack()))
				res = e.panicResponse(r)
			}
		}()
		e.begin(r.mode)
		switch r.kind {
		case kindSingle:
			res.v = e.exec(r.args)
		case kindBatch:
			res.vs = make([]resp.Value, len(r.batch))
			for i, c := range r.batch {
				res.vs[i] = e.exec(c)
			}
		case kindFunc:
			r.fn(&State{e: e})
		}
	}()

	if err := e.commit(); err != nil {
		e.log.Error("persisting changes failed", "err", err)
		fail := resp.Error("MISCONF Errors writing to the append only file: " + err.Error())
		switch r.kind {
		case kindSingle:
			if res.v.Kind != resp.KindError {
				res.v = fail
			}
		case kindBatch:
			for i := range res.vs {
				if res.vs[i].Kind != resp.KindError {
					res.vs[i] = fail
				}
			}
		default:
			res.err = errors.Join(res.err, err)
		}
	}
	e.mode = modeClient
	r.reply <- res
}

func (e *Engine) panicResponse(r *request) response {
	e.pending = e.pending[:0] // do not log half-done work
	switch r.kind {
	case kindSingle:
		return response{v: errInternal}
	case kindBatch:
		vs := make([]resp.Value, len(r.batch))
		for i := range vs {
			vs[i] = errInternal
		}
		return response{vs: vs}
	}
	return response{err: fmt.Errorf("engine: panic while running function")}
}

// exec runs one command with the policies of the current mode.
func (e *Engine) exec(args []string) resp.Value {
	spec, errReply := command.Resolve(args)
	if spec == nil {
		return errReply
	}
	if e.mode == modeClient {
		if spec.Write && e.readOnly {
			return errReadOnly
		}
		if spec.DenyOOM {
			if bad := e.reclaim(); bad {
				return errOOM
			}
		}
	}
	e.cx.Reset(args)
	reply := spec.Run(&e.cx)
	if e.mode != modeLoading {
		e.pending = append(e.pending, e.cx.Propagation(spec, reply)...)
	}
	return reply
}

// reclaim frees memory until we are under maxmemory. It reports true if it
// could not, in which case the write must be refused.
func (e *Engine) reclaim() (oom bool) {
	limit := e.opts.MaxMemory
	if limit <= 0 {
		return false
	}
	for e.db.UsedMemory() > limit {
		if e.opts.Policy == store.NoEviction {
			return true
		}
		if _, ok := e.db.Evict(e.opts.Policy, e.opts.EvictSamples); !ok {
			return true
		}
	}
	return false
}

// onRemove is called by the DB when it deletes a key itself (expiry or
// eviction). Replicas and the AOF must see an explicit DEL, because they do
// not run their own expiry.
func (e *Engine) onRemove(key string) {
	if e.mode != modeLoading {
		e.pending = append(e.pending, []string{"DEL", key})
	}
}

// commit hands the pending commands to the Propagator and asks it to make
// them durable.
func (e *Engine) commit() error {
	defer func() {
		clear(e.pending)
		e.pending = e.pending[:0]
	}()
	p := e.opts.Propagator
	if p == nil || len(e.pending) == 0 {
		return nil
	}
	for _, c := range e.pending {
		p.Propagate(c)
	}
	return p.Commit()
}

// tick runs the periodic work: active expiry.
func (e *Engine) tick() {
	defer func() {
		if p := recover(); p != nil {
			e.log.Error("panic in engine tick", "panic", p, "stack", string(debug.Stack()))
		}
	}()
	e.begin(modeClient)
	e.db.ActiveExpire(e.opts.ExpireSample, e.opts.ExpireBudget)
	if err := e.commit(); err != nil {
		e.log.Error("persisting expiry deletions failed", "err", err)
	}
}

// SetReadOnly implements command.Control. It is called from commands, which
// run on the engine goroutine.
func (e *Engine) SetReadOnly(on bool) {
	e.readOnly = on
	e.db.SetReplica(on)
}

// State is the view of the engine given to Exec callbacks. It is valid only
// during the callback, on the engine goroutine.
type State struct{ e *Engine }

// DB returns the keyspace.
func (s *State) DB() *store.DB { return s.e.db }

// Apply runs a command the way persisted data is replayed: no read-only or
// maxmemory checks and nothing is logged again. It returns the reply, which
// loaders normally ignore.
func (s *State) Apply(args []string) resp.Value {
	saved := s.e.mode
	s.e.mode = modeLoading
	defer func() { s.e.mode = saved }()
	return s.e.exec(args)
}

// SetReadOnly turns replica mode on or off.
func (s *State) SetReadOnly(on bool) { s.e.SetReadOnly(on) }

// ReadOnly reports whether the engine is in replica mode.
func (s *State) ReadOnly() bool { return s.e.readOnly }
