// Package persist ties the append-only file and snapshots to the engine.
//
// Design around the single-owner engine (decision 007):
//
//   - The engine goroutine is the only writer of the AOF: after each request it
//     calls Propagate for every changing command and then Commit, which may
//     fsync before the reply is released (appendfsync always).
//   - SAVE runs inside the engine and so blocks it while the file is written.
//   - BGSAVE copies the keyspace inside the engine (store.DB.Dump, a brief
//     pause that is measured and logged) and encodes and writes that copy in a
//     background goroutine. Go cannot fork, so a copy replaces Redis's
//     copy-on-write child process.
//   - BGREWRITEAOF starts capturing new commands, copies the keyspace, writes
//     the compacted file in the background, and then, back inside the engine,
//     appends the captured commands and swaps the file in atomically. Writes
//     made during the rewrite are therefore never lost.
//
// Concurrency: Manager's methods named in engine.Propagator and command.Hooks
// run on the engine goroutine. Background goroutines (BGSAVE writer, rewrite
// writer, snapshot scheduler) are owned by the Manager's WaitGroup; Close
// stops and waits for all of them.
package persist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/aof"
	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/snapshot"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// Config selects what is persisted and where.
type Config struct {
	// Dir holds the files; it is created if missing.
	Dir string
	// AppendOnly enables the append-only file.
	AppendOnly bool
	// Fsync is the AOF fsync policy (default everysec).
	Fsync aof.Policy
	// DBFilename is the snapshot file name inside Dir (default dump.mrdb).
	DBFilename string
	// SnapshotInterval, if positive, takes a background snapshot that often
	// and one more at shutdown.
	SnapshotInterval time.Duration

	Logger *slog.Logger
	// AOFTicks and SnapshotTicks replace the real timers in tests.
	AOFTicks      func() (<-chan time.Time, func())
	SnapshotTicks func() (<-chan time.Time, func())
}

// Manager owns persistence for one node.
type Manager struct {
	cfg      Config
	log      *slog.Logger
	db       *store.DB // engine-owned; read directly only after the engine stopped
	eng      *engine.Engine
	aofLog   atomic.Pointer[aof.Log] // set once by Load; read by the engine goroutine
	aofPath  string
	snapPath string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	bgSaving     atomic.Bool
	lastSaveUnix atomic.Int64
	lastCopyUS   atomic.Int64
	statusMu     sync.Mutex
	lastBGStatus string
}

// New prepares a Manager for db. Call Attach, then Load, before serving.
func New(cfg Config, db *store.DB) (*Manager, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DBFilename == "" {
		cfg.DBFilename = snapshot.DefaultFileName
	}
	if cfg.Fsync == 0 {
		cfg.Fsync = aof.FsyncEverySec
	}
	if cfg.Dir == "" {
		cfg.Dir = "."
	}
	if cfg.SnapshotInterval < 0 {
		return nil, errors.New("persist: snapshot interval must not be negative")
	}
	if filepath.Base(cfg.DBFilename) != cfg.DBFilename {
		return nil, fmt.Errorf("persist: -dbfilename %q must be a file name, not a path", cfg.DBFilename)
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("persist: create data directory: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg:          cfg,
		log:          cfg.Logger,
		db:           db,
		aofPath:      filepath.Join(cfg.Dir, aof.FileName),
		snapPath:     filepath.Join(cfg.Dir, cfg.DBFilename),
		ctx:          ctx,
		cancel:       cancel,
		lastBGStatus: "ok",
	}, nil
}

// Attach gives the Manager the engine it runs background steps through.
func (m *Manager) Attach(e *engine.Engine) { m.eng = e }

func fileSize(path string) (int64, bool) {
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return 0, false
	}
	return st.Size(), true
}

// Load restores the data at startup, before any client is served.
//
//   - AOF enabled and a non-empty AOF exists: replay it. The AOF wins over a
//     snapshot if both exist (it is the more recent record).
//   - Otherwise, if a snapshot exists: load it, and with AOF enabled write a
//     fresh AOF from it so logging continues from a complete base.
//   - A torn final AOF record (crash while writing) is cut off with a warning;
//     damage anywhere else makes Load fail so we never start on bad data.
//
// Finally it opens the AOF for appending.
func (m *Manager) Load(ctx context.Context) error {
	aofSize, haveAOF := fileSize(m.aofPath)
	haveAOF = haveAOF && aofSize > 0
	_, haveSnap := fileSize(m.snapPath)

	switch {
	case m.cfg.AppendOnly && haveAOF:
		if haveSnap {
			m.log.Info("both an append-only file and a snapshot exist; loading the append-only file (it wins)",
				"aof", m.aofPath, "snapshot", m.snapPath)
		}
		if err := m.loadAOF(ctx); err != nil {
			return err
		}
	case haveSnap:
		if err := m.loadSnapshot(ctx); err != nil {
			return err
		}
	default:
		if haveAOF && !m.cfg.AppendOnly {
			m.log.Warn("an append-only file exists but -appendonly is off; its data was NOT loaded", "path", m.aofPath)
		}
	}

	if !m.cfg.AppendOnly {
		return nil
	}
	l, err := aof.Open(aof.Options{
		Policy: m.cfg.Fsync, Path: m.aofPath, Ticks: m.cfg.AOFTicks, Logger: m.log,
	})
	if err != nil {
		return err
	}
	m.aofLog.Store(l)
	return nil
}

func (m *Manager) loadAOF(ctx context.Context) error {
	f, err := os.Open(m.aofPath)
	if err != nil {
		return fmt.Errorf("persist: open append-only file: %w", err)
	}
	defer f.Close()

	var (
		valid, count int64
		torn         bool
		rerr         error
	)
	start := time.Now()
	execErr := m.eng.Exec(ctx, func(st *engine.State) {
		valid, torn, rerr = aof.Replay(f, func(args []string) error {
			if reply := st.Apply(args); reply.Kind == resp.KindError {
				return fmt.Errorf("%s", reply.Str)
			}
			count++
			return nil
		})
	})
	if execErr != nil {
		return fmt.Errorf("persist: replay: %w", execErr)
	}
	if rerr != nil {
		return fmt.Errorf("refusing to start: the append-only file %s is damaged: %w "+
			"(fix or remove the file, or restore it from a backup)", m.aofPath, rerr)
	}
	if torn {
		size, _ := fileSize(m.aofPath)
		m.log.Warn("the append-only file ends with an incomplete command (probably a crash during a write); truncating it",
			"path", m.aofPath, "keeping_bytes", valid, "dropping_bytes", size-valid)
		if err := aof.TruncateTo(m.aofPath, valid); err != nil {
			return fmt.Errorf("persist: truncate torn append-only file: %w", err)
		}
	}
	m.log.Info("append-only file loaded", "commands", count, "elapsed", time.Since(start))
	return nil
}

func (m *Manager) loadSnapshot(ctx context.Context) error {
	items, err := snapshot.ReadFile(m.snapPath)
	if err != nil {
		return fmt.Errorf("refusing to start: cannot load snapshot: %w", err)
	}
	var loadErr error
	if err := m.eng.Exec(ctx, func(st *engine.State) { loadErr = st.DB().Load(items) }); err != nil {
		return err
	}
	if loadErr != nil {
		return fmt.Errorf("refusing to start: snapshot contents rejected: %w", loadErr)
	}
	m.lastSaveUnix.Store(time.Now().Unix())
	m.log.Info("snapshot loaded", "keys", len(items), "path", m.snapPath)

	if m.cfg.AppendOnly {
		// Start the AOF from the loaded data, so replaying it later yields
		// the same state.
		if err := m.writeBaseAOF(items); err != nil {
			return err
		}
	}
	return nil
}

// writeBaseAOF writes a new AOF containing exactly items.
func (m *Manager) writeBaseAOF(items []store.Item) error {
	tmp := m.aofPath + ".base"
	if err := writeAOFFile(tmp, items); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, m.aofPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("persist: install base append-only file: %w", err)
	}
	return nil
}

// writeAOFFile writes the compacted form of items to path and fsyncs it.
func writeAOFFile(path string, items []store.Item) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("persist: create %s: %w", path, err)
	}
	err = aof.WriteItems(f, items)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// ---- engine.Propagator ----

// Propagate logs one changing command (engine goroutine).
func (m *Manager) Propagate(cmd []string) {
	if l := m.aofLog.Load(); l != nil {
		l.Propagate(cmd)
	}
}

// Commit makes the logged commands durable per the fsync policy (engine goroutine).
func (m *Manager) Commit() error {
	if l := m.aofLog.Load(); l != nil {
		return l.Commit()
	}
	return nil
}

// ---- command.Hooks (engine goroutine) ----

var errBGSaveRunning = errors.New("Background save already in progress")

// Save writes a snapshot synchronously. It blocks the engine, and so every
// client, until the file is on disk.
func (m *Manager) Save(db *store.DB) error {
	if m.bgSaving.Load() {
		return errBGSaveRunning
	}
	start := time.Now()
	items := db.Dump()
	if err := snapshot.WriteFile(m.snapPath, items); err != nil {
		return err
	}
	m.lastSaveUnix.Store(time.Now().Unix())
	m.log.Info("snapshot saved", "keys", len(items), "elapsed", time.Since(start))
	return nil
}

// BGSave copies the keyspace and writes the copy in the background. The copy
// is the only pause; its duration is measured and logged.
func (m *Manager) BGSave(db *store.DB) (string, error) {
	if !m.bgSaving.CompareAndSwap(false, true) {
		return "", errBGSaveRunning
	}
	start := time.Now()
	items := db.Dump()
	pause := time.Since(start)
	m.lastCopyUS.Store(pause.Microseconds())
	m.log.Info("background save: keyspace copied", "keys", len(items), "engine_pause", pause)

	m.wg.Go(func() {
		defer m.bgSaving.Store(false)
		begin := time.Now()
		err := snapshot.WriteFile(m.snapPath, items)
		m.statusMu.Lock()
		defer m.statusMu.Unlock()
		if err != nil {
			m.lastBGStatus = "err"
			m.log.Error("background save failed", "err", err)
			return
		}
		m.lastBGStatus = "ok"
		m.lastSaveUnix.Store(time.Now().Unix())
		m.log.Info("background save finished", "keys", len(items), "write_time", time.Since(begin))
	})
	return "Background saving started", nil
}

// RewriteAOF compacts the append-only file in the background without losing
// writes that arrive meanwhile.
func (m *Manager) RewriteAOF(db *store.DB) (string, error) {
	l := m.aofLog.Load()
	if l == nil {
		return "", errors.New("the append-only file is not enabled (-appendonly)")
	}
	if err := l.BeginRewrite(); err != nil {
		return "", errors.New("Background append only file rewriting already in progress")
	}
	start := time.Now()
	items := db.Dump() // same engine step as BeginRewrite: snapshot + capture = full state
	m.log.Info("aof rewrite: keyspace copied", "keys", len(items), "engine_pause", time.Since(start))

	tmp := m.aofPath + ".rewrite"
	m.wg.Go(func() { m.finishRewrite(tmp, items) })
	return "Background append only file rewriting started", nil
}

// finishRewrite runs in a background goroutine.
func (m *Manager) finishRewrite(tmp string, items []store.Item) {
	if err := writeAOFFile(tmp, items); err != nil {
		m.log.Error("aof rewrite failed while writing the new file", "err", err)
		m.abortRewrite(tmp)
		return
	}
	var ferr error
	err := m.eng.Exec(m.ctx, func(*engine.State) { ferr = m.aofLog.Load().FinishRewrite(tmp) })
	switch {
	case err != nil:
		// The engine is gone or we are shutting down: nothing more to merge.
		m.log.Warn("aof rewrite abandoned", "err", err)
		m.abortRewrite(tmp)
	case ferr != nil:
		m.log.Error("aof rewrite failed while installing the new file", "err", ferr)
	default:
		m.log.Info("aof rewrite finished")
	}
}

// abortRewrite discards a rewrite from outside the engine goroutine.
func (m *Manager) abortRewrite(tmp string) {
	err := m.eng.Exec(m.ctx, func(*engine.State) { m.aofLog.Load().AbortRewrite(tmp) })
	if err != nil {
		// The engine stopped. Wait until it has really exited so the log's
		// engine-only state can be touched safely.
		<-m.eng.Done()
		m.aofLog.Load().AbortRewrite(tmp)
	}
}

// ResetAOF replaces the append-only file's contents with exactly items. A
// replica calls it (on the engine goroutine) after a full resync replaced its
// data, so that replaying the file later gives the same state. It fails if a
// background rewrite is running; the caller retries.
func (m *Manager) ResetAOF(items []store.Item) error {
	l := m.aofLog.Load()
	if l == nil {
		return nil
	}
	if err := l.BeginRewrite(); err != nil {
		return err
	}
	tmp := m.aofPath + ".rewrite"
	if err := writeAOFFile(tmp, items); err != nil {
		l.AbortRewrite(tmp)
		return err
	}
	return l.FinishRewrite(tmp)
}

// InfoSection supplies the "persistence" section of INFO.
func (m *Manager) InfoSection(name string) (string, bool) {
	if name != "persistence" {
		return "", false
	}
	m.statusMu.Lock()
	status := m.lastBGStatus
	m.statusMu.Unlock()
	b2i := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	l := m.aofLog.Load()
	rewriting := l != nil && l.Rewriting()
	return fmt.Sprintf("# Persistence\r\nloading:0\r\nrdb_bgsave_in_progress:%d\r\nrdb_last_save_time:%d\r\n"+
		"rdb_last_bgsave_status:%s\r\nrdb_last_bgsave_copy_usec:%d\r\naof_enabled:%d\r\naof_fsync:%s\r\n"+
		"aof_rewrite_in_progress:%d\r\n",
		b2i(m.bgSaving.Load()), m.lastSaveUnix.Load(), status, m.lastCopyUS.Load(),
		b2i(m.cfg.AppendOnly), m.cfg.Fsync, b2i(rewriting)), true
}

// ---- lifecycle ----

// StartScheduler begins periodic background snapshots if an interval was
// configured. The goroutine is stopped by Close.
func (m *Manager) StartScheduler() {
	if m.cfg.SnapshotInterval <= 0 {
		return
	}
	var ticks <-chan time.Time
	release := func() {}
	if m.cfg.SnapshotTicks != nil {
		ticks, release = m.cfg.SnapshotTicks()
	} else {
		t := time.NewTicker(m.cfg.SnapshotInterval)
		ticks, release = t.C, t.Stop
	}
	m.wg.Go(func() {
		defer release()
		for {
			select {
			case <-ticks:
				err := m.eng.Exec(m.ctx, func(st *engine.State) {
					if _, err := m.BGSave(st.DB()); err != nil && !errors.Is(err, errBGSaveRunning) {
						m.log.Error("scheduled snapshot failed", "err", err)
					}
				})
				if err != nil {
					return // engine closed or shutting down
				}
			case <-m.ctx.Done():
				return
			}
		}
	})
}

// Close stops the background goroutines, takes a final snapshot if snapshots
// are scheduled, and flushes, fsyncs and closes the append-only file. Call it
// after the engine has been closed.
func (m *Manager) Close() error {
	m.cancel()
	m.wg.Wait()
	var errs []error
	if m.cfg.SnapshotInterval > 0 {
		// The engine has stopped, so the DB can be read directly.
		items := m.db.Dump()
		if err := snapshot.WriteFile(m.snapPath, items); err != nil {
			errs = append(errs, fmt.Errorf("final snapshot: %w", err))
		} else {
			m.log.Info("final snapshot saved", "keys", len(items))
		}
	}
	if l := m.aofLog.Load(); l != nil {
		errs = append(errs, l.Close())
	}
	return errors.Join(errs...)
}
