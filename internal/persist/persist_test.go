package persist

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/aof"
	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/snapshot"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// newManager builds a Manager attached to a running engine, as app does.
func newManager(t *testing.T, cfg Config) (*Manager, *engine.Engine) {
	t.Helper()
	cfg.Logger = quiet
	db := store.New(nil)
	m, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(engine.Options{DB: db, Propagator: m, Logger: quiet})
	m.Attach(e)
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(); _ = m.Close() })
	return m, e
}

var bg = context.Background()

func TestNewValidation(t *testing.T) {
	dir := t.TempDir()
	for name, cfg := range map[string]Config{
		"negative interval": {Dir: dir, SnapshotInterval: -time.Second},
		"path as filename":  {Dir: dir, DBFilename: "sub/dump"},
		"dir is a file": func() Config {
			f := filepath.Join(dir, "afile")
			_ = os.WriteFile(f, nil, 0o644)
			return Config{Dir: f}
		}(),
	} {
		if _, err := New(cfg, store.New(nil)); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
	m, err := New(Config{Dir: dir}, store.New(nil))
	if err != nil {
		t.Fatal(err)
	}
	if m.cfg.DBFilename != snapshot.DefaultFileName || m.cfg.Fsync != aof.FsyncEverySec {
		t.Errorf("defaults not applied: %+v", m.cfg)
	}
	_ = m.Close()
}

func TestInfoSectionOnlyForPersistence(t *testing.T) {
	m, _ := newManager(t, Config{Dir: t.TempDir()})
	if _, ok := m.InfoSection("replication"); ok {
		t.Error("answered a section that is not its own")
	}
	if s, ok := m.InfoSection("persistence"); !ok || !strings.Contains(s, "aof_enabled:0") {
		t.Errorf("persistence section = %q, %v", s, ok)
	}
}

func TestSaveFailsWhileBackgroundSaveRuns(t *testing.T) {
	m, e := newManager(t, Config{Dir: t.TempDir()})
	m.bgSaving.Store(true) // pretend a BGSAVE is in flight
	var saveErr, bgErr error
	_ = e.Exec(bg, func(st *engine.State) {
		saveErr = m.Save(st.DB())
		_, bgErr = m.BGSave(st.DB())
	})
	for name, err := range map[string]error{"SAVE": saveErr, "BGSAVE": bgErr} {
		if err == nil || !strings.Contains(err.Error(), "already in progress") {
			t.Errorf("%s during a background save: %v", name, err)
		}
	}
	m.bgSaving.Store(false)
}

// A snapshot that cannot be written must be reported, not hidden, and must
// leave the server able to try again.
func TestSnapshotWriteFailures(t *testing.T) {
	dir := t.TempDir()
	m, e := newManager(t, Config{Dir: dir})
	// A directory where the snapshot file should go makes the final rename fail.
	if err := os.Mkdir(filepath.Join(dir, snapshot.DefaultFileName), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshot.DefaultFileName, "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	var saveErr error
	_ = e.Exec(bg, func(st *engine.State) { saveErr = m.Save(st.DB()) })
	if saveErr == nil {
		t.Error("SAVE reported success although the file could not be replaced")
	}

	_ = e.Exec(bg, func(st *engine.State) {
		if _, err := m.BGSave(st.DB()); err != nil {
			t.Errorf("BGSAVE refused to start: %v", err)
		}
	})
	waitFor(t, "the failed background save to be recorded", func() bool {
		s, _ := m.InfoSection("persistence")
		return strings.Contains(s, "rdb_last_bgsave_status:err") && strings.Contains(s, "rdb_bgsave_in_progress:0")
	})
}

func TestRewriteNeedsAOFAndRejectsConcurrentRewrite(t *testing.T) {
	// Without an AOF.
	m, e := newManager(t, Config{Dir: t.TempDir()})
	_ = e.Exec(bg, func(st *engine.State) {
		if _, err := m.RewriteAOF(st.DB()); err == nil || !strings.Contains(err.Error(), "not enabled") {
			t.Errorf("rewrite without an AOF: %v", err)
		}
	})

	// With one: starting twice inside ONE engine step is deterministic - the
	// first begins, the second must be refused.
	m2, e2 := newManager(t, Config{Dir: t.TempDir(), AppendOnly: true, Fsync: aof.FsyncNo})
	var first, second error
	_ = e2.Exec(bg, func(st *engine.State) {
		_, first = m2.RewriteAOF(st.DB())
		_, second = m2.RewriteAOF(st.DB())
	})
	if first != nil || second == nil || !strings.Contains(second.Error(), "already in progress") {
		t.Errorf("first = %v, second = %v", first, second)
	}
	waitFor(t, "the rewrite to finish", func() bool {
		s, _ := m2.InfoSection("persistence")
		return strings.Contains(s, "aof_rewrite_in_progress:0")
	})
}

// A rewrite whose new file cannot be created is abandoned cleanly: the log
// keeps working and a later rewrite succeeds.
func TestFailedRewriteIsAbortedCleanly(t *testing.T) {
	dir := t.TempDir()
	m, e := newManager(t, Config{Dir: dir, AppendOnly: true, Fsync: aof.FsyncAlways})
	tmp := filepath.Join(dir, aof.FileName+".rewrite")
	if err := os.Mkdir(tmp, 0o755); err != nil { // a directory in the way
		t.Fatal(err)
	}

	_ = e.Exec(bg, func(st *engine.State) {
		if _, err := m.RewriteAOF(st.DB()); err != nil {
			t.Errorf("RewriteAOF: %v", err)
		}
	})
	waitFor(t, "the failed rewrite to be aborted", func() bool {
		s, _ := m.InfoSection("persistence")
		return strings.Contains(s, "aof_rewrite_in_progress:0")
	})
	if r := e.Execute(bg, []string{"SET", "after", "failed-rewrite"}); r.Str != "OK" {
		t.Errorf("writes broken after a failed rewrite: %+v", r)
	}

	// The abort removed whatever was at the temp path, so a retry can succeed.
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Errorf("temp path still exists after the abort: %v", err)
	}
	_ = e.Exec(bg, func(st *engine.State) {
		if _, err := m.RewriteAOF(st.DB()); err != nil {
			t.Errorf("second RewriteAOF: %v", err)
		}
	})
	waitFor(t, "the second rewrite to finish", func() bool {
		s, _ := m.InfoSection("persistence")
		return strings.Contains(s, "aof_rewrite_in_progress:0")
	})
	data, _ := os.ReadFile(filepath.Join(dir, aof.FileName))
	if !strings.Contains(string(data), "failed-rewrite") {
		t.Errorf("the write made after the failed rewrite is missing from the log: %q", data)
	}
}

// ResetAOF (used by a replica after a full resync) replaces the log's content.
func TestResetAOF(t *testing.T) {
	dir := t.TempDir()
	m, e := newManager(t, Config{Dir: dir, AppendOnly: true, Fsync: aof.FsyncAlways})
	e.Execute(bg, []string{"SET", "old", "1"})

	items := []store.Item{{Key: "fresh", Kind: store.KindString, Str: "2"}}
	var err error
	_ = e.Exec(bg, func(*engine.State) { err = m.ResetAOF(items) })
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, aof.FileName))
	if strings.Contains(string(data), "old") || !strings.Contains(string(data), "fresh") {
		t.Errorf("log after ResetAOF = %q", data)
	}

	// Without an AOF it does nothing and succeeds.
	m2, e2 := newManager(t, Config{Dir: t.TempDir()})
	_ = e2.Exec(bg, func(*engine.State) { err = m2.ResetAOF(items) })
	if err != nil {
		t.Errorf("ResetAOF without an AOF: %v", err)
	}
}

func TestLoadRejectsBadSnapshotAndKeepsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, snapshot.DefaultFileName)
	if err := os.WriteFile(path, []byte("not a snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	db := store.New(nil)
	m, err := New(Config{Dir: dir, Logger: quiet}, db)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(engine.Options{DB: db, Logger: quiet})
	defer e.Close()
	m.Attach(e)
	if err := m.Load(bg); err == nil || !strings.Contains(err.Error(), "refusing to start") {
		t.Errorf("Load = %v", err)
	}
	if b, _ := os.ReadFile(path); string(b) != "not a snapshot" {
		t.Error("the bad snapshot file was modified")
	}
	_ = m.Close()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
