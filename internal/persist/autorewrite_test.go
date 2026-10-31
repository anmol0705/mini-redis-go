package persist

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/aof"
	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/snapshot"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

func TestRewriteDue(t *testing.T) {
	tests := []struct {
		name               string
		current, base, min int64
		percentage         int
		want               bool
	}{
		{"grown past percentage and min size", 2001, 1000, 1500, 100, true},
		{"exactly at the threshold does not trigger", 2000, 1000, 1500, 100, false},
		{"below the percentage threshold", 1900, 1000, 1500, 100, false},
		{"below min size even though grown a lot", 900, 100, 1000, 100, false},
		{"exactly min size counts as reached", 1000, 0, 1000, 100, true},
		{"percentage 0 disables", 1 << 30, 100, 0, 0, false},
		{"base 0 uses only the min size (reached)", 5000, 0, 1000, 100, true},
		{"base 0 uses only the min size (not reached)", 999, 0, 1000, 100, false},
		{"50 percent", 1501, 1000, 0, 50, true},
		{"50 percent not reached", 1500, 1000, 0, 50, false},
		{"huge percentage cannot overflow", 1 << 62, 1 << 61, 0, 1 << 30, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rewriteDue(tt.current, tt.base, tt.min, tt.percentage); got != tt.want {
				t.Errorf("rewriteDue(%d, %d, %d, %d) = %v; want %v",
					tt.current, tt.base, tt.min, tt.percentage, got, tt.want)
			}
		})
	}
}

// ---- helpers ----

func aofPathOf(m *Manager) string { return m.aofPath }

func aofFileSize(t *testing.T, m *Manager) int64 {
	t.Helper()
	st, err := os.Stat(aofPathOf(m))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func logOf(m *Manager) *aof.Log { return m.aofLog.Load() }

// writeKeys runs n SET commands on one key, which grows the log by a known
// kind of amount while keeping the data tiny.
func writeKeys(e *engine.Engine, n int) {
	for i := range n {
		e.Execute(bg, []string{"SET", "churn", fmt.Sprint(i)})
	}
}

func waitRewriteDone(t *testing.T, m *Manager) {
	t.Helper()
	waitFor(t, "the rewrite to finish", func() bool { return !logOf(m).Rewriting() })
}

func infoField(t *testing.T, m *Manager, name string) string {
	t.Helper()
	s, _ := m.InfoSection("persistence")
	for line := range strings.SplitSeq(s, "\r\n") {
		if v, ok := strings.CutPrefix(line, name+":"); ok {
			return v
		}
	}
	t.Fatalf("INFO persistence has no %s:\n%s", name, s)
	return ""
}

// manualRewrite runs BGREWRITEAOF's code path and waits for it to finish.
func manualRewrite(t *testing.T, m *Manager, e *engine.Engine) {
	t.Helper()
	var err error
	_ = e.Exec(bg, func(st *engine.State) { _, err = m.RewriteAOF(st.DB()) })
	if err != nil {
		t.Fatal(err)
	}
	waitRewriteDone(t, m)
}

func autoCfg(dir string, minSize int64, pct int) Config {
	return Config{
		Dir: dir, AppendOnly: true, Fsync: aof.FsyncNo,
		AOFRewritePercentage: pct, AOFRewriteMinSize: minSize,
		AOFRewriteRetry: time.Hour,
	}
}

// ---- the trigger ----

func TestAutoRewriteTriggersWhenPastMinSizeAndPercentage(t *testing.T) {
	dir := t.TempDir()
	m, e := newManager(t, autoCfg(dir, 1000, 100))
	writeKeys(e, 100)
	before := aofFileSize(t, m)
	if before < 1000 {
		t.Fatalf("test setup: log is only %d bytes", before)
	}
	if !m.autoRewriteCheck() {
		t.Fatal("check reported the engine gone")
	}
	waitRewriteDone(t, m)

	if got := m.autoRewrites.Load(); got != 1 {
		t.Fatalf("automatic rewrites = %d; want 1", got)
	}
	if got := infoField(t, m, "aof_rewrites_auto"); got != "1" {
		t.Errorf("INFO aof_rewrites_auto = %s", got)
	}
	after := aofFileSize(t, m)
	if after >= before/10 {
		t.Errorf("log went from %d to %d bytes; expected a big reduction", before, after)
	}
	if cur, base := logOf(m).Size(), logOf(m).BaseSize(); cur != after || base != after {
		t.Errorf("after the rewrite size = %d, base = %d; both should equal the file size %d", cur, base, after)
	}
}

func TestAutoRewriteDoesNotTrigger(t *testing.T) {
	tests := []struct {
		name    string
		minSize int64
		pct     int
	}{
		{"below min size", 1 << 20, 100},
		{"percentage 0 disables it", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, e := newManager(t, autoCfg(t.TempDir(), tt.minSize, tt.pct))
			writeKeys(e, 100)
			m.StartScheduler() // with percentage 0 this must start nothing
			for range 3 {
				m.autoRewriteCheck()
			}
			if got := m.autoRewrites.Load(); got != 0 {
				t.Errorf("automatic rewrites = %d; want 0", got)
			}
			if logOf(m).Rewriting() {
				t.Error("a rewrite is running")
			}
		})
	}
}

func TestAutoRewriteNeedsGrowthBeyondThePercentage(t *testing.T) {
	m, e := newManager(t, autoCfg(t.TempDir(), 1, 100))
	for i := range 20 {
		e.Execute(bg, []string{"SET", fmt.Sprint("key", i), "value"})
	}
	manualRewrite(t, m, e)
	base := logOf(m).BaseSize()
	if base == 0 || base != aofFileSize(t, m) {
		t.Fatalf("base after the manual rewrite = %d, file = %d", base, aofFileSize(t, m))
	}

	// Grow to 1.5x the base: past neither threshold of 100%.
	for logOf(m).Size() <= base+base/2 {
		e.Execute(bg, []string{"SET", "churn", "x"})
	}
	if logOf(m).Size() > 2*base {
		t.Fatalf("test setup overshot: size %d, base %d", logOf(m).Size(), base)
	}
	m.autoRewriteCheck()
	if got := m.autoRewrites.Load(); got != 0 {
		t.Fatalf("triggered at 1.5x the base size (rewrites = %d)", got)
	}

	// Now past 2x.
	for logOf(m).Size() <= 2*base {
		e.Execute(bg, []string{"SET", "churn", "x"})
	}
	m.autoRewriteCheck()
	waitRewriteDone(t, m)
	if got := m.autoRewrites.Load(); got != 1 {
		t.Fatalf("automatic rewrites = %d; want 1", got)
	}
}

func TestAutoRewriteNeverStartsASecondRewrite(t *testing.T) {
	m, e := newManager(t, autoCfg(t.TempDir(), 1000, 100))
	writeKeys(e, 100)

	started, release := make(chan struct{}), make(chan struct{})
	stepDone := make(chan struct{})
	go func() {
		defer close(stepDone)
		_ = e.Exec(bg, func(st *engine.State) {
			if _, err := m.RewriteAOF(st.DB()); err != nil {
				t.Errorf("RewriteAOF: %v", err)
			}
			close(started)
			<-release // hold the engine: the rewrite stays "running"
		})
	}()
	<-started
	if !logOf(m).Rewriting() {
		t.Fatal("test setup: no rewrite is running")
	}
	for range 3 {
		m.autoRewriteCheck() // due by size, but a rewrite is running
	}
	close(release)
	<-stepDone
	waitRewriteDone(t, m)
	if got := m.autoRewrites.Load(); got != 0 {
		t.Errorf("automatic rewrites = %d while a manual one was running; want 0", got)
	}
}

// A failed rewrite is not retried until the retry delay has passed.
func TestAutoRewriteFailureIsNotRetriedInATightLoop(t *testing.T) {
	dir := t.TempDir()
	m, e := newManager(t, autoCfg(dir, 1000, 100))
	writeKeys(e, 100)
	tmp := filepath.Join(dir, aof.FileName+".rewrite")
	if err := os.Mkdir(tmp, 0o755); err != nil { // a directory in the way: the rewrite fails
		t.Fatal(err)
	}

	m.autoRewriteCheck()
	waitRewriteDone(t, m)
	if m.rewriteFailedAt.Load() == 0 {
		t.Fatal("the failure was not recorded")
	}
	for range 5 {
		m.autoRewriteCheck()
	}
	if got := m.autoRewrites.Load(); got != 1 {
		t.Fatalf("automatic rewrites = %d after a failure; want exactly the 1 that failed", got)
	}

	// After the retry delay (simulated) and with the obstacle gone, it works.
	_ = os.RemoveAll(tmp)
	m.rewriteFailedAt.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	m.autoRewriteCheck()
	waitRewriteDone(t, m)
	if got := m.autoRewrites.Load(); got != 2 {
		t.Fatalf("automatic rewrites = %d after the retry delay; want 2", got)
	}
	if size := aofFileSize(t, m); size >= 1000 {
		t.Errorf("log is still %d bytes after the successful retry", size)
	}
}

// The ticker-driven goroutine calls the same check. Two ticks on an
// unbuffered channel guarantee the first one has been fully handled.
func TestAutoRewriteSchedulerRunsOnTicks(t *testing.T) {
	ticks := make(chan time.Time)
	cfg := autoCfg(t.TempDir(), 1000, 100)
	cfg.AOFRewriteTicks = func() (<-chan time.Time, func()) { return ticks, func() {} }
	m, e := newManager(t, cfg)
	writeKeys(e, 100)

	m.StartScheduler()
	for range 2 {
		select {
		case ticks <- time.Now():
		case <-time.After(10 * time.Second):
			t.Fatal("the scheduler is not reading ticks")
		}
	}
	waitFor(t, "the automatic rewrite", func() bool { return m.autoRewrites.Load() == 1 })
	waitRewriteDone(t, m)
	if got := infoField(t, m, "aof_rewrites_auto"); got != "1" {
		t.Errorf("INFO aof_rewrites_auto = %s", got)
	}
}

// ---- base size ----

func TestBaseSizeAfterStartup(t *testing.T) {
	tornTail := func(t *testing.T, dir string) {
		f, err := os.OpenFile(filepath.Join(dir, aof.FileName), os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		_, _ = f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nk") // a command cut off mid-write
	}
	fromSnapshot := func(t *testing.T, dir string) {
		_ = os.Remove(filepath.Join(dir, aof.FileName))
		items := []store.Item{{Key: "a", Kind: store.KindString, Str: "1"}, {Key: "b", Kind: store.KindString, Str: "2"}}
		if err := snapshot.WriteFile(filepath.Join(dir, snapshot.DefaultFileName), items); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		damage func(t *testing.T, dir string)
	}{
		{"clean restart", func(*testing.T, string) {}},
		{"torn tail is truncated first", tornTail},
		{"started from a snapshot", fromSnapshot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			first, e := newManager(t, autoCfg(dir, 1<<30, 100))
			writeKeys(e, 50)
			e.Close()
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			tt.damage(t, dir)

			second, _ := newManager(t, autoCfg(dir, 1<<30, 100))
			size := aofFileSize(t, second)
			if size == 0 {
				t.Fatal("the restarted log is empty")
			}
			if cur, base := logOf(second).Size(), logOf(second).BaseSize(); cur != size || base != size {
				t.Errorf("after startup size = %d, base = %d; file is %d bytes", cur, base, size)
			}
		})
	}
}

func TestBaseSizeAfterManualRewriteAndGrowthAfterwards(t *testing.T) {
	m, e := newManager(t, autoCfg(t.TempDir(), 1<<30, 100))
	writeKeys(e, 100)
	if logOf(m).BaseSize() != 0 {
		t.Fatalf("base of a new empty log = %d; want 0", logOf(m).BaseSize())
	}
	manualRewrite(t, m, e)
	after := aofFileSize(t, m)
	if logOf(m).BaseSize() != after || logOf(m).Size() != after {
		t.Errorf("after BGREWRITEAOF size = %d, base = %d, file = %d", logOf(m).Size(), logOf(m).BaseSize(), after)
	}
	e.Execute(bg, []string{"SET", "more", "data"})
	if logOf(m).BaseSize() != after || logOf(m).Size() <= after {
		t.Errorf("a later write must grow size but not base: size = %d, base = %d", logOf(m).Size(), logOf(m).BaseSize())
	}
	if got, want := infoField(t, m, "aof_base_size"), fmt.Sprint(after); got != want {
		t.Errorf("INFO aof_base_size = %s; want %s", got, want)
	}
	if got, want := infoField(t, m, "aof_current_size"), fmt.Sprint(aofFileSize(t, m)); got != want {
		t.Errorf("INFO aof_current_size = %s; want %s", got, want)
	}
}

func TestInfoAOFSizesAreZeroWithoutAOF(t *testing.T) {
	m, _ := newManager(t, Config{Dir: t.TempDir()})
	for _, f := range []string{"aof_current_size", "aof_base_size", "aof_rewrites_auto"} {
		if got := infoField(t, m, f); got != "0" {
			t.Errorf("%s = %s without an AOF; want 0", f, got)
		}
	}
}

func TestNewRejectsNegativeRewriteSettings(t *testing.T) {
	for name, cfg := range map[string]Config{
		"percentage": {Dir: t.TempDir(), AOFRewritePercentage: -1},
		"min size":   {Dir: t.TempDir(), AOFRewriteMinSize: -1},
	} {
		if _, err := New(cfg, store.New(nil)); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

// ---- lifecycle ----

func TestCloseStopsTheAutoRewriteScheduler(t *testing.T) {
	baseline := runtime.NumGoroutine()
	cfg := autoCfg(t.TempDir(), 1000, 100)
	cfg.Logger = quiet
	cfg.Fsync = aof.FsyncEverySec // also starts the log's own syncer goroutine
	db := store.New(nil)
	m, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(engine.Options{DB: db, Propagator: m, Logger: quiet})
	m.Attach(e)
	if err := m.Load(bg); err != nil {
		t.Fatal(err)
	}
	m.StartScheduler() // real one-second ticker
	writeKeys(e, 10)

	e.Close()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "all goroutines to exit", func() bool { return runtime.NumGoroutine() <= baseline })
}
