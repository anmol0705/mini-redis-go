package aof

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeFile records writes and counts Sync calls.
type fakeFile struct {
	mu      sync.Mutex
	data    bytes.Buffer
	syncs   int
	syncErr error
	closed  bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data.Write(p)
}
func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return f.syncErr
}
func (f *fakeFile) Close() error { f.mu.Lock(); f.closed = true; f.mu.Unlock(); return nil }
func (f *fakeFile) syncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncs
}
func (f *fakeFile) setSyncErr(err error) { f.mu.Lock(); f.syncErr = err; f.mu.Unlock() }
func (f *fakeFile) bytes() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.data.Bytes())
}

// manualTicks gives the test control of the "once per second" trigger.
func manualTicks() (chan time.Time, func() (<-chan time.Time, func())) {
	ch := make(chan time.Time)
	return ch, func() (<-chan time.Time, func()) { return ch, func() {} }
}

func TestParsePolicy(t *testing.T) {
	for _, p := range []Policy{FsyncAlways, FsyncEverySec, FsyncNo} {
		got, err := ParsePolicy(p.String())
		if err != nil || got != p {
			t.Errorf("round trip of %v = %v, %v", p, got, err)
		}
	}
	if got, _ := ParsePolicy("ALWAYS"); got != FsyncAlways {
		t.Error("policy names must be case-insensitive")
	}
	if _, err := ParsePolicy("sometimes"); err == nil {
		t.Error("ParsePolicy accepted nonsense")
	}
	if Policy(0).String() != "unknown" {
		t.Error("zero Policy name")
	}
}

// ---- fsync policy behaviour (decision 007) ----

func TestFsyncAlwaysSyncsOnEveryCommit(t *testing.T) {
	f := &fakeFile{}
	l := New(f, Options{Policy: FsyncAlways, Logger: quiet})
	defer l.Close()

	for i := 1; i <= 3; i++ {
		l.Propagate([]string{"SET", "k", fmt.Sprint(i)})
		if err := l.Commit(); err != nil {
			t.Fatal(err)
		}
		if got := f.syncCount(); got != i {
			t.Fatalf("after commit %d: %d syncs; want %d", i, got, i)
		}
	}
	// Several commands in one commit share one fsync (group commit).
	l.Propagate([]string{"SET", "a", "1"})
	l.Propagate([]string{"SET", "b", "2"})
	l.Commit()
	if got := f.syncCount(); got != 4 {
		t.Errorf("two commands in one commit used %d syncs total; want 4", got)
	}
	if !bytes.Contains(f.bytes(), []byte("SET")) {
		t.Error("nothing written")
	}
}

func TestFsyncAlwaysReportsFailure(t *testing.T) {
	f := &fakeFile{syncErr: errors.New("disk on fire")}
	l := New(f, Options{Policy: FsyncAlways, Logger: quiet})
	defer l.Close()
	l.Propagate([]string{"SET", "k", "v"})
	if err := l.Commit(); err == nil || !strings.Contains(err.Error(), "disk on fire") {
		t.Errorf("Commit = %v; want the fsync error", err)
	}
}

func TestFsyncNoNeverSyncs(t *testing.T) {
	f := &fakeFile{}
	l := New(f, Options{Policy: FsyncNo, Logger: quiet})
	for range 5 {
		l.Propagate([]string{"SET", "k", "v"})
		l.Commit()
	}
	if got := f.syncCount(); got != 0 {
		t.Errorf("%d syncs under appendfsync no", got)
	}
	if !bytes.Contains(f.bytes(), []byte("SET")) {
		t.Error("data must still be written to the file")
	}
	l.Close() // Close does sync once, which is not a per-commit sync
}

func TestEverySecSyncsOncePerTickOnlyWhenDirty(t *testing.T) {
	f := &fakeFile{}
	ticks, mk := manualTicks()
	l := New(f, Options{Policy: FsyncEverySec, Ticks: mk, Logger: quiet})
	defer l.Close()

	for range 10 {
		l.Propagate([]string{"SET", "k", "v"})
		l.Commit()
	}
	if got := f.syncCount(); got != 0 {
		t.Fatalf("everysec synced %d times inside commits; want 0", got)
	}

	ticks <- time.Now() // the syncer takes it...
	ticks <- time.Now() // ...and has finished the first before accepting this one
	if got := f.syncCount(); got != 1 {
		t.Errorf("after two ticks with one burst of writes: %d syncs; want 1 (the clean tick must not sync)", got)
	}

	l.Propagate([]string{"SET", "k", "v2"})
	l.Commit()
	ticks <- time.Now()
	ticks <- time.Now()
	if got := f.syncCount(); got != 2 {
		t.Errorf("after more writes and ticks: %d syncs; want 2", got)
	}
}

func TestEverySecBackgroundErrorSurfacesAndClears(t *testing.T) {
	f := &fakeFile{}
	ticks, mk := manualTicks()
	l := New(f, Options{Policy: FsyncEverySec, Ticks: mk, Logger: quiet})
	defer l.Close()

	f.setSyncErr(errors.New("no space left"))
	l.Propagate([]string{"SET", "k", "v"})
	if err := l.Commit(); err != nil {
		t.Fatalf("first Commit = %v (the failure has not happened yet)", err)
	}
	ticks <- time.Now()
	ticks <- time.Now() // the second send proves the first sync finished

	l.Propagate([]string{"SET", "k", "v"})
	if err := l.Commit(); err == nil || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("Commit after a failed background fsync = %v; want the error", err)
	}

	f.setSyncErr(nil) // the disk recovers; the syncer retries on its next tick
	ticks <- time.Now()
	ticks <- time.Now()
	l.Propagate([]string{"SET", "k", "v"})
	if err := l.Commit(); err != nil {
		t.Errorf("Commit after recovery = %v", err)
	}
}

func TestCloseSyncsClosesAndStopsTheSyncer(t *testing.T) {
	before := runtime.NumGoroutine()
	f := &fakeFile{}
	_, mk := manualTicks()
	l := New(f, Options{Policy: FsyncEverySec, Ticks: mk, Logger: quiet})
	l.Propagate([]string{"SET", "k", "v"})
	// Not committed: Close must still flush the buffered command.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if !f.closed || f.syncCount() != 1 || !bytes.Contains(f.bytes(), []byte("SET")) {
		t.Errorf("closed=%v syncs=%d data=%q", f.closed, f.syncCount(), f.bytes())
	}
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if runtime.NumGoroutine() > before {
		t.Error("syncer goroutine still running after Close")
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	l := New(&fakeFile{}, Options{Policy: FsyncNo, Logger: quiet})
	l.Close()
	l.Propagate([]string{"SET", "k", "v"})
	if err := l.Commit(); err == nil {
		t.Error("Commit after Close succeeded")
	}
}

// ---- replay ----

func encodeAll(cmds [][]string) []byte {
	var b []byte
	for _, c := range cmds {
		b = resp.AppendCommand(b, c)
	}
	return b
}

func replayAll(data []byte) (cmds [][]string, valid int64, torn bool, err error) {
	valid, torn, err = Replay(bytes.NewReader(data), func(a []string) error {
		cmds = append(cmds, slices.Clone(a))
		return nil
	})
	return
}

func TestReplayWholeFile(t *testing.T) {
	want := [][]string{{"SET", "a", "1"}, {"LPUSH", "l", "x", "y"}, {"DEL", "a"}}
	got, valid, torn, err := replayAll(encodeAll(want))
	if err != nil || torn || valid != int64(len(encodeAll(want))) || !reflect.DeepEqual(got, want) {
		t.Errorf("got %v valid=%d torn=%v err=%v", got, valid, torn, err)
	}
	if _, valid, torn, err := replayAll(nil); valid != 0 || torn || err != nil {
		t.Errorf("empty file: valid=%d torn=%v err=%v", valid, torn, err)
	}
}

// Whatever byte a crash cuts the file at, replay must recover exactly the
// complete commands before it and report a torn tail iff the cut is not on a
// command boundary.
func TestReplayTornTailAtEveryOffset(t *testing.T) {
	cmds := [][]string{
		{"SET", "key", "value"},
		{"RPUSH", "list", "a", "b", "c"},
		{"SET", "", ""},
		{"HSET", "h", "f", strings.Repeat("v", 300)},
	}
	full := encodeAll(cmds)
	var boundaries []int
	off := 0
	for _, c := range cmds {
		off += resp.EncodedLen(c)
		boundaries = append(boundaries, off)
	}

	for cut := 0; cut <= len(full); cut++ {
		got, valid, torn, err := replayAll(full[:cut])
		if err != nil {
			t.Fatalf("cut %d: unexpected error %v", cut, err)
		}
		wantN, wantValid := 0, 0
		for i, b := range boundaries {
			if b <= cut {
				wantN, wantValid = i+1, b
			}
		}
		if len(got) != wantN || valid != int64(wantValid) {
			t.Fatalf("cut %d: recovered %d commands valid=%d; want %d and %d", cut, len(got), valid, wantN, wantValid)
		}
		if torn != (cut != wantValid) {
			t.Fatalf("cut %d: torn=%v; want %v", cut, torn, cut != wantValid)
		}
	}
}

func TestReplayCorruptionInTheMiddleIsAnError(t *testing.T) {
	good1 := encodeAll([][]string{{"SET", "a", "1"}})
	good2 := encodeAll([][]string{{"SET", "b", "2"}})
	data := append(append(slices.Clone(good1), []byte("this is not resp\r\n")...), good2...)

	var applied int
	valid, torn, err := Replay(bytes.NewReader(data), func([]string) error { applied++; return nil })
	var ce *CorruptError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v; want *CorruptError", err)
	}
	if ce.Offset != int64(len(good1)) || valid != int64(len(good1)) || torn || applied != 1 {
		t.Errorf("offset=%d valid=%d torn=%v applied=%d", ce.Offset, valid, torn, applied)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(len(good1))) {
		t.Errorf("error text %q lacks the offset", err)
	}
}

func TestReplayStopsWhenApplyFails(t *testing.T) {
	data := encodeAll([][]string{{"A"}, {"B"}, {"C"}})
	boom := errors.New("boom")
	n := 0
	_, _, err := Replay(bytes.NewReader(data), func([]string) error {
		n++
		if n == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || n != 2 {
		t.Errorf("err=%v after %d commands", err, n)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func TestReplayReportsIOErrors(t *testing.T) {
	if _, _, err := Replay(failingReader{}, func([]string) error { return nil }); err == nil || strings.Contains(err.Error(), "corrupt") {
		t.Errorf("err = %v; want a plain I/O error", err)
	}
}

// ---- compaction ----

// apply runs commands against a DB the way startup replay does.
func applyTo(t *testing.T, db *store.DB, cmds [][]string) {
	t.Helper()
	x := &command.Ctx{DB: db, Env: &command.Env{}}
	for _, c := range cmds {
		if r := command.Execute(x, c); r.Kind == resp.KindError {
			t.Fatalf("%v failed: %s", c, r.Str)
		}
	}
}

func sampleDB(now int64) *store.DB {
	db := store.New(rand.New(rand.NewPCG(3, 4)))
	db.SetNow(now)
	db.Set("str", "hello", 0, false)
	db.Set("str-ttl", "v", now+60_000, false)
	db.Set("empty-str", "", 0, false)
	db.Push("list", false, "a", "b", "c")
	db.ExpireAt("list", now+120_000)
	db.HSet("hash", []string{"f1", "v1", "f2", "v2"})
	db.SAdd("set", []string{"x", "y", "z"})
	db.ZAdd("zset", []store.ZMember{{Member: "low", Score: -1.5}, {Member: "pi", Score: math.Pi}, {Member: "top", Score: math.Inf(1)}, {Member: "bottom", Score: math.Inf(-1)}})
	db.ExpireAt("zset", now+9_000)
	return db
}

func TestWriteItemsRoundTrip(t *testing.T) {
	const now = 1_700_000_000_000
	src := sampleDB(now)
	// A big collection exercises chunking.
	for i := range 700 {
		src.SAdd("bigset", []string{fmt.Sprint("m", i)})
		src.HSet("bighash", []string{fmt.Sprint("f", i), fmt.Sprint("v", i)})
		src.Push("biglist", false, fmt.Sprint("e", i))
		src.ZAdd("bigz", []store.ZMember{{Member: fmt.Sprint("z", i), Score: float64(i) / 3}})
	}

	var buf bytes.Buffer
	if err := WriteItems(&buf, src.Dump()); err != nil {
		t.Fatal(err)
	}

	dst := store.New(nil)
	dst.SetNow(now)
	_, torn, err := Replay(&buf, func(a []string) error {
		applyTo(t, dst, [][]string{a})
		return nil
	})
	if err != nil || torn {
		t.Fatalf("replay: torn=%v err=%v", torn, err)
	}
	if !reflect.DeepEqual(store.Canonical(dst.Dump()), store.Canonical(src.Dump())) {
		t.Error("data after replaying the compacted file differs from the original")
	}
}

func TestWriteItemsIsCompact(t *testing.T) {
	db := store.New(nil)
	db.SetNow(1000)
	for range 1000 {
		db.IncrBy("counter", 1) // 1000 operations, one value
	}
	var buf bytes.Buffer
	if err := WriteItems(&buf, db.Dump()); err != nil {
		t.Fatal(err)
	}
	cmds, _, _, _ := replayAll(buf.Bytes())
	if len(cmds) != 1 || cmds[0][0] != "SET" || cmds[0][2] != "1000" {
		t.Errorf("compacted = %v; want a single SET counter 1000", cmds)
	}
}

func TestWriteItemsRejectsBadItem(t *testing.T) {
	if err := WriteItems(io.Discard, []store.Item{{Key: "k", Kind: 0}}); err == nil {
		t.Error("WriteItems accepted an item of unknown kind")
	}
}

func newFileLog(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := Open(Options{Policy: FsyncNo, Path: filepath.Join(dir, FileName), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func commitCmds(t *testing.T, l *Log, cmds ...[]string) {
	t.Helper()
	for _, c := range cmds {
		l.Propagate(c)
	}
	if err := l.Commit(); err != nil {
		t.Fatal(err)
	}
}

func replayFile(t *testing.T, path string) *store.DB {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	db := store.New(nil)
	db.SetNow(1_700_000_000_000)
	_, torn, err := Replay(bytes.NewReader(data), func(a []string) error {
		applyTo(t, db, [][]string{a})
		return nil
	})
	if err != nil || torn {
		t.Fatalf("replay of %s: torn=%v err=%v", path, torn, err)
	}
	return db
}

// Writes that arrive while the rewrite is being produced must survive it.
func TestRewriteKeepsWritesMadeDuringIt(t *testing.T) {
	dir := t.TempDir()
	l := newFileLog(t, dir)
	path := filepath.Join(dir, FileName)

	const now = 1_700_000_000_000
	// History: many writes, most of them superseded.
	live := store.New(nil)
	live.SetNow(now)
	history := [][]string{{"SET", "a", "1"}, {"SET", "a", "2"}, {"RPUSH", "l", "x"}, {"SET", "gone", "1"}, {"DEL", "gone"}}
	for _, c := range history {
		commitCmds(t, l, c)
	}
	applyTo(t, live, history)
	sizeBefore := fileSize(t, path)

	// Engine step: snapshot the data and start capturing.
	items := live.Dump()
	if err := l.BeginRewrite(); err != nil {
		t.Fatal(err)
	}
	if err := l.BeginRewrite(); !errors.Is(err, ErrRewriteInProgress) {
		t.Errorf("second BeginRewrite = %v", err)
	}
	if !l.Rewriting() {
		t.Error("Rewriting() = false")
	}

	// Writes that happen while the background goroutine writes the new file.
	during := [][]string{{"SET", "during", "yes"}, {"RPUSH", "l", "y"}, {"DEL", "a"}}
	for _, c := range during {
		commitCmds(t, l, c)
	}
	applyTo(t, live, during)

	// Background step: write the compacted file.
	tmp := path + ".rewrite"
	f, err := os.Create(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteItems(f, items); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Engine step: finish.
	if err := l.FinishRewrite(tmp); err != nil {
		t.Fatal(err)
	}
	if l.Rewriting() {
		t.Error("still Rewriting() after FinishRewrite")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("temp file left behind")
	}

	// Writes after the swap go to the new file.
	after := [][]string{{"SET", "after", "swap"}}
	commitCmds(t, l, after...)
	applyTo(t, live, after)
	l.Close()

	got := replayFile(t, path)
	if !reflect.DeepEqual(store.Canonical(got.Dump()), store.Canonical(live.Dump())) {
		t.Errorf("after rewrite and replay:\n got %+v\nwant %+v", store.Canonical(got.Dump()), store.Canonical(live.Dump()))
	}
	for _, key := range []string{"during", "after", "l"} {
		if !got.Exists(key) {
			t.Errorf("%q was lost across the rewrite", key)
		}
	}
	if got.Exists("a") || got.Exists("gone") {
		t.Error("deleted keys came back")
	}
	if fileSize(t, path) >= sizeBefore+int64(len(encodeAll(during))+len(encodeAll(after))) {
		t.Errorf("file did not shrink: before %d, after %d", sizeBefore, fileSize(t, path))
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestAbortRewriteDiscardsCaptureAndTempFile(t *testing.T) {
	dir := t.TempDir()
	l := newFileLog(t, dir)
	path := filepath.Join(dir, FileName)
	commitCmds(t, l, []string{"SET", "a", "1"})

	tmp := path + ".rewrite"
	os.WriteFile(tmp, []byte("partial"), 0o644)
	if err := l.BeginRewrite(); err != nil {
		t.Fatal(err)
	}
	commitCmds(t, l, []string{"SET", "b", "2"})
	l.AbortRewrite(tmp)

	if l.Rewriting() {
		t.Error("Rewriting() after abort")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("temp file not removed")
	}
	commitCmds(t, l, []string{"SET", "c", "3"})
	l.Close()
	db := replayFile(t, path)
	if db.Len() != 3 {
		t.Errorf("log lost data after an aborted rewrite: %d keys", db.Len())
	}
	// A new rewrite can start.
	l2 := newFileLog(t, t.TempDir())
	if err := l2.BeginRewrite(); err != nil {
		t.Error(err)
	}
}

func TestFinishRewriteFailureKeepsOldLog(t *testing.T) {
	dir := t.TempDir()
	l := newFileLog(t, dir)
	path := filepath.Join(dir, FileName)
	commitCmds(t, l, []string{"SET", "a", "1"})
	l.BeginRewrite()

	// The compacted file does not exist, so finishing must fail cleanly.
	if err := l.FinishRewrite(path + ".missing"); err == nil {
		t.Fatal("FinishRewrite succeeded without a temp file")
	}
	if l.Rewriting() {
		t.Error("still rewriting after a failed finish")
	}
	commitCmds(t, l, []string{"SET", "b", "2"})
	l.Close()
	if db := replayFile(t, path); db.Len() != 2 {
		t.Errorf("old log damaged: %d keys", db.Len())
	}
}

func TestFinishRewriteNeedsPath(t *testing.T) {
	l := New(&fakeFile{}, Options{Policy: FsyncNo, Logger: quiet})
	defer l.Close()
	l.BeginRewrite()
	if err := l.FinishRewrite("x"); err == nil {
		t.Error("FinishRewrite without a path succeeded")
	}
}

func TestOpenFailure(t *testing.T) {
	if _, err := Open(Options{Path: filepath.Join(t.TempDir(), "no", "such", "dir", FileName)}); err == nil {
		t.Error("Open in a missing directory succeeded")
	}
}

func TestTruncateTo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	os.WriteFile(path, []byte("0123456789"), 0o644)
	if err := TruncateTo(path, 4); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "0123" {
		t.Errorf("file = %q", b)
	}
}
