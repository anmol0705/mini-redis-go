package app

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/aof"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/testclient"
)

// testNode is a node whose lifetime the test controls, so it can be stopped
// and started again on the same directory.
type testNode struct {
	*Node
	addr string
	stop func() error
}

func startAt(t *testing.T, clock *fakeClock, mod func(*Config)) (*testNode, error) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", Logger: quiet, Clock: clock.Now, Version: "test"}
	if mod != nil {
		mod(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n, err := Start(ctx, cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	var once sync.Once
	var stopErr error
	tn := &testNode{Node: n, addr: n.Addr().String()}
	tn.stop = func() error {
		once.Do(func() { cancel(); stopErr = n.Wait() })
		return stopErr
	}
	t.Cleanup(func() { _ = tn.stop() })
	return tn, nil
}

func mustStart(t *testing.T, clock *fakeClock, mod func(*Config)) *testNode {
	t.Helper()
	n, err := startAt(t, clock, mod)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return n
}

func withDir(dir string, extra func(*Config)) func(*Config) {
	return func(c *Config) {
		c.Dir = dir
		if extra != nil {
			extra(c)
		}
	}
}

func aofOn(policy string) func(*Config) {
	return func(c *Config) { c.AppendOnly = true; c.AppendFsync = policy }
}

// populate writes every data type, some with relative expiries.
func populate(t *testing.T, c *testclient.Client) {
	t.Helper()
	for _, cmd := range [][]string{
		{"SET", "str", "hello"},
		{"SET", "ttl", "v", "EX", "100"},
		{"INCRBY", "counter", "41"},
		{"INCR", "counter"},
		{"RPUSH", "list", "a", "b", "c"},
		{"LPOP", "list"},
		{"HSET", "hash", "f1", "v1", "f2", "v2"},
		{"HDEL", "hash", "f2"},
		{"SADD", "set", "x", "y", "z"},
		{"SREM", "set", "y"},
		{"ZADD", "zset", "1.5", "a", "2.5", "b", "-3", "c"},
		{"EXPIRE", "list", "500"},
		{"SET", "doomed", "x"},
		{"DEL", "doomed"},
	} {
		if got := c.Cmd(t, cmd...); strings.HasPrefix(got, "-") {
			t.Fatalf("%v = %s", cmd, got)
		}
	}
}

// verify checks the state produced by populate, `elapsed` after it was made.
func verify(t *testing.T, c *testclient.Client, elapsed time.Duration) {
	t.Helper()
	checks := []struct {
		args []string
		want string
	}{
		{[]string{"GET", "str"}, "$hello"},
		{[]string{"GET", "counter"}, "$42"},
		{[]string{"LRANGE", "list", "0", "-1"}, "*[$b $c]"},
		{[]string{"HGETALL", "hash"}, "*[$f1 $v1]"},
		{[]string{"SMEMBERS", "set"}, ""}, // order unspecified: checked below
		{[]string{"ZRANGE", "zset", "0", "-1", "WITHSCORES"}, "*[$c $-3 $a $1.5 $b $2.5]"},
		{[]string{"EXISTS", "doomed"}, ":0"},
		{[]string{"DBSIZE"}, ":7"},
	}
	for _, ch := range checks {
		got := c.Cmd(t, ch.args...)
		if ch.want != "" && got != ch.want {
			t.Errorf("%v = %s; want %s", ch.args, got, ch.want)
		}
	}
	if got := c.Reply(t, "SMEMBERS", "set").Strings(); len(got) != 2 {
		t.Errorf("SMEMBERS = %v", got)
	}
	// Relative expiries must still be measured from the ORIGINAL time.
	wantTTL := 100 - int64(elapsed/time.Second)
	if got := c.Reply(t, "TTL", "ttl").Int; got != wantTTL {
		t.Errorf("TTL ttl = %d; want %d", got, wantTTL)
	}
	if got := c.Reply(t, "TTL", "list").Int; got != 500-int64(elapsed/time.Second) {
		t.Errorf("TTL list = %d", got)
	}
}

func TestAOFRestartKeepsEverything(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			dir, clock := t.TempDir(), newClock()
			a := mustStart(t, clock, withDir(dir, aofOn(policy)))
			populate(t, testclient.MustDial(t, a.addr))
			if err := a.stop(); err != nil {
				t.Fatal(err)
			}

			clock.Advance(30 * time.Second) // time passes while the server is down
			b := mustStart(t, clock, withDir(dir, aofOn(policy)))
			verify(t, testclient.MustDial(t, b.addr), 30*time.Second)
		})
	}
}

func TestSnapshotRestartKeepsEverything(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, nil))
	c := testclient.MustDial(t, a.addr)
	populate(t, c)
	if got := c.Cmd(t, "SAVE"); got != "+OK" {
		t.Fatalf("SAVE = %s", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "dump.mrdb")); err != nil {
		t.Fatalf("no snapshot file: %v", err)
	}
	if err := a.stop(); err != nil {
		t.Fatal(err)
	}

	clock.Advance(10 * time.Second)
	b := mustStart(t, clock, withDir(dir, nil))
	verify(t, testclient.MustDial(t, b.addr), 10*time.Second)
}

func TestBGSaveWritesSnapshotInBackground(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, nil))
	c := testclient.MustDial(t, a.addr)
	populate(t, c)

	if got := c.Cmd(t, "BGSAVE"); got != "+Background saving started" {
		t.Fatalf("BGSAVE = %s", got)
	}
	waitUntil(t, "BGSAVE to finish", func() bool {
		info := c.Reply(t, "INFO", "persistence").Str
		return strings.Contains(info, "rdb_bgsave_in_progress:0") && strings.Contains(info, "rdb_last_bgsave_status:ok") &&
			!strings.Contains(info, "rdb_last_save_time:0\r")
	})
	if info := c.Reply(t, "INFO", "persistence").Str; strings.Contains(info, "rdb_last_bgsave_copy_usec:0\r") {
		t.Logf("copy pause rounded to 0us: %q", info)
	}
	if err := a.stop(); err != nil {
		t.Fatal(err)
	}
	b := mustStart(t, clock, withDir(dir, nil))
	verify(t, testclient.MustDial(t, b.addr), 0)
}

func TestSaveWithCustomFilename(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, func(c *Config) { c.DBFilename = "custom.snap" }))
	c := testclient.MustDial(t, a.addr)
	c.Cmd(t, "SET", "k", "v")
	c.Cmd(t, "SAVE")
	if _, err := os.Stat(filepath.Join(dir, "custom.snap")); err != nil {
		t.Errorf("custom snapshot name not used: %v", err)
	}
	if err := a.stop(); err != nil {
		t.Fatal(err)
	}
	b := mustStart(t, clock, withDir(dir, func(c *Config) { c.DBFilename = "custom.snap" }))
	if got := testclient.MustDial(t, b.addr).Cmd(t, "GET", "k"); got != "$v" {
		t.Errorf("GET = %s", got)
	}
}

func TestAOFWinsOverSnapshot(t *testing.T) {
	dir, clock := t.TempDir(), newClock()

	// Step 1: an old snapshot containing only "old".
	a := mustStart(t, clock, withDir(dir, nil))
	c := testclient.MustDial(t, a.addr)
	c.Cmd(t, "SET", "from", "snapshot")
	c.Cmd(t, "SAVE")
	a.stop()

	// Step 2: a node with AOF on starts from that snapshot, then writes more.
	b := mustStart(t, clock, withDir(dir, aofOn("always")))
	cb := testclient.MustDial(t, b.addr)
	if got := cb.Cmd(t, "GET", "from"); got != "$snapshot" {
		t.Fatalf("snapshot not used as the base: %s", got)
	}
	cb.Cmd(t, "SET", "from", "aof")
	cb.Cmd(t, "SET", "only-in-aof", "1")
	b.stop()

	// Step 3: both files exist; the AOF must win.
	n := mustStart(t, clock, withDir(dir, aofOn("always")))
	cn := testclient.MustDial(t, n.addr)
	if got := cn.Cmd(t, "GET", "from"); got != "$aof" {
		t.Errorf("GET from = %s; the snapshot won over the AOF", got)
	}
	if got := cn.Cmd(t, "GET", "only-in-aof"); got != "$1" {
		t.Errorf("GET only-in-aof = %s", got)
	}
}

func TestAOFOffIgnoresAOFFile(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	testclient.MustDial(t, a.addr).Cmd(t, "SET", "k", "v")
	a.stop()

	b := mustStart(t, clock, withDir(dir, nil)) // -appendonly no
	if got := testclient.MustDial(t, b.addr).Cmd(t, "GET", "k"); got != "$nil" {
		t.Errorf("AOF data loaded although -appendonly is off: %s", got)
	}
}

func TestExpiredDuringDowntime(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	c := testclient.MustDial(t, a.addr)
	c.Cmd(t, "SET", "short", "v", "EX", "10")
	c.Cmd(t, "SET", "long", "v", "EX", "1000")
	a.stop()

	clock.Advance(60 * time.Second)
	b := mustStart(t, clock, withDir(dir, aofOn("always")))
	cb := testclient.MustDial(t, b.addr)
	if got := cb.Cmd(t, "EXISTS", "short"); got != ":0" {
		t.Errorf("expired key came back: %s", got)
	}
	if got := cb.Cmd(t, "TTL", "long"); got != ":940" {
		t.Errorf("TTL long = %s; want :940", got)
	}
}

func TestFlushAllIsPersisted(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	c := testclient.MustDial(t, a.addr)
	c.Cmd(t, "SET", "a", "1")
	c.Cmd(t, "FLUSHALL")
	c.Cmd(t, "SET", "b", "2")
	a.stop()
	b := mustStart(t, clock, withDir(dir, aofOn("always")))
	cb := testclient.MustDial(t, b.addr)
	if cb.Cmd(t, "EXISTS", "a") != ":0" || cb.Cmd(t, "GET", "b") != "$2" {
		t.Error("FLUSHALL not replayed correctly")
	}
}

// appendfsync always: the command is in the file before the client has its OK.
func TestAlwaysPolicyWritesBeforeAck(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	c := testclient.MustDial(t, a.addr)
	if got := c.Cmd(t, "SET", "durable", "yes"); got != "+OK" {
		t.Fatal(got)
	}
	data, err := os.ReadFile(filepath.Join(dir, aof.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, resp.AppendCommand(nil, []string{"SET", "durable", "yes"})) {
		t.Errorf("acknowledged write is not in the file: %q", data)
	}
}

// A copy of the AOF taken while the server runs is what a crash would leave.
func TestCrashRecoveryFromCopiedAOF(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	c := testclient.MustDial(t, a.addr)
	const n = 200
	for i := range n {
		if got := c.Cmd(t, "SET", fmt.Sprint("key", i), fmt.Sprint("val", i)); got != "+OK" {
			t.Fatal(got)
		}
	}

	crashDir := t.TempDir()
	data, err := os.ReadFile(filepath.Join(dir, aof.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(crashDir, aof.FileName), data, 0o644); err != nil {
		t.Fatal(err)
	}

	b := mustStart(t, clock, withDir(crashDir, aofOn("always")))
	cb := testclient.MustDial(t, b.addr)
	if got := cb.Cmd(t, "DBSIZE"); got != fmt.Sprintf(":%d", n) {
		t.Errorf("DBSIZE after recovery = %s; want %d", got, n)
	}
	if got := cb.Cmd(t, "GET", "key199"); got != "$val199" {
		t.Errorf("GET key199 = %s", got)
	}
}

func TestTornTailIsTruncatedAndLogged(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	path := filepath.Join(dir, aof.FileName)
	good := resp.AppendCommand(nil, []string{"SET", "a", "1"})
	good = resp.AppendCommand(good, []string{"SET", "b", "2"})
	torn := resp.AppendCommand(nil, []string{"SET", "c", "3333333"})
	// Cut the third command in half: a crash in the middle of a write.
	if err := os.WriteFile(path, append(append([]byte(nil), good...), torn[:len(torn)-6]...), 0o644); err != nil {
		t.Fatal(err)
	}

	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	c := testclient.MustDial(t, a.addr)
	if got := c.Cmd(t, "DBSIZE"); got != ":2" {
		t.Errorf("DBSIZE = %s; want the two complete commands", got)
	}
	if got := c.Cmd(t, "EXISTS", "c"); got != ":0" {
		t.Errorf("half-written command was applied: %s", got)
	}
	// The file was cut back to the last complete command...
	if st, _ := os.Stat(path); st.Size() != int64(len(good)) {
		t.Errorf("file size %d; want it truncated to %d", st.Size(), len(good))
	}
	// ...so new writes are appended to a clean file and survive another restart.
	c.Cmd(t, "SET", "d", "4")
	a.stop()
	b := mustStart(t, clock, withDir(dir, aofOn("always")))
	cb := testclient.MustDial(t, b.addr)
	if got := cb.Cmd(t, "DBSIZE"); got != ":3" {
		t.Errorf("DBSIZE after second restart = %s; want 3", got)
	}
}

func TestCorruptionInTheMiddleFailsStartup(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	path := filepath.Join(dir, aof.FileName)
	data := resp.AppendCommand(nil, []string{"SET", "a", "1"})
	data = append(data, []byte("garbage that is not resp\r\n")...)
	data = resp.AppendCommand(data, []string{"SET", "b", "2"})
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := startAt(t, clock, withDir(dir, aofOn("always")))
	if err == nil {
		t.Fatal("server started on a corrupt append-only file")
	}
	for _, want := range []string{"refusing to start", aof.FileName, "damaged", "byte"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	// The file must be left untouched for the operator to inspect.
	if after, _ := os.ReadFile(path); !bytes.Equal(after, data) {
		t.Error("the corrupt file was modified")
	}
}

func TestAOFWithFailingCommandFailsStartup(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	data := resp.AppendCommand(nil, []string{"NOSUCHCOMMAND", "x"})
	os.WriteFile(filepath.Join(dir, aof.FileName), data, 0o644)
	_, err := startAt(t, clock, withDir(dir, aofOn("always")))
	if err == nil || !strings.Contains(err.Error(), "NOSUCHCOMMAND") {
		t.Errorf("err = %v; want a failure naming the bad command", err)
	}
}

func TestCorruptSnapshotFailsStartup(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, nil))
	c := testclient.MustDial(t, a.addr)
	c.Cmd(t, "SET", "k", "v")
	c.Cmd(t, "SAVE")
	a.stop()

	path := filepath.Join(dir, "dump.mrdb")
	data, _ := os.ReadFile(path)
	data[len(data)/2] ^= 0xff
	os.WriteFile(path, data, 0o644)

	_, err := startAt(t, clock, withDir(dir, nil))
	if err == nil || !strings.Contains(err.Error(), "refusing to start") || !strings.Contains(err.Error(), "corrupt") {
		t.Errorf("err = %v", err)
	}
}

// BGREWRITEAOF while clients keep writing: nothing may be lost.
func TestRewriteUnderLoadLosesNothing(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("everysec")))
	c := testclient.MustDial(t, a.addr)
	for i := range 2000 {
		c.Cmd(t, "SET", fmt.Sprint("seed", i), "v")
	}
	// Churn: many operations on few keys, so compaction has work to do.
	for range 3000 {
		c.Cmd(t, "INCR", "churn")
	}
	sizeBefore, _ := os.Stat(filepath.Join(dir, aof.FileName))

	var written atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		w, err := testclient.Dial(a.addr)
		if err != nil {
			t.Error(err)
			return
		}
		defer w.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, err := w.Do("INCR", "during")
			if err != nil || r.Kind != ':' {
				t.Errorf("INCR during rewrite = %v, %v", r, err)
				return
			}
			written.Store(r.Int)
		}
	})

	if got := c.Cmd(t, "BGREWRITEAOF"); !strings.HasPrefix(got, "+Background append only file rewriting started") {
		t.Fatalf("BGREWRITEAOF = %s", got)
	}
	if got := c.Cmd(t, "BGREWRITEAOF"); !strings.HasPrefix(got, "-ERR") {
		t.Logf("second BGREWRITEAOF = %s (rewrite may already have finished)", got)
	}
	waitUntil(t, "the rewrite to finish", func() bool {
		return strings.Contains(c.Reply(t, "INFO", "persistence").Str, "aof_rewrite_in_progress:0")
	})
	close(stop)
	wg.Wait()
	total := written.Load()
	if total == 0 {
		t.Log("no writes landed during the rewrite window; the test still checks integrity")
	}
	sizeAfter, _ := os.Stat(filepath.Join(dir, aof.FileName))
	t.Logf("AOF %d -> %d bytes; %d increments during the rewrite", sizeBefore.Size(), sizeAfter.Size(), total)
	if err := a.stop(); err != nil {
		t.Fatal(err)
	}

	b := mustStart(t, clock, withDir(dir, aofOn("everysec")))
	cb := testclient.MustDial(t, b.addr)
	if got := cb.Cmd(t, "GET", "churn"); got != "$3000" {
		t.Errorf("churn = %s; want 3000", got)
	}
	if got := cb.Cmd(t, "GET", "during"); total > 0 && got != fmt.Sprintf("$%d", total) {
		t.Errorf("during = %s; want $%d (writes made during the rewrite were lost)", got, total)
	}
	if got := cb.Cmd(t, "DBSIZE"); got != ":2002" {
		t.Errorf("DBSIZE = %s", got)
	}
}

func TestRewriteShrinksTheFile(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	a := mustStart(t, clock, withDir(dir, aofOn("always")))
	c := testclient.MustDial(t, a.addr)
	for range 500 {
		c.Cmd(t, "INCR", "n")
	}
	path := filepath.Join(dir, aof.FileName)
	before, _ := os.Stat(path)
	c.Cmd(t, "BGREWRITEAOF")
	waitUntil(t, "the rewrite to finish", func() bool {
		return strings.Contains(c.Reply(t, "INFO", "persistence").Str, "aof_rewrite_in_progress:0")
	})
	after, _ := os.Stat(path)
	if after.Size() >= before.Size()/10 {
		t.Errorf("file went from %d to %d bytes; expected a big reduction", before.Size(), after.Size())
	}
	c.Cmd(t, "INCR", "n") // the log keeps working after the swap
	a.stop()
	b := mustStart(t, clock, withDir(dir, aofOn("always")))
	if got := testclient.MustDial(t, b.addr).Cmd(t, "GET", "n"); got != "$501" {
		t.Errorf("n = %s; want 501", got)
	}
}

func TestBGRewriteWithoutAOFIsAnError(t *testing.T) {
	a := mustStart(t, newClock(), withDir(t.TempDir(), nil))
	c := testclient.MustDial(t, a.addr)
	if got := c.Cmd(t, "BGREWRITEAOF"); !strings.HasPrefix(got, "-ERR") || !strings.Contains(got, "not enabled") {
		t.Errorf("got %s", got)
	}
}

func TestSnapshotSchedulerAndFinalSnapshot(t *testing.T) {
	dir, clock := t.TempDir(), newClock()
	ticks := make(chan time.Time)
	a := mustStart(t, clock, withDir(dir, func(c *Config) {
		c.SnapshotInterval = time.Minute
		c.SnapshotTicks = func() (<-chan time.Time, func()) { return ticks, func() {} }
	}))
	c := testclient.MustDial(t, a.addr)
	c.Cmd(t, "SET", "k", "1")

	ticks <- time.Now() // the scheduler takes the tick and runs BGSAVE
	snap := filepath.Join(dir, "dump.mrdb")
	waitUntil(t, "the scheduled snapshot", func() bool { _, err := os.Stat(snap); return err == nil })

	// A later change is saved by the final snapshot at shutdown.
	c.Cmd(t, "SET", "k", "2")
	if err := a.stop(); err != nil {
		t.Fatal(err)
	}
	b := mustStart(t, clock, withDir(dir, nil))
	if got := testclient.MustDial(t, b.addr).Cmd(t, "GET", "k"); got != "$2" {
		t.Errorf("GET k = %s; the shutdown snapshot is missing", got)
	}
}

func TestInfoPersistenceSection(t *testing.T) {
	a := mustStart(t, newClock(), withDir(t.TempDir(), aofOn("always")))
	info := testclient.MustDial(t, a.addr).Reply(t, "INFO", "persistence").Str
	for _, want := range []string{"# Persistence", "aof_enabled:1", "aof_fsync:always", "rdb_bgsave_in_progress:0"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO persistence lacks %q:\n%s", want, info)
		}
	}
}

func TestPersistConfigValidation(t *testing.T) {
	clock := newClock()
	for name, mod := range map[string]func(*Config){
		"path as dbfilename": func(c *Config) { c.DBFilename = "../escape.snap" },
		"bad fsync":          func(c *Config) { c.AppendFsync = "whenever" },
		"negative interval":  func(c *Config) { c.SnapshotInterval = -time.Second },
		"dir is a file": func(c *Config) {
			f := filepath.Join(t.TempDir(), "afile")
			os.WriteFile(f, nil, 0o644)
			c.Dir = f
		},
	} {
		t.Run(name, func(t *testing.T) {
			if n, err := startAt(t, clock, mod); err == nil {
				n.stop()
				t.Error("Start succeeded")
			}
		})
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(testclient.Timeout)
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
