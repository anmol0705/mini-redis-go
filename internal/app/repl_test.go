package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/testclient"
)

// ---- helpers ----

// link lets a test cut the replica's connection and hold back reconnection.
type link struct {
	mu    sync.Mutex
	conns []net.Conn
	gate  chan struct{} // when non-nil, dials wait until it is closed
}

func (l *link) dial(ctx context.Context, addr string) (net.Conn, error) {
	l.mu.Lock()
	gate := l.gate
	l.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", addr)
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

// hold makes new connection attempts wait; release lets them through.
func (l *link) hold() {
	l.mu.Lock()
	l.gate = make(chan struct{})
	l.mu.Unlock()
}

func (l *link) release() {
	l.mu.Lock()
	if l.gate != nil {
		close(l.gate)
		l.gate = nil
	}
	l.mu.Unlock()
}

// cut closes every connection made so far.
func (l *link) cut() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		_ = c.Close()
	}
	l.conns = nil
}

func replicaCfg(l *link) func(*Config) {
	return func(c *Config) {
		c.ReplRetry = 5 * time.Millisecond
		if l != nil {
			c.ReplDial = l.dial
		}
	}
}

func chain(mods ...func(*Config)) func(*Config) {
	return func(c *Config) {
		for _, m := range mods {
			if m != nil {
				m(c)
			}
		}
	}
}

func becomeReplica(t *testing.T, c *testclient.Client, primaryAddr string) {
	t.Helper()
	host, port, _ := net.SplitHostPort(primaryAddr)
	if got := c.Cmd(t, "REPLICAOF", host, port); got != "+OK" {
		t.Fatalf("REPLICAOF = %s", got)
	}
}

// field returns one "name:value" line of an INFO section.
func field(t *testing.T, c *testclient.Client, section, name string) string {
	t.Helper()
	for _, line := range strings.Split(c.Reply(t, "INFO", section).Str, "\r\n") {
		if v, ok := strings.CutPrefix(line, name+":"); ok {
			return v
		}
	}
	return ""
}

func waitLinkUp(t *testing.T, replica *testclient.Client) {
	t.Helper()
	waitUntil(t, "the replication link to come up", func() bool {
		return field(t, replica, "replication", "master_link_status") == "up"
	})
}

func waitInSync(t *testing.T, master, replica *testclient.Client) {
	t.Helper()
	waitUntil(t, "the replica to catch up with the primary", func() bool {
		m := field(t, master, "replication", "master_repl_offset")
		return m != "" && m == field(t, replica, "replication", "slave_repl_offset") &&
			field(t, replica, "replication", "master_link_status") == "up"
	})
}

// state captures the whole visible keyspace in a comparable form.
func state(t *testing.T, c *testclient.Client) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, k := range c.Reply(t, "KEYS", "*").Strings() {
		typ := c.Cmd(t, "TYPE", k)
		var val string
		switch typ {
		case "+string":
			val = c.Cmd(t, "GET", k)
		case "+list":
			val = c.Cmd(t, "LRANGE", k, "0", "-1")
		case "+hash":
			val = fmt.Sprint(sorted(c.Reply(t, "HGETALL", k).Strings()))
		case "+set":
			val = fmt.Sprint(sorted(c.Reply(t, "SMEMBERS", k).Strings()))
		case "+zset":
			val = c.Cmd(t, "ZRANGE", k, "0", "-1", "WITHSCORES")
		}
		out[k] = typ + "|" + val + "|ttl=" + c.Cmd(t, "PTTL", k)
	}
	return out
}

func sorted(s []string) []string { sort.Strings(s); return s }

func assertSameState(t *testing.T, a, b *testclient.Client) {
	t.Helper()
	sa, sb := state(t, a), state(t, b)
	if len(sa) != len(sb) {
		t.Errorf("key counts differ: %d vs %d", len(sa), len(sb))
	}
	for k, v := range sa {
		if sb[k] != v {
			t.Errorf("key %q differs:\n primary: %s\n replica: %s", k, v, sb[k])
		}
	}
	for k := range sb {
		if _, ok := sa[k]; !ok {
			t.Errorf("replica has extra key %q", k)
		}
	}
}

// ---- tests ----

func TestFullSyncThenLiveStream(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	populate(t, mc)

	replica := mustStart(t, clock, replicaCfg(nil))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)

	verify(t, rc, 0) // every type and TTL arrived
	assertSameState(t, mc, rc)

	if got := field(t, mc, "replication", "connected_slaves"); got != "1" {
		t.Errorf("connected_slaves = %s", got)
	}
	if got := field(t, mc, "replication", "sync_full"); got != "1" {
		t.Errorf("sync_full = %s", got)
	}
	if got := field(t, rc, "replication", "role"); got != "slave" {
		t.Errorf("replica role = %s", got)
	}
	if got := field(t, rc, "replication", "master_replid"); got != field(t, mc, "replication", "master_replid") {
		t.Errorf("replication ids differ: %s", got)
	}

	// Live writes of every kind follow.
	for _, cmd := range [][]string{
		{"SET", "live", "1"}, {"INCR", "counter"}, {"RPUSH", "list", "d"}, {"HSET", "hash", "f9", "v9"},
		{"SADD", "set", "new"}, {"ZADD", "zset", "9", "z9"}, {"DEL", "str"}, {"EXPIRE", "live", "50"},
		{"SET", "ex", "v", "EX", "30"}, {"FLUSHALL"}, {"SET", "after-flush", "1"},
	} {
		mc.Cmd(t, cmd...)
		if cmd[0] == "FLUSHALL" {
			waitInSync(t, mc, rc)
			assertSameState(t, mc, rc)
		}
	}
	waitInSync(t, mc, rc)
	assertSameState(t, mc, rc)
	if got := rc.Cmd(t, "GET", "after-flush"); got != "$1" {
		t.Errorf("replica GET after-flush = %s", got)
	}
}

func TestReplicaIsReadOnlyUntilPromoted(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	mc.Cmd(t, "SET", "k", "v")

	replica := mustStart(t, clock, replicaCfg(nil))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)

	for _, cmd := range [][]string{{"SET", "k", "x"}, {"DEL", "k"}, {"LPUSH", "l", "a"}, {"FLUSHALL"}, {"EXPIRE", "k", "9"}} {
		if got := rc.Cmd(t, cmd...); !strings.HasPrefix(got, "-READONLY") {
			t.Errorf("%v on a replica = %s; want a READONLY error", cmd, got)
		}
	}
	if got := rc.Cmd(t, "GET", "k"); got != "$v" {
		t.Errorf("read on a replica = %s", got)
	}
	if got := rc.Cmd(t, "PING"); got != "+PONG" {
		t.Errorf("PING = %s", got)
	}
	oldID := field(t, rc, "replication", "master_replid")

	if got := rc.Cmd(t, "REPLICAOF", "NO", "ONE"); got != "+OK" {
		t.Fatalf("REPLICAOF NO ONE = %s", got)
	}
	if got := rc.Cmd(t, "SET", "k", "mine"); got != "+OK" {
		t.Errorf("write after promotion = %s", got)
	}
	if field(t, rc, "replication", "role") != "master" {
		t.Error("role is not master after REPLICAOF NO ONE")
	}
	if newID := field(t, rc, "replication", "master_replid"); newID == oldID || len(newID) != 40 {
		t.Errorf("promotion kept the old history id: %s", newID)
	}
	// It no longer follows the old primary.
	mc.Cmd(t, "SET", "k", "from-old-primary")
	waitUntil(t, "the old primary to notice the replica left", func() bool {
		return field(t, mc, "replication", "connected_slaves") == "0"
	})
	if got := rc.Cmd(t, "GET", "k"); got != "$mine" {
		t.Errorf("promoted node still follows the old primary: %s", got)
	}
}

func TestWritesDuringSyncAreNotLost(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	// A decent amount of data so the full sync takes a moment.
	if err := mc.Write(testclientPipeline(20000, func(i int) []string {
		return []string{"SET", fmt.Sprint("seed:", i), strings.Repeat("v", 50)}
	})); err != nil {
		t.Fatal(err)
	}
	for range 20000 {
		mc.MustRead(t)
	}

	var counter atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		w, err := testclient.Dial(master.addr)
		if err != nil {
			t.Error(err)
			return
		}
		defer w.Close()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := w.Do("INCR", "counter"); err != nil {
				t.Error(err)
				return
			}
			counter.Add(1)
			if _, err := w.Do("SET", fmt.Sprint("during:", i), "x"); err != nil {
				t.Error(err)
				return
			}
		}
	})

	replica := mustStart(t, clock, replicaCfg(nil))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	close(stop)
	wg.Wait()
	waitInSync(t, mc, rc)

	if got, want := rc.Cmd(t, "GET", "counter"), fmt.Sprintf("$%d", counter.Load()); got != want {
		t.Errorf("counter on the replica = %s; want %s (writes during the sync were lost)", got, want)
	}
	if a, b := mc.Cmd(t, "DBSIZE"), rc.Cmd(t, "DBSIZE"); a != b {
		t.Errorf("DBSIZE primary %s, replica %s", a, b)
	}
	t.Logf("%d increments raced with the full sync", counter.Load())
}

func testclientPipeline(n int, cmd func(i int) []string) []byte {
	var out []byte
	for i := range n {
		out = append(out, testclient.Encode(cmd(i)...)...)
	}
	return out
}

func TestPartialResyncAfterDisconnect(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	mc.Cmd(t, "SET", "before", "1")

	lk := &link{}
	replica := mustStart(t, clock, replicaCfg(lk))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)

	// Cut the link and keep it down while the primary takes writes.
	lk.hold()
	lk.cut()
	waitUntil(t, "the primary to see the replica leave", func() bool {
		return field(t, mc, "replication", "connected_slaves") == "0"
	})
	for i := range 10 {
		mc.Cmd(t, "SET", fmt.Sprint("missed:", i), "v")
	}
	mc.Cmd(t, "INCR", "counter")
	lk.release()

	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)
	assertSameState(t, mc, rc)
	if got := field(t, mc, "replication", "sync_partial_ok"); got != "1" {
		t.Errorf("sync_partial_ok = %s; want 1", got)
	}
	if got := field(t, mc, "replication", "sync_full"); got != "1" {
		t.Errorf("sync_full = %s; the reconnect must not have triggered a full sync", got)
	}
}

func TestFullResyncWhenBacklogTooSmall(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, func(c *Config) { c.ReplBacklogSize = 256 })
	mc := testclient.MustDial(t, master.addr)

	lk := &link{}
	replica := mustStart(t, clock, replicaCfg(lk))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)

	lk.hold()
	lk.cut()
	waitUntil(t, "the replica to leave", func() bool { return field(t, mc, "replication", "connected_slaves") == "0" })
	for i := range 50 { // far more than 256 bytes of stream
		mc.Cmd(t, "SET", fmt.Sprint("gap:", i), strings.Repeat("x", 40))
	}
	lk.release()

	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)
	assertSameState(t, mc, rc)
	if got := field(t, mc, "replication", "sync_full"); got != "2" {
		t.Errorf("sync_full = %s; want 2 (the missing bytes were no longer in the backlog)", got)
	}
	if got := field(t, mc, "replication", "sync_partial_ok"); got != "0" {
		t.Errorf("sync_partial_ok = %s", got)
	}
}

func TestExpiryIsPropagatedAsDelete(t *testing.T) {
	t.Run("primary expiry deletes on the replica", func(t *testing.T) {
		mclock, rclock := newClock(), newClock()
		master := mustStart(t, mclock, nil)
		mc := testclient.MustDial(t, master.addr)
		mc.Cmd(t, "SET", "short", "v", "PX", "100")
		mc.Cmd(t, "SET", "long", "v")

		replica := mustStart(t, rclock, replicaCfg(nil))
		rc := testclient.MustDial(t, replica.addr)
		becomeReplica(t, rc, master.addr)
		waitLinkUp(t, rc)
		waitInSync(t, mc, rc)
		if rc.Cmd(t, "DBSIZE") != ":2" {
			t.Fatal("setup failed")
		}

		// Only the PRIMARY's clock moves. The replica cannot know the key
		// expired; it learns from the DEL the primary's sampler sends.
		mclock.Advance(time.Second)
		waitUntil(t, "the DEL to reach the replica", func() bool { return rc.Cmd(t, "DBSIZE") == ":1" })
		if got := rc.Cmd(t, "EXISTS", "short"); got != ":0" {
			t.Errorf("EXISTS short on the replica = %s", got)
		}
	})

	t.Run("a replica never deletes expired keys by itself", func(t *testing.T) {
		mclock, rclock := newClock(), newClock()
		master := mustStart(t, mclock, nil)
		mc := testclient.MustDial(t, master.addr)
		mc.Cmd(t, "SET", "k", "v", "PX", "100")

		replica := mustStart(t, rclock, replicaCfg(nil))
		rc := testclient.MustDial(t, replica.addr)
		becomeReplica(t, rc, master.addr)
		waitLinkUp(t, rc)
		waitInSync(t, mc, rc)

		rclock.Advance(time.Second) // only the replica's clock moves
		if got := rc.Cmd(t, "GET", "k"); got != "$nil" {
			t.Errorf("GET of a logically expired key on the replica = %s; want nil", got)
		}
		// Give the replica's sampler time to (wrongly) act, then check.
		for range 30 {
			rc.Cmd(t, "PING")
		}
		if got := rc.Cmd(t, "DBSIZE"); got != ":1" {
			t.Errorf("the replica removed the key on its own (DBSIZE = %s)", got)
		}
		if got := mc.Cmd(t, "GET", "k"); got != "$v" {
			t.Errorf("the primary lost the key: %s", got)
		}
	})
}

func TestSlowReplicaIsDroppedAndPrimaryKeepsWorking(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, func(c *Config) { c.ReplQueueSize = 4 })
	mc := testclient.MustDial(t, master.addr)

	// A fake replica: completes the handshake, then never reads again.
	raw, err := net.Dial("tcp", master.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Write(testclient.Encode("MRSYNC", "?", "-1")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(raw).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "+FULLRESYNC") {
		t.Fatalf("handshake = %q, %v", line, err)
	}
	waitUntil(t, "the fake replica to be registered", func() bool {
		return field(t, mc, "replication", "connected_slaves") == "1"
	})

	big := strings.Repeat("x", 512<<10)
	deadline := time.Now().Add(testclient.Timeout)
	for i := 0; field(t, mc, "replication", "connected_slaves") != "0"; i++ {
		if time.Now().After(deadline) {
			t.Fatal("the stalled replica was never dropped")
		}
		start := time.Now()
		if got := mc.Cmd(t, "SET", fmt.Sprint("k", i), big); got != "+OK" {
			t.Fatalf("primary write failed while a replica stalled: %s", got)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("a write took %v: the primary was stalled by its slow replica", d)
		}
	}
	if got := field(t, mc, "replication", "replicas_dropped"); got != "1" {
		t.Errorf("replicas_dropped = %s", got)
	}
	// The primary still serves everyone.
	if got := mc.Cmd(t, "PING"); got != "+PONG" {
		t.Errorf("PING = %s", got)
	}
	// Its connection gets closed.
	_ = raw.SetReadDeadline(time.Now().Add(testclient.Timeout))
	_, err = io.Copy(io.Discard, raw) // drains what was already sent, then must see the close
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Error("the dropped replica's connection was left open")
	}
}

func TestACloneOfAReplicaCannotSyncFromIt(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	replica := mustStart(t, clock, replicaCfg(nil))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)

	c := testclient.MustDial(t, replica.addr)
	got, err := c.Do("MRSYNC", "?", "-1")
	if err != nil || got.Kind != '-' || !strings.Contains(got.Str, "replica") {
		t.Errorf("MRSYNC against a replica = %v, %v", got, err)
	}
	c.ExpectClosed(t)
}

func TestMRSYNCArgumentErrors(t *testing.T) {
	master := mustStart(t, newClock(), nil)
	for _, args := range [][]string{{"MRSYNC"}, {"MRSYNC", "id"}, {"MRSYNC", "id", "x"}, {"MRSYNC", "id", "-5"}, {"MRSYNC", strings.Repeat("a", 100), "0"}} {
		c := testclient.MustDial(t, master.addr)
		if got := c.Cmd(t, args...); !strings.HasPrefix(got, "-ERR") {
			t.Errorf("%v = %s", args, got)
		}
	}
}

func TestStartupReplicaOf(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	mc.Cmd(t, "SET", "k", "v")

	replica := mustStart(t, clock, chain(replicaCfg(nil), func(c *Config) { c.ReplicaOf = master.addr }))
	rc := testclient.MustDial(t, replica.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)
	if got := rc.Cmd(t, "GET", "k"); got != "$v" {
		t.Errorf("GET = %s", got)
	}
	if got := rc.Cmd(t, "SET", "x", "y"); !strings.HasPrefix(got, "-READONLY") {
		t.Errorf("a node started with -replicaof accepted a write: %s", got)
	}

	for _, bad := range []string{"nonsense", "host notaport", "127.0.0.1:0"} {
		if n, err := startAt(t, clock, func(c *Config) { c.ReplicaOf = bad }); err == nil {
			n.stop()
			t.Errorf("Start with ReplicaOf=%q succeeded", bad)
		}
	}
}

func TestReplicaCommandErrors(t *testing.T) {
	n := mustStart(t, newClock(), nil)
	c := testclient.MustDial(t, n.addr)
	for _, port := range []string{"0", "abc", "99999"} {
		if got := c.Cmd(t, "REPLICAOF", "127.0.0.1", port); !strings.HasPrefix(got, "-ERR") {
			t.Errorf("REPLICAOF 127.0.0.1 %s = %s", port, got)
		}
	}
	if got := c.Cmd(t, "SET", "k", "v"); got != "+OK" {
		t.Errorf("a failed REPLICAOF made the node read-only: %s", got)
	}
	if got := c.Cmd(t, "REPLICAOF", "NO", "ONE"); got != "+OK" {
		t.Errorf("REPLICAOF NO ONE on a primary = %s", got)
	}
}

func TestUnreachableOrBrokenPrimaryKeepsLinkDown(t *testing.T) {
	// A port nothing listens on.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()

	// A "primary" that answers nonsense.
	junk, _ := net.Listen("tcp", "127.0.0.1:0")
	t.Cleanup(func() { junk.Close() })
	go func() {
		for {
			c, err := junk.Accept()
			if err != nil {
				return
			}
			go func() { c.Write([]byte("garbage\r\n")); time.Sleep(time.Millisecond); c.Close() }()
		}
	}()

	for name, addr := range map[string]string{"closed port": dead, "garbage primary": junk.Addr().String()} {
		t.Run(name, func(t *testing.T) {
			n := mustStart(t, newClock(), replicaCfg(nil))
			c := testclient.MustDial(t, n.addr)
			becomeReplica(t, c, addr)
			for range 20 {
				c.Cmd(t, "PING")
			}
			if got := field(t, c, "replication", "master_link_status"); got != "down" {
				t.Errorf("master_link_status = %s", got)
			}
			if got := c.Cmd(t, "REPLICAOF", "NO", "ONE"); got != "+OK" {
				t.Errorf("could not leave the retry loop: %s", got)
			}
		})
	}
}

func TestReplicaKeepsItsOwnAOF(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	populate(t, mc)

	dir := t.TempDir()
	replica := mustStart(t, clock, chain(replicaCfg(nil), withDir(dir, aofOn("always"))))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)
	mc.Cmd(t, "SET", "streamed", "later")
	waitInSync(t, mc, rc)
	if err := replica.stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "appendonly.aof")); err != nil {
		t.Fatal(err)
	}

	// Restart the replica on its own: snapshot content AND later commands.
	alone := mustStart(t, clock, withDir(dir, aofOn("always")))
	ac := testclient.MustDial(t, alone.addr)
	if got := ac.Cmd(t, "GET", "streamed"); got != "$later" {
		t.Errorf("streamed key lost after restart: %s", got)
	}
	assertSameState(t, mc, ac)
}

func TestShutdownWithReplicationLeavesNoGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	clock := newClock()
	master, err := startAt(t, clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	mc := testclient.MustDial(t, master.addr)
	mc.Cmd(t, "SET", "k", "v")
	replica, err := startAt(t, clock, replicaCfg(nil))
	if err != nil {
		t.Fatal(err)
	}
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)
	waitInSync(t, mc, rc)

	// Stop the primary first (its stream to the replica ends), then the replica.
	if err := master.stop(); err != nil {
		t.Errorf("primary: %v", err)
	}
	waitUntil(t, "the replica to notice the primary leave", func() bool {
		return field(t, rc, "replication", "master_link_status") == "down"
	})
	if err := replica.stop(); err != nil {
		t.Errorf("replica: %v", err)
	}
	_ = mc.Close()
	_ = rc.Close()
	waitUntil(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= baseline })
}

func TestStopPrimaryWithReplicaAttached(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	replica := mustStart(t, clock, replicaCfg(nil))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)

	done := make(chan error, 1)
	go func() { done <- master.stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("primary stopped with %v", err)
		}
	case <-time.After(testclient.Timeout):
		t.Fatal("the primary could not shut down with a replica attached")
	}
}

// Replicating an increasing set of writes keeps order: the replica ends up
// with exactly the primary's list.
func TestOrderIsPreserved(t *testing.T) {
	clock := newClock()
	master := mustStart(t, clock, nil)
	mc := testclient.MustDial(t, master.addr)
	replica := mustStart(t, clock, replicaCfg(nil))
	rc := testclient.MustDial(t, replica.addr)
	becomeReplica(t, rc, master.addr)
	waitLinkUp(t, rc)

	var want []string
	for i := range 500 {
		v := strconv.Itoa(i)
		mc.Cmd(t, "RPUSH", "l", v)
		want = append(want, v)
	}
	waitInSync(t, mc, rc)
	got := rc.Reply(t, "LRANGE", "l", "0", "-1").Strings()
	if !slices.Equal(got, want) {
		t.Errorf("replica list differs from the primary's (len %d vs %d)", len(got), len(want))
	}
}
