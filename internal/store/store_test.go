package store

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
	"time"
)

func newDB(t *testing.T) *DB {
	t.Helper()
	db := New(rand.New(rand.NewPCG(1, 2)))
	db.SetNow(1_000_000)
	return db
}

// check verifies the internal invariants after any sequence of operations:
// indexes agree with the map, and the memory total equals a from-scratch
// recount.
func check(t *testing.T, db *DB) {
	t.Helper()
	if len(db.keys) != len(db.m) {
		t.Fatalf("keys slice has %d entries, map has %d", len(db.keys), len(db.m))
	}
	vol := 0
	var mem int64
	for i, k := range db.keys {
		e := db.m[k]
		if e == nil || e.idx != i {
			t.Fatalf("key %q: index slot %d does not match entry %+v", k, i, e)
		}
		if e.expireAt != 0 {
			vol++
			if e.vidx < 0 || db.volatile[e.vidx] != k {
				t.Fatalf("key %q has an expiry but is not correctly indexed as volatile", k)
			}
		} else if e.vidx != -1 {
			t.Fatalf("key %q has no expiry but vidx = %d", k, e.vidx)
		}
		if got := estimate(e); got != e.size {
			t.Fatalf("key %q (%s): size field %d, recount %d", k, e.kind, e.size, got)
		}
		mem += int64(entryOverhead + len(k) + e.size)
	}
	if vol != len(db.volatile) {
		t.Fatalf("%d keys have expiry but volatile has %d", vol, len(db.volatile))
	}
	if mem != db.mem {
		t.Fatalf("memory total %d, recount %d", db.mem, mem)
	}
}

// estimate recomputes an entry's size from its content.
func estimate(e *entry) int {
	n := 0
	switch e.kind {
	case KindString:
		n = len(e.str)
	case KindList:
		for i := range e.list.n {
			n += len(e.list.at(i)) + listElemOverhead
		}
	case KindHash:
		for f, v := range e.hash {
			n += len(f) + len(v) + hashFieldOverhead
		}
	case KindSet:
		for m := range e.set {
			n += len(m) + setMemberOverhead
		}
	case KindZSet:
		for m := range e.zset.dict {
			n += len(m) + zsetMemberOverhead
		}
	}
	return n
}

func TestStrings(t *testing.T) {
	db := newDB(t)
	db.Set("k", "v", 0, false)
	if v, ok, err := db.Get("k"); v != "v" || !ok || err != nil {
		t.Fatalf("Get = %q,%v,%v", v, ok, err)
	}
	if _, ok, _ := db.Get("missing"); ok {
		t.Error("Get(missing) found")
	}
	db.Set("k", "longer value", 0, false)
	if v, _, _ := db.Get("k"); v != "longer value" {
		t.Errorf("overwrite failed: %q", v)
	}
	if n, _ := db.Append("k", "!"); n != len("longer value!") {
		t.Errorf("Append length = %d", n)
	}
	if n, _ := db.Append("new", "abc"); n != 3 {
		t.Errorf("Append to missing = %d", n)
	}
	if n, _ := db.StrLen("k"); n != 13 {
		t.Errorf("StrLen = %d", n)
	}
	if n, err := db.StrLen("missing"); n != 0 || err != nil {
		t.Errorf("StrLen(missing) = %d, %v", n, err)
	}
	check(t, db)
}

func TestSetReplacesAnyTypeAndTTL(t *testing.T) {
	db := newDB(t)
	db.Push("k", false, "a", "b")
	db.ExpireAt("k", db.Now()+5000)
	db.Set("k", "str", 0, false)
	if db.Type("k") != KindString {
		t.Fatalf("type = %v", db.Type("k"))
	}
	if ms, _ := db.ExpireAtOf("k"); ms != 0 {
		t.Error("plain SET kept the old TTL")
	}
	check(t, db)

	db.Set("k", "x", db.Now()+9000, false)
	db.Set("k", "y", 0, true) // KEEPTTL
	if ms, _ := db.ExpireAtOf("k"); ms != db.Now()+9000 {
		t.Errorf("KEEPTTL lost the TTL: %d", ms)
	}
	check(t, db)
}

func TestSetWithPastExpiryDeletes(t *testing.T) {
	db := newDB(t)
	db.Set("k", "v", 0, false)
	db.Set("k", "v2", db.Now()-1, false)
	if db.Exists("k") {
		t.Error("key with past expiry still exists")
	}
	check(t, db)
}

func TestIncrBy(t *testing.T) {
	db := newDB(t)
	if n, err := db.IncrBy("c", 5); n != 5 || err != nil {
		t.Fatalf("IncrBy = %d, %v", n, err)
	}
	if n, _ := db.IncrBy("c", -7); n != -2 {
		t.Errorf("IncrBy = %d; want -2", n)
	}
	db.Set("text", "abc", 0, false)
	if _, err := db.IncrBy("text", 1); !errors.Is(err, ErrNotInteger) {
		t.Errorf("err = %v; want ErrNotInteger", err)
	}
	for _, bad := range []string{" 1", "+1", "01", "1.5", "", "9223372036854775808"} {
		db.Set("bad", bad, 0, false)
		if _, err := db.IncrBy("bad", 1); !errors.Is(err, ErrNotInteger) {
			t.Errorf("IncrBy on %q: err = %v; want ErrNotInteger", bad, err)
		}
	}
	db.Set("max", fmt.Sprint(math.MaxInt64), 0, false)
	if _, err := db.IncrBy("max", 1); !errors.Is(err, ErrOverflow) {
		t.Errorf("err = %v; want ErrOverflow", err)
	}
	db.Set("min", fmt.Sprint(math.MinInt64), 0, false)
	if _, err := db.IncrBy("min", -1); !errors.Is(err, ErrOverflow) {
		t.Errorf("err = %v; want ErrOverflow", err)
	}
	db.Push("list", false, "x")
	if _, err := db.IncrBy("list", 1); !errors.Is(err, ErrWrongType) {
		t.Errorf("err = %v; want ErrWrongType", err)
	}
	check(t, db)
}

func TestWrongTypeEverywhere(t *testing.T) {
	db := newDB(t)
	db.Set("s", "v", 0, false)
	ops := map[string]func() error{
		"push":    func() error { _, err := db.Push("s", true, "a"); return err },
		"pop":     func() error { _, _, err := db.Pop("s", true, 1); return err },
		"llen":    func() error { _, err := db.ListLen("s"); return err },
		"lrange":  func() error { _, err := db.ListRange("s", 0, -1); return err },
		"lindex":  func() error { _, _, err := db.ListIndex("s", 0); return err },
		"hset":    func() error { _, err := db.HSet("s", []string{"a", "b"}); return err },
		"hget":    func() error { _, _, err := db.HGet("s", "a"); return err },
		"hdel":    func() error { _, err := db.HDel("s", []string{"a"}); return err },
		"hgetall": func() error { _, err := db.HGetAll("s"); return err },
		"hlen":    func() error { _, err := db.HLen("s"); return err },
		"sadd":    func() error { _, err := db.SAdd("s", []string{"a"}); return err },
		"srem":    func() error { _, err := db.SRem("s", []string{"a"}); return err },
		"sismem":  func() error { _, err := db.SIsMember("s", "a"); return err },
		"smem":    func() error { _, err := db.SMembers("s"); return err },
		"scard":   func() error { _, err := db.SCard("s"); return err },
		"zadd":    func() error { _, err := db.ZAdd("s", []ZMember{{"a", 1}}); return err },
		"zrem":    func() error { _, err := db.ZRem("s", []string{"a"}); return err },
		"zscore":  func() error { _, _, err := db.ZScore("s", "a"); return err },
		"zrank":   func() error { _, _, err := db.ZRank("s", "a"); return err },
		"zrange":  func() error { _, err := db.ZRange("s", 0, -1); return err },
		"zcard":   func() error { _, err := db.ZCard("s"); return err },
	}
	for name, op := range ops {
		if err := op(); !errors.Is(err, ErrWrongType) {
			t.Errorf("%s on a string: err = %v; want ErrWrongType", name, err)
		}
	}
	db.Push("l", false, "x")
	if _, _, err := db.Get("l"); !errors.Is(err, ErrWrongType) {
		t.Errorf("Get on a list: err = %v", err)
	}
	if _, err := db.Append("l", "x"); !errors.Is(err, ErrWrongType) {
		t.Errorf("Append on a list: err = %v", err)
	}
	check(t, db)
}

func TestLazyExpiry(t *testing.T) {
	db := newDB(t)
	var removed []string
	db.SetOnRemove(func(k string) { removed = append(removed, k) })

	db.Set("k", "v", db.Now()+100, false)
	db.SetNow(db.Now() + 99)
	if !db.Exists("k") {
		t.Fatal("key expired early")
	}
	db.SetNow(db.Now() + 1) // exactly at the expiry instant: expired
	if db.Exists("k") {
		t.Fatal("key still exists at its expiry time")
	}
	if !slices.Equal(removed, []string{"k"}) {
		t.Errorf("onRemove calls = %v; want [k]", removed)
	}
	if db.Stats().ExpiredKeys != 1 || db.Len() != 0 {
		t.Errorf("stats = %+v, Len = %d", db.Stats(), db.Len())
	}
	check(t, db)
}

func TestReplicaHidesButKeepsExpiredKeys(t *testing.T) {
	db := newDB(t)
	db.SetReplica(true)
	called := false
	db.SetOnRemove(func(string) { called = true })

	db.Set("k", "v", db.Now()+10, false)
	db.SetNow(db.Now() + 50)
	if db.Exists("k") {
		t.Error("replica shows an expired key")
	}
	if db.Len() != 1 || called {
		t.Errorf("replica deleted an expired key itself (Len=%d, onRemove=%v)", db.Len(), called)
	}
	if n := db.ActiveExpire(20, time.Second); n != 0 {
		t.Errorf("replica ActiveExpire removed %d keys", n)
	}
	// The primary's SET for the same key must replace the hidden one.
	db.Set("k", "fresh", 0, false)
	if v, ok, _ := db.Get("k"); !ok || v != "fresh" {
		t.Errorf("Get = %q, %v", v, ok)
	}
	check(t, db)
}

func TestActiveExpire(t *testing.T) {
	db := newDB(t)
	var removed int
	db.SetOnRemove(func(string) { removed++ })

	for i := range 200 {
		db.Set(fmt.Sprint("dead", i), "v", db.Now()+10, false)
	}
	for i := range 20 {
		db.Set(fmt.Sprint("live", i), "v", db.Now()+1_000_000, false)
	}
	db.Set("forever", "v", 0, false)
	db.SetNow(db.Now() + 100)

	// Most sampled keys are expired, so the cycle keeps repeating. It stops
	// once fewer than ~25% of a sample is expired, which leaves a few
	// stragglers for the next tick - that is the intended trade-off.
	n := db.ActiveExpire(20, 10*time.Second)
	if n < 150 {
		t.Errorf("first ActiveExpire removed only %d of 200", n)
	}
	total := n
	for range 1000 {
		total += db.ActiveExpire(20, 10*time.Second)
	}
	if total != 200 || removed != 200 {
		t.Errorf("after repeated ticks removed %d (callbacks %d); want 200", total, removed)
	}
	if db.Len() != 21 || db.VolatileLen() != 20 {
		t.Errorf("Len=%d Volatile=%d; want 21 and 20", db.Len(), db.VolatileLen())
	}
	check(t, db)
}

func TestActiveExpireStopsWhenFewAreExpired(t *testing.T) {
	db := newDB(t)
	// 1 expired key among 100 volatile ones: the sample usually misses it
	// and the loop must stop after one round, not scan everything.
	for i := range 99 {
		db.Set(fmt.Sprint("live", i), "v", db.Now()+1_000_000, false)
	}
	db.Set("dead", "v", db.Now()+1, false)
	db.SetNow(db.Now() + 10)
	if n := db.ActiveExpire(5, time.Second); n > 1 {
		t.Errorf("removed %d", n)
	}
	check(t, db)
}

func TestActiveExpireRespectsBudget(t *testing.T) {
	db := newDB(t)
	for i := range 1000 {
		db.Set(fmt.Sprint(i), "v", db.Now()+1, false)
	}
	db.SetNow(db.Now() + 10)
	// A zero budget allows exactly one sampling round.
	if n := db.ActiveExpire(10, 0); n != 10 {
		t.Errorf("removed %d with zero budget; want one round of 10", n)
	}
}

func TestExpireAtPersistTTL(t *testing.T) {
	db := newDB(t)
	if db.ExpireAt("missing", 5) {
		t.Error("ExpireAt on missing key returned true")
	}
	db.Set("k", "v", 0, false)
	if db.Persist("k") {
		t.Error("Persist without TTL returned true")
	}
	db.ExpireAt("k", db.Now()+1000)
	if ms, ok := db.ExpireAtOf("k"); !ok || ms != db.Now()+1000 {
		t.Errorf("ExpireAtOf = %d, %v", ms, ok)
	}
	if !db.Persist("k") {
		t.Error("Persist returned false")
	}
	if ms, _ := db.ExpireAtOf("k"); ms != 0 {
		t.Errorf("TTL after Persist = %d", ms)
	}
	check(t, db)
}

func TestKeys(t *testing.T) {
	db := newDB(t)
	for _, k := range []string{"user:1", "user:2", "order:1", "gone"} {
		db.Set(k, "v", 0, false)
	}
	db.ExpireAt("gone", db.Now()+1)
	db.SetNow(db.Now() + 5)

	got := db.Keys("user:*")
	sort.Strings(got)
	if !slices.Equal(got, []string{"user:1", "user:2"}) {
		t.Errorf("Keys(user:*) = %v", got)
	}
	if all := db.Keys("*"); len(all) != 3 {
		t.Errorf("Keys(*) = %v; want 3 live keys", all)
	}
	if got := db.Keys("order:?"); len(got) != 1 {
		t.Errorf("Keys(order:?) = %v", got)
	}
}

func TestListOps(t *testing.T) {
	db := newDB(t)
	if n, _ := db.Push("l", true, "a", "b", "c"); n != 3 { // c b a
		t.Fatalf("Push = %d", n)
	}
	db.Push("l", false, "d", "e") // c b a d e
	got, _ := db.ListRange("l", 0, -1)
	if !slices.Equal(got, []string{"c", "b", "a", "d", "e"}) {
		t.Fatalf("list = %v", got)
	}
	if got, _ := db.ListRange("l", -2, -1); !slices.Equal(got, []string{"d", "e"}) {
		t.Errorf("range -2..-1 = %v", got)
	}
	if got, _ := db.ListRange("l", 3, 1); got != nil {
		t.Errorf("inverted range = %v", got)
	}
	if got, _ := db.ListRange("l", 0, 100); len(got) != 5 {
		t.Errorf("clipped range = %v", got)
	}
	if v, ok, _ := db.ListIndex("l", -1); !ok || v != "e" {
		t.Errorf("index -1 = %q, %v", v, ok)
	}
	if _, ok, _ := db.ListIndex("l", 9); ok {
		t.Error("index 9 found")
	}

	vals, found, _ := db.Pop("l", true, 2)
	if !found || !slices.Equal(vals, []string{"c", "b"}) {
		t.Errorf("pop front 2 = %v", vals)
	}
	vals, _, _ = db.Pop("l", false, 10) // more than exist
	if !slices.Equal(vals, []string{"e", "d", "a"}) {
		t.Errorf("pop back 10 = %v", vals)
	}
	if db.Exists("l") {
		t.Error("empty list was not removed")
	}
	if _, found, _ := db.Pop("l", true, 1); found {
		t.Error("Pop on missing key reported found")
	}
	check(t, db)
}

func TestDequeAgainstSlice(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	var d deque
	var ref []string
	for step := range 5000 {
		switch rng.IntN(5) {
		case 0, 1:
			s := fmt.Sprint(step)
			d.pushBack(s)
			ref = append(ref, s)
		case 2:
			s := fmt.Sprint(step)
			d.pushFront(s)
			ref = append([]string{s}, ref...)
		case 3:
			if len(ref) > 0 {
				if got := d.popFront(); got != ref[0] {
					t.Fatalf("popFront = %q; want %q", got, ref[0])
				}
				ref = ref[1:]
			}
		case 4:
			if len(ref) > 0 {
				if got := d.popBack(); got != ref[len(ref)-1] {
					t.Fatalf("popBack = %q; want %q", got, ref[len(ref)-1])
				}
				ref = ref[:len(ref)-1]
			}
		}
		if d.n != len(ref) {
			t.Fatalf("length %d; want %d", d.n, len(ref))
		}
	}
	for i, want := range ref {
		if d.at(i) != want {
			t.Fatalf("at(%d) = %q; want %q", i, d.at(i), want)
		}
	}
}

func TestHashOps(t *testing.T) {
	db := newDB(t)
	if n, _ := db.HSet("h", []string{"a", "1", "b", "2", "a", "3"}); n != 2 {
		t.Errorf("HSet new fields = %d; want 2", n)
	}
	if v, ok, _ := db.HGet("h", "a"); !ok || v != "3" {
		t.Errorf("HGet a = %q, %v", v, ok)
	}
	if ok, _ := db.HExists("h", "b"); !ok {
		t.Error("HExists b = false")
	}
	if n, _ := db.HLen("h"); n != 2 {
		t.Errorf("HLen = %d", n)
	}
	all, _ := db.HGetAll("h")
	if len(all) != 4 {
		t.Errorf("HGetAll = %v", all)
	}
	if n, _ := db.HDel("h", []string{"a", "zz"}); n != 1 {
		t.Errorf("HDel = %d", n)
	}
	check(t, db)
	db.HDel("h", []string{"b"})
	if db.Exists("h") {
		t.Error("empty hash not removed")
	}
	if all, _ := db.HGetAll("h"); all != nil {
		t.Errorf("HGetAll on missing = %v", all)
	}
	check(t, db)
}

func TestSetOps(t *testing.T) {
	db := newDB(t)
	if n, _ := db.SAdd("s", []string{"a", "b", "a"}); n != 2 {
		t.Errorf("SAdd = %d; want 2", n)
	}
	if ok, _ := db.SIsMember("s", "a"); !ok {
		t.Error("SIsMember a = false")
	}
	if n, _ := db.SCard("s"); n != 2 {
		t.Errorf("SCard = %d", n)
	}
	m, _ := db.SMembers("s")
	sort.Strings(m)
	if !slices.Equal(m, []string{"a", "b"}) {
		t.Errorf("SMembers = %v", m)
	}
	if n, _ := db.SRem("s", []string{"a", "x"}); n != 1 {
		t.Errorf("SRem = %d", n)
	}
	db.SRem("s", []string{"b"})
	if db.Exists("s") {
		t.Error("empty set not removed")
	}
	check(t, db)
}

func TestZSetOps(t *testing.T) {
	db := newDB(t)
	n, _ := db.ZAdd("z", []ZMember{{"b", 2}, {"a", 1}, {"c", 3}, {"a", 1}})
	if n != 3 {
		t.Errorf("ZAdd new = %d; want 3", n)
	}
	if n, _ := db.ZAdd("z", []ZMember{{"a", 10}}); n != 0 { // update, not new
		t.Errorf("ZAdd update reported %d new", n)
	}
	// order is now b(2) c(3) a(10)
	got, _ := db.ZRange("z", 0, -1)
	want := []ZMember{{"b", 2}, {"c", 3}, {"a", 10}}
	if !slices.Equal(got, want) {
		t.Errorf("ZRange = %v; want %v", got, want)
	}
	if r, ok, _ := db.ZRank("z", "a"); !ok || r != 2 {
		t.Errorf("ZRank a = %d, %v", r, ok)
	}
	if _, ok, _ := db.ZRank("z", "nope"); ok {
		t.Error("ZRank of missing member found")
	}
	if s, ok, _ := db.ZScore("z", "c"); !ok || s != 3 {
		t.Errorf("ZScore c = %v, %v", s, ok)
	}
	if got, _ := db.ZRange("z", -2, -1); !slices.Equal(got, want[1:]) {
		t.Errorf("ZRange -2..-1 = %v", got)
	}
	if n, _ := db.ZCard("z"); n != 3 {
		t.Errorf("ZCard = %d", n)
	}
	check(t, db)
	if n, _ := db.ZRem("z", []string{"a", "b", "nope"}); n != 2 {
		t.Errorf("ZRem = %d", n)
	}
	db.ZRem("z", []string{"c"})
	if db.Exists("z") {
		t.Error("empty zset not removed")
	}
	check(t, db)
}

// Random operations over all types: after each step the indexes and the
// memory total must agree with a full recount, and deleting everything must
// bring memory back to zero.
func TestRandomOperationsKeepInvariants(t *testing.T) {
	for seed := uint64(1); seed <= 10; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		db := New(rand.New(rand.NewPCG(seed, 8)))
		db.SetNow(1000)
		keyOf := func() string { return fmt.Sprint("k", rng.IntN(25)) }
		word := func() string { return fmt.Sprint("w", rng.IntN(12)) }

		for step := range 1500 {
			k := keyOf()
			switch rng.IntN(22) {
			case 0:
				db.Set(k, word(), 0, false)
			case 1:
				db.Set(k, word(), db.Now()+int64(rng.IntN(50)), rng.IntN(2) == 0)
			case 2:
				db.IncrBy(k, int64(rng.IntN(9)-4))
			case 3:
				db.Append(k, word())
			case 4:
				db.Push(k, rng.IntN(2) == 0, word(), word())
			case 5:
				db.Pop(k, rng.IntN(2) == 0, rng.IntN(3)+1)
			case 6:
				db.HSet(k, []string{word(), word(), word(), word()})
			case 7:
				db.HDel(k, []string{word(), word()})
			case 8:
				db.SAdd(k, []string{word(), word()})
			case 9:
				db.SRem(k, []string{word()})
			case 10:
				db.ZAdd(k, []ZMember{{word(), float64(rng.IntN(5))}, {word(), 1}})
			case 11:
				db.ZRem(k, []string{word()})
			case 12:
				db.Delete(k)
			case 13:
				db.ExpireAt(k, db.Now()+int64(rng.IntN(30)))
			case 14:
				db.Persist(k)
			case 15:
				db.SetNow(db.Now() + int64(rng.IntN(10)))
			case 16:
				db.ActiveExpire(5, time.Second)
			case 17:
				db.Evict(Policy(1+rng.IntN(3)), 3)
			case 18:
				db.Get(k)
				db.ListRange(k, 0, -1)
			case 19:
				db.Set(k, "x", 0, false) // type replacement
			case 20:
				items := db.Dump()
				db2 := New(nil)
				db2.SetNow(db.Now())
				if err := db2.Load(items); err != nil {
					t.Fatalf("seed %d step %d: Load: %v", seed, step, err)
				}
				check(t, db2)
			case 21:
				if rng.IntN(20) == 0 {
					db.Clear()
				}
			}
			check(t, db)
		}
		for _, k := range slices.Clone(db.keys) {
			db.Delete(k)
		}
		if db.UsedMemory() != 0 || db.Len() != 0 {
			t.Errorf("seed %d: after deleting everything mem=%d len=%d", seed, db.UsedMemory(), db.Len())
		}
	}
}

func TestDumpLoadRoundTrip(t *testing.T) {
	db := newDB(t)
	db.Set("s", "str", 0, false)
	db.Set("s-ttl", "v", db.Now()+5000, false)
	db.Push("l", false, "a", "b", "c")
	db.HSet("h", []string{"f1", "v1", "f2", "v2"})
	db.SAdd("set", []string{"x", "y"})
	db.ZAdd("z", []ZMember{{"m1", 1.5}, {"m2", -2}, {"m3", math.Inf(1)}})
	db.ExpireAt("z", db.Now()+9000)
	db.Set("expired", "v", db.Now()+1, false)
	db.SetNow(db.Now() + 10)

	items := db.Dump()
	if len(items) != 6 {
		t.Fatalf("Dump returned %d items (expired key should be skipped)", len(items))
	}

	db2 := New(nil)
	db2.SetNow(db.Now())
	if err := db2.Load(items); err != nil {
		t.Fatal(err)
	}
	check(t, db2)
	if db2.Len() != 6 || db2.UsedMemory() != db.UsedMemory()-int64(entryOverhead+len("expired")+len("v")) {
		t.Errorf("Len=%d mem=%d (orig mem %d)", db2.Len(), db2.UsedMemory(), db.UsedMemory())
	}
	if l, _ := db2.ListRange("l", 0, -1); !slices.Equal(l, []string{"a", "b", "c"}) {
		t.Errorf("list = %v", l)
	}
	if z, _ := db2.ZRange("z", 0, -1); !slices.Equal(z, []ZMember{{"m2", -2}, {"m1", 1.5}, {"m3", math.Inf(1)}}) {
		t.Errorf("zset = %v", z)
	}
	if ms, _ := db2.ExpireAtOf("z"); ms != db.Now()-10+9000 {
		t.Errorf("zset TTL = %d", ms)
	}
	if ms, _ := db2.ExpireAtOf("s-ttl"); ms != db.Now()-10+5000 {
		t.Errorf("string TTL = %d", ms)
	}

	// The dump is a deep copy: changing the DB must not change it.
	db.Push("l", false, "zzz")
	for _, it := range items {
		if it.Key == "l" && len(it.List) != 3 {
			t.Error("Dump shares memory with the live list")
		}
	}
}

func TestLoadRejectsBadItems(t *testing.T) {
	bad := []Item{
		{Key: "k", Kind: 0},
		{Key: "k", Kind: KindList},
		{Key: "k", Kind: KindHash, Hash: []string{"odd"}},
		{Key: "k", Kind: KindSet},
		{Key: "k", Kind: KindZSet},
	}
	for _, it := range bad {
		db := newDB(t)
		db.Set("keep", "v", 0, false)
		if err := db.Load([]Item{it}); !errors.Is(err, ErrInvalid) {
			t.Errorf("Load(%+v) = %v; want ErrInvalid", it, err)
		}
		if db.Len() != 0 {
			t.Errorf("DB not empty after failed Load")
		}
	}
}

func TestEvictPolicies(t *testing.T) {
	t.Run("noeviction", func(t *testing.T) {
		db := newDB(t)
		db.Set("a", "v", 0, false)
		if _, ok := db.Evict(NoEviction, 5); ok {
			t.Error("noeviction evicted")
		}
	})
	t.Run("empty", func(t *testing.T) {
		if _, ok := newDB(t).Evict(AllKeysLRU, 5); ok {
			t.Error("evicted from an empty DB")
		}
	})
	t.Run("lru prefers the oldest access", func(t *testing.T) {
		db := newDB(t)
		for i := range 50 {
			db.SetNow(int64(1000 + i))
			db.Set(fmt.Sprint("k", i), "v", 0, false)
		}
		// Sampling every key makes the choice exact: the oldest goes.
		db.SetNow(5000)
		var removed []string
		db.SetOnRemove(func(k string) { removed = append(removed, k) })
		for range 50 {
			db.Evict(AllKeysLRU, 5000) // hugely oversampled
			break
		}
		if len(removed) != 1 || removed[0] != "k0" {
			t.Errorf("evicted %v; want [k0]", removed)
		}
		if db.Stats().EvictedKeys != 1 {
			t.Errorf("stats = %+v", db.Stats())
		}
		check(t, db)
	})
	t.Run("lru: recently read key survives", func(t *testing.T) {
		db := newDB(t)
		db.SetNow(1)
		db.Set("old", "v", 0, false)
		db.Set("new", "v", 0, false)
		db.SetNow(100)
		db.Get("old") // touching refreshes its access time
		key, _ := db.Evict(AllKeysLRU, 1000)
		if key != "new" {
			t.Errorf("evicted %q; want new", key)
		}
	})
	t.Run("random", func(t *testing.T) {
		db := newDB(t)
		for i := range 10 {
			db.Set(fmt.Sprint("k", i), "v", 0, false)
		}
		if _, ok := db.Evict(AllKeysRandom, 5); !ok || db.Len() != 9 {
			t.Errorf("Len = %d", db.Len())
		}
		check(t, db)
	})
	t.Run("volatile-ttl", func(t *testing.T) {
		db := newDB(t)
		db.Set("persistent", "v", 0, false)
		db.Set("soon", "v", db.Now()+10, false)
		db.Set("later", "v", db.Now()+9000, false)
		if key, ok := db.Evict(VolatileTTL, 100); !ok || key != "soon" {
			t.Errorf("evicted %q, %v; want soon", key, ok)
		}
		db.Delete("later")
		if _, ok := db.Evict(VolatileTTL, 100); ok {
			t.Error("volatile-ttl evicted a key without expiry")
		}
		check(t, db)
	})
}

func TestParsePolicy(t *testing.T) {
	for _, p := range []Policy{NoEviction, AllKeysLRU, AllKeysRandom, VolatileTTL} {
		got, err := ParsePolicy(p.String())
		if err != nil || got != p {
			t.Errorf("round trip of %v = %v, %v", p, got, err)
		}
	}
	if _, err := ParsePolicy("lfu"); err == nil {
		t.Error("ParsePolicy accepted an unknown name")
	}
	if Policy(99).String() != "unknown" {
		t.Error("out-of-range policy name")
	}
}

func TestTypeAndKindNames(t *testing.T) {
	db := newDB(t)
	db.Set("s", "v", 0, false)
	db.Push("l", false, "v")
	db.HSet("h", []string{"a", "b"})
	db.SAdd("set", []string{"a"})
	db.ZAdd("z", []ZMember{{"a", 1}})
	want := map[string]string{"s": "string", "l": "list", "h": "hash", "set": "set", "z": "zset", "none": "none"}
	for key, name := range want {
		if got := db.Type(key).String(); got != name {
			t.Errorf("Type(%s) = %s; want %s", key, got, name)
		}
	}
}

func TestClear(t *testing.T) {
	db := newDB(t)
	db.Set("a", "1", db.Now()+10, false)
	db.Clear()
	if db.Len() != 0 || db.UsedMemory() != 0 || db.VolatileLen() != 0 {
		t.Errorf("Len=%d mem=%d volatile=%d", db.Len(), db.UsedMemory(), db.VolatileLen())
	}
	db.Set("b", "2", 0, false)
	check(t, db)
}

func TestParseInt(t *testing.T) {
	good := map[string]int64{"0": 0, "-1": -1, "42": 42, "9223372036854775807": math.MaxInt64}
	for s, want := range good {
		if got, err := ParseInt(s); err != nil || got != want {
			t.Errorf("ParseInt(%q) = %d, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "-0", "+1", "01", " 1", "1 ", "0x10", "1e3", "abc"} {
		if _, err := ParseInt(s); !errors.Is(err, ErrNotInteger) {
			t.Errorf("ParseInt(%q) err = %v", s, err)
		}
	}
}

// Regression: on a replica, the primary's DEL for a key that is already
// logically expired must physically remove it.
func TestReplicaDeleteRemovesHiddenExpiredKey(t *testing.T) {
	db := newDB(t)
	db.SetReplica(true)
	db.Set("k", "v", db.Now()+10, false)
	db.SetNow(db.Now() + 50)
	if db.Delete("k") {
		t.Error("Delete reported a logically expired key as existing")
	}
	if db.Len() != 0 {
		t.Errorf("hidden key still stored (Len=%d)", db.Len())
	}
	check(t, db)
}
