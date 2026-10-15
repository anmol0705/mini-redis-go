package store

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
)

const benchKeys = 100_000

func benchDB(b *testing.B) *DB {
	b.Helper()
	db := New(rand.New(rand.NewPCG(1, 2)))
	db.SetNow(1_000_000)
	for i := range benchKeys {
		db.Set("key:"+strconv.Itoa(i), "value-"+strconv.Itoa(i), 0, false)
	}
	return db
}

func BenchmarkSet(b *testing.B) {
	db := benchDB(b)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i*97%benchKeys)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		db.Set(keys[i&1023], "some value of moderate length", 0, false)
	}
}

func BenchmarkGet(b *testing.B) {
	db := benchDB(b)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i*97%benchKeys)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		db.Get(keys[i&1023])
	}
}

func BenchmarkIncrBy(b *testing.B) {
	db := benchDB(b)
	b.ReportAllocs()
	for b.Loop() {
		db.IncrBy("counter", 1)
	}
}

func BenchmarkPushPop(b *testing.B) {
	db := benchDB(b)
	b.ReportAllocs()
	for b.Loop() {
		db.Push("list", false, "element")
		db.Pop("list", true, 1)
	}
}

func BenchmarkHSetHGet(b *testing.B) {
	db := benchDB(b)
	fields := make([]string, 512)
	for i := range fields {
		fields[i] = "field" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		f := fields[i&511]
		db.HSet("hash", []string{f, "v"})
		db.HGet("hash", f)
	}
}

func BenchmarkZAdd(b *testing.B) {
	db := benchDB(b)
	rng := rand.New(rand.NewPCG(3, 4))
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		db.ZAdd("z", []ZMember{{Member: "m" + strconv.Itoa(i&65535), Score: rng.Float64()}})
	}
}

func BenchmarkZRankAndRange(b *testing.B) {
	db := benchDB(b)
	rng := rand.New(rand.NewPCG(3, 4))
	for i := range 100_000 {
		db.ZAdd("z", []ZMember{{Member: "m" + strconv.Itoa(i), Score: rng.Float64()}})
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		db.ZRank("z", "m"+strconv.Itoa(i%100_000))
		db.ZRange("z", 5000, 5009)
	}
}

func BenchmarkActiveExpire(b *testing.B) {
	db := New(rand.New(rand.NewPCG(1, 2)))
	db.SetNow(1000)
	for i := range 10_000 {
		db.Set(fmt.Sprint("k", i), "v", 1500, false)
	}
	b.ReportAllocs()
	for b.Loop() {
		db.ActiveExpire(20, 0) // nothing is expired yet: the cost of one sampling round
	}
}

func BenchmarkDump(b *testing.B) {
	db := benchDB(b)
	b.ReportAllocs()
	for b.Loop() {
		db.Dump()
	}
}
