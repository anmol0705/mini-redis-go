// Package store holds the in-memory keyspace: typed values (strings, lists,
// hashes, sets, sorted sets), key expiry, a memory estimate and the
// sampling needed for eviction.
//
// Concurrency: a DB has NO locks. It is owned by a single goroutine (the
// engine) and every call must come from it. That is the whole point of the
// single-owner design (docs/decisions/005-single-owner-engine.md): no
// mutexes on the data, no data races, and multi-key commands such as
// transactions are atomic for free.
//
// Time: the DB never reads the clock. The owner calls SetNow before running a
// command (or a batch), so tests are deterministic and every command in an
// atomic batch sees the same instant.
package store

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/glob"
)

// Errors returned by DB operations. Their text is exactly what Redis sends to
// clients, so the command layer can reply with err.Error().
var (
	ErrWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
	ErrTooBig     = errors.New("ERR string exceeds maximum allowed size (512MB)")
	ErrInvalid    = errors.New("store: invalid dump item")
)

// maxStringLen is the largest string value, as in Redis.
const maxStringLen = 512 * 1024 * 1024

// Kind is the type of the value stored under a key.
type Kind uint8

// The value kinds. The zero Kind is invalid on purpose.
const (
	KindString Kind = iota + 1
	KindList
	KindHash
	KindSet
	KindZSet
)

// String returns the name TYPE reports.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	case KindSet:
		return "set"
	case KindZSet:
		return "zset"
	}
	return "none"
}

// entry is one key's value plus bookkeeping. Only the field for its kind is
// used; a union via one struct keeps lookups free of type assertions.
type entry struct {
	kind Kind
	str  string
	list *deque
	hash map[string]string
	set  map[string]struct{}
	zset *zset

	expireAt int64 // unix milliseconds; 0 means no expiry
	access   int64 // unix milliseconds of the last access (for LRU)
	size     int   // estimated bytes of the value part (see memory.go)

	idx  int // position in DB.keys
	vidx int // position in DB.volatile, or -1 when there is no expiry
}

// DB is a single keyspace. See the package comment for the ownership rule.
type DB struct {
	m        map[string]*entry
	keys     []string // every key; lets us pick a random key in O(1)
	volatile []string // keys that have an expiry
	now      int64    // unix milliseconds, set by the owner
	rng      *rand.Rand
	replica  bool
	onRemove func(key string)
	mem      int64

	expired int64
	evicted int64
}

// New returns an empty DB. rng drives every random choice (expiry sampling,
// eviction sampling, skip list heights); pass a seeded one in tests, or nil
// for a randomly seeded generator.
func New(rng *rand.Rand) *DB {
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &DB{m: make(map[string]*entry), rng: rng}
}

// SetNow sets the current time in unix milliseconds. The owner calls it
// before running commands.
func (db *DB) SetNow(ms int64) { db.now = ms }

// Now returns the time last given to SetNow.
func (db *DB) Now() int64 { return db.now }

// SetReplica switches replica mode on or off. A replica never deletes
// expired keys by itself (the primary sends explicit deletions); it only
// hides them from reads.
func (db *DB) SetReplica(on bool) { db.replica = on }

// SetOnRemove registers fn to be called with the key whenever the DB removes
// a key on its own (expiry or eviction), so the owner can propagate a DEL.
func (db *DB) SetOnRemove(fn func(key string)) { db.onRemove = fn }

func (db *DB) notify(key string) {
	if db.onRemove != nil {
		db.onRemove(key)
	}
}

// ParseInt parses a canonical base-10 int64: no spaces, no '+', no leading
// zeros. This is the strictness Redis applies to integer arguments and to
// string values used by INCR.
func ParseInt(s string) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(v, 10) != s {
		return 0, ErrNotInteger
	}
	return v, nil
}

func newEntry(kind Kind) *entry { return &entry{kind: kind, vidx: -1} }

// lookup returns the live entry for key, deleting it first if it has
// expired (lazy expiry). It also records the access time.
func (db *DB) lookup(key string) *entry {
	e := db.m[key]
	if e == nil {
		return nil
	}
	if e.expireAt != 0 && e.expireAt <= db.now {
		if db.replica {
			return nil
		}
		db.remove(key, e)
		db.expired++
		db.notify(key)
		return nil
	}
	e.access = db.now
	return e
}

// typed returns the live entry for key if it has the wanted kind, nil if the
// key is missing, and ErrWrongType if it holds something else.
func (db *DB) typed(key string, kind Kind) (*entry, error) {
	e := db.lookup(key)
	if e == nil {
		return nil, nil
	}
	if e.kind != kind {
		return nil, ErrWrongType
	}
	return e, nil
}

// put stores e under key, replacing any existing entry.
func (db *DB) put(key string, e *entry) {
	if old := db.m[key]; old != nil {
		db.remove(key, old)
	}
	ttl := e.expireAt
	e.expireAt, e.vidx = 0, -1
	e.access = db.now
	e.idx = len(db.keys)
	db.keys = append(db.keys, key)
	db.m[key] = e
	db.mem += int64(entryOverhead + len(key) + e.size)
	db.setExpireAt(key, e, ttl)
}

// remove deletes key's entry from every index.
func (db *DB) remove(key string, e *entry) {
	db.setExpireAt(key, e, 0)

	last := len(db.keys) - 1
	if e.idx != last {
		moved := db.keys[last]
		db.keys[e.idx] = moved
		db.m[moved].idx = e.idx
	}
	db.keys[last] = "" // let the string be collected
	db.keys = db.keys[:last]

	delete(db.m, key)
	db.mem -= int64(entryOverhead + len(key) + e.size)
}

// setExpireAt changes e's expiry and keeps the volatile index in sync.
// ms == 0 removes the expiry.
func (db *DB) setExpireAt(key string, e *entry, ms int64) {
	switch {
	case ms == 0 && e.vidx >= 0:
		last := len(db.volatile) - 1
		if e.vidx != last {
			moved := db.volatile[last]
			db.volatile[e.vidx] = moved
			db.m[moved].vidx = e.vidx
		}
		db.volatile[last] = ""
		db.volatile = db.volatile[:last]
		e.vidx = -1
	case ms != 0 && e.vidx < 0:
		e.vidx = len(db.volatile)
		db.volatile = append(db.volatile, key)
	}
	e.expireAt = ms
}

// resize adjusts an entry's size estimate and the DB total together.
func (db *DB) resize(e *entry, delta int) {
	e.size += delta
	db.mem += int64(delta)
}

// Exists reports whether key holds a live value.
func (db *DB) Exists(key string) bool { return db.lookup(key) != nil }

// Delete removes key and reports whether it existed.
func (db *DB) Delete(key string) bool {
	e := db.lookup(key)
	if e == nil {
		// A replica hides expired keys without removing them. The primary's
		// explicit DEL must really remove the hidden entry, or it would
		// linger forever.
		if raw := db.m[key]; raw != nil && db.replica {
			db.remove(key, raw)
		}
		return false
	}
	db.remove(key, e)
	return true
}

// Type returns the kind of key's value, or 0 if it does not exist.
func (db *DB) Type(key string) Kind {
	if e := db.lookup(key); e != nil {
		return e.kind
	}
	return 0
}

// ExpireAt sets key's absolute expiry (unix ms) and reports whether the key
// existed. Passing a time that is already past does not delete the key; the
// caller decides (the command layer deletes and propagates a DEL).
func (db *DB) ExpireAt(key string, ms int64) bool {
	e := db.lookup(key)
	if e == nil {
		return false
	}
	db.setExpireAt(key, e, ms)
	return true
}

// ExpireAtOf returns key's absolute expiry in unix ms (0 if it has none) and
// whether the key exists.
func (db *DB) ExpireAtOf(key string) (ms int64, exists bool) {
	e := db.lookup(key)
	if e == nil {
		return 0, false
	}
	return e.expireAt, true
}

// Persist removes key's expiry and reports whether there was one.
func (db *DB) Persist(key string) bool {
	e := db.lookup(key)
	if e == nil || e.expireAt == 0 {
		return false
	}
	db.setExpireAt(key, e, 0)
	return true
}

// Keys returns the live keys matching the glob pattern. It walks the whole
// keyspace, so on a big database it blocks the owner for a while; Redis has
// the same property.
func (db *DB) Keys(pattern string) []string {
	var out []string
	all := pattern == "*"
	for key, e := range db.m {
		if e.expireAt != 0 && e.expireAt <= db.now {
			continue
		}
		if all || glob.Match(pattern, key) {
			out = append(out, key)
		}
	}
	return out
}

// Len returns the number of keys, including expired ones that have not been
// removed yet (like Redis's DBSIZE).
func (db *DB) Len() int { return len(db.m) }

// VolatileLen returns how many keys have an expiry.
func (db *DB) VolatileLen() int { return len(db.volatile) }

// Clear removes every key.
func (db *DB) Clear() {
	db.m = make(map[string]*entry)
	db.keys = nil
	db.volatile = nil
	db.mem = 0
}

// Stats are counters reported by INFO.
type Stats struct {
	ExpiredKeys int64
	EvictedKeys int64
}

// Stats returns the removal counters.
func (db *DB) Stats() Stats { return Stats{ExpiredKeys: db.expired, EvictedKeys: db.evicted} }

// ActiveExpire deletes expired keys by sampling, in the style of Redis: look
// at up to sample random keys that have an expiry, delete the expired ones,
// and repeat immediately while more than about 25% of the sample was expired,
// stopping when budget of real time is spent. It returns the number deleted.
// Replicas do nothing: they wait for the primary's DELs.
func (db *DB) ActiveExpire(sample int, budget time.Duration) int {
	start := time.Now()
	if db.replica || sample <= 0 {
		return 0
	}
	total := 0
	for {
		n := min(sample, len(db.volatile))
		if n == 0 {
			return total
		}
		expired := 0
		for range n {
			if len(db.volatile) == 0 {
				break
			}
			key := db.volatile[db.rng.IntN(len(db.volatile))]
			if e := db.m[key]; e.expireAt <= db.now {
				db.remove(key, e)
				db.expired++
				db.notify(key)
				expired++
			}
		}
		total += expired
		if expired*4 <= n || time.Since(start) >= budget {
			return total
		}
	}
}
