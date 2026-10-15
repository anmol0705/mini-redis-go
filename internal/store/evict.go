package store

import "fmt"

// Policy says which key to evict when memory is full.
type Policy int

// The supported eviction policies (names match Redis's maxmemory-policy).
const (
	// NoEviction never evicts; writes that need memory fail instead.
	NoEviction Policy = iota
	// AllKeysLRU evicts the approximately least recently used key.
	AllKeysLRU
	// AllKeysRandom evicts a random key.
	AllKeysRandom
	// VolatileTTL evicts the key with the nearest expiry, among keys that have one.
	VolatileTTL
)

var policyNames = [...]string{"noeviction", "allkeys-lru", "allkeys-random", "volatile-ttl"}

// String returns the Redis name of the policy.
func (p Policy) String() string {
	if p < 0 || int(p) >= len(policyNames) {
		return "unknown"
	}
	return policyNames[p]
}

// ParsePolicy converts a maxmemory-policy name into a Policy.
func ParsePolicy(s string) (Policy, error) {
	for i, n := range policyNames {
		if n == s {
			return Policy(i), nil
		}
	}
	return 0, fmt.Errorf("unknown eviction policy %q (want noeviction, allkeys-lru, allkeys-random or volatile-ttl)", s)
}

// Evict removes one key chosen by policy p and returns it. It is
// approximate on purpose: it looks at samples random keys and takes the best
// candidate among them, instead of keeping an exact LRU list. That costs no
// extra memory per key and no work on every access, and in practice evicts
// nearly the same keys. ok is false if there is nothing evictable.
func (db *DB) Evict(p Policy, samples int) (key string, ok bool) {
	pool := db.keys
	if p == VolatileTTL {
		pool = db.volatile
	}
	if p == NoEviction || len(pool) == 0 {
		return "", false
	}
	if p == AllKeysRandom {
		samples = 1
	}
	samples = max(samples, 1)

	var best string
	var bestE *entry
	for range samples {
		k := pool[db.rng.IntN(len(pool))]
		e := db.m[k]
		better := bestE == nil ||
			(p == AllKeysLRU && e.access < bestE.access) ||
			(p == VolatileTTL && e.expireAt < bestE.expireAt)
		if better {
			best, bestE = k, e
		}
	}
	db.remove(best, bestE)
	db.evicted++
	db.notify(best)
	return best, true
}
