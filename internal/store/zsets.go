package store

import (
	"math/rand/v2"

	"github.com/anmol0705/mini-redis-go/internal/skiplist"
)

// ZMember is a sorted-set member with its score.
type ZMember struct {
	Member string
	Score  float64
}

// zset pairs a member->score map (O(1) score lookup) with a skip list
// (ordered by score, O(log n) rank and range). Both are kept in step by the
// methods below.
type zset struct {
	dict map[string]float64
	sl   *skiplist.List
}

func (db *DB) newZSet() *zset {
	// Each skip list gets its own generator derived from the DB's, so a
	// seeded DB behaves reproducibly.
	rng := rand.New(rand.NewPCG(db.rng.Uint64(), db.rng.Uint64()))
	return &zset{dict: make(map[string]float64), sl: skiplist.New(rng)}
}

// ZAdd sets scores for members in the sorted set at key, creating it if
// needed, and returns how many members were new. Scores must not be NaN
// (the command layer checks).
func (db *DB) ZAdd(key string, members []ZMember) (int, error) {
	e, err := db.typed(key, KindZSet)
	if err != nil {
		return 0, err
	}
	if e == nil {
		e = newEntry(KindZSet)
		e.zset = db.newZSet()
		db.put(key, e)
	}
	added := 0
	for _, m := range members {
		old, exists := e.zset.dict[m.Member]
		switch {
		case !exists:
			e.zset.dict[m.Member] = m.Score
			e.zset.sl.Insert(m.Score, m.Member)
			db.resize(e, len(m.Member)+zsetMemberOverhead)
			added++
		case old != m.Score:
			e.zset.sl.Delete(old, m.Member)
			e.zset.sl.Insert(m.Score, m.Member)
			e.zset.dict[m.Member] = m.Score
		}
	}
	return added, nil
}

// ZRem removes members and returns how many existed. An emptied sorted set
// is deleted.
func (db *DB) ZRem(key string, members []string) (int, error) {
	e, err := db.typed(key, KindZSet)
	if e == nil {
		return 0, err
	}
	removed := 0
	for _, m := range members {
		if score, ok := e.zset.dict[m]; ok {
			delete(e.zset.dict, m)
			e.zset.sl.Delete(score, m)
			db.resize(e, -(len(m) + zsetMemberOverhead))
			removed++
		}
	}
	if len(e.zset.dict) == 0 {
		db.remove(key, e)
	}
	return removed, nil
}

// ZScore returns member's score.
func (db *DB) ZScore(key, member string) (float64, bool, error) {
	e, err := db.typed(key, KindZSet)
	if e == nil {
		return 0, false, err
	}
	s, ok := e.zset.dict[member]
	return s, ok, nil
}

// ZRank returns member's zero-based position in ascending order.
func (db *DB) ZRank(key, member string) (int, bool, error) {
	e, err := db.typed(key, KindZSet)
	if e == nil {
		return 0, false, err
	}
	score, ok := e.zset.dict[member]
	if !ok {
		return 0, false, nil
	}
	rank, _ := e.zset.sl.Rank(score, member)
	return rank, true, nil
}

// ZRange returns members at positions start..stop inclusive in ascending
// order (negative indexes count from the end).
func (db *DB) ZRange(key string, start, stop int) ([]ZMember, error) {
	e, err := db.typed(key, KindZSet)
	if e == nil {
		return nil, err
	}
	s, t, ok := normRange(start, stop, len(e.zset.dict))
	if !ok {
		return nil, nil
	}
	els := e.zset.sl.Range(s, t)
	out := make([]ZMember, len(els))
	for i, el := range els {
		out[i] = ZMember{Member: el.Member, Score: el.Score}
	}
	return out, nil
}

// ZCard returns the number of members.
func (db *DB) ZCard(key string) (int, error) {
	e, err := db.typed(key, KindZSet)
	if e == nil {
		return 0, err
	}
	return len(e.zset.dict), nil
}
