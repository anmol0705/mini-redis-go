package store

import (
	"fmt"
	"slices"
	"strings"
)

// Item is one key with its whole value as plain data. A slice of Items is
// the currency of persistence and replication: the AOF rewrite, snapshots
// and full resync all start from DB.Dump.
type Item struct {
	Key      string
	Kind     Kind
	ExpireAt int64    // unix ms, 0 for none
	Str      string   // KindString
	List     []string // KindList, head first
	Hash     []string // KindHash, [field, value, field, value, ...]
	Set      []string // KindSet
	ZSet     []ZMember
}

// Dump returns a deep copy of every live key. Because it copies, the result
// can be written to disk or a socket by another goroutine while the owner
// keeps changing the DB. The copy is the only pause that costs the owner time.
func (db *DB) Dump() []Item {
	items := make([]Item, 0, len(db.keys))
	for _, key := range db.keys {
		e := db.m[key]
		if e.expireAt != 0 && e.expireAt <= db.now {
			continue
		}
		it := Item{Key: key, Kind: e.kind, ExpireAt: e.expireAt}
		switch e.kind {
		case KindString:
			it.Str = e.str
		case KindList:
			it.List = make([]string, e.list.n)
			for i := range e.list.n {
				it.List[i] = e.list.at(i)
			}
		case KindHash:
			it.Hash = make([]string, 0, 2*len(e.hash))
			for f, v := range e.hash {
				it.Hash = append(it.Hash, f, v)
			}
		case KindSet:
			it.Set = make([]string, 0, len(e.set))
			for m := range e.set {
				it.Set = append(it.Set, m)
			}
		case KindZSet:
			els := e.zset.sl.Range(0, len(e.zset.dict))
			it.ZSet = make([]ZMember, len(els))
			for i, el := range els {
				it.ZSet[i] = ZMember{Member: el.Member, Score: el.Score}
			}
		}
		items = append(items, it)
	}
	return items
}

// Load replaces the whole keyspace with items. Items that are already
// expired are skipped. On error the DB is left empty-but-consistent.
func (db *DB) Load(items []Item) error {
	db.Clear()
	for _, it := range items {
		if it.ExpireAt != 0 && it.ExpireAt <= db.now {
			continue
		}
		var err error
		switch it.Kind {
		case KindString:
			db.Set(it.Key, it.Str, 0, false)
		case KindList:
			if len(it.List) == 0 {
				err = fmt.Errorf("%w: empty list %q", ErrInvalid, it.Key)
				break
			}
			_, err = db.Push(it.Key, false, it.List...)
		case KindHash:
			if len(it.Hash) == 0 || len(it.Hash)%2 != 0 {
				err = fmt.Errorf("%w: bad hash %q", ErrInvalid, it.Key)
				break
			}
			_, err = db.HSet(it.Key, it.Hash)
		case KindSet:
			if len(it.Set) == 0 {
				err = fmt.Errorf("%w: empty set %q", ErrInvalid, it.Key)
				break
			}
			_, err = db.SAdd(it.Key, it.Set)
		case KindZSet:
			if len(it.ZSet) == 0 {
				err = fmt.Errorf("%w: empty sorted set %q", ErrInvalid, it.Key)
				break
			}
			_, err = db.ZAdd(it.Key, it.ZSet)
		default:
			err = fmt.Errorf("%w: unknown kind %d for %q", ErrInvalid, it.Kind, it.Key)
		}
		if err != nil {
			db.Clear()
			return err
		}
		if it.ExpireAt != 0 {
			db.ExpireAt(it.Key, it.ExpireAt)
		}
	}
	return nil
}

// Canonical sorts items and the unordered parts inside them (hash pairs, set
// members) so two dumps of the same data compare equal and encode to the same
// bytes. Lists and sorted sets are already ordered. It sorts in place.
func Canonical(items []Item) []Item {
	for i := range items {
		it := &items[i]
		if len(it.Hash) > 1 {
			pairs := make([][2]string, 0, len(it.Hash)/2)
			for j := 0; j+1 < len(it.Hash); j += 2 {
				pairs = append(pairs, [2]string{it.Hash[j], it.Hash[j+1]})
			}
			slices.SortFunc(pairs, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
			for j, p := range pairs {
				it.Hash[2*j], it.Hash[2*j+1] = p[0], p[1]
			}
		}
		slices.Sort(it.Set)
	}
	slices.SortFunc(items, func(a, b Item) int { return strings.Compare(a.Key, b.Key) })
	return items
}
