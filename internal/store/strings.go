package store

import (
	"math"
	"strconv"
)

// Get returns the string stored at key. ok is false if the key is missing.
func (db *DB) Get(key string) (value string, ok bool, err error) {
	e, err := db.typed(key, KindString)
	if e == nil {
		return "", false, err
	}
	return e.str, true, nil
}

// Set stores value at key, replacing whatever was there (of any type).
// expireAt is an absolute expiry in unix ms (0 for none). With keepTTL the
// previous expiry is kept instead. An expiry that is already in the past
// deletes the key.
func (db *DB) Set(key, value string, expireAt int64, keepTTL bool) {
	old := db.lookup(key)
	if keepTTL && old != nil {
		expireAt = old.expireAt
	}
	if expireAt != 0 && expireAt <= db.now {
		if old != nil {
			db.remove(key, old)
		}
		return
	}
	if old != nil && old.kind == KindString {
		db.resize(old, len(value)-len(old.str))
		old.str = value
		db.setExpireAt(key, old, expireAt)
		return
	}
	e := newEntry(KindString)
	e.str, e.size, e.expireAt = value, len(value), expireAt
	db.put(key, e)
}

// IncrBy adds delta to the integer stored at key (a missing key counts as 0)
// and returns the new value.
func (db *DB) IncrBy(key string, delta int64) (int64, error) {
	e, err := db.typed(key, KindString)
	if err != nil {
		return 0, err
	}
	var cur int64
	if e != nil {
		if cur, err = ParseInt(e.str); err != nil {
			return 0, err
		}
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	next := cur + delta
	s := strconv.FormatInt(next, 10)
	if e != nil {
		db.resize(e, len(s)-len(e.str))
		e.str = s
		return next, nil
	}
	ne := newEntry(KindString)
	ne.str, ne.size = s, len(s)
	db.put(key, ne)
	return next, nil
}

// Append adds s to the end of the string at key (creating it if missing) and
// returns the new length.
func (db *DB) Append(key, s string) (int, error) {
	e, err := db.typed(key, KindString)
	if err != nil {
		return 0, err
	}
	if e == nil {
		ne := newEntry(KindString)
		ne.str, ne.size = s, len(s)
		db.put(key, ne)
		return len(s), nil
	}
	if len(e.str)+len(s) > maxStringLen {
		return 0, ErrTooBig
	}
	db.resize(e, len(s))
	e.str += s
	return len(e.str), nil
}

// StrLen returns the length of the string at key (0 if missing).
func (db *DB) StrLen(key string) (int, error) {
	e, err := db.typed(key, KindString)
	if e == nil {
		return 0, err
	}
	return len(e.str), nil
}
