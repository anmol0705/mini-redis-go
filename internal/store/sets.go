package store

// SAdd adds members to the set at key, creating it if needed, and returns
// how many were new.
func (db *DB) SAdd(key string, members []string) (int, error) {
	e, err := db.typed(key, KindSet)
	if err != nil {
		return 0, err
	}
	if e == nil {
		e = newEntry(KindSet)
		e.set = make(map[string]struct{})
		db.put(key, e)
	}
	added := 0
	for _, m := range members {
		if _, ok := e.set[m]; !ok {
			e.set[m] = struct{}{}
			db.resize(e, len(m)+setMemberOverhead)
			added++
		}
	}
	return added, nil
}

// SRem removes members and returns how many existed. An emptied set is
// deleted.
func (db *DB) SRem(key string, members []string) (int, error) {
	e, err := db.typed(key, KindSet)
	if e == nil {
		return 0, err
	}
	removed := 0
	for _, m := range members {
		if _, ok := e.set[m]; ok {
			delete(e.set, m)
			db.resize(e, -(len(m) + setMemberOverhead))
			removed++
		}
	}
	if len(e.set) == 0 {
		db.remove(key, e)
	}
	return removed, nil
}

// SIsMember reports whether member is in the set.
func (db *DB) SIsMember(key, member string) (bool, error) {
	e, err := db.typed(key, KindSet)
	if e == nil {
		return false, err
	}
	_, ok := e.set[member]
	return ok, nil
}

// SMembers returns every member, in unspecified order.
func (db *DB) SMembers(key string) ([]string, error) {
	e, err := db.typed(key, KindSet)
	if e == nil {
		return nil, err
	}
	out := make([]string, 0, len(e.set))
	for m := range e.set {
		out = append(out, m)
	}
	return out, nil
}

// SCard returns the number of members.
func (db *DB) SCard(key string) (int, error) {
	e, err := db.typed(key, KindSet)
	if e == nil {
		return 0, err
	}
	return len(e.set), nil
}
