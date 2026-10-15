package store

// HSet sets field/value pairs (pairs is [f1, v1, f2, v2, ...]) in the hash at
// key, creating it if needed, and returns how many fields were new.
func (db *DB) HSet(key string, pairs []string) (int, error) {
	e, err := db.typed(key, KindHash)
	if err != nil {
		return 0, err
	}
	if e == nil {
		e = newEntry(KindHash)
		e.hash = make(map[string]string)
		db.put(key, e)
	}
	added := 0
	for i := 0; i+1 < len(pairs); i += 2 {
		f, v := pairs[i], pairs[i+1]
		if old, ok := e.hash[f]; ok {
			db.resize(e, len(v)-len(old))
		} else {
			added++
			db.resize(e, len(f)+len(v)+hashFieldOverhead)
		}
		e.hash[f] = v
	}
	return added, nil
}

// HGet returns the value of field.
func (db *DB) HGet(key, field string) (string, bool, error) {
	e, err := db.typed(key, KindHash)
	if e == nil {
		return "", false, err
	}
	v, ok := e.hash[field]
	return v, ok, nil
}

// HDel removes fields and returns how many existed. An emptied hash is
// deleted.
func (db *DB) HDel(key string, fields []string) (int, error) {
	e, err := db.typed(key, KindHash)
	if e == nil {
		return 0, err
	}
	removed := 0
	for _, f := range fields {
		if v, ok := e.hash[f]; ok {
			delete(e.hash, f)
			db.resize(e, -(len(f) + len(v) + hashFieldOverhead))
			removed++
		}
	}
	if len(e.hash) == 0 {
		db.remove(key, e)
	}
	return removed, nil
}

// HGetAll returns every field and value as [f1, v1, f2, v2, ...]. The order
// is unspecified.
func (db *DB) HGetAll(key string) ([]string, error) {
	e, err := db.typed(key, KindHash)
	if e == nil {
		return nil, err
	}
	out := make([]string, 0, 2*len(e.hash))
	for f, v := range e.hash {
		out = append(out, f, v)
	}
	return out, nil
}

// HExists reports whether field is in the hash.
func (db *DB) HExists(key, field string) (bool, error) {
	_, ok, err := db.HGet(key, field)
	return ok, err
}

// HLen returns the number of fields.
func (db *DB) HLen(key string) (int, error) {
	e, err := db.typed(key, KindHash)
	if e == nil {
		return 0, err
	}
	return len(e.hash), nil
}
