package store

// deque is a ring buffer of strings: push and pop at either end are O(1),
// indexing is O(1). len(buf) is always a power of two so wrapping is a mask.
type deque struct {
	buf  []string
	head int
	n    int
}

func (d *deque) at(i int) string { return d.buf[(d.head+i)&(len(d.buf)-1)] }

func (d *deque) grow() {
	size := max(4, 2*len(d.buf))
	nb := make([]string, size)
	for i := range d.n {
		nb[i] = d.at(i)
	}
	d.buf, d.head = nb, 0
}

func (d *deque) pushBack(s string) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.buf[(d.head+d.n)&(len(d.buf)-1)] = s
	d.n++
}

func (d *deque) pushFront(s string) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.head = (d.head - 1) & (len(d.buf) - 1)
	d.buf[d.head] = s
	d.n++
}

func (d *deque) popFront() string {
	s := d.buf[d.head]
	d.buf[d.head] = "" // release the string for the garbage collector
	d.head = (d.head + 1) & (len(d.buf) - 1)
	d.n--
	return s
}

func (d *deque) popBack() string {
	i := (d.head + d.n - 1) & (len(d.buf) - 1)
	s := d.buf[i]
	d.buf[i] = ""
	d.n--
	return s
}

// normRange turns Redis-style inclusive indexes (negative counts from the
// end) into a clipped [start, end] over n elements. ok is false when the
// range is empty.
func normRange(start, stop, n int) (s, e int, ok bool) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start = max(start, 0)
	if start > stop || start >= n {
		return 0, 0, false
	}
	return start, min(stop, n-1), true
}

// Push adds vals to the head (front) or tail of the list at key, creating it
// if needed, and returns the new length. Pushing several values to the front
// reverses their order, as in Redis (LPUSH k a b c gives c b a).
func (db *DB) Push(key string, front bool, vals ...string) (int, error) {
	e, err := db.typed(key, KindList)
	if err != nil {
		return 0, err
	}
	if e == nil {
		e = newEntry(KindList)
		e.list = &deque{}
		db.put(key, e)
	}
	for _, v := range vals {
		if front {
			e.list.pushFront(v)
		} else {
			e.list.pushBack(v)
		}
		db.resize(e, len(v)+listElemOverhead)
	}
	return e.list.n, nil
}

// Pop removes up to count elements from the head or tail. found is false
// when the key does not exist. An emptied list is deleted.
func (db *DB) Pop(key string, front bool, count int) (vals []string, found bool, err error) {
	e, err := db.typed(key, KindList)
	if e == nil {
		return nil, false, err
	}
	count = min(count, e.list.n)
	vals = make([]string, 0, count)
	for range count {
		var v string
		if front {
			v = e.list.popFront()
		} else {
			v = e.list.popBack()
		}
		db.resize(e, -(len(v) + listElemOverhead))
		vals = append(vals, v)
	}
	if e.list.n == 0 {
		db.remove(key, e)
	}
	return vals, true, nil
}

// ListLen returns the length of the list at key (0 if missing).
func (db *DB) ListLen(key string) (int, error) {
	e, err := db.typed(key, KindList)
	if e == nil {
		return 0, err
	}
	return e.list.n, nil
}

// ListRange returns elements start..stop inclusive (negative indexes count
// from the end).
func (db *DB) ListRange(key string, start, stop int) ([]string, error) {
	e, err := db.typed(key, KindList)
	if e == nil {
		return nil, err
	}
	s, t, ok := normRange(start, stop, e.list.n)
	if !ok {
		return nil, nil
	}
	out := make([]string, 0, t-s+1)
	for i := s; i <= t; i++ {
		out = append(out, e.list.at(i))
	}
	return out, nil
}

// ListIndex returns the element at idx (negative counts from the end).
func (db *DB) ListIndex(key string, idx int) (string, bool, error) {
	e, err := db.typed(key, KindList)
	if e == nil {
		return "", false, err
	}
	if idx < 0 {
		idx += e.list.n
	}
	if idx < 0 || idx >= e.list.n {
		return "", false, nil
	}
	return e.list.at(idx), true, nil
}
