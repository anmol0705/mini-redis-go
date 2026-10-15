package repl

// ring is the replication backlog: a fixed-size buffer holding the most
// recent bytes of the replication stream. Positions are absolute stream
// offsets (bytes produced since the stream began), so a replica that says
// "I have everything up to offset N" can be answered by copying from N to the
// end - if N is still inside the window.
//
// A ring is not safe for concurrent use; the engine goroutine owns it.
type ring struct {
	buf        []byte
	start, end int64 // the buffer holds stream bytes [start, end)
}

func newRing(capacity int) *ring {
	return &ring{buf: make([]byte, max(capacity, 1))}
}

// reset empties the ring and makes offset the next byte's position.
func (r *ring) reset(offset int64) { r.start, r.end = offset, offset }

// write appends p, discarding the oldest bytes if the window is full.
func (r *ring) write(p []byte) {
	c := int64(len(r.buf))
	if int64(len(p)) > c {
		// Only the last c bytes can be kept; account for the rest.
		skipped := int64(len(p)) - c
		r.end += skipped
		p = p[skipped:]
	}
	pos := r.end % c
	n := copy(r.buf[pos:], p)
	copy(r.buf, p[n:])
	r.end += int64(len(p))
	if r.end-r.start > c {
		r.start = r.end - c
	}
}

// from returns a copy of the stream bytes from offset to the end. ok is false
// if offset is outside the window, in which case the replica must do a full
// resync.
func (r *ring) from(offset int64) (data []byte, ok bool) {
	if offset < r.start || offset > r.end {
		return nil, false
	}
	n := r.end - offset
	out := make([]byte, n)
	c := int64(len(r.buf))
	pos := offset % c
	first := copy(out, r.buf[pos:min(pos+n, c)])
	copy(out[first:], r.buf[:n-int64(first)])
	return out, true
}

// held returns how many bytes the window currently holds.
func (r *ring) held() int64 { return r.end - r.start }
