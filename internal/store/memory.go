package store

// Memory accounting is an ESTIMATE, not what the Go runtime really uses
// (see docs/known-limitations.md). Each key costs
//
//	entryOverhead + len(key) + size of the value
//
// where the value size is the sum of the bytes of its strings plus a fixed
// per-element constant for the container bookkeeping (map slots, string
// headers, skip list nodes). The constants are rough but stable, which is
// what eviction needs: bigger data must look bigger, and deleting data must
// give the same bytes back.
const (
	entryOverhead      = 96 // map slot, entry struct, two index slots
	listElemOverhead   = 16 // one string header in the ring buffer
	hashFieldOverhead  = 48 // map slot plus two string headers
	setMemberOverhead  = 32 // map slot plus one string header
	zsetMemberOverhead = 96 // dict slot plus skip list node and levels
)

// UsedMemory returns the estimated bytes used by all keys and values.
func (db *DB) UsedMemory() int64 { return db.mem }
