package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

func cmdDel(x *Ctx) resp.Value {
	var n int64
	for _, k := range x.Args[1:] {
		if x.DB.Delete(k) {
			n++
		}
	}
	if n == 0 {
		x.skipPropagation()
	}
	return resp.Integer(n)
}

// cmdExists counts a key once per mention, as Redis does:
// EXISTS a a returns 2 when a exists.
func cmdExists(x *Ctx) resp.Value {
	var n int64
	for _, k := range x.Args[1:] {
		if x.DB.Exists(k) {
			n++
		}
	}
	return resp.Integer(n)
}

func cmdDBSize(x *Ctx) resp.Value { return resp.Integer(int64(x.DB.Len())) }

// cmdFlush serves FLUSHALL and FLUSHDB. We have a single database, so they
// are the same. The optional ASYNC/SYNC word is accepted and ignored.
func cmdFlush(x *Ctx) resp.Value {
	args := x.Args
	if len(args) > 2 {
		return errSyntax
	}
	if len(args) == 2 && !strings.EqualFold(args[1], "ASYNC") && !strings.EqualFold(args[1], "SYNC") {
		return errSyntax
	}
	x.DB.Clear()
	x.emit("FLUSHALL")
	return okReply
}

func cmdType(x *Ctx) resp.Value {
	return resp.SimpleString(x.DB.Type(x.Args[1]).String())
}

func cmdKeys(x *Ctx) resp.Value {
	keys := x.DB.Keys(x.Args[1])
	out := make([]resp.Value, len(keys))
	for i, k := range keys {
		out[i] = resp.BulkString(k)
	}
	return resp.Array(out...)
}

// expire is the shared body of EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT.
// unit is the number of milliseconds per argument unit; absolute says the
// argument is a point in time rather than a delay.
func expire(x *Ctx, unit int64, absolute bool) resp.Value {
	name := strings.ToLower(x.Args[0])
	key := x.Args[1]
	n, err := store.ParseInt(x.Args[2])
	if err != nil {
		return errNotInt
	}
	if n > math.MaxInt64/unit || n < math.MinInt64/unit {
		return invalidExpire(name)
	}
	ms := n * unit
	if !absolute {
		var ok bool
		if ms, ok = addMs(x.DB.Now(), ms); !ok {
			return invalidExpire(name)
		}
	}

	if !x.DB.Exists(key) {
		x.skipPropagation()
		return resp.Integer(0)
	}
	if ms <= x.DB.Now() {
		// A time in the past means "expire now": delete, like Redis.
		x.DB.Delete(key)
		x.emit("DEL", key)
		return resp.Integer(1)
	}
	x.DB.ExpireAt(key, ms)
	// Always log the absolute form so replaying later does not stretch the TTL.
	x.emit("PEXPIREAT", key, strconv.FormatInt(ms, 10))
	return resp.Integer(1)
}

func cmdExpire(x *Ctx) resp.Value    { return expire(x, 1000, false) }
func cmdPExpire(x *Ctx) resp.Value   { return expire(x, 1, false) }
func cmdExpireAt(x *Ctx) resp.Value  { return expire(x, 1000, true) }
func cmdPExpireAt(x *Ctx) resp.Value { return expire(x, 1, true) }

// ttl returns the remaining life of a key: -2 if it does not exist, -1 if it
// has no expiry, otherwise the time left in milliseconds.
func ttl(x *Ctx) int64 {
	ms, exists := x.DB.ExpireAtOf(x.Args[1])
	switch {
	case !exists:
		return -2
	case ms == 0:
		return -1
	}
	return max(ms-x.DB.Now(), 0)
}

func cmdPTTL(x *Ctx) resp.Value { return resp.Integer(ttl(x)) }

func cmdTTL(x *Ctx) resp.Value {
	n := ttl(x)
	if n < 0 {
		return resp.Integer(n)
	}
	return resp.Integer((n + 500) / 1000) // round to the nearest second, like Redis
}

func cmdPersist(x *Ctx) resp.Value {
	if x.DB.Persist(x.Args[1]) {
		return resp.Integer(1)
	}
	x.skipPropagation()
	return resp.Integer(0)
}
