package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

func invalidExpire(cmd string) resp.Value {
	return resp.Error("ERR invalid expire time in '" + cmd + "' command")
}

// addMs returns now + delta, reporting false on overflow.
func addMs(now, delta int64) (int64, bool) {
	if delta > math.MaxInt64-now {
		return 0, false
	}
	return now + delta, true
}

// cmdSet implements SET key value [NX|XX] [KEEPTTL|EX s|PX ms|EXAT s|PXAT ms].
func cmdSet(x *Ctx) resp.Value {
	args := x.Args
	key, value := args[1], args[2]
	now := x.DB.Now()

	var nx, xx, keepTTL bool
	var expireAt int64
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "NX":
			if xx {
				return errSyntax
			}
			nx = true
		case "XX":
			if nx {
				return errSyntax
			}
			xx = true
		case "KEEPTTL":
			if expireAt != 0 || keepTTL {
				return errSyntax
			}
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if expireAt != 0 || keepTTL || i+1 >= len(args) {
				return errSyntax
			}
			n, err := store.ParseInt(args[i+1])
			if err != nil {
				return errNotInt
			}
			i++
			var ok bool
			switch {
			case n <= 0:
				ok = false
			case opt == "EX":
				if n <= math.MaxInt64/1000 {
					expireAt, ok = addMs(now, n*1000)
				}
			case opt == "PX":
				expireAt, ok = addMs(now, n)
			case opt == "EXAT":
				if n <= math.MaxInt64/1000 {
					expireAt, ok = n*1000, true
				}
			default: // PXAT
				expireAt, ok = n, true
			}
			if !ok {
				return invalidExpire("set")
			}
		default:
			return errSyntax
		}
	}

	exists := x.DB.Exists(key)
	if (nx && exists) || (xx && !exists) {
		x.skipPropagation()
		return resp.Null()
	}
	x.DB.Set(key, value, expireAt, keepTTL)

	// Log the effect in a form that does not depend on the clock or on the
	// condition that has already been checked.
	switch {
	case len(args) == 3:
		// verbatim
	case expireAt != 0:
		x.emit("SET", key, value, "PXAT", strconv.FormatInt(expireAt, 10))
	case keepTTL:
		x.emit("SET", key, value, "KEEPTTL")
	default:
		x.emit("SET", key, value)
	}
	return okReply
}

func cmdGet(x *Ctx) resp.Value {
	v, ok, err := x.DB.Get(x.Args[1])
	if err != nil {
		return errReply(err)
	}
	if !ok {
		return resp.Null()
	}
	return resp.BulkString(v)
}

// cmdMGet returns null for keys that are missing or not strings, as Redis does.
func cmdMGet(x *Ctx) resp.Value {
	out := make([]resp.Value, 0, len(x.Args)-1)
	for _, k := range x.Args[1:] {
		if v, ok, err := x.DB.Get(k); ok && err == nil {
			out = append(out, resp.BulkString(v))
		} else {
			out = append(out, resp.Null())
		}
	}
	return resp.Array(out...)
}

func cmdMSet(x *Ctx) resp.Value {
	args := x.Args
	if len(args)%2 != 1 {
		return wrongArgs("mset")
	}
	for i := 1; i < len(args); i += 2 {
		x.DB.Set(args[i], args[i+1], 0, false)
	}
	return okReply
}

func incr(x *Ctx, delta int64) resp.Value {
	n, err := x.DB.IncrBy(x.Args[1], delta)
	if err != nil {
		return errReply(err)
	}
	return resp.Integer(n)
}

func cmdIncr(x *Ctx) resp.Value { return incr(x, 1) }
func cmdDecr(x *Ctx) resp.Value { return incr(x, -1) }

func cmdIncrBy(x *Ctx) resp.Value {
	n, err := store.ParseInt(x.Args[2])
	if err != nil {
		return errNotInt
	}
	return incr(x, n)
}

func cmdDecrBy(x *Ctx) resp.Value {
	n, err := store.ParseInt(x.Args[2])
	if err != nil {
		return errNotInt
	}
	if n == math.MinInt64 { // -n would overflow
		return errReply(store.ErrOverflow)
	}
	return incr(x, -n)
}

func cmdAppend(x *Ctx) resp.Value {
	n, err := x.DB.Append(x.Args[1], x.Args[2])
	if err != nil {
		return errReply(err)
	}
	return resp.Integer(int64(n))
}

func cmdStrLen(x *Ctx) resp.Value {
	n, err := x.DB.StrLen(x.Args[1])
	if err != nil {
		return errReply(err)
	}
	return resp.Integer(int64(n))
}
