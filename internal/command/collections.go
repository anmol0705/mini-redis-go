package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// clampInt narrows an int64 index to int without wrapping.
func clampInt(n int64) int {
	return int(max(min(n, math.MaxInt32), math.MinInt32))
}

func bulkArray(vals []string) resp.Value {
	out := make([]resp.Value, len(vals))
	for i, v := range vals {
		out[i] = resp.BulkString(v)
	}
	return resp.Array(out...)
}

func intReply(n int, err error) resp.Value {
	if err != nil {
		return errReply(err)
	}
	return resp.Integer(int64(n))
}

// ---- lists ----

func cmdLPush(x *Ctx) resp.Value { return intReply(x.DB.Push(x.Args[1], true, x.Args[2:]...)) }
func cmdRPush(x *Ctx) resp.Value { return intReply(x.DB.Push(x.Args[1], false, x.Args[2:]...)) }
func cmdLLen(x *Ctx) resp.Value  { return intReply(x.DB.ListLen(x.Args[1])) }

func pop(x *Ctx, front bool) resp.Value {
	if len(x.Args) > 3 {
		return wrongArgs(strings.ToLower(x.Args[0]))
	}
	count, withCount := 1, len(x.Args) == 3
	if withCount {
		n, err := store.ParseInt(x.Args[2])
		if err != nil || n < 0 {
			return resp.Error("ERR value is out of range, must be positive")
		}
		count = clampInt(n)
	}
	vals, found, err := x.DB.Pop(x.Args[1], front, count)
	if err != nil {
		return errReply(err)
	}
	if !found {
		x.skipPropagation()
		return resp.Null()
	}
	if withCount {
		return bulkArray(vals)
	}
	return resp.BulkString(vals[0])
}

func cmdLPop(x *Ctx) resp.Value { return pop(x, true) }
func cmdRPop(x *Ctx) resp.Value { return pop(x, false) }

func parseRange(a, b string) (int, int, bool) {
	start, err1 := store.ParseInt(a)
	stop, err2 := store.ParseInt(b)
	return clampInt(start), clampInt(stop), err1 == nil && err2 == nil
}

func cmdLRange(x *Ctx) resp.Value {
	start, stop, ok := parseRange(x.Args[2], x.Args[3])
	if !ok {
		return errNotInt
	}
	vals, err := x.DB.ListRange(x.Args[1], start, stop)
	if err != nil {
		return errReply(err)
	}
	return bulkArray(vals)
}

func cmdLIndex(x *Ctx) resp.Value {
	idx, err := store.ParseInt(x.Args[2])
	if err != nil {
		return errNotInt
	}
	v, ok, err := x.DB.ListIndex(x.Args[1], clampInt(idx))
	if err != nil {
		return errReply(err)
	}
	if !ok {
		return resp.Null()
	}
	return resp.BulkString(v)
}

// ---- hashes ----

func cmdHSet(x *Ctx) resp.Value {
	if len(x.Args)%2 != 0 {
		return wrongArgs("hset")
	}
	return intReply(x.DB.HSet(x.Args[1], x.Args[2:]))
}

func cmdHGet(x *Ctx) resp.Value {
	v, ok, err := x.DB.HGet(x.Args[1], x.Args[2])
	if err != nil {
		return errReply(err)
	}
	if !ok {
		return resp.Null()
	}
	return resp.BulkString(v)
}

func cmdHDel(x *Ctx) resp.Value { return intReply(x.DB.HDel(x.Args[1], x.Args[2:])) }
func cmdHLen(x *Ctx) resp.Value { return intReply(x.DB.HLen(x.Args[1])) }

func cmdHGetAll(x *Ctx) resp.Value {
	vals, err := x.DB.HGetAll(x.Args[1])
	if err != nil {
		return errReply(err)
	}
	return bulkArray(vals)
}

func boolReply(ok bool, err error) resp.Value {
	if err != nil {
		return errReply(err)
	}
	if ok {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func cmdHExists(x *Ctx) resp.Value { return boolReply(x.DB.HExists(x.Args[1], x.Args[2])) }

// ---- sets ----

func cmdSAdd(x *Ctx) resp.Value  { return intReply(x.DB.SAdd(x.Args[1], x.Args[2:])) }
func cmdSRem(x *Ctx) resp.Value  { return intReply(x.DB.SRem(x.Args[1], x.Args[2:])) }
func cmdSCard(x *Ctx) resp.Value { return intReply(x.DB.SCard(x.Args[1])) }

func cmdSIsMember(x *Ctx) resp.Value { return boolReply(x.DB.SIsMember(x.Args[1], x.Args[2])) }

func cmdSMembers(x *Ctx) resp.Value {
	vals, err := x.DB.SMembers(x.Args[1])
	if err != nil {
		return errReply(err)
	}
	return bulkArray(vals)
}

// ---- sorted sets ----

// parseScore parses a ZADD score. NaN is rejected; +inf and -inf are fine.
func parseScore(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// formatScore prints a score the way Redis does: shortest form that reads
// back exactly, with inf/-inf spelled out.
func formatScore(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func cmdZAdd(x *Ctx) resp.Value {
	args := x.Args
	if len(args)%2 != 0 {
		return errSyntax
	}
	members := make([]store.ZMember, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		score, ok := parseScore(args[i])
		if !ok {
			return errFloat
		}
		members = append(members, store.ZMember{Member: args[i+1], Score: score})
	}
	return intReply(x.DB.ZAdd(args[1], members))
}

func cmdZRem(x *Ctx) resp.Value  { return intReply(x.DB.ZRem(x.Args[1], x.Args[2:])) }
func cmdZCard(x *Ctx) resp.Value { return intReply(x.DB.ZCard(x.Args[1])) }

func cmdZScore(x *Ctx) resp.Value {
	s, ok, err := x.DB.ZScore(x.Args[1], x.Args[2])
	if err != nil {
		return errReply(err)
	}
	if !ok {
		return resp.Null()
	}
	return resp.BulkString(formatScore(s))
}

func cmdZRank(x *Ctx) resp.Value {
	r, ok, err := x.DB.ZRank(x.Args[1], x.Args[2])
	if err != nil {
		return errReply(err)
	}
	if !ok {
		return resp.Null()
	}
	return resp.Integer(int64(r))
}

func cmdZRange(x *Ctx) resp.Value {
	args := x.Args
	withScores := false
	switch len(args) {
	case 4:
	case 5:
		if !strings.EqualFold(args[4], "WITHSCORES") {
			return errSyntax
		}
		withScores = true
	default:
		return errSyntax
	}
	start, stop, ok := parseRange(args[2], args[3])
	if !ok {
		return errNotInt
	}
	members, err := x.DB.ZRange(args[1], start, stop)
	if err != nil {
		return errReply(err)
	}
	out := make([]resp.Value, 0, len(members)*2)
	for _, m := range members {
		out = append(out, resp.BulkString(m.Member))
		if withScores {
			out = append(out, resp.BulkString(formatScore(m.Score)))
		}
	}
	return resp.Array(out...)
}
