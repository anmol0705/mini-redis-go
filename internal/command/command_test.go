package command

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

const t0 = 1_700_000_000_000 // an arbitrary "now" in unix ms

type fakeInfo Info

func (f fakeInfo) Info() Info { return Info(f) }

type fakeSections map[string]string

func (f fakeSections) InfoSection(name string) (string, bool) { s, ok := f[name]; return s, ok }

type fakePublisher struct{ got []string }

func (f *fakePublisher) Publish(ch, msg string) int {
	f.got = append(f.got, ch+"="+msg)
	return 2
}

// harness runs commands against a fresh DB the way the engine does.
type harness struct {
	t  *testing.T
	db *store.DB
	x  *Ctx
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	db := store.New(rand.New(rand.NewPCG(1, 2)))
	db.SetNow(t0)
	env := &Env{
		Info:      fakeInfo{Version: "9.9.9", TCPPort: 6380, UptimeSeconds: 42, ConnectedClients: 3},
		Sections:  fakeSections{"replication": "# Replication\r\nrole:master\r\n"},
		Publisher: &fakePublisher{},
		MaxMemory: 1000,
		Policy:    store.AllKeysLRU,
	}
	return &harness{t: t, db: db, x: &Ctx{DB: db, Env: env}}
}

// do runs one command.
func (h *harness) do(args ...string) resp.Value { return Execute(h.x, args) }

// prop runs one command and returns what it would log/replicate.
func (h *harness) prop(args ...string) (resp.Value, [][]string) {
	h.t.Helper()
	s, errv := Resolve(args)
	if s == nil {
		return errv, nil
	}
	h.x.Reset(args)
	reply := s.Run(h.x)
	out := slices.Clone(h.x.Propagation(s, reply))
	return reply, out
}

func errv(msg string) resp.Value { return resp.Error(msg) }

func bulks(vals ...string) resp.Value {
	out := make([]resp.Value, len(vals))
	for i, v := range vals {
		out[i] = resp.BulkString(v)
	}
	return resp.Array(out...)
}

const (
	wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
	notInt    = "ERR value is not an integer or out of range"
)

func TestExecute(t *testing.T) {
	tests := []struct {
		name string
		cmds [][]string // all but the last are setup; the last is checked
		want resp.Value
	}{
		{"ping", [][]string{{"PING"}}, resp.SimpleString("PONG")},
		{"ping with message", [][]string{{"PING", "hi"}}, resp.BulkString("hi")},
		{"ping too many", [][]string{{"PING", "a", "b"}}, errv("ERR wrong number of arguments for 'ping' command")},
		{"echo", [][]string{{"ECHO", "hey"}}, resp.BulkString("hey")},
		{"echo no arg", [][]string{{"ECHO"}}, errv("ERR wrong number of arguments for 'echo' command")},
		{"lower case name", [][]string{{"ping"}}, resp.SimpleString("PONG")},
		{"mixed case name", [][]string{{"PiNg"}}, resp.SimpleString("PONG")},

		// strings
		{"set", [][]string{{"SET", "k", "v"}}, resp.SimpleString("OK")},
		{"set missing value", [][]string{{"SET", "k"}}, errv("ERR wrong number of arguments for 'set' command")},
		{"get existing", [][]string{{"SET", "k", "v"}, {"GET", "k"}}, resp.BulkString("v")},
		{"get empty value", [][]string{{"SET", "k", ""}, {"GET", "k"}}, resp.BulkString("")},
		{"get missing", [][]string{{"GET", "k"}}, resp.Null()},
		{"get wrong type", [][]string{{"RPUSH", "k", "a"}, {"GET", "k"}}, errv(wrongType)},
		{"set overwrites a list", [][]string{{"RPUSH", "k", "a"}, {"SET", "k", "v"}, {"TYPE", "k"}}, resp.SimpleString("string")},
		{"set nx on existing", [][]string{{"SET", "k", "1"}, {"SET", "k", "2", "NX"}}, resp.Null()},
		{"set nx on new", [][]string{{"SET", "k", "2", "nx"}}, resp.SimpleString("OK")},
		{"set xx on missing", [][]string{{"SET", "k", "2", "XX"}}, resp.Null()},
		{"set xx on existing", [][]string{{"SET", "k", "1"}, {"SET", "k", "2", "XX"}, {"GET", "k"}}, resp.BulkString("2")},
		{"set nx xx", [][]string{{"SET", "k", "2", "NX", "XX"}}, errv("ERR syntax error")},
		{"set ex px", [][]string{{"SET", "k", "2", "EX", "1", "PX", "5"}}, errv("ERR syntax error")},
		{"set ex keepttl", [][]string{{"SET", "k", "2", "EX", "1", "KEEPTTL"}}, errv("ERR syntax error")},
		{"set ex missing arg", [][]string{{"SET", "k", "2", "EX"}}, errv("ERR syntax error")},
		{"set ex not int", [][]string{{"SET", "k", "2", "EX", "abc"}}, errv(notInt)},
		{"set ex zero", [][]string{{"SET", "k", "2", "EX", "0"}}, errv("ERR invalid expire time in 'set' command")},
		{"set ex huge", [][]string{{"SET", "k", "2", "EX", "9223372036854775807"}}, errv("ERR invalid expire time in 'set' command")},
		{"set px huge", [][]string{{"SET", "k", "2", "PX", "9223372036854775807"}}, errv("ERR invalid expire time in 'set' command")},
		{"set unknown option", [][]string{{"SET", "k", "2", "WAT"}}, errv("ERR syntax error")},
		{"set ex ttl", [][]string{{"SET", "k", "2", "EX", "10"}, {"TTL", "k"}}, resp.Integer(10)},
		{"set px pttl", [][]string{{"SET", "k", "2", "PX", "1500"}, {"PTTL", "k"}}, resp.Integer(1500)},
		{"set exat", [][]string{{"SET", "k", "2", "EXAT", fmt.Sprint(t0/1000 + 20)}, {"TTL", "k"}}, resp.Integer(20)},
		{"set pxat in the past deletes", [][]string{{"SET", "k", "2", "PXAT", "5"}, {"EXISTS", "k"}}, resp.Integer(0)},
		{"set clears ttl", [][]string{{"SET", "k", "1", "EX", "10"}, {"SET", "k", "2"}, {"TTL", "k"}}, resp.Integer(-1)},
		{"set keepttl", [][]string{{"SET", "k", "1", "EX", "10"}, {"SET", "k", "2", "KEEPTTL"}, {"TTL", "k"}}, resp.Integer(10)},
		{"mget", [][]string{{"SET", "a", "1"}, {"RPUSH", "l", "x"}, {"MGET", "a", "l", "nope"}}, resp.Array(resp.BulkString("1"), resp.Null(), resp.Null())},
		{"mset", [][]string{{"MSET", "a", "1", "b", "2"}, {"MGET", "a", "b"}}, bulks("1", "2")},
		{"mset odd", [][]string{{"MSET", "a", "1", "b"}}, errv("ERR wrong number of arguments for 'mset' command")},
		{"incr new", [][]string{{"INCR", "c"}}, resp.Integer(1)},
		{"incr twice", [][]string{{"INCR", "c"}, {"INCR", "c"}}, resp.Integer(2)},
		{"decr", [][]string{{"SET", "c", "10"}, {"DECR", "c"}}, resp.Integer(9)},
		{"incrby", [][]string{{"INCRBY", "c", "-5"}}, resp.Integer(-5)},
		{"decrby", [][]string{{"DECRBY", "c", "5"}}, resp.Integer(-5)},
		{"decrby minint", [][]string{{"DECRBY", "c", "-9223372036854775808"}}, errv("ERR increment or decrement would overflow")},
		{"incrby not int", [][]string{{"INCRBY", "c", "x"}}, errv(notInt)},
		{"incr on text", [][]string{{"SET", "c", "abc"}, {"INCR", "c"}}, errv(notInt)},
		{"incr overflow", [][]string{{"SET", "c", "9223372036854775807"}, {"INCR", "c"}}, errv("ERR increment or decrement would overflow")},
		{"incr wrong type", [][]string{{"RPUSH", "c", "1"}, {"INCR", "c"}}, errv(wrongType)},
		{"append", [][]string{{"SET", "k", "ab"}, {"APPEND", "k", "cd"}}, resp.Integer(4)},
		{"append new", [][]string{{"APPEND", "k", "cd"}}, resp.Integer(2)},
		{"strlen", [][]string{{"SET", "k", "hello"}, {"STRLEN", "k"}}, resp.Integer(5)},
		{"strlen missing", [][]string{{"STRLEN", "k"}}, resp.Integer(0)},
		{"strlen wrong type", [][]string{{"SADD", "k", "a"}, {"STRLEN", "k"}}, errv(wrongType)},

		// keys
		{"del counts removed keys", [][]string{{"SET", "a", "1"}, {"SET", "b", "2"}, {"DEL", "a", "b", "c"}}, resp.Integer(2)},
		{"del then get", [][]string{{"SET", "a", "1"}, {"DEL", "a"}, {"GET", "a"}}, resp.Null()},
		{"del no keys", [][]string{{"DEL"}}, errv("ERR wrong number of arguments for 'del' command")},
		{"exists counts each mention", [][]string{{"SET", "a", "1"}, {"EXISTS", "a", "a", "b"}}, resp.Integer(2)},
		{"type none", [][]string{{"TYPE", "nope"}}, resp.SimpleString("none")},
		{"type list", [][]string{{"LPUSH", "k", "a"}, {"TYPE", "k"}}, resp.SimpleString("list")},
		{"type hash", [][]string{{"HSET", "k", "a", "b"}, {"TYPE", "k"}}, resp.SimpleString("hash")},
		{"type set", [][]string{{"SADD", "k", "a"}, {"TYPE", "k"}}, resp.SimpleString("set")},
		{"type zset", [][]string{{"ZADD", "k", "1", "a"}, {"TYPE", "k"}}, resp.SimpleString("zset")},
		{"expire", [][]string{{"SET", "k", "v"}, {"EXPIRE", "k", "100"}}, resp.Integer(1)},
		{"expire missing", [][]string{{"EXPIRE", "k", "100"}}, resp.Integer(0)},
		{"expire not int", [][]string{{"EXPIRE", "k", "x"}}, errv(notInt)},
		{"expire overflow", [][]string{{"SET", "k", "v"}, {"EXPIRE", "k", "9223372036854775807"}}, errv("ERR invalid expire time in 'expire' command")},
		{"expire negative deletes", [][]string{{"SET", "k", "v"}, {"EXPIRE", "k", "-1"}, {"EXISTS", "k"}}, resp.Integer(0)},
		{"ttl", [][]string{{"SET", "k", "v"}, {"EXPIRE", "k", "100"}, {"TTL", "k"}}, resp.Integer(100)},
		{"pexpire pttl", [][]string{{"SET", "k", "v"}, {"PEXPIRE", "k", "2500"}, {"PTTL", "k"}}, resp.Integer(2500)},
		{"ttl rounds", [][]string{{"SET", "k", "v"}, {"PEXPIRE", "k", "1600"}, {"TTL", "k"}}, resp.Integer(2)},
		{"expireat", [][]string{{"SET", "k", "v"}, {"EXPIREAT", "k", fmt.Sprint(t0/1000 + 50)}, {"TTL", "k"}}, resp.Integer(50)},
		{"pexpireat", [][]string{{"SET", "k", "v"}, {"PEXPIREAT", "k", fmt.Sprint(t0 + 700)}, {"PTTL", "k"}}, resp.Integer(700)},
		{"ttl missing", [][]string{{"TTL", "k"}}, resp.Integer(-2)},
		{"ttl no expiry", [][]string{{"SET", "k", "v"}, {"TTL", "k"}}, resp.Integer(-1)},
		{"pttl missing", [][]string{{"PTTL", "k"}}, resp.Integer(-2)},
		{"persist", [][]string{{"SET", "k", "v", "EX", "9"}, {"PERSIST", "k"}}, resp.Integer(1)},
		{"persist then ttl", [][]string{{"SET", "k", "v", "EX", "9"}, {"PERSIST", "k"}, {"TTL", "k"}}, resp.Integer(-1)},
		{"persist without ttl", [][]string{{"SET", "k", "v"}, {"PERSIST", "k"}}, resp.Integer(0)},
		{"keys", [][]string{{"SET", "user:1", "a"}, {"SET", "order:1", "b"}, {"KEYS", "user:*"}}, bulks("user:1")},
		{"keys none", [][]string{{"KEYS", "*"}}, resp.Array()},
		{"dbsize empty", [][]string{{"DBSIZE"}}, resp.Integer(0)},
		{"dbsize", [][]string{{"SET", "a", "1"}, {"SET", "b", "1"}, {"DBSIZE"}}, resp.Integer(2)},
		{"dbsize extra arg", [][]string{{"DBSIZE", "x"}}, errv("ERR wrong number of arguments for 'dbsize' command")},
		{"flushall", [][]string{{"SET", "a", "1"}, {"FLUSHALL"}}, resp.SimpleString("OK")},
		{"flushall empties", [][]string{{"SET", "a", "1"}, {"FLUSHALL"}, {"DBSIZE"}}, resp.Integer(0)},
		{"flushdb async", [][]string{{"SET", "a", "1"}, {"FLUSHDB", "async"}, {"DBSIZE"}}, resp.Integer(0)},
		{"flushall bad option", [][]string{{"FLUSHALL", "now"}}, errv("ERR syntax error")},
		{"flushall too many", [][]string{{"FLUSHALL", "sync", "x"}}, errv("ERR syntax error")},

		// lists
		{"lpush", [][]string{{"LPUSH", "l", "a", "b"}}, resp.Integer(2)},
		{"lpush order", [][]string{{"LPUSH", "l", "a", "b", "c"}, {"LRANGE", "l", "0", "-1"}}, bulks("c", "b", "a")},
		{"rpush order", [][]string{{"RPUSH", "l", "a", "b", "c"}, {"LRANGE", "l", "0", "-1"}}, bulks("a", "b", "c")},
		{"lrange negative", [][]string{{"RPUSH", "l", "a", "b", "c"}, {"LRANGE", "l", "-2", "-1"}}, bulks("b", "c")},
		{"lrange missing", [][]string{{"LRANGE", "l", "0", "-1"}}, resp.Array()},
		{"lrange not int", [][]string{{"LRANGE", "l", "a", "-1"}}, errv(notInt)},
		{"llen", [][]string{{"RPUSH", "l", "a", "b"}, {"LLEN", "l"}}, resp.Integer(2)},
		{"lindex", [][]string{{"RPUSH", "l", "a", "b"}, {"LINDEX", "l", "-1"}}, resp.BulkString("b")},
		{"lindex out of range", [][]string{{"RPUSH", "l", "a"}, {"LINDEX", "l", "5"}}, resp.Null()},
		{"lindex not int", [][]string{{"LINDEX", "l", "x"}}, errv(notInt)},
		{"lpop", [][]string{{"RPUSH", "l", "a", "b"}, {"LPOP", "l"}}, resp.BulkString("a")},
		{"rpop", [][]string{{"RPUSH", "l", "a", "b"}, {"RPOP", "l"}}, resp.BulkString("b")},
		{"lpop count", [][]string{{"RPUSH", "l", "a", "b", "c"}, {"LPOP", "l", "2"}}, bulks("a", "b")},
		{"lpop count zero", [][]string{{"RPUSH", "l", "a"}, {"LPOP", "l", "0"}}, resp.Array()},
		{"lpop negative count", [][]string{{"LPOP", "l", "-1"}}, errv("ERR value is out of range, must be positive")},
		{"lpop missing", [][]string{{"LPOP", "l"}}, resp.Null()},
		{"lpop too many args", [][]string{{"LPOP", "l", "1", "2"}}, errv("ERR wrong number of arguments for 'lpop' command")},
		{"emptied list removed", [][]string{{"RPUSH", "l", "a"}, {"LPOP", "l"}, {"EXISTS", "l"}}, resp.Integer(0)},
		{"list wrong type", [][]string{{"SET", "l", "a"}, {"LPUSH", "l", "b"}}, errv(wrongType)},

		// hashes
		{"hset", [][]string{{"HSET", "h", "a", "1", "b", "2"}}, resp.Integer(2)},
		{"hset update", [][]string{{"HSET", "h", "a", "1"}, {"HSET", "h", "a", "2"}}, resp.Integer(0)},
		{"hset odd", [][]string{{"HSET", "h", "a", "1", "b"}}, errv("ERR wrong number of arguments for 'hset' command")},
		{"hget", [][]string{{"HSET", "h", "a", "1"}, {"HGET", "h", "a"}}, resp.BulkString("1")},
		{"hget missing field", [][]string{{"HSET", "h", "a", "1"}, {"HGET", "h", "z"}}, resp.Null()},
		{"hget missing key", [][]string{{"HGET", "h", "z"}}, resp.Null()},
		{"hdel", [][]string{{"HSET", "h", "a", "1", "b", "2"}, {"HDEL", "h", "a", "q"}}, resp.Integer(1)},
		{"hexists", [][]string{{"HSET", "h", "a", "1"}, {"HEXISTS", "h", "a"}}, resp.Integer(1)},
		{"hexists no", [][]string{{"HSET", "h", "a", "1"}, {"HEXISTS", "h", "b"}}, resp.Integer(0)},
		{"hlen", [][]string{{"HSET", "h", "a", "1", "b", "2"}, {"HLEN", "h"}}, resp.Integer(2)},
		{"hgetall missing", [][]string{{"HGETALL", "h"}}, resp.Array()},
		{"emptied hash removed", [][]string{{"HSET", "h", "a", "1"}, {"HDEL", "h", "a"}, {"EXISTS", "h"}}, resp.Integer(0)},
		{"hash wrong type", [][]string{{"SET", "h", "a"}, {"HGET", "h", "a"}}, errv(wrongType)},

		// sets
		{"sadd", [][]string{{"SADD", "s", "a", "b", "a"}}, resp.Integer(2)},
		{"srem", [][]string{{"SADD", "s", "a", "b"}, {"SREM", "s", "a", "z"}}, resp.Integer(1)},
		{"sismember", [][]string{{"SADD", "s", "a"}, {"SISMEMBER", "s", "a"}}, resp.Integer(1)},
		{"sismember no", [][]string{{"SADD", "s", "a"}, {"SISMEMBER", "s", "b"}}, resp.Integer(0)},
		{"scard", [][]string{{"SADD", "s", "a", "b"}, {"SCARD", "s"}}, resp.Integer(2)},
		{"smembers missing", [][]string{{"SMEMBERS", "s"}}, resp.Array()},
		{"emptied set removed", [][]string{{"SADD", "s", "a"}, {"SREM", "s", "a"}, {"EXISTS", "s"}}, resp.Integer(0)},
		{"set wrong type", [][]string{{"SET", "s", "a"}, {"SADD", "s", "b"}}, errv(wrongType)},

		// sorted sets
		{"zadd", [][]string{{"ZADD", "z", "1", "a", "2", "b"}}, resp.Integer(2)},
		{"zadd update", [][]string{{"ZADD", "z", "1", "a"}, {"ZADD", "z", "5", "a"}}, resp.Integer(0)},
		{"zadd bad score", [][]string{{"ZADD", "z", "x", "a"}}, errv("ERR value is not a valid float")},
		{"zadd nan", [][]string{{"ZADD", "z", "nan", "a"}}, errv("ERR value is not a valid float")},
		{"zadd odd", [][]string{{"ZADD", "z", "1", "a", "2"}}, errv("ERR syntax error")},
		{"zscore", [][]string{{"ZADD", "z", "1.5", "a"}, {"ZSCORE", "z", "a"}}, resp.BulkString("1.5")},
		{"zscore integer", [][]string{{"ZADD", "z", "3", "a"}, {"ZSCORE", "z", "a"}}, resp.BulkString("3")},
		{"zscore inf", [][]string{{"ZADD", "z", "+inf", "a"}, {"ZSCORE", "z", "a"}}, resp.BulkString("inf")},
		{"zscore -inf", [][]string{{"ZADD", "z", "-inf", "a"}, {"ZSCORE", "z", "a"}}, resp.BulkString("-inf")},
		{"zscore missing", [][]string{{"ZADD", "z", "1", "a"}, {"ZSCORE", "z", "b"}}, resp.Null()},
		{"zrank", [][]string{{"ZADD", "z", "2", "b", "1", "a"}, {"ZRANK", "z", "b"}}, resp.Integer(1)},
		{"zrank missing", [][]string{{"ZADD", "z", "1", "a"}, {"ZRANK", "z", "q"}}, resp.Null()},
		{"zrange", [][]string{{"ZADD", "z", "2", "b", "1", "a", "3", "c"}, {"ZRANGE", "z", "0", "1"}}, bulks("a", "b")},
		{"zrange negative", [][]string{{"ZADD", "z", "2", "b", "1", "a", "3", "c"}, {"ZRANGE", "z", "-2", "-1"}}, bulks("b", "c")},
		{"zrange withscores", [][]string{{"ZADD", "z", "2", "b", "1", "a"}, {"ZRANGE", "z", "0", "-1", "withscores"}}, bulks("a", "1", "b", "2")},
		{"zrange bad option", [][]string{{"ZRANGE", "z", "0", "-1", "SCORES"}}, errv("ERR syntax error")},
		{"zrange too many", [][]string{{"ZRANGE", "z", "0", "-1", "WITHSCORES", "x"}}, errv("ERR syntax error")},
		{"zrange not int", [][]string{{"ZRANGE", "z", "a", "b"}}, errv(notInt)},
		{"zrange missing", [][]string{{"ZRANGE", "z", "0", "-1"}}, resp.Array()},
		{"zcard", [][]string{{"ZADD", "z", "1", "a", "2", "b"}, {"ZCARD", "z"}}, resp.Integer(2)},
		{"zrem", [][]string{{"ZADD", "z", "1", "a", "2", "b"}, {"ZREM", "z", "a", "q"}}, resp.Integer(1)},
		{"emptied zset removed", [][]string{{"ZADD", "z", "1", "a"}, {"ZREM", "z", "a"}, {"EXISTS", "z"}}, resp.Integer(0)},
		{"zset wrong type", [][]string{{"SET", "z", "a"}, {"ZADD", "z", "1", "a"}}, errv(wrongType)},

		// server
		{"command", [][]string{{"COMMAND"}}, resp.Array()},
		{"command docs", [][]string{{"COMMAND", "DOCS"}}, resp.Array()},
		{"client setinfo", [][]string{{"CLIENT", "SETINFO", "LIB-NAME", "x"}}, resp.SimpleString("OK")},
		{"client setname", [][]string{{"CLIENT", "SETNAME", "me"}}, resp.SimpleString("OK")},
		{"client setname arity", [][]string{{"CLIENT", "SETNAME"}}, errv("ERR wrong number of arguments for 'client|setname' command")},
		{"client unknown", [][]string{{"CLIENT", "NOPE"}}, errv(`ERR unknown subcommand "NOPE". Try CLIENT HELP.`)},
		{"config get", [][]string{{"CONFIG", "GET", "save"}}, resp.Array()},
		{"config get without pattern", [][]string{{"CONFIG", "GET"}}, errv("ERR wrong number of arguments for 'config|get' command")},
		{"config set unsupported", [][]string{{"CONFIG", "SET", "a", "b"}}, errv(`ERR unknown subcommand "SET". Try CONFIG HELP.`)},
		{"publish", [][]string{{"PUBLISH", "ch", "hi"}}, resp.Integer(2)},
		{"save without hooks", [][]string{{"SAVE"}}, errNoHooks},
		{"bgsave without hooks", [][]string{{"BGSAVE"}}, errNoHooks},
		{"bgrewriteaof without hooks", [][]string{{"BGREWRITEAOF"}}, errNoHooks},
		{"replicaof without hooks", [][]string{{"REPLICAOF", "h", "1"}}, errNoHooks},

		{"unknown command", [][]string{{"NOSUCH", "a", "b"}}, errv(`ERR unknown command "NOSUCH", with args beginning with: "a" "b"`)},
		{"empty command", [][]string{{}}, errv("ERR empty command")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			var got resp.Value
			for _, c := range tt.cmds {
				got = h.do(c...)
			}
			if !sameValue(got, tt.want) {
				t.Errorf("got %+v; want %+v", got, tt.want)
			}
		})
	}
}

func TestHGetAllAndSMembersContents(t *testing.T) {
	h := newHarness(t)
	h.do("HSET", "h", "a", "1", "b", "2")
	h.do("SADD", "s", "x", "y")

	got := h.do("HGETALL", "h")
	var flat []string
	for _, e := range got.Elems {
		flat = append(flat, e.Str)
	}
	pairs := map[string]string{}
	for i := 0; i < len(flat); i += 2 {
		pairs[flat[i]] = flat[i+1]
	}
	if !reflect.DeepEqual(pairs, map[string]string{"a": "1", "b": "2"}) {
		t.Errorf("HGETALL = %v", flat)
	}

	var members []string
	for _, e := range h.do("SMEMBERS", "s").Elems {
		members = append(members, e.Str)
	}
	sort.Strings(members)
	if !slices.Equal(members, []string{"x", "y"}) {
		t.Errorf("SMEMBERS = %v", members)
	}
}

func TestExpiredKeysDisappear(t *testing.T) {
	h := newHarness(t)
	h.do("SET", "k", "v", "PX", "100")
	h.db.SetNow(t0 + 100)
	if got := h.do("GET", "k"); got.Kind != resp.KindNull {
		t.Errorf("GET after expiry = %+v", got)
	}
	if got := h.do("TTL", "k"); got.Int != -2 {
		t.Errorf("TTL after expiry = %+v", got)
	}
}

// ---- what gets logged and replicated ----

func TestPropagation(t *testing.T) {
	ts := fmt.Sprint(t0 + 10_000)
	tests := []struct {
		name  string
		setup [][]string
		cmd   []string
		want  [][]string
	}{
		{"plain set is verbatim", nil, []string{"SET", "k", "v"}, [][]string{{"SET", "k", "v"}}},
		{"set ex becomes pxat", nil, []string{"SET", "k", "v", "EX", "10"}, [][]string{{"SET", "k", "v", "PXAT", ts}}},
		{"set px becomes pxat", nil, []string{"SET", "k", "v", "PX", "10000"}, [][]string{{"SET", "k", "v", "PXAT", ts}}},
		{"set exat becomes pxat", nil, []string{"SET", "k", "v", "EXAT", fmt.Sprint(t0/1000 + 10)}, [][]string{{"SET", "k", "v", "PXAT", ts}}},
		{"set nx drops the condition", nil, []string{"SET", "k", "v", "NX"}, [][]string{{"SET", "k", "v"}}},
		{"set keepttl", nil, []string{"SET", "k", "v", "XX", "KEEPTTL"}, nil}, // key missing: XX fails
		{"failed set nx not logged", [][]string{{"SET", "k", "1"}}, []string{"SET", "k", "v", "NX"}, nil},
		{"expire becomes pexpireat", [][]string{{"SET", "k", "v"}}, []string{"EXPIRE", "k", "10"}, [][]string{{"PEXPIREAT", "k", ts}}},
		{"pexpire becomes pexpireat", [][]string{{"SET", "k", "v"}}, []string{"PEXPIRE", "k", "10000"}, [][]string{{"PEXPIREAT", "k", ts}}},
		{"expireat becomes pexpireat", [][]string{{"SET", "k", "v"}}, []string{"EXPIREAT", "k", fmt.Sprint(t0/1000 + 10)}, [][]string{{"PEXPIREAT", "k", ts}}},
		{"expire in the past becomes del", [][]string{{"SET", "k", "v"}}, []string{"EXPIRE", "k", "-5"}, [][]string{{"DEL", "k"}}},
		{"expire on missing key not logged", nil, []string{"EXPIRE", "k", "10"}, nil},
		{"del of nothing not logged", nil, []string{"DEL", "k"}, nil},
		{"del is verbatim", [][]string{{"SET", "k", "v"}}, []string{"DEL", "k", "z"}, [][]string{{"DEL", "k", "z"}}},
		{"persist logged when it changed something", [][]string{{"SET", "k", "v", "EX", "9"}}, []string{"PERSIST", "k"}, [][]string{{"PERSIST", "k"}}},
		{"persist noop not logged", [][]string{{"SET", "k", "v"}}, []string{"PERSIST", "k"}, nil},
		{"lpush verbatim", nil, []string{"LPUSH", "l", "a"}, [][]string{{"LPUSH", "l", "a"}}},
		{"lpop on missing not logged", nil, []string{"LPOP", "l"}, nil},
		{"failed write not logged", [][]string{{"SET", "k", "v"}}, []string{"LPUSH", "k", "a"}, nil},
		{"read not logged", [][]string{{"SET", "k", "v"}}, []string{"GET", "k"}, nil},
		{"flushall", [][]string{{"SET", "k", "v"}}, []string{"FLUSHDB"}, [][]string{{"FLUSHALL"}}},
		{"incr verbatim", nil, []string{"INCR", "c"}, [][]string{{"INCR", "c"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			for _, c := range tt.setup {
				h.do(c...)
			}
			_, got := h.prop(tt.cmd...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("propagated %q; want %q", got, tt.want)
			}
		})
	}
}

// Replaying what was logged must rebuild the same data, whenever it happens.
func TestReplayOfPropagatedCommandsGivesSameState(t *testing.T) {
	h := newHarness(t)
	var log [][]string
	for _, c := range [][]string{
		{"SET", "a", "1", "EX", "100"},
		{"EXPIRE", "a", "500"},
		{"SET", "b", "2", "PX", "900"},
		{"RPUSH", "l", "x", "y"},
		{"LPOP", "l"},
		{"HSET", "h", "f", "v"},
		{"SET", "gone", "1"},
		{"EXPIRE", "gone", "-1"},
	} {
		_, p := h.prop(c...)
		log = append(log, p...)
	}

	// Replay later, on a server whose clock has moved on.
	r := newHarness(t)
	r.db.SetNow(t0 + 50)
	for _, c := range log {
		r.do(c...)
	}
	for _, key := range []string{"a", "b"} {
		want, _ := h.db.ExpireAtOf(key)
		got, _ := r.db.ExpireAtOf(key)
		if want != got {
			t.Errorf("expiry of %s after replay = %d; want %d", key, got, want)
		}
	}
	if r.db.Exists("gone") {
		t.Error("deleted key came back after replay")
	}
	if got := r.do("LRANGE", "l", "0", "-1"); !reflect.DeepEqual(got, bulks("y")) {
		t.Errorf("list after replay = %+v", got)
	}
}

func TestResolveAndLookup(t *testing.T) {
	if s := Lookup("set"); s == nil || !s.Write || !s.DenyOOM {
		t.Errorf("Lookup(set) = %+v", s)
	}
	if s := Lookup("get"); s == nil || s.Write {
		t.Errorf("Lookup(get) = %+v", s)
	}
	if Lookup("nosuch") != nil {
		t.Error("Lookup found an unknown command")
	}
	if s, e := Resolve([]string{"GET"}); s != nil || e.Kind != resp.KindError {
		t.Errorf("Resolve with bad arity = %v, %+v", s, e)
	}
	if len(Names()) < 50 {
		t.Errorf("Names() has only %d entries", len(Names()))
	}
}

// Every table entry must have a sane definition.
func TestTableIsConsistent(t *testing.T) {
	for name, s := range table {
		if name != strings.ToUpper(s.Name) {
			t.Errorf("%s registered under %q", s.Name, name)
		}
		if s.Arity == 0 || s.run == nil {
			t.Errorf("%s has arity %d / nil handler", name, s.Arity)
		}
		if s.DenyOOM && !s.Write {
			t.Errorf("%s is DenyOOM but not Write", name)
		}
	}
}

// Client text must never reach the wire raw: a command name containing
// CRLF must not be able to start a new protocol line.
func TestUnknownCommandEscapesClientText(t *testing.T) {
	got := newHarness(t).do("BAD\r\n+OK", "arg\r\n:1")
	if got.Kind != resp.KindError {
		t.Fatalf("kind = %v; want error", got.Kind)
	}
	if strings.ContainsAny(got.Str, "\r\n") {
		t.Errorf("reply contains raw CR/LF: %q", got.Str)
	}
	if !strings.Contains(got.Str, `\r\n`) {
		t.Errorf("reply %q does not show the escaped bytes", got.Str)
	}
}

func TestUnknownCommandTruncatesLongText(t *testing.T) {
	got := newHarness(t).do(strings.Repeat("A", 10000), "x", "y", "z", "w")
	if len(got.Str) > 500 {
		t.Errorf("error reply is %d bytes; want it capped", len(got.Str))
	}
	if strings.Contains(got.Str, `"w"`) {
		t.Errorf("echoed more than three arguments: %q", got.Str)
	}
}

func TestInfo(t *testing.T) {
	h := newHarness(t)

	all := h.do("INFO")
	if all.Kind != resp.KindBulkString {
		t.Fatalf("kind = %v; want bulk string", all.Kind)
	}
	for _, want := range []string{
		"# Server\r\n", "redis_version:9.9.9\r\n", "tcp_port:6380\r\n", "uptime_in_seconds:42\r\n",
		"# Clients\r\n", "connected_clients:3\r\n", "# Memory\r\n", "used_memory:0\r\n",
		"maxmemory:1000\r\n", "maxmemory_policy:allkeys-lru\r\n", "# Stats\r\n", "# Replication\r\nrole:master\r\n",
		"# Keyspace\r\n",
	} {
		if !strings.Contains(all.Str, want) {
			t.Errorf("INFO is missing %q; got %q", want, all.Str)
		}
	}
	if strings.Contains(all.Str, "db0:") {
		t.Errorf("empty keyspace must not list db0: %q", all.Str)
	}

	h.do("SET", "a", "1")
	h.do("SET", "b", "1", "EX", "10")
	if ks := h.do("INFO", "keyspace").Str; ks != "# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0\r\n" {
		t.Errorf("INFO keyspace = %q", ks)
	}
	if mem := h.do("INFO", "memory").Str; strings.Contains(mem, "used_memory:0") {
		t.Errorf("INFO memory did not change after SET: %q", mem)
	}
	if got := h.do("INFO", "CLIENTS").Str; got != "# Clients\r\nconnected_clients:3\r\n" {
		t.Errorf("INFO CLIENTS = %q", got)
	}
	if got := h.do("INFO", "nosuchsection").Str; got != "" {
		t.Errorf("unknown section = %q; want empty", got)
	}
	if got := h.do("INFO", "persistence").Str; got != "" {
		t.Errorf("persistence without a source = %q; want empty", got)
	}
	if got := h.do("INFO", "a", "b"); got.Str != "ERR syntax error" {
		t.Errorf("INFO a b = %+v", got)
	}
}

func TestInfoWithoutEnv(t *testing.T) {
	db := store.New(nil)
	x := &Ctx{DB: db}
	if got := Execute(x, []string{"INFO", "clients"}).Str; got != "# Clients\r\nconnected_clients:0\r\n" {
		t.Errorf("got %q", got)
	}
	if got := Execute(x, []string{"PUBLISH", "c", "m"}); got.Int != 0 {
		t.Errorf("PUBLISH without a publisher = %+v", got)
	}
}

// ---- hooks ----

type fakeHooks struct {
	calls []string
	err   error
}

func (f *fakeHooks) Save(*store.DB) error { f.calls = append(f.calls, "save"); return f.err }
func (f *fakeHooks) BGSave(*store.DB) (string, error) {
	f.calls = append(f.calls, "bgsave")
	return "Background saving started", f.err
}
func (f *fakeHooks) RewriteAOF(*store.DB) (string, error) {
	f.calls = append(f.calls, "rewrite")
	return "Background append only file rewriting started", f.err
}
func (f *fakeHooks) ReplicaOf(h, p string) error {
	f.calls = append(f.calls, "replicaof "+h+" "+p)
	return f.err
}
func (f *fakeHooks) ReplicaOfNoOne() error { f.calls = append(f.calls, "noone"); return f.err }

type fakeControl struct{ readOnly bool }

func (f *fakeControl) SetReadOnly(on bool) { f.readOnly = on }

func TestHookCommands(t *testing.T) {
	h := newHarness(t)
	hooks, ctl := &fakeHooks{}, &fakeControl{}
	h.x.Env.Hooks, h.x.Env.Control = hooks, ctl

	if got := h.do("SAVE"); got.Str != "OK" {
		t.Errorf("SAVE = %+v", got)
	}
	if got := h.do("BGSAVE"); got.Str != "Background saving started" {
		t.Errorf("BGSAVE = %+v", got)
	}
	if got := h.do("BGSAVE", "SCHEDULE"); got.Kind != resp.KindSimpleString {
		t.Errorf("BGSAVE SCHEDULE = %+v", got)
	}
	if got := h.do("BGSAVE", "nope"); got.Str != "ERR syntax error" {
		t.Errorf("BGSAVE nope = %+v", got)
	}
	if got := h.do("BGREWRITEAOF"); got.Kind != resp.KindSimpleString {
		t.Errorf("BGREWRITEAOF = %+v", got)
	}
	if got := h.do("REPLICAOF", "10.0.0.1", "6379"); got.Str != "OK" || !ctl.readOnly {
		t.Errorf("REPLICAOF = %+v, readOnly=%v", got, ctl.readOnly)
	}
	if got := h.do("replicaof", "no", "one"); got.Str != "OK" || ctl.readOnly {
		t.Errorf("REPLICAOF NO ONE = %+v, readOnly=%v", got, ctl.readOnly)
	}
	for _, port := range []string{"0", "70000", "abc", "-1", ""} {
		if got := h.do("REPLICAOF", "h", port); got.Str != "ERR Invalid master port" {
			t.Errorf("REPLICAOF h %q = %+v", port, got)
		}
	}
	want := []string{"save", "bgsave", "bgsave", "rewrite", "replicaof 10.0.0.1 6379", "noone"}
	if !slices.Equal(hooks.calls, want) {
		t.Errorf("hook calls = %v; want %v", hooks.calls, want)
	}

	hooks.err = fmt.Errorf("disk full")
	for _, cmd := range [][]string{{"SAVE"}, {"BGSAVE"}, {"BGREWRITEAOF"}, {"REPLICAOF", "h", "1"}, {"REPLICAOF", "NO", "ONE"}} {
		if got := h.do(cmd...); got.Str != "ERR disk full" {
			t.Errorf("%v with failing hook = %+v", cmd, got)
		}
	}
}

// sameValue compares replies structurally; unlike reflect.DeepEqual it treats
// a nil and an empty element list as equal.
func sameValue(a, b resp.Value) bool {
	if a.Kind != b.Kind || a.Str != b.Str || a.Int != b.Int || len(a.Elems) != len(b.Elems) {
		return false
	}
	for i := range a.Elems {
		if !sameValue(a.Elems[i], b.Elems[i]) {
			return false
		}
	}
	return true
}
