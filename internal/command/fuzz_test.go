package command

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// FuzzExecute runs random commands against a DB. Execution must never panic
// and must always produce a valid reply; whatever a write asks to have logged
// must itself be a valid command, because the AOF and replicas replay it.
//
// The input is split on NUL bytes into arguments, so the fuzzer can build
// "SET\x00k\x00v" style commands; several commands are separated by 0xFF.
func FuzzExecute(f *testing.F) {
	for _, s := range []string{
		"SET\x00k\x00v",
		"SET\x00k\x00v\x00EX\x0010",
		"SET\x00k\x00v\x00PXAT\x005\xffGET\x00k",
		"INCRBY\x00c\x009223372036854775807\xffINCR\x00c",
		"LPUSH\x00l\x00a\x00b\xffLPOP\x00l\x002\xffLRANGE\x00l\x000\x00-1",
		"HSET\x00h\x00f\x00v\xffHGETALL\x00h",
		"SADD\x00s\x00a\xffSMEMBERS\x00s\xffSREM\x00s\x00a",
		"ZADD\x00z\x001.5\x00a\x00inf\x00b\xffZRANGE\x00z\x000\x00-1\x00WITHSCORES\xffZRANK\x00z\x00b",
		"EXPIRE\x00k\x00-1",
		"EXPIRE\x00k\x009223372036854775807",
		"PEXPIREAT\x00k\x00-9223372036854775808",
		"KEYS\x00[a-\x00",
		"KEYS\x00*a*a*a*a*b",
		"MSET\x00a\x001\x00b",
		"FLUSHALL\x00ASYNC\xffDBSIZE",
		"CONFIG\x00GET\x00*",
		"INFO\x00everything",
		"REPLICAOF\x00NO\x00ONE",
		"SAVE\xffBGSAVE\xffBGREWRITEAOF",
		"PUBLISH\x00c\x00m",
		"\x00",
		"",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		db := store.New(rand.New(rand.NewPCG(1, 2)))
		db.SetNow(1_700_000_000_000)
		x := &Ctx{DB: db, Env: &Env{Publisher: &fakePublisher{}}}

		for _, cmd := range bytes.Split(data, []byte{0xff}) {
			var args []string
			for _, a := range bytes.Split(cmd, []byte{0}) {
				args = append(args, string(a))
			}
			spec, errReply := Resolve(args)
			if spec == nil {
				if errReply.Kind != resp.KindError {
					t.Fatalf("Resolve(%q) failed without an error reply", args)
				}
				continue
			}
			x.Reset(args)
			reply := spec.Run(x)
			if reply.Kind < resp.KindSimpleString || reply.Kind > resp.KindArray {
				t.Fatalf("%q produced an invalid reply kind %d", args, reply.Kind)
			}
			for _, logged := range x.Propagation(spec, reply) {
				if s, e := Resolve(logged); s == nil {
					t.Fatalf("%q asked to log %q, which is not a valid command: %s", args, logged, e.Str)
				}
			}
			db.SetNow(db.Now() + 1)
		}
	})
}
