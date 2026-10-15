package aof

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// FuzzReplay runs the AOF loader on garbage. It must never panic, must report
// damage as an error rather than silently accepting it, and must keep its
// accounting honest: valid never exceeds the input, and a torn tail is never
// reported together with an error.
func FuzzReplay(f *testing.F) {
	good := resp.AppendCommand(nil, []string{"SET", "a", "1"})
	good = resp.AppendCommand(good, []string{"RPUSH", "l", "x", "y"})
	good = resp.AppendCommand(good, []string{"PEXPIREAT", "a", "9999999999999"})
	for _, s := range [][]byte{
		good,
		good[:len(good)-3],
		append(bytes.Clone(good), "garbage\r\n"...),
		append([]byte("*2\r\n$3\r\nGET\r\n"), good...),
		[]byte("*1\r\n$4\r\nPI"),
		[]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$99999999\r\nv"),
		[]byte("*1\r\n$7\r\nNOSUCH!\r\n"),
		{},
		{0, 1, 2, 255},
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		db := store.New(rand.New(rand.NewPCG(1, 2)))
		db.SetNow(1_700_000_000_000)
		x := &command.Ctx{DB: db, Env: &command.Env{}}

		valid, torn, err := Replay(bytes.NewReader(data), func(args []string) error {
			if r := command.Execute(x, args); r.Kind == resp.KindError {
				return errors.New(r.Str)
			}
			return nil
		})
		if valid < 0 || valid > int64(len(data)) {
			t.Fatalf("valid = %d for %d bytes of input", valid, len(data))
		}
		if torn && err != nil {
			t.Fatalf("torn tail reported together with an error: %v", err)
		}
		if err == nil && !torn && valid != int64(len(data)) {
			t.Fatalf("clean replay consumed %d of %d bytes", valid, len(data))
		}
	})
}
