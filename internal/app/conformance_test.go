package app

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/testclient"
)

// TestConformanceCases replays conformance/cases/*.in against a real node and
// compares with the .out files. conformance/run.sh does the same with
// redis-cli; this test proves the expected files are right for THIS server
// (the only part it cannot check is redis-cli's own output formatting).
func TestConformanceCases(t *testing.T) {
	files, err := filepath.Glob("../../conformance/cases/*.in")
	if err != nil || len(files) == 0 {
		t.Fatalf("no conformance cases found: %v", err)
	}
	_, addr, _ := startNode(t, nil)

	for _, in := range files {
		name := strings.TrimSuffix(filepath.Base(in), ".in")
		t.Run(name, func(t *testing.T) {
			cmds, err := os.ReadFile(in)
			if err != nil {
				t.Fatal(err)
			}
			wantRaw, err := os.ReadFile(strings.TrimSuffix(in, ".in") + ".out")
			if err != nil {
				t.Fatal(err)
			}

			c := testclient.MustDial(t, addr) // one connection per case, like redis-cli < file
			var got []string
			for _, line := range strings.Split(strings.TrimSpace(string(cmds)), "\n") {
				args := splitLikeRedisCLI(t, line)
				got = append(got, flatten(c.Reply(t, args...))...)
			}
			want := nonEmpty(strings.Split(string(wantRaw), "\n"))
			if len(got) != len(want) {
				t.Errorf("got %d output lines, want %d\n got: %q\nwant: %q", len(got), len(want), got, want)
				return
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("line %d: got %q, want %q", i+1, got[i], want[i])
				}
			}
		})
	}
}

// flatten renders a reply the way redis-cli prints it in raw mode, minus
// blank lines.
func flatten(r testclient.Reply) []string {
	switch r.Kind {
	case '*':
		var out []string
		for _, e := range r.Elems {
			out = append(out, flatten(e)...)
		}
		return out
	case '$':
		if r.Null {
			return nil
		}
		return nonEmpty([]string{r.Str})
	case ':':
		return []string{strconv.FormatInt(r.Int, 10)}
	}
	return nonEmpty([]string{r.Str})
}

func nonEmpty(lines []string) []string {
	var out []string
	for _, l := range lines {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// splitLikeRedisCLI splits a line on spaces, honouring double quotes.
func splitLikeRedisCLI(t *testing.T, line string) []string {
	t.Helper()
	var args []string
	var cur strings.Builder
	inQuote, have := false, false
	for _, r := range line {
		switch {
		case r == '"':
			inQuote, have = !inQuote, true
		case r == ' ' && !inQuote:
			if have {
				args = append(args, cur.String())
				cur.Reset()
				have = false
			}
		default:
			cur.WriteRune(r)
			have = true
		}
	}
	if inQuote {
		t.Fatalf("unterminated quote in %q", line)
	}
	if have {
		args = append(args, cur.String())
	}
	return args
}
