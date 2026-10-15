package glob

import (
	"strings"
	"testing"
)

// FuzzMatch checks that matching never panics and that two basic laws hold
// for any input: "*" matches everything, and a pattern with no special
// characters matches only itself.
func FuzzMatch(f *testing.F) {
	for _, s := range [][2]string{
		{"h?llo", "hello"}, {"h*llo", "heeello"}, {"[a-c]x", "bx"}, {"[^a]", "b"}, {`\*`, "*"},
		{"[", "a"}, {"[a-", "a"}, {`\`, `\`}, {"*a*a*a*a*b", strings.Repeat("a", 40)}, {"", ""},
	} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, pattern, s string) {
		_ = Match(pattern, s) // must not panic or hang
		if !Match("*", s) {
			t.Fatalf(`"*" did not match %q`, s)
		}
		if !strings.ContainsAny(pattern, `*?[\`) && Match(pattern, s) != (pattern == s) {
			t.Fatalf("literal pattern %q vs %q", pattern, s)
		}
	})
}
