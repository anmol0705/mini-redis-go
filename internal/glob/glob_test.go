package glob

import (
	"strings"
	"testing"
	"time"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"", "", true},
		{"", "a", false},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"abc", "ab", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"h*llo", "hllo", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "hellx", false},
		{"*suffix", "has-suffix", true},
		{"prefix*", "prefix", true},
		{"user:*:name", "user:42:name", true},
		{"user:*:name", "user:42:nam", false},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{"h[c-a]llo", "hbllo", true}, // reversed range is swapped
		{`h\*llo`, "h*llo", true},
		{`h\*llo`, "hxllo", false},
		{`a\`, `a\`, true},
		{"[", "a", false},
		{"[a", "a", true}, // unterminated class runs to the end
		{"**", "x", true},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
		{"é?", "éa", true}, // matching is per byte: é is two bytes, ? is the third
		{"é??", "éa", false},
		{"é?", "é", false},
	}
	for _, tt := range tests {
		if got := Match(tt.pattern, tt.s); got != tt.want {
			t.Errorf("Match(%q, %q) = %v; want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}

// A pattern full of stars must not blow up exponentially.
func TestMatchPathologicalPattern(t *testing.T) {
	pattern := strings.Repeat("*a", 30) + "*b"
	s := strings.Repeat("a", 5000)

	done := make(chan bool, 1)
	go func() { done <- Match(pattern, s) }()
	select {
	case got := <-done:
		if got {
			t.Error("pathological pattern matched; want false")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Match did not finish: matching is not linear-ish")
	}
}
