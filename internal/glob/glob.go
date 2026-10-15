// Package glob implements Redis-style glob matching for KEYS.
//
// Supported syntax: * (any run of bytes), ? (any one byte), [abc] and [a-z]
// (character classes), [^abc] (negated class) and \x (the literal x).
// Matching works on bytes, like Redis. A pattern is matched with a
// linear-time two-pointer algorithm (it remembers only the most recent '*'),
// so a hostile pattern such as "*a*a*a*a*b" cannot make it exponential.
package glob

// Match reports whether s matches the whole pattern.
func Match(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, 0 // position after the last '*' seen, and where in s it started

	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if ok, next := matchClass(pattern, p, s[i]); ok {
					p, i = next, i+1
					continue
				}
			case '\\':
				if p+1 < len(pattern) && pattern[p+1] == s[i] {
					p, i = p+2, i+1
					continue
				}
				if p+1 == len(pattern) && s[i] == '\\' { // lone trailing backslash is literal
					p, i = p+1, i+1
					continue
				}
			default:
				if pattern[p] == s[i] {
					p, i = p+1, i+1
					continue
				}
			}
		}
		// Mismatch: let the last '*' swallow one more byte and retry.
		if starP >= 0 {
			starI++
			i, p = starI, starP+1
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass tests c against the class starting at pattern[start] == '['.
// It returns whether c is in the class and the index just after the class.
// An unterminated class extends to the end of the pattern.
func matchClass(pattern string, start int, c byte) (matched bool, next int) {
	p := start + 1
	negate := p < len(pattern) && pattern[p] == '^'
	if negate {
		p++
	}
	for p < len(pattern) && pattern[p] != ']' {
		switch {
		case pattern[p] == '\\' && p+1 < len(pattern):
			p++
			if pattern[p] == c {
				matched = true
			}
			p++
		case p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']':
			lo, hi := pattern[p], pattern[p+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			p += 3
		default:
			if pattern[p] == c {
				matched = true
			}
			p++
		}
	}
	if p < len(pattern) { // skip the closing ']'
		p++
	}
	return matched != negate, p
}
