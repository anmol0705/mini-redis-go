package repl

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

func TestRingBasic(t *testing.T) {
	r := newRing(8)
	r.write([]byte("abcd"))
	if got, ok := r.from(0); !ok || string(got) != "abcd" {
		t.Errorf("from(0) = %q, %v", got, ok)
	}
	if got, ok := r.from(2); !ok || string(got) != "cd" {
		t.Errorf("from(2) = %q, %v", got, ok)
	}
	if got, ok := r.from(4); !ok || len(got) != 0 {
		t.Errorf("from(end) = %q, %v; want empty and ok", got, ok)
	}
	if _, ok := r.from(5); ok {
		t.Error("from beyond the end reported ok")
	}

	r.write([]byte("efghij")) // 10 bytes written in total; window holds the last 8
	if _, ok := r.from(1); ok {
		t.Error("from(1) is outside the window but reported ok")
	}
	if got, ok := r.from(2); !ok || string(got) != "cdefghij" {
		t.Errorf("from(2) = %q, %v", got, ok)
	}
	if r.held() != 8 {
		t.Errorf("held = %d", r.held())
	}
}

func TestRingWriteLargerThanCapacity(t *testing.T) {
	r := newRing(4)
	r.write([]byte("0123456789"))
	if _, ok := r.from(5); ok {
		t.Error("from(5) should be outside the window")
	}
	if got, ok := r.from(6); !ok || string(got) != "6789" {
		t.Errorf("from(6) = %q, %v", got, ok)
	}
}

func TestRingReset(t *testing.T) {
	r := newRing(8)
	r.write([]byte("hello"))
	r.reset(100)
	if _, ok := r.from(0); ok {
		t.Error("old data visible after reset")
	}
	r.write([]byte("xyz"))
	if got, ok := r.from(100); !ok || string(got) != "xyz" {
		t.Errorf("from(100) = %q, %v", got, ok)
	}
}

// Random writes against a naive "keep everything" slice.
func TestRingAgainstNaive(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewPCG(seed, 1))
		capacity := 1 + rng.IntN(64)
		r := newRing(capacity)
		var all []byte

		for step := range 120 {
			chunk := make([]byte, rng.IntN(capacity*2+2))
			for i := range chunk {
				chunk[i] = byte(rng.IntN(256))
			}
			r.write(chunk)
			all = append(all, chunk...)

			end := int64(len(all))
			oldest := max(end-int64(capacity), 0)
			// Check the interesting offsets exhaustively: around the oldest
			// byte still held, around the end, and a few random ones.
			offsets := []int64{0, end, end + 1}
			for d := int64(-3); d <= 3; d++ {
				offsets = append(offsets, oldest+d, end+d)
			}
			for range 10 {
				offsets = append(offsets, rng.Int64N(end+2))
			}
			for _, off := range offsets {
				if off < 0 {
					continue
				}
				got, ok := r.from(off)
				wantOK := off >= oldest && off <= end
				if ok != wantOK {
					t.Fatalf("seed %d step %d cap %d: from(%d) ok=%v want %v", seed, step, capacity, off, ok, wantOK)
				}
				if ok && !bytes.Equal(got, all[off:]) {
					t.Fatalf("seed %d step %d cap %d: from(%d) = %q want %q", seed, step, capacity, off, got, all[off:])
				}
			}
		}
	}
}
