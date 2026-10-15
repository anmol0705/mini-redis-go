package skiplist

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

func newSeeded(seed uint64) *List { return New(rand.New(rand.NewPCG(seed, seed+1))) }

func TestBasicOrderAndRank(t *testing.T) {
	l := newSeeded(1)
	l.Insert(3, "c")
	l.Insert(1, "a")
	l.Insert(2, "b")
	l.Insert(2, "a2") // same score as b: ordered by member

	want := []Element{{"a", 1}, {"a2", 2}, {"b", 2}, {"c", 3}}
	if got := l.Range(0, -1+l.Len()); !slices.Equal(got, want) {
		t.Fatalf("Range = %v; want %v", got, want)
	}
	for i, e := range want {
		if r, ok := l.Rank(e.Score, e.Member); !ok || r != i {
			t.Errorf("Rank(%v) = %d, %v; want %d", e, r, ok, i)
		}
		if got, ok := l.At(i); !ok || got != e {
			t.Errorf("At(%d) = %v, %v; want %v", i, got, ok, e)
		}
	}
	if _, ok := l.Rank(9, "zz"); ok {
		t.Error("Rank of a missing element reported found")
	}
	if _, ok := l.At(4); ok {
		t.Error("At(4) out of range reported found")
	}
	if _, ok := l.At(-1); ok {
		t.Error("At(-1) reported found")
	}
}

func TestDelete(t *testing.T) {
	l := newSeeded(2)
	l.Insert(1, "a")
	l.Insert(2, "b")
	if l.Delete(1, "zzz") || l.Delete(5, "a") {
		t.Error("Delete of a non-matching element returned true")
	}
	if !l.Delete(1, "a") || l.Len() != 1 {
		t.Fatalf("Delete(1,a) failed, Len=%d", l.Len())
	}
	if err := l.validate(); err != nil {
		t.Fatal(err)
	}
	if got := l.Range(0, 10); !slices.Equal(got, []Element{{"b", 2}}) {
		t.Errorf("Range = %v", got)
	}
	l.Delete(2, "b")
	if l.Len() != 0 || len(l.Range(0, 5)) != 0 {
		t.Error("list not empty after deleting everything")
	}
}

func TestRangeClipping(t *testing.T) {
	l := newSeeded(3)
	for i := range 5 {
		l.Insert(float64(i), fmt.Sprint(i))
	}
	tests := []struct {
		start, stop int
		n           int
	}{{0, 4, 5}, {-5, 100, 5}, {2, 3, 2}, {3, 2, 0}, {5, 9, 0}, {4, 4, 1}}
	for _, tt := range tests {
		if got := len(l.Range(tt.start, tt.stop)); got != tt.n {
			t.Errorf("Range(%d,%d) returned %d elements; want %d", tt.start, tt.stop, got, tt.n)
		}
	}
}

// naive is the reference implementation: a sorted slice.
type naive []Element

func compare(a, b Element) int {
	if c := cmp.Compare(a.Score, b.Score); c != 0 {
		return c
	}
	return cmp.Compare(a.Member, b.Member)
}

func (n *naive) insert(e Element) {
	i, _ := slices.BinarySearchFunc(*n, e, compare)
	*n = slices.Insert(*n, i, e)
}

func (n *naive) delete(e Element) bool {
	i, found := slices.BinarySearchFunc(*n, e, compare)
	if found {
		*n = slices.Delete(*n, i, i+1)
	}
	return found
}

func (n naive) rank(e Element) (int, bool) { return slices.BinarySearchFunc(n, e, compare) }

// TestAgainstNaive applies the same random operations to the skip list and
// to a sorted slice and compares every observable result.
func TestAgainstNaive(t *testing.T) {
	for seed := uint64(1); seed <= 25; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			l := newSeeded(seed)
			var ref naive
			present := map[string]float64{}

			for step := range 1500 {
				// A small universe of members and scores forces ties,
				// repeated deletes and re-inserts.
				member := fmt.Sprintf("m%02d", rng.IntN(60))
				score := float64(rng.IntN(15)) / 2

				switch op := rng.IntN(10); {
				case op < 5: // insert (or move) a member
					if old, ok := present[member]; ok {
						if !l.Delete(old, member) || !ref.delete(Element{member, old}) {
							t.Fatalf("step %d: delete of existing %s failed", step, member)
						}
					}
					l.Insert(score, member)
					ref.insert(Element{member, score})
					present[member] = score
				case op < 8: // delete
					old, ok := present[member]
					if ok {
						delete(present, member)
					} else {
						old = score
					}
					got, want := l.Delete(old, member), ref.delete(Element{member, old})
					if got != want {
						t.Fatalf("step %d: Delete = %v; want %v", step, got, want)
					}
				default: // rank lookup
					e := Element{member, score}
					gr, gok := l.Rank(e.Score, e.Member)
					wr, wok := ref.rank(e)
					if gok != wok || (gok && gr != wr) {
						t.Fatalf("step %d: Rank(%v) = %d,%v; want %d,%v", step, e, gr, gok, wr, wok)
					}
				}

				if l.Len() != len(ref) {
					t.Fatalf("step %d: Len = %d; want %d", step, l.Len(), len(ref))
				}
				if step%25 == 0 {
					if err := l.validate(); err != nil {
						t.Fatalf("step %d: %v", step, err)
					}
					checkRanges(t, l, ref, rng)
				}
			}
			if err := l.validate(); err != nil {
				t.Fatal(err)
			}
			checkRanges(t, l, ref, rng)
		})
	}
}

func checkRanges(t *testing.T, l *List, ref naive, rng *rand.Rand) {
	t.Helper()
	if got := l.Range(0, len(ref)); !slices.Equal(got, []Element(ref)) && !(len(got) == 0 && len(ref) == 0) {
		t.Fatalf("full Range differs:\n got %v\nwant %v", got, ref)
	}
	for range 5 {
		a, b := rng.IntN(len(ref)+3)-1, rng.IntN(len(ref)+3)-1
		got := l.Range(a, b)
		lo, hi := max(a, 0), min(b, len(ref)-1)
		var want []Element
		if lo <= hi {
			want = ref[lo : hi+1]
		}
		if !slices.Equal(got, want) {
			t.Fatalf("Range(%d,%d) = %v; want %v", a, b, got, want)
		}
		for i := lo; i <= hi; i++ {
			if e, ok := l.At(i); !ok || e != ref[i] {
				t.Fatalf("At(%d) = %v,%v; want %v", i, e, ok, ref[i])
			}
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	l := newSeeded(7)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; b.Loop(); i++ {
		l.Insert(rng.Float64(), fmt.Sprint(i))
	}
}
