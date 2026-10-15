// Package skiplist implements an ordered list of (score, member) pairs with
// span counts, so that insert, delete, rank lookup and access by rank are all
// O(log n) on average. It is the index behind sorted sets.
//
// Elements are ordered by score, then by member (byte order). A List does not
// check for duplicate members: the sorted set above it keeps a member->score
// map and calls Delete before re-inserting a changed score.
//
// Concurrency: a List is not safe for concurrent use; the keyspace's single
// owner goroutine is the only caller.
package skiplist

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

const (
	maxLevel = 32
	// levelP is the chance that a node also appears on the next level up.
	// 1/4 gives about n*4/3 forward pointers and a good search cost.
	levelP = 0.25
)

type level struct {
	forward *node
	// span is how many bottom-level steps this pointer skips. Summing
	// spans along a search path gives a node's rank without walking the list.
	span int
}

type node struct {
	member   string
	score    float64
	backward *node
	levels   []level
}

// Element is a member with its score.
type Element struct {
	Member string
	Score  float64
}

// List is a skip list with span counts.
type List struct {
	head   *node
	tail   *node
	length int
	level  int // number of levels currently in use
	rng    *rand.Rand
}

// New returns an empty List. rng chooses node heights; pass a seeded
// generator for reproducible tests, or nil for a randomly seeded one.
func New(rng *rand.Rand) *List {
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &List{
		head:  &node{levels: make([]level, maxLevel)},
		level: 1,
		rng:   rng,
	}
}

// Len returns the number of elements.
func (l *List) Len() int { return l.length }

func (l *List) randomLevel() int {
	n := 1
	for n < maxLevel && l.rng.Float64() < levelP {
		n++
	}
	return n
}

// before reports whether n sorts strictly before (score, member).
func before(n *node, score float64, member string) bool {
	return n.score < score || (n.score == score && n.member < member)
}

// Insert adds (score, member). The caller must ensure member is not already
// present.
func (l *List) Insert(score float64, member string) {
	var update [maxLevel]*node
	var rank [maxLevel]int // rank[i] = steps from head to update[i]

	x := l.head
	for i := l.level - 1; i >= 0; i-- {
		if i < l.level-1 {
			rank[i] = rank[i+1]
		}
		for x.levels[i].forward != nil && before(x.levels[i].forward, score, member) {
			rank[i] += x.levels[i].span
			x = x.levels[i].forward
		}
		update[i] = x
	}

	lvl := l.randomLevel()
	if lvl > l.level {
		for i := l.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = l.head
			update[i].levels[i].span = l.length
		}
		l.level = lvl
	}

	n := &node{member: member, score: score, levels: make([]level, lvl)}
	for i := range lvl {
		n.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = n

		n.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = rank[0] - rank[i] + 1
	}
	// Levels above the new node's height now jump over one more element.
	for i := lvl; i < l.level; i++ {
		update[i].levels[i].span++
	}

	if update[0] != l.head {
		n.backward = update[0]
	}
	if n.levels[0].forward != nil {
		n.levels[0].forward.backward = n
	} else {
		l.tail = n
	}
	l.length++
}

// Delete removes (score, member) and reports whether it was present.
func (l *List) Delete(score float64, member string) bool {
	var update [maxLevel]*node
	x := l.head
	for i := l.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && before(x.levels[i].forward, score, member) {
			x = x.levels[i].forward
		}
		update[i] = x
	}

	x = x.levels[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := 0; i < l.level; i++ {
		if update[i].levels[i].forward == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].forward = x.levels[i].forward
		} else {
			update[i].levels[i].span--
		}
	}
	if x.levels[0].forward != nil {
		x.levels[0].forward.backward = x.backward
	} else {
		l.tail = x.backward
	}
	for l.level > 1 && l.head.levels[l.level-1].forward == nil {
		l.level--
	}
	l.length--
	return true
}

// Rank returns the zero-based position of (score, member), or false if it is
// not in the list.
func (l *List) Rank(score float64, member string) (int, bool) {
	rank := 0
	x := l.head
	for i := l.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil {
			f := x.levels[i].forward
			if before(f, score, member) || (f.score == score && f.member == member) {
				rank += x.levels[i].span
				x = f
			} else {
				break
			}
		}
	}
	if x != l.head && x.score == score && x.member == member {
		return rank - 1, true
	}
	return 0, false
}

// At returns the element at zero-based position rank.
func (l *List) At(rank int) (Element, bool) {
	n := l.nodeAt(rank)
	if n == nil {
		return Element{}, false
	}
	return Element{Member: n.member, Score: n.score}, true
}

func (l *List) nodeAt(rank int) *node {
	if rank < 0 || rank >= l.length {
		return nil
	}
	target := rank + 1
	traversed := 0
	x := l.head
	for i := l.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && traversed+x.levels[i].span <= target {
			traversed += x.levels[i].span
			x = x.levels[i].forward
		}
		if traversed == target {
			return x
		}
	}
	return nil
}

// Range returns the elements with zero-based positions start..stop
// inclusive. Out-of-range bounds are clipped; an empty slice results if
// nothing is left. Cost: O(log n + number returned).
func (l *List) Range(start, stop int) []Element {
	start = max(start, 0)
	stop = min(stop, l.length-1)
	if start > stop {
		return nil
	}
	out := make([]Element, 0, stop-start+1)
	for n, i := l.nodeAt(start), start; n != nil && i <= stop; n, i = n.levels[0].forward, i+1 {
		out = append(out, Element{Member: n.member, Score: n.score})
	}
	return out
}

// validate checks the structural invariants (used by tests): every level is
// sorted, spans add up, backward pointers and the tail are right.
func (l *List) validate() error {
	// Level 0: order, backward pointers, count.
	var prev *node
	count := 0
	for n := l.head.levels[0].forward; n != nil; n = n.levels[0].forward {
		if n.backward != prev {
			return fmt.Errorf("backward pointer wrong at %q", n.member)
		}
		if prev != nil && !before(prev, n.score, n.member) {
			return fmt.Errorf("order violated between %q and %q", prev.member, n.member)
		}
		prev = n
		count++
	}
	if count != l.length {
		return fmt.Errorf("length %d but %d nodes", l.length, count)
	}
	if l.tail != prev {
		return errors.New("tail pointer wrong")
	}
	// Every level: each span equals the number of bottom-level steps.
	position := map[*node]int{l.head: 0}
	i := 0
	for n := l.head.levels[0].forward; n != nil; n = n.levels[0].forward {
		i++
		position[n] = i
	}
	for lv := 0; lv < l.level; lv++ {
		for n := l.head; n != nil; n = n.levels[lv].forward {
			f := n.levels[lv].forward
			end := l.length // a nil forward pointer "points" past the last node
			if f != nil {
				end = position[f]
			}
			if got, want := n.levels[lv].span, end-position[n]; got != want {
				return fmt.Errorf("level %d span %d, want %d", lv, got, want)
			}
		}
	}
	return nil
}
