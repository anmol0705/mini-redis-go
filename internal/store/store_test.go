package store

import (
	"strconv"
	"sync"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("name", "Anmol")
	got, ok := s.Get("name")
	if !ok || got != "Anmol" {
		t.Errorf("Get(name) = %q, %v; want %q, true", got, ok, "Anmol")
	}
}

func TestGetMissing(t *testing.T) {
	if v, ok := New().Get("nope"); ok || v != "" {
		t.Errorf("Get(nope) = %q, %v; want \"\", false", v, ok)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New()
	s.Set("k", "1")
	s.Set("k", "2")
	if got, _ := s.Get("k"); got != "2" {
		t.Errorf("Get(k) = %q; want 2", got)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d; want 1", s.Len())
	}
}

func TestDeleteExists(t *testing.T) {
	s := New()
	s.Set("k", "v")
	if !s.Exists("k") {
		t.Fatal("Exists(k) = false after Set")
	}
	if !s.Delete("k") {
		t.Error("Delete(k) = false; want true for an existing key")
	}
	if s.Delete("k") {
		t.Error("second Delete(k) = true; want false")
	}
	if s.Exists("k") {
		t.Error("Exists(k) = true after Delete")
	}
}

func TestLenAndClear(t *testing.T) {
	s := New()
	for i := range 5 {
		s.Set(strconv.Itoa(i), "v")
	}
	if s.Len() != 5 {
		t.Fatalf("Len() = %d; want 5", s.Len())
	}
	s.Clear()
	if s.Len() != 0 || s.Exists("0") {
		t.Errorf("after Clear: Len() = %d, Exists(0) = %v", s.Len(), s.Exists("0"))
	}
	s.Set("x", "y") // the store must stay usable after Clear
	if got, ok := s.Get("x"); !ok || got != "y" {
		t.Errorf("Get(x) after Clear = %q, %v", got, ok)
	}
}

// Run with -race: this fails loudly if any method forgets the lock.
func TestConcurrentUse(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				k := strconv.Itoa(g*1000 + i)
				s.Set(k, "v")
				s.Get(k)
				s.Exists(k)
				s.Len()
				if i%50 == 0 {
					s.Clear()
				}
				s.Delete(k)
			}
		}()
	}
	wg.Wait()
}
