package store

import "testing"

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("name", "Anmol")
	got, ok := s.Get("name")
	if !ok || got != "Anmol" {
		t.Errorf("Get(name) = %q, %v; want %q, true", got, ok, "Anmol")
	}
}
