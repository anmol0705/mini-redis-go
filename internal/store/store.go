// Package store holds the in-memory keyspace.
//
// Concurrency: in Phase 1 a Store is shared by every connection goroutine,
// so each method takes a sync.RWMutex (readers share it, writers exclude
// everyone). This is deliberately the simplest correct design. Phase 2
// replaces it with a single goroutine that owns the data, after benchmarking
// both; see docs/decisions.
package store

import "sync"

// Store is an in-memory key-value store. It is safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Set saves value under key, replacing any existing value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value for key. ok is false if the key does not exist.
func (s *Store) Get(key string) (value string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok = s.data[key]
	return value, ok
}

// Delete removes key and reports whether it existed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.data[key]
	delete(s.data, key)
	return existed
}

// Exists reports whether key exists.
func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.data[key]
	return exists
}

// Len returns the number of keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Clear removes every key.
func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Allocating a new map (instead of deleting key by key) lets the
	// garbage collector reclaim the old buckets.
	s.data = make(map[string]string)
}
