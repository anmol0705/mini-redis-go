package store

// Store is an in-memory key-value store.
type Store struct {
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Set saves value under key, replacing any existing value.
func (s *Store) Set(key, value string) {
	s.data[key] = value
}

// Get returns the value for key. ok is false if the key does not exist.
func (s *Store) Get(key string) (value string, ok bool) {
	value, ok = s.data[key]
	return value, ok
}

// Delete remove key and reports whether it existed
func (s *Store) Delete(key string) bool {
	_, existed := s.data[key]
	delete(s.data, key)
	return existed
}

// Check for existance of a certain key
func (s *Store) Exists(key string) bool {
	_, exists := s.data[key]
	return exists
}
