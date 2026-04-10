package hive

import (
	"fmt"
	"time"
)

// ValueStore is a typed key/value store backed by a Node. Keys are namespaced
// as {name}:v:{key} to prevent collisions with other stores on the same node.
//
// Create one ValueStore per logical value type and share a single Node:
//
//	sessions := hive.NewValueStore[Session](node, "sessions")
//	users    := hive.NewValueStore[User](node, "users")
type ValueStore[T any] struct {
	cache  *Cache
	prefix string
}

// NewValueStore creates a typed value store backed by cache.
// name is used as the namespace — use a distinct name per value type.
func NewValueStore[T any](cache *Cache, name string) *ValueStore[T] {
	return &ValueStore[T]{cache: cache, prefix: name + ":v:"}
}

// Set encodes value using msgpack and stores it under key.
func (s *ValueStore[T]) Set(key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("hive: %s.Set %q: %w", s.prefix, key, err)
	}
	return s.cache.cluster.Set(s.prefix+key, data)
}

// Get retrieves and decodes the value stored under key.
// Returns an error if the key does not exist or has expired.
func (s *ValueStore[T]) Get(key string) (T, error) {
	var zero T
	data, err := s.cache.cluster.Get(s.prefix + key)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q: %w", s.prefix, key, err)
	}
	value, err := decode[T](data)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q: decode: %w", s.prefix, key, err)
	}
	return value, nil
}

// Del removes key from the store.
func (s *ValueStore[T]) Del(key string) error {
	return s.cache.cluster.Del(s.prefix + key)
}

// Expire sets a TTL on key. The entry is deleted automatically after ttl elapses.
// A ttl of 0 removes any existing expiry.
func (s *ValueStore[T]) Expire(key string, ttl time.Duration) error {
	return s.cache.cluster.Expire(s.prefix+key, ttl)
}
