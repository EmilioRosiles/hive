package hive

import (
	"fmt"
	"time"
)

// HashStore is a typed key/field/value store backed by a Node. Keys are namespaced
// as {name}:h:{key} to prevent collisions with other stores on the same node.
//
// Fields within a key can carry independent TTLs, making HashStore well-suited
// for tracking per-entity state with automatic eviction:
//
//	streams := hive.NewHashStore[Stream](node, "streams")
//	streams.Set("user:123", "stream:abc", Stream{StartedAt: time.Now()})
//	streams.ExpireField("user:123", "stream:abc", 30*time.Minute)
//
//	fields, _ := streams.Fields("user:123") // active stream IDs
type HashStore[T any] struct {
	node   *Node
	prefix string
}

// NewHashStore creates a typed hash store backed by node.
// name is used as the namespace — use a distinct name per hash type.
func NewHashStore[T any](node *Node, name string) *HashStore[T] {
	return &HashStore[T]{node: node, prefix: name + ":h:"}
}

// Set encodes value using msgpack and stores it under key/field with no expiry.
func (h *HashStore[T]) HSet(key, field string, value T) error {
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("hive: %s.Set %q %q: %w", h.prefix, key, field, err)
	}
	return h.node.cluster.HSet(h.prefix+key, field, data, 0)
}

// Get retrieves and decodes the value stored under key/field.
// Returns an error if the key or field does not exist or has expired.
func (h *HashStore[T]) HGet(key, field string) (T, error) {
	var zero T
	data, err := h.node.cluster.HGet(h.prefix+key, field)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q %q: %w", h.prefix, key, field, err)
	}
	value, err := decode[T](data)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q %q: decode: %w", h.prefix, key, field, err)
	}
	return value, nil
}

// Del removes field from the hash at key.
func (h *HashStore[T]) HDel(key, field string) error {
	return h.node.cluster.HDel(h.prefix+key, field)
}

// GetAll retrieves and decodes all live fields under key.
func (h *HashStore[T]) HGetAll(key string) (map[string]T, error) {
	raw, err := h.node.cluster.HGetAll(h.prefix + key)
	if err != nil {
		return nil, fmt.Errorf("hive: %s.GetAll %q: %w", h.prefix, key, err)
	}
	out := make(map[string]T, len(raw))
	for field, data := range raw {
		v, err := decode[T](data)
		if err != nil {
			return nil, fmt.Errorf("hive: %s.GetAll %q: decode field %q: %w", h.prefix, key, field, err)
		}
		out[field] = v
	}
	return out, nil
}

// Fields returns the names of all live fields under key.
func (h *HashStore[T]) HKeys(key string) ([]string, error) {
	return h.node.cluster.HKeys(h.prefix + key)
}

// Del removes the entire hash at key.
func (h *HashStore[T]) Del(key string) error {
	return h.node.cluster.Del(h.prefix + key)
}

// Expire sets a key-level TTL. The entire hash is deleted after ttl elapses.
// A ttl of 0 removes any existing expiry.
func (h *HashStore[T]) Expire(key string, ttl time.Duration) error {
	return h.node.cluster.Expire(h.prefix+key, ttl)
}

// ExpireField sets a TTL on a single field. The field is evicted after ttl elapses
// without affecting other fields or the key itself.
func (h *HashStore[T]) HExpireField(key, field string, ttl time.Duration) error {
	return h.node.cluster.HExpireField(h.prefix+key, field, ttl)
}
