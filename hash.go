package hive

import (
	"fmt"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// HashStore is a typed key/field/value store backed by a Cache. Keys are namespaced
// as {name}:h:{key} to prevent collisions with other stores on the same node.
//
//	streams := hive.NewHashStore[Stream](cache, "streams")
//	streams.HSet("user:123", "stream:abc", Stream{StartedAt: time.Now()})
//	streams.HExpireField("user:123", "stream:abc", 30*time.Minute)
type HashStore[T any] struct {
	cache  *Cache
	prefix string
}

// NewHashStore creates a typed hash store backed by cache.
// name is used as the namespace — use a distinct name per hash type.
func NewHashStore[T any](cache *Cache, name string) *HashStore[T] {
	return &HashStore[T]{cache: cache, prefix: name + ":h:"}
}

// HSet encodes value using msgpack and stores it under key/field.
func (h *HashStore[T]) HSet(key, field string, value T) error {
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("hive: %s.HSet %q %q: %w", h.prefix, key, field, err)
	}
	_, err = h.cache.exec(transport.OpHSet, h.prefix+key, []byte(field), data)
	return err
}

// HSetWithTTL encodes value and stores it under key/field with a per-field TTL.
func (h *HashStore[T]) HSetWithTTL(key, field string, value T, ttl time.Duration) error {
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("hive: %s.HSetWithTTL %q %q: %w", h.prefix, key, field, err)
	}
	_, err = h.cache.exec(transport.OpHSet, h.prefix+key, []byte(field), data, encodeTTL(ttl))
	return err
}

// HGet retrieves and decodes the value stored under key/field.
// Returns an error if the key or field does not exist or has expired.
func (h *HashStore[T]) HGet(key, field string) (T, error) {
	var zero T
	results, err := h.cache.exec(transport.OpHGet, h.prefix+key, []byte(field))
	if err != nil {
		return zero, fmt.Errorf("hive: %s.HGet %q %q: %w", h.prefix, key, field, err)
	}
	value, err := decode[T](results[0])
	if err != nil {
		return zero, fmt.Errorf("hive: %s.HGet %q %q: decode: %w", h.prefix, key, field, err)
	}
	return value, nil
}

// HDel removes field from the hash at key.
func (h *HashStore[T]) HDel(key, field string) error {
	_, err := h.cache.exec(transport.OpHDel, h.prefix+key, []byte(field))
	return err
}

// HGetAll retrieves and decodes all live fields under key.
// Results are returned as alternating field/value pairs, same as Redis HGETALL.
func (h *HashStore[T]) HGetAll(key string) (map[string]T, error) {
	results, err := h.cache.exec(transport.OpHGetAll, h.prefix+key)
	if err != nil {
		return nil, fmt.Errorf("hive: %s.HGetAll %q: %w", h.prefix, key, err)
	}
	out := make(map[string]T, len(results)/2)
	for i := 0; i+1 < len(results); i += 2 {
		v, err := decode[T](results[i+1])
		if err != nil {
			return nil, fmt.Errorf("hive: %s.HGetAll %q: decode field %q: %w", h.prefix, key, string(results[i]), err)
		}
		out[string(results[i])] = v
	}
	return out, nil
}

// HKeys returns the names of all live fields under key.
func (h *HashStore[T]) HKeys(key string) ([]string, error) {
	results, err := h.cache.exec(transport.OpHKeys, h.prefix+key)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(results))
	for i, b := range results {
		out[i] = string(b)
	}
	return out, nil
}

// Del removes the entire hash at key.
func (h *HashStore[T]) Del(key string) error {
	_, err := h.cache.exec(transport.OpDel, h.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire hash is deleted after ttl elapses.
func (h *HashStore[T]) Expire(key string, ttl time.Duration) error {
	_, err := h.cache.exec(transport.OpExpire, h.prefix+key, encodeTTL(ttl))
	return err
}

// HExpireField sets a TTL on a single field. The field is evicted after ttl
// elapses without affecting other fields or the key itself.
func (h *HashStore[T]) HExpireField(key, field string, ttl time.Duration) error {
	_, err := h.cache.exec(transport.OpHExpireField, h.prefix+key, []byte(field), encodeTTL(ttl))
	return err
}
