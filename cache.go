package hive

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Cache is a typed view over a Node. All keys are automatically prefixed
// with the cache name to prevent collisions between caches on the same node.
//
// Create one Cache per logical data type and share a single Node across them:
//
//	sessions := hive.NewCache[Session](node, "sessions")
//	users    := hive.NewCache[User](node, "users")
type Cache[T any] struct {
	node   *Node
	prefix string
}

// NewCache creates a typed cache backed by node.
// The name is used as a key prefix — use a distinct name per value type.
func NewCache[T any](node *Node, name string) *Cache[T] {
	return &Cache[T]{node: node, prefix: name + ":"}
}

// Set encodes value using gob and stores it under key.
func (c *Cache[T]) Set(key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("hive: %s.Set %q: %w", c.prefix, key, err)
	}
	return c.node.cluster.Set(c.prefix+key, data)
}

// Get retrieves and decodes the value stored under key.
// Returns ErrNotFound if the key does not exist or has expired.
func (c *Cache[T]) Get(key string) (T, error) {
	var zero T
	data, err := c.node.cluster.Get(c.prefix + key)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q: %w", c.prefix, key, err)
	}
	value, err := decode[T](data)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q: decode: %w", c.prefix, key, err)
	}
	return value, nil
}

// Del removes key from the cache.
func (c *Cache[T]) Del(key string) error {
	return c.node.cluster.Del(c.prefix + key)
}

// Expire sets a TTL on key. The entry is deleted automatically after ttl elapses.
// A ttl of 0 removes any existing expiry.
func (c *Cache[T]) Expire(key string, ttl time.Duration) error {
	return c.node.cluster.Expire(c.prefix+key, ttl)
}

func encode[T any](v T) ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := msgpack.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	// Copy out before returning buf to the pool.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func decode[T any](data []byte) (T, error) {
	var v T
	err := msgpack.Unmarshal(data, &v)
	return v, err
}
