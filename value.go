package hive

import (
	"context"
	"fmt"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// ValueStore is a typed key/value store backed by a Cluster. Keys are namespaced
// as {name}:v:{key} to prevent collisions with other stores on the same node.
//
//	sessions := hive.NewValueStore[Session](cluster, "sessions")
//	users    := hive.NewValueStore[User](cluster, "users")
type ValueStore[T any] struct {
	cluster *Cluster
	prefix  string
}

// NewValueStore creates a typed value store backed by cluster.
// name is used as the namespace — use a distinct name per value type.
func NewValueStore[T any](cluster *Cluster, name string) *ValueStore[T] {
	return &ValueStore[T]{cluster: cluster, prefix: name + ":v:"}
}

// Set encodes value using msgpack and stores it under key.
func (s *ValueStore[T]) Set(ctx context.Context, key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("hive: %s.Set %q: %w", s.prefix, key, err)
	}
	_, err = s.cluster.exec(ctx, transport.OpValueSet, s.prefix+key, data)
	return err
}

// Get retrieves and decodes the value stored under key.
// Returns an error if the key does not exist or has expired.
func (s *ValueStore[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	results, err := s.cluster.exec(ctx, transport.OpValueGet, s.prefix+key)
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q: %w", s.prefix, key, err)
	}
	value, err := decode[T](results[0])
	if err != nil {
		return zero, fmt.Errorf("hive: %s.Get %q: decode: %w", s.prefix, key, err)
	}
	return value, nil
}

// Del removes key from the store.
func (s *ValueStore[T]) Del(ctx context.Context, key string) error {
	_, err := s.cluster.exec(ctx, transport.OpDel, s.prefix+key)
	return err
}

// Expire sets a TTL on key. The entry is deleted automatically after ttl elapses.
func (s *ValueStore[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_, err := s.cluster.exec(ctx, transport.OpExpire, s.prefix+key, encodeTTL(ttl))
	return err
}

// Lock acquires a distributed lock on key, valid for ttl. Returns ErrKeyLocked
// if key is already locked.
func (s *ValueStore[T]) Lock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	return newLock(ctx, s.cluster, s.prefix+key, ttl)
}

// Atomic waits for a lock on key, then runs fn with the lock's authorized
// context and releases the lock when fn returns. ttl bounds how long the
// lock is held; ctx bounds how long Atomic waits to acquire it.
func (s *ValueStore[T]) Atomic(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	return lockAndRun(ctx, s.cluster, s.prefix+key, ttl, fn)
}
