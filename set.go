package hive

import (
	"context"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// SetStore is a distributed string set backed by a Cluster. Keys are namespaced
// as {name}:s:{key} to prevent collisions with other stores on the same node.
//
//	online := hive.NewSetStore(cluster, "online_users")
//	online.SAdd(ctx, "room:1", "user:123")
//	online.Expire(ctx, "room:1", 30*time.Second)
type SetStore struct {
	cluster *Cluster
	prefix  string
}

// NewSetStore creates a set store backed by cluster.
// name is used as the namespace — use a distinct name per set.
func NewSetStore(cluster *Cluster, name string) *SetStore {
	return &SetStore{cluster: cluster, prefix: name + ":s:"}
}

// SAdd adds member to the set at key.
func (s *SetStore) SAdd(ctx context.Context, key, member string) error {
	_, err := s.cluster.exec(ctx, transport.OpSAdd, s.prefix+key, []byte(member))
	return err
}

// SRem removes member from the set at key.
func (s *SetStore) SRem(ctx context.Context, key, member string) error {
	_, err := s.cluster.exec(ctx, transport.OpSRem, s.prefix+key, []byte(member))
	return err
}

// SIsMember reports whether member exists in the set at key.
func (s *SetStore) SIsMember(ctx context.Context, key, member string) (bool, error) {
	results, err := s.cluster.exec(ctx, transport.OpSIsMember, s.prefix+key, []byte(member))
	if err != nil {
		return false, err
	}
	return results[0][0] == 1, nil
}

// SMembers returns all members of the set at key.
func (s *SetStore) SMembers(ctx context.Context, key string) ([]string, error) {
	results, err := s.cluster.exec(ctx, transport.OpSMembers, s.prefix+key)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(results))
	for i, b := range results {
		out[i] = string(b)
	}
	return out, nil
}

// SCard returns the number of members in the set at key.
func (s *SetStore) SCard(ctx context.Context, key string) (int, error) {
	results, err := s.cluster.exec(ctx, transport.OpSCard, s.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// Del removes the entire set at key.
func (s *SetStore) Del(ctx context.Context, key string) error {
	_, err := s.cluster.exec(ctx, transport.OpDel, s.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire set is deleted after ttl elapses.
func (s *SetStore) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_, err := s.cluster.exec(ctx, transport.OpExpire, s.prefix+key, encodeTTL(ttl))
	return err
}

// Lock acquires a distributed lock on key, valid for ttl. Returns ErrKeyLocked
// if key is already locked.
func (s *SetStore) Lock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	return newLock(ctx, s.cluster, s.prefix+key, ttl)
}

// Atomic waits for a lock on key, then runs fn with the lock's authorized
// context and releases the lock when fn returns. ttl bounds how long the
// lock is held; ctx bounds how long Atomic waits to acquire it.
func (s *SetStore) Atomic(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	return lockAndRun(ctx, s.cluster, s.prefix+key, ttl, fn)
}
