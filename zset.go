package hive

import (
	"context"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// ZSetEntry is a member/score pair returned by ZRange and ZRangeByScore.
type ZSetEntry struct {
	Member string
	Score  float64
}

// ZSetStore is a distributed sorted set backed by a Cluster. Members are unique
// strings each associated with a float64 score. The set is always kept in
// ascending score order (ties broken lexicographically). Keys are namespaced
// as {name}:z:{key}.
//
//	scores := hive.NewZSetStore(cluster, "leaderboard")
//	scores.ZAdd(ctx, "game:1", 9500.0, "alice")
//	top, _ := scores.ZRange(ctx, "game:1", -3, -1)
type ZSetStore struct {
	cluster *Cluster
	prefix  string
}

// NewZSetStore creates a sorted-set store backed by cluster.
func NewZSetStore(cluster *Cluster, name string) *ZSetStore {
	return &ZSetStore{cluster: cluster, prefix: name + ":z:"}
}

// ZAdd inserts or updates member with score.
func (z *ZSetStore) ZAdd(ctx context.Context, key string, score float64, member string) error {
	_, err := z.cluster.exec(ctx, transport.OpZAdd, z.prefix+key,
		encodeFloat64(score), []byte(member))
	return err
}

// ZRem removes member. No-op if member does not exist.
func (z *ZSetStore) ZRem(ctx context.Context, key, member string) error {
	_, err := z.cluster.exec(ctx, transport.OpZRem, z.prefix+key, []byte(member))
	return err
}

// ZScore returns the score for member. Returns ErrNotFound if member does not exist.
func (z *ZSetStore) ZScore(ctx context.Context, key, member string) (float64, error) {
	results, err := z.cluster.exec(ctx, transport.OpZScore, z.prefix+key, []byte(member))
	if err != nil {
		return 0, err
	}
	return decodeFloat64(results[0]), nil
}

// ZRank returns the 0-based rank of member in ascending score order (lowest = 0).
// Returns ErrNotFound if member does not exist.
func (z *ZSetStore) ZRank(ctx context.Context, key, member string) (int, error) {
	results, err := z.cluster.exec(ctx, transport.OpZRank, z.prefix+key, []byte(member))
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// ZRevRank returns the 0-based rank in descending score order (highest = 0).
// Returns ErrNotFound if member does not exist.
func (z *ZSetStore) ZRevRank(ctx context.Context, key, member string) (int, error) {
	results, err := z.cluster.exec(ctx, transport.OpZRevRank, z.prefix+key, []byte(member))
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// ZCard returns the number of members.
func (z *ZSetStore) ZCard(ctx context.Context, key string) (int, error) {
	results, err := z.cluster.exec(ctx, transport.OpZCard, z.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// ZRange returns members from rank start to rank stop inclusive.
// Negative indices count from the end (highest rank). Out-of-range bounds are
// clipped silently.
func (z *ZSetStore) ZRange(ctx context.Context, key string, start, stop int) ([]ZSetEntry, error) {
	results, err := z.cluster.exec(ctx, transport.OpZRange, z.prefix+key,
		encodeInt64(int64(start)), encodeInt64(int64(stop)))
	if err != nil {
		return nil, err
	}
	return decodeZSetEntries(results), nil
}

// ZRangeByScore returns all members with min <= score <= max in ascending order.
func (z *ZSetStore) ZRangeByScore(ctx context.Context, key string, min, max float64) ([]ZSetEntry, error) {
	results, err := z.cluster.exec(ctx, transport.OpZRangeByScore, z.prefix+key,
		encodeFloat64(min), encodeFloat64(max))
	if err != nil {
		return nil, err
	}
	return decodeZSetEntries(results), nil
}

// Del removes the entire sorted set at key.
func (z *ZSetStore) Del(ctx context.Context, key string) error {
	_, err := z.cluster.exec(ctx, transport.OpDel, z.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire sorted set is deleted after ttl elapses.
func (z *ZSetStore) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_, err := z.cluster.exec(ctx, transport.OpExpire, z.prefix+key, encodeTTL(ttl))
	return err
}

// Lock acquires a distributed lock on key, valid for ttl. Returns ErrKeyLocked
// if key is already locked.
func (z *ZSetStore) Lock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	return newLock(ctx, z.cluster, z.prefix+key, ttl)
}

// Atomic waits for a lock on key, then runs fn with the lock's authorized
// context and releases the lock when fn returns. ttl bounds how long the
// lock is held; ctx bounds how long Atomic waits to acquire it.
func (z *ZSetStore) Atomic(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	return lockAndRun(ctx, z.cluster, z.prefix+key, ttl, fn)
}

// decodeZSetEntries parses alternating [member, score, ...] byte slices.
func decodeZSetEntries(results [][]byte) []ZSetEntry {
	out := make([]ZSetEntry, 0, len(results)/2)
	for i := 0; i+1 < len(results); i += 2 {
		out = append(out, ZSetEntry{
			Member: string(results[i]),
			Score:  decodeFloat64(results[i+1]),
		})
	}
	return out
}
