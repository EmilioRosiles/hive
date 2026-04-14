package hive

import (
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// ZSetEntry is a member/score pair returned by ZRange and ZRangeByScore.
type ZSetEntry struct {
	Member string
	Score  float64
}

// ZSetStore is a distributed sorted set backed by a Cache. Members are unique
// strings each associated with a float64 score. The set is always kept in
// ascending score order (ties broken lexicographically). Keys are namespaced
// as {name}:z:{key}.
//
//	scores := hive.NewZSetStore(cache, "leaderboard")
//	scores.ZAdd("game:1", 9500.0, "alice")
//	top, _ := scores.ZRange("game:1", -3, -1)
type ZSetStore struct {
	cache  *Cache
	prefix string
}

// NewZSetStore creates a sorted-set store backed by cache.
func NewZSetStore(cache *Cache, name string) *ZSetStore {
	return &ZSetStore{cache: cache, prefix: name + ":z:"}
}

// ZAdd inserts or updates member with score.
func (z *ZSetStore) ZAdd(key string, score float64, member string) error {
	_, err := z.cache.exec(transport.OpZAdd, z.prefix+key,
		encodeFloat64(score), []byte(member))
	return err
}

// ZRem removes member. No-op if member does not exist.
func (z *ZSetStore) ZRem(key, member string) error {
	_, err := z.cache.exec(transport.OpZRem, z.prefix+key, []byte(member))
	return err
}

// ZScore returns the score for member. Returns ErrNotFound if member does not exist.
func (z *ZSetStore) ZScore(key, member string) (float64, error) {
	results, err := z.cache.exec(transport.OpZScore, z.prefix+key, []byte(member))
	if err != nil {
		return 0, err
	}
	return decodeFloat64(results[0]), nil
}

// ZRank returns the 0-based rank of member in ascending score order (lowest = 0).
// Returns ErrNotFound if member does not exist.
func (z *ZSetStore) ZRank(key, member string) (int, error) {
	results, err := z.cache.exec(transport.OpZRank, z.prefix+key, []byte(member))
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// ZRevRank returns the 0-based rank in descending score order (highest = 0).
// Returns ErrNotFound if member does not exist.
func (z *ZSetStore) ZRevRank(key, member string) (int, error) {
	results, err := z.cache.exec(transport.OpZRevRank, z.prefix+key, []byte(member))
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// ZCard returns the number of members.
func (z *ZSetStore) ZCard(key string) (int, error) {
	results, err := z.cache.exec(transport.OpZCard, z.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// ZRange returns members from rank start to rank stop inclusive.
// Negative indices count from the end (highest rank). Out-of-range bounds are
// clipped silently.
func (z *ZSetStore) ZRange(key string, start, stop int) ([]ZSetEntry, error) {
	results, err := z.cache.exec(transport.OpZRange, z.prefix+key,
		encodeInt64(int64(start)), encodeInt64(int64(stop)))
	if err != nil {
		return nil, err
	}
	return decodeZSetEntries(results), nil
}

// ZRangeByScore returns all members with min <= score <= max in ascending order.
func (z *ZSetStore) ZRangeByScore(key string, min, max float64) ([]ZSetEntry, error) {
	results, err := z.cache.exec(transport.OpZRangeByScore, z.prefix+key,
		encodeFloat64(min), encodeFloat64(max))
	if err != nil {
		return nil, err
	}
	return decodeZSetEntries(results), nil
}

// Del removes the entire sorted set at key.
func (z *ZSetStore) Del(key string) error {
	_, err := z.cache.exec(transport.OpDel, z.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire sorted set is deleted after ttl elapses.
func (z *ZSetStore) Expire(key string, ttl time.Duration) error {
	_, err := z.cache.exec(transport.OpExpire, z.prefix+key, encodeTTL(ttl))
	return err
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
