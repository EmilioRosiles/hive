package store

import (
	"slices"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// zsetEntry is one element in the score-ordered slice.
type zsetEntry struct {
	Member string
	Score  float64
}

// ZSetStructure is a set of unique string members each associated with a float64
// score, kept in ascending score order. Ties are broken lexicographically by
// member name (Redis-compatible). The shard lock in DataStore protects all
// field access.
type ZSetStructure struct {
	sizeBase
	mtimeBase
	scores    map[string]float64 // O(1) lookup by member
	sorted    []zsetEntry        // score-ordered slice for rank / range ops
	expiresAt int64              // unix seconds, 0 = no expiry
}

func NewZSetStructure() *ZSetStructure {
	return &ZSetStructure{scores: make(map[string]float64)}
}

func (z *ZSetStructure) Kind() Kind           { return KindZSet }
func (z *ZSetStructure) KeyExpiry() int64     { return z.expiresAt }
func (z *ZSetStructure) SetKeyExpiry(t int64) { z.expiresAt = t }

func (z *ZSetStructure) ByteSize() int64 {
	var n int64
	for m := range z.scores {
		// scores map: string key + float64 value + map bucket overhead
		n += int64(len(m)) + mapEntryOverhead
		// sorted slice: zsetEntry struct (string header + float64) per element
		n += zsetEntryOverhead
	}
	return n + mtimeSize + keyExpirySize
}

// ZAdd inserts or updates member with score. The sorted slice is kept in order.
func (z *ZSetStructure) ZAdd(member string, score float64) {
	if _, exists := z.scores[member]; exists {
		z.remove(member)
	}
	z.scores[member] = score
	z.insert(member, score)
}

// ZRem removes member. No-op if member does not exist.
func (z *ZSetStructure) ZRem(member string) {
	if _, ok := z.scores[member]; !ok {
		return
	}
	z.remove(member)
	delete(z.scores, member)
}

// ZScore returns the score for member and whether it exists.
func (z *ZSetStructure) ZScore(member string) (float64, bool) {
	s, ok := z.scores[member]
	return s, ok
}

// ZCard returns the number of members.
func (z *ZSetStructure) ZCard() int { return len(z.scores) }

// ZRank returns the 0-based rank of member in ascending order (lowest score = 0).
// Returns (0, false) if member does not exist.
func (z *ZSetStructure) ZRank(member string) (int, bool) {
	if _, ok := z.scores[member]; !ok {
		return 0, false
	}
	idx, _ := z.searchMember(member)
	return idx, true
}

// ZRevRank returns the 0-based rank in descending order (highest score = 0).
func (z *ZSetStructure) ZRevRank(member string) (int, bool) {
	rank, ok := z.ZRank(member)
	if !ok {
		return 0, false
	}
	return len(z.sorted) - 1 - rank, true
}

// ZRange returns entries from rank start to rank stop inclusive.
// Negative indices are supported (Python-style). Out-of-range bounds are clipped.
func (z *ZSetStructure) ZRange(start, stop int) []zsetEntry {
	n := len(z.sorted)
	if n == 0 {
		return nil
	}
	s := normalizeListBound(start, n)
	e := normalizeListBound(stop, n)
	if s > e {
		return nil
	}
	return z.sorted[s : e+1]
}

// ZRangeByScore returns all entries with min <= score <= max in ascending order.
func (z *ZSetStructure) ZRangeByScore(min, max float64) []zsetEntry {
	if min > max {
		return nil
	}
	// Find first entry with score >= min.
	lo, _ := slices.BinarySearchFunc(z.sorted, min, func(e zsetEntry, target float64) int {
		if e.Score < target {
			return -1
		}
		if e.Score > target {
			return 1
		}
		return 0
	})
	// Collect entries while score <= max.
	var out []zsetEntry
	for _, e := range z.sorted[lo:] {
		if e.Score > max {
			break
		}
		out = append(out, e)
	}
	return out
}

// Cleanup is a no-op — ZSet has no per-member TTL.
func (z *ZSetStructure) Cleanup(_ time.Time) bool { return false }

// -- internal sorted-slice helpers --

// cmpEntry defines the ordering for the sorted slice: ascending score,
// lexicographic member name as tiebreaker.
func cmpEntry(a, b zsetEntry) int {
	if a.Score < b.Score {
		return -1
	}
	if a.Score > b.Score {
		return 1
	}
	if a.Member < b.Member {
		return -1
	}
	if a.Member > b.Member {
		return 1
	}
	return 0
}

// insert adds a new entry into the sorted slice at the correct position.
func (z *ZSetStructure) insert(member string, score float64) {
	e := zsetEntry{Member: member, Score: score}
	pos, _ := slices.BinarySearchFunc(z.sorted, e, cmpEntry)
	z.sorted = slices.Insert(z.sorted, pos, e)
}

// remove deletes member from the sorted slice.
func (z *ZSetStructure) remove(member string) {
	idx, _ := z.searchMember(member)
	z.sorted = slices.Delete(z.sorted, idx, idx+1)
}

// searchMember finds the exact position of member in the sorted slice.
// Assumes member is present (only called after existence check).
func (z *ZSetStructure) searchMember(member string) (int, bool) {
	score := z.scores[member]
	e := zsetEntry{Member: member, Score: score}
	return slices.BinarySearchFunc(z.sorted, e, cmpEntry)
}

// -- serialization --

type wireZSetEntry struct {
	Member string  `msgpack:"m"`
	Score  float64 `msgpack:"s"`
}

type wireZSet struct {
	Entries   []wireZSetEntry `msgpack:"e"`
	ExpiresAt int64           `msgpack:"ea"`
	MTime     uint32          `msgpack:"mt"`
}

func (z *ZSetStructure) Encode() ([]byte, error) {
	entries := make([]wireZSetEntry, len(z.sorted))
	for i, e := range z.sorted {
		entries[i] = wireZSetEntry{Member: e.Member, Score: e.Score}
	}
	return msgpack.Marshal(wireZSet{Entries: entries, ExpiresAt: z.expiresAt, MTime: z.mtime})
}

func DecodeZSetStructure(data []byte) (*ZSetStructure, error) {
	var w wireZSet
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	zs := NewZSetStructure()
	zs.expiresAt = w.ExpiresAt
	zs.mtime = w.MTime
	// Rebuild from the already-ordered entries list; skip insert's binary search.
	zs.sorted = make([]zsetEntry, len(w.Entries))
	for i, we := range w.Entries {
		zs.sorted[i] = zsetEntry{Member: we.Member, Score: we.Score}
		zs.scores[we.Member] = we.Score
	}
	return zs, nil
}
