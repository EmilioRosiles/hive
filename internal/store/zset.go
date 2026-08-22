package store

import (
	"math"
	"math/bits"
	"math/rand/v2"

	"github.com/vmihailenco/msgpack/v5"
)

// zsetEntry is one (member, score) pair, used for range/rank query results
// and as the wire entry shape.
type zsetEntry struct {
	Member string
	Score  float64
}

// zsetMaxLevel bounds a node's skip-list tower height. At p=0.25 (see
// zsetRandomLevel), 16 levels comfortably covers any realistic per-key
// member count (4^16 members) while keeping the head sentinel small.
const zsetMaxLevel = 16

// zsetLevel is one forward pointer in a node's tower, plus the number of
// nodes it skips over (its span). Spans are what make rank queries O(log n):
// summing the spans traversed from the head yields a node's 1-based position.
type zsetLevel struct {
	forward *zsetNode
	span    int
}

// zsetNode is one element of the skip list. level has one entry per tower
// level; len(level) is this node's randomly assigned height (>= 1).
type zsetNode struct {
	member   string
	score    float64
	backward *zsetNode
	level    []zsetLevel
}

// ZSetStructure is a set of unique string members each associated with a float64
// score, kept in ascending score order via a skip list. Ties are broken
// lexicographically by member name (Redis-compatible). The shard lock in
// DataStore protects all field access.
type ZSetStructure struct {
	sizeBase
	mtimeBase
	scores    map[string]float64 // O(1) lookup by member
	head      *zsetNode          // sentinel; head.level grows lazily, never shrinks
	tail      *zsetNode
	length    int
	level     int   // highest tower level currently in use
	nodeBytes int64 // incrementally maintained byte cost of head + all nodes
	expiresAt int64 // unix seconds, 0 = no expiry
}

func NewZSetStructure() *ZSetStructure {
	z := &ZSetStructure{
		scores: make(map[string]float64),
		head:   &zsetNode{level: make([]zsetLevel, 1)},
		level:  1,
	}
	z.nodeBytes = zsetNodeOverhead + zsetLevelSize
	return z
}

func (z *ZSetStructure) Kind() Kind           { return KindZSet }
func (z *ZSetStructure) KeyExpiry() int64     { return z.expiresAt }
func (z *ZSetStructure) SetKeyExpiry(t int64) { z.expiresAt = t }

// ByteSize is O(1): nodeBytes is maintained incrementally by insertNode/
// deleteNode rather than recomputed by walking the skip list on every call
// (ByteSize runs on every write via DataStore's capacity accounting).
func (z *ZSetStructure) ByteSize() int64 {
	return z.nodeBytes + int64(len(z.scores))*mapEntryOverhead + mtimeSize + keyExpirySize
}

// zsetRandomLevel returns a tower height in [1, zsetMaxLevel] with
// P(height >= k) = 0.25^(k-1) — two random bits spent per level instead of
// Redis's one-draw-per-level rejection loop.
func zsetRandomLevel() int {
	lvl := 1 + bits.TrailingZeros32(rand.Uint32())/2
	if lvl > zsetMaxLevel {
		lvl = zsetMaxLevel
	}
	return lvl
}

// randomLevel is an indirection point so tests can install a deterministic
// height generator.
var randomLevel = zsetRandomLevel

// nodeBefore reports whether n sorts strictly before (score, member):
// ascending score, then ascending member. Used by insert/delete descents,
// which must stop at the predecessor of the target.
func nodeBefore(n *zsetNode, score float64, member string) bool {
	return n.score < score || (n.score == score && n.member < member)
}

// nodeBeforeOrEqual reports whether n sorts at or before (score, member).
// Used by rank lookups, which must be able to land on the target itself.
func nodeBeforeOrEqual(n *zsetNode, score float64, member string) bool {
	return n.score < score || (n.score == score && n.member <= member)
}

// insertNode splices a new (member, score) node into the skip list.
// Precondition: member is not currently present (ZAdd removes first).
func (z *ZSetStructure) insertNode(member string, score float64) {
	var update [zsetMaxLevel]*zsetNode
	var rank [zsetMaxLevel]int

	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil && nodeBefore(x.level[i].forward, score, member) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	lvl := randomLevel()
	if lvl > z.level {
		for i := z.level; i < lvl; i++ {
			if i >= len(z.head.level) {
				z.head.level = append(z.head.level, zsetLevel{})
				z.nodeBytes += zsetLevelSize
			}
			rank[i] = 0
			update[i] = z.head
			z.head.level[i].span = z.length
			z.head.level[i].forward = nil
		}
		z.level = lvl
	}

	n := &zsetNode{member: member, score: score, level: make([]zsetLevel, lvl)}
	for i := 0; i < lvl; i++ {
		n.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = n
		n.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	for i := lvl; i < z.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != z.head {
		n.backward = update[0]
	}
	if n.level[0].forward != nil {
		n.level[0].forward.backward = n
	} else {
		z.tail = n
	}
	z.length++
	z.nodeBytes += int64(len(member)) + zsetNodeOverhead + int64(lvl)*zsetLevelSize
}

// deleteNode removes the node for (member, score). Returns false if not found.
func (z *ZSetStructure) deleteNode(member string, score float64) bool {
	var update [zsetMaxLevel]*zsetNode

	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && nodeBefore(x.level[i].forward, score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := 0; i < z.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		z.tail = x.backward
	}
	for z.level > 1 && z.head.level[z.level-1].forward == nil {
		z.level--
	}
	z.length--
	z.nodeBytes -= int64(len(x.member)) + zsetNodeOverhead + int64(len(x.level))*zsetLevelSize
	return true
}

// rankOf returns the 0-based rank of (member, score), or -1 if absent.
func (z *ZSetStructure) rankOf(member string, score float64) int {
	x := z.head
	rank := 0
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && nodeBeforeOrEqual(x.level[i].forward, score, member) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != z.head && x.member == member {
			return rank - 1 // spans are 1-based from the head sentinel
		}
	}
	return -1
}

// nodeByRank returns the node at 0-based rank r, or nil if out of range.
func (z *ZSetStructure) nodeByRank(r int) *zsetNode {
	if r < 0 || r >= z.length {
		return nil
	}
	target := r + 1
	traversed := 0
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= target {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == target {
			return x
		}
	}
	return nil
}

// ZAdd inserts or updates member with score. A no-op if the score is
// unchanged (avoids re-rolling the node's tower height on a pure rewrite).
// NaN scores are silently ignored — no defined position in the ordering.
func (z *ZSetStructure) ZAdd(member string, score float64) {
	if math.IsNaN(score) {
		return
	}
	if old, exists := z.scores[member]; exists {
		if old == score {
			return
		}
		z.deleteNode(member, old)
	}
	z.scores[member] = score
	z.insertNode(member, score)
}

// ZRem removes member. No-op if member does not exist.
func (z *ZSetStructure) ZRem(member string) {
	score, ok := z.scores[member]
	if !ok {
		return
	}
	z.deleteNode(member, score)
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
	score, ok := z.scores[member]
	if !ok {
		return 0, false
	}
	r := z.rankOf(member, score)
	if r < 0 {
		return 0, false
	}
	return r, true
}

// ZRevRank returns the 0-based rank in descending order (highest score = 0).
func (z *ZSetStructure) ZRevRank(member string) (int, bool) {
	rank, ok := z.ZRank(member)
	if !ok {
		return 0, false
	}
	return z.length - 1 - rank, true
}

// ZRange returns entries from rank start to rank stop inclusive.
// Negative indices are supported (Python-style). Out-of-range bounds are clipped.
func (z *ZSetStructure) ZRange(start, stop int) []zsetEntry {
	n := z.length
	if n == 0 {
		return nil
	}
	s := normalizeListBound(start, n)
	e := normalizeListBound(stop, n)
	if s > e {
		return nil
	}
	out := make([]zsetEntry, 0, e-s+1)
	for x := z.nodeByRank(s); x != nil && len(out) < e-s+1; x = x.level[0].forward {
		out = append(out, zsetEntry{Member: x.member, Score: x.score})
	}
	return out
}

// ZRangeByScore returns all entries with min <= score <= max in ascending order.
func (z *ZSetStructure) ZRangeByScore(min, max float64) []zsetEntry {
	if min > max {
		return nil
	}
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score < min {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward

	var out []zsetEntry
	for ; x != nil && x.score <= max; x = x.level[0].forward {
		out = append(out, zsetEntry{Member: x.member, Score: x.score})
	}
	return out
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
	entries := make([]wireZSetEntry, 0, z.length)
	for x := z.head.level[0].forward; x != nil; x = x.level[0].forward {
		entries = append(entries, wireZSetEntry{Member: x.member, Score: x.score})
	}
	return msgpack.Marshal(wireZSet{Entries: entries, ExpiresAt: z.expiresAt, MTime: z.mtime})
}

// bulkLoad builds the skip list from entries in one O(n) pass instead of n
// individual O(log n) inserts. Assumes entries is already ascending-sorted
// and duplicate-free, which holds for anything produced by Encode.
func (z *ZSetStructure) bulkLoad(entries []wireZSetEntry) {
	var last [zsetMaxLevel]*zsetNode
	var rankAt [zsetMaxLevel]int
	for i := range last {
		last[i] = z.head
	}

	var prev *zsetNode
	for idx, we := range entries {
		pos := idx + 1
		lvl := randomLevel()
		for lvl > len(z.head.level) {
			z.head.level = append(z.head.level, zsetLevel{})
			z.nodeBytes += zsetLevelSize
		}
		if lvl > z.level {
			z.level = lvl
		}

		n := &zsetNode{member: we.Member, score: we.Score, level: make([]zsetLevel, lvl), backward: prev}
		for i := 0; i < lvl; i++ {
			last[i].level[i].forward = n
			last[i].level[i].span = pos - rankAt[i]
			last[i] = n
			rankAt[i] = pos
		}

		prev = n
		z.scores[we.Member] = we.Score
		z.nodeBytes += int64(len(we.Member)) + zsetNodeOverhead + int64(lvl)*zsetLevelSize
	}

	z.length = len(entries)
	z.tail = prev
	for i := 0; i < z.level; i++ {
		last[i].level[i].forward = nil
		last[i].level[i].span = z.length - rankAt[i]
	}
}

func DecodeZSetStructure(data []byte) (*ZSetStructure, error) {
	var w wireZSet
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	zs := NewZSetStructure()
	zs.expiresAt = w.ExpiresAt
	zs.mtime = w.MTime
	zs.bulkLoad(w.Entries)
	return zs, nil
}
