package store

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// ListStructure is an ordered sequence of byte slices. items[0] is the head
// (left end). LPush prepends; RPush appends. There is no per-element TTL —
// only a key-level expiry applies.
// The shard lock in DataStore protects all field access.
type ListStructure struct {
	sizeBase
	writeAtBase
	items     [][]byte
	expiresAt int64 // unix seconds, 0 = no expiry
}

func NewListStructure() *ListStructure {
	return &ListStructure{}
}

func (l *ListStructure) Kind() Kind           { return KindList }
func (l *ListStructure) KeyExpiry() int64     { return l.expiresAt }
func (l *ListStructure) SetKeyExpiry(t int64) { l.expiresAt = t }

func (l *ListStructure) ByteSize() int64 {
	var n int64
	for _, item := range l.items {
		n += int64(len(item)) + sliceItemOverhead
	}
	return n + writeAtSize + keyExpirySize
}

// LPush prepends data to the head of the list.
func (l *ListStructure) LPush(data []byte) {
	l.items = append([][]byte{data}, l.items...)
}

// RPush appends data to the tail of the list.
func (l *ListStructure) RPush(data []byte) {
	l.items = append(l.items, data)
}

// LPop removes and returns the head element. Returns (nil, false) if empty.
func (l *ListStructure) LPop() ([]byte, bool) {
	if len(l.items) == 0 {
		return nil, false
	}
	v := l.items[0]
	l.items = l.items[1:]
	return v, true
}

// RPop removes and returns the tail element. Returns (nil, false) if empty.
func (l *ListStructure) RPop() ([]byte, bool) {
	if len(l.items) == 0 {
		return nil, false
	}
	v := l.items[len(l.items)-1]
	l.items = l.items[:len(l.items)-1]
	return v, true
}

// Len returns the number of elements.
func (l *ListStructure) Len() int { return len(l.items) }

// Index returns the element at position i (negative counts from tail).
// Returns (nil, false) if the index is out of bounds.
func (l *ListStructure) Index(i int) ([]byte, bool) {
	idx, ok := resolveListIndex(i, len(l.items))
	if !ok {
		return nil, false
	}
	return l.items[idx], true
}

// Set overwrites the element at position i. Returns false if out of bounds.
func (l *ListStructure) Set(i int, data []byte) bool {
	idx, ok := resolveListIndex(i, len(l.items))
	if !ok {
		return false
	}
	l.items[idx] = data
	return true
}

// Range returns elements from start to stop inclusive (negative indices supported).
// Out-of-range bounds are clipped silently; an empty slice is never an error.
func (l *ListStructure) Range(start, stop int) [][]byte {
	n := len(l.items)
	if n == 0 {
		return nil
	}
	s := normalizeListBound(start, n)
	e := normalizeListBound(stop, n)
	if s > e {
		return nil
	}
	out := make([][]byte, e-s+1)
	copy(out, l.items[s:e+1])
	return out
}

// Cleanup is a no-op — list elements do not independently expire.
func (l *ListStructure) Cleanup(_ time.Time) bool { return false }

// resolveListIndex normalises a possibly-negative index into an absolute
// position. Returns (pos, true) on success or (0, false) if out of bounds.
func resolveListIndex(i, length int) (int, bool) {
	if i < 0 {
		i += length
	}
	if i < 0 || i >= length {
		return 0, false
	}
	return i, true
}

// normalizeListBound clamps a possibly-negative bound for Range.
func normalizeListBound(b, length int) int {
	if b < 0 {
		b += length
	}
	if b < 0 {
		b = 0
	}
	if b >= length {
		b = length - 1
	}
	return b
}

// -- serialization --

type wireList struct {
	Items     [][]byte `msgpack:"i"`
	ExpiresAt int64    `msgpack:"e"`
	WriteAt   int64    `msgpack:"wa"`
}

func (l *ListStructure) Encode() ([]byte, error) {
	return msgpack.Marshal(wireList{Items: l.items, ExpiresAt: l.expiresAt, WriteAt: l.writeAt})
}

func DecodeListStructure(data []byte) (*ListStructure, error) {
	var w wireList
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	ls := &ListStructure{items: w.Items, expiresAt: w.ExpiresAt}
	ls.writeAt = w.WriteAt
	if ls.items == nil {
		ls.items = [][]byte{}
	}
	return ls, nil
}
