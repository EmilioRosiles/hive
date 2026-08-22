package store

import (
	"github.com/vmihailenco/msgpack/v5"
)

// ListStructure is an ordered sequence of byte slices, backed by a circular
// buffer so push/pop at both ends are O(1) amortized. There is no
// per-element TTL — only a key-level expiry applies.
// The shard lock in DataStore protects all field access.
type ListStructure struct {
	sizeBase
	buf    [][]byte // circular backing array; len(buf) is current capacity
	head   int      // physical index of logical position 0
	length int      // number of live elements
	mtimeBase
	expiresAt uint32 // unix seconds, 0 = no expiry
	lockBase
}

func NewListStructure() *ListStructure {
	return &ListStructure{}
}

func (l *ListStructure) Kind() Kind            { return KindList }
func (l *ListStructure) KeyExpiry() uint32     { return l.expiresAt }
func (l *ListStructure) SetKeyExpiry(t uint32) { l.expiresAt = t }

func (l *ListStructure) ByteSize() int64 {
	var n int64
	for i := 0; i < l.length; i++ {
		n += int64(len(l.at(i))) + sliceItemOverhead
	}
	return n + mtimeSize + keyExpirySize
}

// at returns the element at physical-mapped logical index i. Caller must
// ensure 0 <= i < l.length.
func (l *ListStructure) at(i int) []byte {
	return l.buf[(l.head+i)%len(l.buf)]
}

// grow doubles the backing array's capacity (minimum 4), copying live
// elements out in logical order starting at physical index 0.
func (l *ListStructure) grow() {
	newCap := 4
	if len(l.buf) > 0 {
		newCap = len(l.buf) * 2
	}
	newBuf := make([][]byte, newCap)
	for i := 0; i < l.length; i++ {
		newBuf[i] = l.at(i)
	}
	l.buf = newBuf
	l.head = 0
}

// LPush prepends data to the head of the list.
func (l *ListStructure) LPush(data []byte) {
	if l.length == len(l.buf) {
		l.grow()
	}
	l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
	l.buf[l.head] = data
	l.length++
}

// RPush appends data to the tail of the list.
func (l *ListStructure) RPush(data []byte) {
	if l.length == len(l.buf) {
		l.grow()
	}
	l.buf[(l.head+l.length)%len(l.buf)] = data
	l.length++
}

// LPop removes and returns the head element. Returns (nil, false) if empty.
func (l *ListStructure) LPop() ([]byte, bool) {
	if l.length == 0 {
		return nil, false
	}
	v := l.buf[l.head]
	l.buf[l.head] = nil // avoid retaining a reference in the backing array
	l.head = (l.head + 1) % len(l.buf)
	l.length--
	return v, true
}

// RPop removes and returns the tail element. Returns (nil, false) if empty.
func (l *ListStructure) RPop() ([]byte, bool) {
	if l.length == 0 {
		return nil, false
	}
	idx := (l.head + l.length - 1) % len(l.buf)
	v := l.buf[idx]
	l.buf[idx] = nil // avoid retaining a reference in the backing array
	l.length--
	return v, true
}

// Len returns the number of elements.
func (l *ListStructure) Len() int { return l.length }

// Index returns the element at position i (negative counts from tail).
// Returns (nil, false) if the index is out of bounds.
func (l *ListStructure) Index(i int) ([]byte, bool) {
	idx, ok := resolveListIndex(i, l.length)
	if !ok {
		return nil, false
	}
	return l.at(idx), true
}

// Set overwrites the element at position i. Returns false if out of bounds.
func (l *ListStructure) Set(i int, data []byte) bool {
	idx, ok := resolveListIndex(i, l.length)
	if !ok {
		return false
	}
	l.buf[(l.head+idx)%len(l.buf)] = data
	return true
}

// Range returns elements from start to stop inclusive (negative indices supported).
// Out-of-range bounds are clipped silently; an empty slice is never an error.
func (l *ListStructure) Range(start, stop int) [][]byte {
	n := l.length
	if n == 0 {
		return nil
	}
	s := normalizeListBound(start, n)
	e := normalizeListBound(stop, n)
	if s > e {
		return nil
	}
	out := make([][]byte, e-s+1)
	for i := range out {
		out[i] = l.at(s + i)
	}
	return out
}

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
	Items         [][]byte `msgpack:"i"`
	ExpiresAt     uint32   `msgpack:"e"`
	MTime         uint32   `msgpack:"mt"`
	LockToken     uint32   `msgpack:"lt"`
	LockExpiresAt uint32   `msgpack:"le"`
}

func (l *ListStructure) Encode() ([]byte, error) {
	items := make([][]byte, l.length)
	for i := range items {
		items[i] = l.at(i)
	}
	return msgpack.Marshal(wireList{
		Items: items, ExpiresAt: l.expiresAt, MTime: l.mtime,
		LockToken: l.lockToken, LockExpiresAt: l.lockExpiresAt,
	})
}

func DecodeListStructure(data []byte) (*ListStructure, error) {
	var w wireList
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	ls := &ListStructure{buf: w.Items, length: len(w.Items), expiresAt: w.ExpiresAt}
	ls.mtime = w.MTime
	ls.lockToken = w.LockToken
	ls.lockExpiresAt = w.LockExpiresAt
	return ls, nil
}
