package store

import (
	"github.com/vmihailenco/msgpack/v5"
)

// HashStructure is a map of string fields to byte-slice values. There is no
// per-field TTL — only a key-level expiry applies.
// The shard lock in DataStore protects all field access — no internal lock needed.
type HashStructure struct {
	sizeBase
	fields map[string][]byte
	mtimeBase
	expiresAt uint32 // key-level expiry, unix seconds, 0 = no expiry
	lockBase
}

func NewHashStructure() *HashStructure {
	return &HashStructure{fields: make(map[string][]byte)}
}

func (h *HashStructure) Kind() Kind            { return KindHash }
func (h *HashStructure) KeyExpiry() uint32     { return h.expiresAt }
func (h *HashStructure) SetKeyExpiry(t uint32) { h.expiresAt = t }

func (h *HashStructure) ByteSize() int64 {
	var n int64
	for name, data := range h.fields {
		n += int64(len(name)+len(data)) + mapBucketOverhead
	}
	return n + mtimeSize + keyExpirySize
}

// HSet sets field to data.
func (h *HashStructure) HSet(field string, data []byte) {
	h.fields[field] = data
}

// HGet returns the data for field and whether it exists.
func (h *HashStructure) HGet(field string) ([]byte, bool) {
	data, ok := h.fields[field]
	return data, ok
}

// HDel removes field. No-op if it does not exist.
func (h *HashStructure) HDel(field string) {
	delete(h.fields, field)
}

// Fields returns the names of all fields.
func (h *HashStructure) Fields() []string {
	out := make([]string, 0, len(h.fields))
	for name := range h.fields {
		out = append(out, name)
	}
	return out
}

// Len returns the number of fields.
func (h *HashStructure) Len() int { return len(h.fields) }

// AppendAll appends field name/value pairs to out as alternating
// [field, value, field, value, ...] []byte slices and returns the result.
// This avoids the intermediate map allocation that a GetAll() map return would require.
func (h *HashStructure) AppendAll(out [][]byte) [][]byte {
	for name, data := range h.fields {
		out = append(out, []byte(name), data)
	}
	return out
}

// -- serialization for rebalance --

type wireHash struct {
	Fields        map[string][]byte `msgpack:"f"`
	ExpiresAt     uint32            `msgpack:"e"`
	MTime         uint32            `msgpack:"mt"`
	LockToken     uint32            `msgpack:"lt"`
	LockExpiresAt uint32            `msgpack:"le"`
}

func (h *HashStructure) Encode() ([]byte, error) {
	return msgpack.Marshal(wireHash{
		Fields: h.fields, ExpiresAt: h.expiresAt, MTime: h.mtime,
		LockToken: h.lockToken, LockExpiresAt: h.lockExpiresAt,
	})
}

func DecodeHashStructure(data []byte) (*HashStructure, error) {
	var w wireHash
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Fields == nil {
		w.Fields = make(map[string][]byte)
	}
	hs := &HashStructure{fields: w.Fields, expiresAt: w.ExpiresAt}
	hs.mtime = w.MTime
	hs.lockToken = w.LockToken
	hs.lockExpiresAt = w.LockExpiresAt
	return hs, nil
}
