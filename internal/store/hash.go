package store

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// hashField is a single field value with an optional per-field TTL.
type hashField struct {
	Data      []byte
	expiresAt int64 // unix nanoseconds, 0 = no expiry
}

func (f hashField) alive(nowNs int64) bool {
	return f.expiresAt == 0 || nowNs <= f.expiresAt
}

// HashStructure is a map of string fields to typed values with optional per-field TTL.
// The shard lock in DataStore protects all field access — no internal lock needed.
type HashStructure struct {
	fields    map[string]hashField
	expiresAt int64 // key-level expiry, unix seconds, 0 = no expiry
}

func NewHashStructure() *HashStructure {
	return &HashStructure{fields: make(map[string]hashField)}
}

func (h *HashStructure) Kind() Kind           { return KindHash }
func (h *HashStructure) KeyExpiry() int64     { return h.expiresAt }
func (h *HashStructure) SetKeyExpiry(t int64) { h.expiresAt = t }

// HSet sets field to data with no expiry. An existing TTL is cleared.
func (h *HashStructure) HSet(field string, data []byte) {
	h.fields[field] = hashField{Data: data}
}

// HSetWithTTL sets field to data, expiring the field after ttl.
func (h *HashStructure) HSetWithTTL(field string, data []byte, ttl time.Duration) {
	h.fields[field] = hashField{Data: data, expiresAt: time.Now().Add(ttl).UnixNano()}
}

// HGet returns the data for field and whether it exists and has not expired.
func (h *HashStructure) HGet(field string) ([]byte, bool) {
	f, ok := h.fields[field]
	if !ok || !f.alive(time.Now().UnixNano()) {
		return nil, false
	}
	return f.Data, true
}

// HDel removes field. No-op if it does not exist.
func (h *HashStructure) HDel(field string) {
	delete(h.fields, field)
}

// Fields returns the names of all live (non-expired) fields.
func (h *HashStructure) Fields() []string {
	now := time.Now().UnixNano()
	out := make([]string, 0, len(h.fields))
	for name, f := range h.fields {
		if f.alive(now) {
			out = append(out, name)
		}
	}
	return out
}

// GetAll returns all live field names and their data.
func (h *HashStructure) GetAll() map[string][]byte {
	now := time.Now().UnixNano()
	out := make(map[string][]byte, len(h.fields))
	for name, f := range h.fields {
		if f.alive(now) {
			out[name] = f.Data
		}
	}
	return out
}

// ExpireField sets a TTL on an existing field. No-op if field does not exist.
func (h *HashStructure) ExpireField(field string, ttl time.Duration) {
	if f, ok := h.fields[field]; ok {
		f.expiresAt = time.Now().Add(ttl).UnixNano()
		h.fields[field] = f
	}
}

// Cleanup removes expired fields and reports whether the hash is empty.
// Called by the DataStore janitor while the shard write lock is held.
func (h *HashStructure) Cleanup(now time.Time) bool {
	nowNs := now.UnixNano()
	for name, f := range h.fields {
		if !f.alive(nowNs) {
			delete(h.fields, name)
		}
	}
	return len(h.fields) == 0
}

// -- serialization for rebalance --

type wireHashField struct {
	Data      []byte `msgpack:"d"`
	ExpiresAt int64  `msgpack:"e"`
}

type wireHash struct {
	Fields    map[string]wireHashField `msgpack:"f"`
	ExpiresAt int64                   `msgpack:"e"`
}

func (h *HashStructure) Encode() ([]byte, error) {
	w := wireHash{
		Fields:    make(map[string]wireHashField, len(h.fields)),
		ExpiresAt: h.expiresAt,
	}
	for name, f := range h.fields {
		w.Fields[name] = wireHashField{Data: f.Data, ExpiresAt: f.expiresAt}
	}
	return msgpack.Marshal(w)
}

func DecodeHashStructure(data []byte) (*HashStructure, error) {
	var w wireHash
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	fields := make(map[string]hashField, len(w.Fields))
	for name, wf := range w.Fields {
		fields[name] = hashField{Data: wf.Data, expiresAt: wf.ExpiresAt}
	}
	return &HashStructure{fields: fields, expiresAt: w.ExpiresAt}, nil
}
