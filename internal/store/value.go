package store

import "time"

// ValueStructure holds a raw byte payload with an optional key-level expiry.
type ValueStructure struct {
	Data      []byte
	expiresAt int64 // unix seconds, 0 = no expiry
}

func NewValueStructure(data []byte) *ValueStructure {
	return &ValueStructure{Data: data}
}

func NewValueStructureWithTTL(data []byte, ttl time.Duration) *ValueStructure {
	return &ValueStructure{Data: data, expiresAt: time.Now().Add(ttl).Unix()}
}

func (v *ValueStructure) Kind() Kind              { return KindValue }
func (v *ValueStructure) KeyExpiry() int64        { return v.expiresAt }
func (v *ValueStructure) SetKeyExpiry(s int64)    { v.expiresAt = s }
func (v *ValueStructure) Encode() ([]byte, error) { return v.Data, nil }

// Cleanup is a no-op for plain values — there are no sub-fields to expire.
// Key-level expiry is handled by the DataStore directly.
func (v *ValueStructure) Cleanup(_ time.Time) bool { return false }
