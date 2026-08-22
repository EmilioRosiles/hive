package store

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// wireValue is the msgpack-serializable form of ValueStructure.
type wireValue struct {
	Data          []byte `msgpack:"d"`
	MTime         uint32 `msgpack:"mt"`
	ExpiresAt     uint32 `msgpack:"e"`
	LockToken     uint32 `msgpack:"lt"`
	LockExpiresAt uint32 `msgpack:"le"`
}

// ValueStructure holds a raw byte payload with an optional key-level expiry.
type ValueStructure struct {
	sizeBase
	mtimeBase
	lockBase
	Data      []byte
	expiresAt uint32 // unix seconds, 0 = no expiry
}

func NewValueStructure(data []byte) *ValueStructure {
	return &ValueStructure{Data: data}
}

func NewValueStructureWithTTL(data []byte, ttl time.Duration) *ValueStructure {
	return &ValueStructure{Data: data, expiresAt: uint32(time.Now().Add(ttl).Unix())}
}

func (v *ValueStructure) Kind() Kind            { return KindValue }
func (v *ValueStructure) KeyExpiry() uint32     { return v.expiresAt }
func (v *ValueStructure) SetKeyExpiry(s uint32) { v.expiresAt = s }
func (v *ValueStructure) ByteSize() int64       { return int64(len(v.Data)) + mtimeSize + keyExpirySize }

func (v *ValueStructure) Encode() ([]byte, error) {
	return msgpack.Marshal(wireValue{
		Data: v.Data, MTime: v.mtime, ExpiresAt: v.expiresAt,
		LockToken: v.lockToken, LockExpiresAt: v.lockExpiresAt,
	})
}

func DecodeValueStructure(data []byte) (*ValueStructure, error) {
	var w wireValue
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	vs := &ValueStructure{Data: w.Data, expiresAt: w.ExpiresAt}
	vs.mtime = w.MTime
	vs.lockToken = w.LockToken
	vs.lockExpiresAt = w.LockExpiresAt
	return vs, nil
}
