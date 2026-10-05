package store

import (
	"math/bits"

	"github.com/vmihailenco/msgpack/v5"
)

// BitmapStructure is a packed bit array addressed by offset. There is no
// per-bit TTL — only a key-level expiry applies.
// The shard lock in DataStore protects all field access — no internal lock needed.
type BitmapStructure struct {
	sizeBase
	bits []byte
	mtimeBase
	expiresAt uint32 // key-level expiry, unix seconds, 0 = no expiry
	lockBase
}

func NewBitmapStructure() *BitmapStructure {
	return &BitmapStructure{}
}

func (b *BitmapStructure) Kind() Kind            { return KindBitmap }
func (b *BitmapStructure) KeyExpiry() uint32     { return b.expiresAt }
func (b *BitmapStructure) SetKeyExpiry(t uint32) { b.expiresAt = t }
func (b *BitmapStructure) ByteSize() int64       { return int64(len(b.bits)) + mtimeSize + keyExpirySize }

// SetBit sets or clears the bit at offset, growing the bitmap as needed.
// Clearing a bit past the end is a no-op.
func (b *BitmapStructure) SetBit(offset uint32, on bool) {
	i := int(offset >> 3)
	mask := byte(1) << (7 - offset&7)
	if i >= len(b.bits) {
		if !on {
			return
		}
		b.bits = append(b.bits, make([]byte, i+1-len(b.bits))...)
	}
	if on {
		b.bits[i] |= mask
	} else {
		b.bits[i] &^= mask
	}
}

// GetBit reports whether the bit at offset is set.
func (b *BitmapStructure) GetBit(offset uint32) bool {
	i := int(offset >> 3)
	if i >= len(b.bits) {
		return false
	}
	return b.bits[i]&(byte(1)<<(7-offset&7)) != 0
}

// Count returns the number of set bits.
func (b *BitmapStructure) Count() int {
	n := 0
	for _, x := range b.bits {
		n += bits.OnesCount8(x)
	}
	return n
}

// -- serialization for rebalance --

// wireBitmap is the msgpack-serializable form of BitmapStructure.
type wireBitmap struct {
	Bits          []byte `msgpack:"b"`
	ExpiresAt     uint32 `msgpack:"e"`
	MTime         uint32 `msgpack:"mt"`
	LockToken     uint32 `msgpack:"lt"`
	LockExpiresAt uint32 `msgpack:"le"`
}

func (b *BitmapStructure) Encode() ([]byte, error) {
	return msgpack.Marshal(wireBitmap{
		Bits: b.bits, ExpiresAt: b.expiresAt, MTime: b.mtime,
		LockToken: b.lockToken, LockExpiresAt: b.lockExpiresAt,
	})
}

func DecodeBitmapStructure(data []byte) (*BitmapStructure, error) {
	var w wireBitmap
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	bs := &BitmapStructure{bits: w.Bits, expiresAt: w.ExpiresAt}
	bs.mtime = w.MTime
	bs.lockToken = w.LockToken
	bs.lockExpiresAt = w.LockExpiresAt
	return bs, nil
}
