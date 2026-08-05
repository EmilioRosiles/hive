package transport

import (
	"encoding/binary"
	"errors"
)

var errShortBuffer = errors.New("transport: truncated binary payload")

// binReader is a bounds-checked cursor over a byte slice, shared by the
// ForwardRequest/ForwardResponse/RebalanceBatch codecs. Every read method
// returns an error instead of panicking on truncated input.
type binReader struct {
	b   []byte
	off int
}

func (r *binReader) uint8() (uint8, error) {
	if r.off+1 > len(r.b) {
		return 0, errShortBuffer
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *binReader) uint32() (uint32, error) {
	if r.off+4 > len(r.b) {
		return 0, errShortBuffer
	}
	v := binary.BigEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v, nil
}

func (r *binReader) int64() (int64, error) {
	if r.off+8 > len(r.b) {
		return 0, errShortBuffer
	}
	v := int64(binary.BigEndian.Uint64(r.b[r.off:]))
	r.off += 8
	return v, nil
}

// count reads a uint32 element count and validates it against the remaining
// buffer size (using minElemSize, each element's minimum encoded size) before
// the caller allocates anything sized by it, so a corrupted count can't
// trigger a huge allocation.
func (r *binReader) count(minElemSize int) (uint32, error) {
	n, err := r.uint32()
	if err != nil {
		return 0, err
	}
	remaining := len(r.b) - r.off
	if uint64(n) > uint64(remaining)/uint64(minElemSize) {
		return 0, errShortBuffer
	}
	return n, nil
}

// bytes reads a length-prefixed byte slice (uint32 length + that many bytes).
// The returned slice ALIASES r.b rather than copying, which is safe only
// because frame payload buffers are never pooled or reused.
func (r *binReader) bytes() ([]byte, error) {
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if n > maxFrameSize || r.off+int(n) > len(r.b) {
		return nil, errShortBuffer
	}
	v := r.b[r.off : r.off+int(n)]
	r.off += int(n)
	return v, nil
}

// string reads a length-prefixed string. Unlike bytes, this always copies
// (Go's string(b) conversion copies), which is correct since decoded keys are
// retained indefinitely in long-lived maps.
func (r *binReader) string() (string, error) {
	b, err := r.bytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// done reports whether the buffer was fully consumed; used as a final
// structural sanity check (rejects trailing garbage) at the end of each
// UnmarshalBinary.
func (r *binReader) done() bool {
	return r.off == len(r.b)
}
