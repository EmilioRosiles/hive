package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var errShortBuffer = errors.New("transport: truncated binary payload")

// binReader is a small bounds-checked cursor over a byte slice, shared by the
// hand-written binary codecs for ForwardRequest, ForwardResponse, and
// RebalanceBatch. Every read method returns an error instead of panicking on
// truncated/malformed input.
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
// buffer size (using minElemSize, each element's minimum possible encoded
// size) BEFORE the caller allocates anything sized by it. This is the guard
// that prevents a single corrupted/malformed count field from triggering a
// multi-gigabyte allocation. minElemSize must be > 0.
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
// The returned slice ALIASES r.b rather than copying — safe because every
// frame payload buffer (see ReadFrame) is freshly allocated per frame and
// never pooled/reused. SAFETY: if frame payload buffers are ever pooled, this
// must switch to copying.
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

// -- ForwardRequest --
//
// Wire layout (all integers big-endian):
//
//	Op       uint8
//	KeyLen   uint32
//	Key      [KeyLen]byte
//	ArgCount uint32
//	repeated ArgCount times:
//	  ArgLen uint32
//	  Arg    [ArgLen]byte

func (r ForwardRequest) MarshalBinary() ([]byte, error) {
	size := 1 + 4 + len(r.Key) + 4
	for _, a := range r.Args {
		size += 4 + len(a)
	}
	buf := make([]byte, size)
	i := 0
	buf[i] = byte(r.Op)
	i++
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Key)))
	i += 4
	i += copy(buf[i:], r.Key)
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Args)))
	i += 4
	for _, a := range r.Args {
		binary.BigEndian.PutUint32(buf[i:], uint32(len(a)))
		i += 4
		i += copy(buf[i:], a)
	}
	return buf, nil
}

func (r *ForwardRequest) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	op, err := br.uint8()
	if err != nil {
		return fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	key, err := br.string()
	if err != nil {
		return fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	argCount, err := br.count(4) // each arg needs at least its own 4-byte length prefix
	if err != nil {
		return fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	args := make([][]byte, 0, argCount)
	for i := uint32(0); i < argCount; i++ {
		a, err := br.bytes()
		if err != nil {
			return fmt.Errorf("transport: decode ForwardRequest: %w", err)
		}
		args = append(args, a)
	}
	if !br.done() {
		return fmt.Errorf("transport: decode ForwardRequest: trailing data")
	}
	r.Op = Op(op)
	r.Key = key
	r.Args = args
	return nil
}

// -- ForwardResponse --
//
// Wire layout:
//
//	ResultCount uint32
//	repeated ResultCount times:
//	  ResultLen uint32
//	  Result    [ResultLen]byte

func (r ForwardResponse) MarshalBinary() ([]byte, error) {
	size := 4
	for _, res := range r.Results {
		size += 4 + len(res)
	}
	buf := make([]byte, size)
	i := 0
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Results)))
	i += 4
	for _, res := range r.Results {
		binary.BigEndian.PutUint32(buf[i:], uint32(len(res)))
		i += 4
		i += copy(buf[i:], res)
	}
	return buf, nil
}

func (r *ForwardResponse) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	count, err := br.count(4)
	if err != nil {
		return fmt.Errorf("transport: decode ForwardResponse: %w", err)
	}
	results := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		res, err := br.bytes()
		if err != nil {
			return fmt.Errorf("transport: decode ForwardResponse: %w", err)
		}
		results = append(results, res)
	}
	if !br.done() {
		return fmt.Errorf("transport: decode ForwardResponse: trailing data")
	}
	r.Results = results
	return nil
}

// -- RebalanceBatch / RebalanceEntry --
//
// RebalanceEntry never travels alone on the wire, only nested in a batch, so
// it gets no MarshalBinary/UnmarshalBinary of its own — just the inline
// layout below, encoded/decoded directly by RebalanceBatch's methods.
//
// Wire layout:
//
//	EntryCount uint32
//	repeated EntryCount times:
//	  KeyLen  uint32
//	  Key     [KeyLen]byte
//	  Kind    uint8
//	  DataLen uint32
//	  Data    [DataLen]byte
//	  TTL     int64

// rebalanceEntryMinSize is each entry's minimum possible encoded size
// (KeyLen + Kind + DataLen + TTL, with zero-length Key/Data) — used to bound
// EntryCount against the remaining buffer before allocating the entries slice.
const rebalanceEntryMinSize = 4 + 1 + 4 + 8

func (b RebalanceBatch) MarshalBinary() ([]byte, error) {
	size := 4
	for _, e := range b.Entries {
		size += 4 + len(e.Key) + 1 + 4 + len(e.Data) + 8
	}
	buf := make([]byte, size)
	i := 0
	binary.BigEndian.PutUint32(buf[i:], uint32(len(b.Entries)))
	i += 4
	for _, e := range b.Entries {
		binary.BigEndian.PutUint32(buf[i:], uint32(len(e.Key)))
		i += 4
		i += copy(buf[i:], e.Key)
		buf[i] = e.Kind
		i++
		binary.BigEndian.PutUint32(buf[i:], uint32(len(e.Data)))
		i += 4
		i += copy(buf[i:], e.Data)
		binary.BigEndian.PutUint64(buf[i:], uint64(e.TTL))
		i += 8
	}
	return buf, nil
}

func (b *RebalanceBatch) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	count, err := br.count(rebalanceEntryMinSize)
	if err != nil {
		return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
	}
	entries := make([]RebalanceEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		key, err := br.string()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		kind, err := br.uint8()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		blob, err := br.bytes()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		ttl, err := br.int64()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		entries = append(entries, RebalanceEntry{Key: key, Kind: kind, Data: blob, TTL: ttl})
	}
	if !br.done() {
		return fmt.Errorf("transport: decode RebalanceBatch: trailing data")
	}
	b.Entries = entries
	return nil
}
