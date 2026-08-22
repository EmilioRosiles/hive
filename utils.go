package hive

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// encodeTTL encodes a TTL as a big-endian uint64 nanosecond byte slice.
// Returns nil for zero (no expiry) — the exec layer treats an absent slot as 0.
func encodeTTL(ttl time.Duration) []byte {
	if ttl == 0 {
		return nil
	}
	return binary.BigEndian.AppendUint64(nil, uint64(ttl.Nanoseconds()))
}

// encodeInt64 encodes n as an 8-byte big-endian slice.
func encodeInt64(n int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(n))
}

// decodeInt decodes a big-endian uint64 result slot and returns it as int.
func decodeInt(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}

// encodeFloat64 encodes a float64 as an 8-byte big-endian IEEE 754 slice.
func encodeFloat64(f float64) []byte {
	return binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
}

// decodeFloat64 decodes a float64 from an 8-byte big-endian IEEE 754 slice.
func decodeFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

// encodeUint32 encodes n as a 4-byte big-endian slice.
func encodeUint32(n uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, n)
}

// encode serializes v to msgpack bytes.
func encode[T any](v T) ([]byte, error) { return msgpack.Marshal(v) }

// decode deserializes msgpack bytes into T.
func decode[T any](b []byte) (T, error) {
	var v T
	return v, msgpack.Unmarshal(b, &v)
}
