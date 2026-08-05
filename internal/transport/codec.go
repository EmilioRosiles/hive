package transport

import (
	"encoding"

	"github.com/vmihailenco/msgpack/v5"
)

// Encode serializes v for wire transmission. Types that implement
// encoding.BinaryMarshaler (ForwardRequest, ForwardResponse, RebalanceBatch —
// see binary.go) use their hand-written fixed-layout binary encoding, which
// avoids msgpack's reflection overhead on these high-frequency cluster-hop
// payloads; everything else (HeartbeatRequest/Response, LeaveRequest) falls
// back to msgpack.
func Encode(v any) ([]byte, error) {
	if bm, ok := v.(encoding.BinaryMarshaler); ok {
		return bm.MarshalBinary()
	}
	return msgpack.Marshal(v)
}

// Decode deserializes data into v, mirroring Encode's dispatch rule.
func Decode(data []byte, v any) error {
	if bu, ok := v.(encoding.BinaryUnmarshaler); ok {
		return bu.UnmarshalBinary(data)
	}
	return msgpack.Unmarshal(data, v)
}
