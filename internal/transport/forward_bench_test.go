package transport

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// These benchmarks compare the hand-written binary codec against
// msgpack.Marshal/Unmarshal called directly on the same values. Run with:
//
//	go test ./internal/transport/... -bench=. -benchmem
//
// Caveat: msgpack v5 auto-detects encoding.BinaryMarshaler/BinaryUnmarshaler
// (see marshalBinaryValue in its encode_value.go) and, once ForwardRequest/
// ForwardResponse/RebalanceBatch implement those interfaces, silently
// delegates to MarshalBinary/UnmarshalBinary and wraps the result in a
// msgpack bin header instead of doing full reflection-based struct encoding.
// So the *Msgpack benchmarks here measure "MarshalBinary + a bin wrapper",
// not the true pre-binary-header baseline (plain struct-field reflection) —
// they're still real numbers (e.g. for any other code path that calls
// msgpack.Marshal generically on these types), just not an apples-to-apples
// "what if we'd never added the binary codec" comparison. That comparison
// requires benchmarking a commit before these MarshalBinary methods existed.

func benchForwardRequest() ForwardRequest {
	return ForwardRequest{
		Op:   OpHSet,
		Key:  "bench-key",
		Args: [][]byte{[]byte("field"), []byte("value"), make([]byte, 8)},
	}
}

func BenchmarkForwardRequest_MarshalBinary(b *testing.B) {
	req := benchForwardRequest()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := req.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardRequest_MarshalMsgpack(b *testing.B) {
	req := benchForwardRequest()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := msgpack.Marshal(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardRequest_UnmarshalBinary(b *testing.B) {
	data, _ := benchForwardRequest().MarshalBinary()
	b.ReportAllocs()
	for b.Loop() {
		var req ForwardRequest
		if err := req.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardRequest_UnmarshalMsgpack(b *testing.B) {
	data, _ := msgpack.Marshal(benchForwardRequest())
	b.ReportAllocs()
	for b.Loop() {
		var req ForwardRequest
		if err := msgpack.Unmarshal(data, &req); err != nil {
			b.Fatal(err)
		}
	}
}

func benchForwardResponse() ForwardResponse {
	return ForwardResponse{Results: [][]byte{[]byte("result-value")}}
}

func BenchmarkForwardResponse_MarshalBinary(b *testing.B) {
	resp := benchForwardResponse()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := resp.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardResponse_MarshalMsgpack(b *testing.B) {
	resp := benchForwardResponse()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := msgpack.Marshal(resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardResponse_UnmarshalBinary(b *testing.B) {
	data, _ := benchForwardResponse().MarshalBinary()
	b.ReportAllocs()
	for b.Loop() {
		var resp ForwardResponse
		if err := resp.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardResponse_UnmarshalMsgpack(b *testing.B) {
	data, _ := msgpack.Marshal(benchForwardResponse())
	b.ReportAllocs()
	for b.Loop() {
		var resp ForwardResponse
		if err := msgpack.Unmarshal(data, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

func benchForwardBatch() ForwardBatch {
	requests := make([]ForwardRequest, 16)
	for i := range requests {
		requests[i] = benchForwardRequest()
	}
	return ForwardBatch{Requests: requests}
}

func BenchmarkForwardBatch_MarshalBinary(b *testing.B) {
	batch := benchForwardBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := batch.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardBatch_MarshalMsgpack(b *testing.B) {
	batch := benchForwardBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := msgpack.Marshal(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardBatch_UnmarshalBinary(b *testing.B) {
	data, _ := benchForwardBatch().MarshalBinary()
	b.ReportAllocs()
	for b.Loop() {
		var batch ForwardBatch
		if err := batch.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkForwardBatch_UnmarshalMsgpack(b *testing.B) {
	data, _ := msgpack.Marshal(benchForwardBatch())
	b.ReportAllocs()
	for b.Loop() {
		var batch ForwardBatch
		if err := msgpack.Unmarshal(data, &batch); err != nil {
			b.Fatal(err)
		}
	}
}
