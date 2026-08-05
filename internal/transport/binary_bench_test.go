package transport

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// These benchmarks compare the hand-written binary codec (binary.go) directly
// against msgpack reflection-based marshaling for the same values, to
// substantiate the allocation/copy overhead this change removes from the
// MsgForward/MsgRebalance hot path. Run with:
//
//	go test ./internal/transport/... -bench=. -benchmem

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

// benchRebalanceBatch builds a 100-entry batch matching the real batchSize
// used by rebalance.go's sendRebalanceBatch.
func benchRebalanceBatch() RebalanceBatch {
	entries := make([]RebalanceEntry, 100)
	for i := range entries {
		entries[i] = RebalanceEntry{
			Key:  "key-000000",
			Kind: 1,
			Data: make([]byte, 64),
			TTL:  3600_000_000_000,
		}
	}
	return RebalanceBatch{Entries: entries}
}

func BenchmarkRebalanceBatch_MarshalBinary(b *testing.B) {
	batch := benchRebalanceBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := batch.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebalanceBatch_MarshalMsgpack(b *testing.B) {
	batch := benchRebalanceBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := msgpack.Marshal(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebalanceBatch_UnmarshalBinary(b *testing.B) {
	data, _ := benchRebalanceBatch().MarshalBinary()
	b.ReportAllocs()
	for b.Loop() {
		var batch RebalanceBatch
		if err := batch.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebalanceBatch_UnmarshalMsgpack(b *testing.B) {
	data, _ := msgpack.Marshal(benchRebalanceBatch())
	b.ReportAllocs()
	for b.Loop() {
		var batch RebalanceBatch
		if err := msgpack.Unmarshal(data, &batch); err != nil {
			b.Fatal(err)
		}
	}
}

// -- outer Frame envelope --

func BenchmarkFrame_WriteRead_Binary(b *testing.B) {
	payload, _ := benchForwardRequest().MarshalBinary()
	f := Frame{ID: 1, Type: MsgForward, Payload: payload}
	buf := make([]byte, 0, 256)
	w := &sliceWriter{buf: buf}
	b.ReportAllocs()
	for b.Loop() {
		w.buf = w.buf[:0]
		if err := WriteFrame(w, f); err != nil {
			b.Fatal(err)
		}
		if _, err := ReadFrame(&sliceReader{b: w.buf}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrame_WriteRead_Msgpack(b *testing.B) {
	payload, _ := benchForwardRequest().MarshalBinary()
	f := Frame{ID: 1, Type: MsgForward, Payload: payload}
	b.ReportAllocs()
	for b.Loop() {
		data, err := msgpack.Marshal(f)
		if err != nil {
			b.Fatal(err)
		}
		var got Frame
		if err := msgpack.Unmarshal(data, &got); err != nil {
			b.Fatal(err)
		}
	}
}

// sliceWriter/sliceReader are minimal io.Writer/io.Reader over a byte slice,
// avoiding bytes.Buffer's own allocation churn so these benchmarks isolate
// WriteFrame/ReadFrame's cost specifically.
type sliceWriter struct{ buf []byte }

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

type sliceReader struct {
	b   []byte
	off int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}
