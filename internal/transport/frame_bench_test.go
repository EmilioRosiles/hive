package transport

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// BenchmarkFrame_WriteRead_Binary/_Msgpack compare the fixed binary header
// (WriteFrame/ReadFrame) against msgpack-encoding the Frame struct directly —
// i.e. how every frame was framed before the binary header existed. Frame
// itself has no MarshalBinary method, so BenchmarkFrame_WriteRead_Msgpack is
// unaffected by the auto-detection caveat noted in forward_bench_test.go.
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
