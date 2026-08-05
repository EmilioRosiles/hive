package transport

import (
	"bytes"
	"testing"
)

func FuzzReadFrame(f *testing.F) {
	var buf bytes.Buffer
	WriteFrame(&buf, Frame{ID: 1, Type: MsgForward, Payload: []byte("hello")})
	f.Add(buf.Bytes())

	buf.Reset()
	WriteFrame(&buf, Frame{ID: 2, Type: MsgRebalance, Err: "boom"})
	f.Add(buf.Bytes())

	f.Add([]byte("garbage"))
	f.Add([]byte(""))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Only requirement: must not panic, regardless of input.
		ReadFrame(bytes.NewReader(data))
	})
}
