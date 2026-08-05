package transport

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func roundTripFrame(t *testing.T, f Frame) Frame {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, f); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return got
}

func TestFrame_RoundTrip_Payload(t *testing.T) {
	f := Frame{ID: 42, Type: MsgForward, Payload: []byte("hello")}
	got := roundTripFrame(t, f)

	if got.ID != f.ID || got.Type != f.Type || string(got.Payload) != string(f.Payload) || got.Err != "" {
		t.Errorf("got %+v, want %+v", got, f)
	}
}

func TestFrame_RoundTrip_EmptyPayload(t *testing.T) {
	f := Frame{ID: 1, Type: MsgHeartbeat}
	got := roundTripFrame(t, f)

	if len(got.Payload) != 0 || got.Err != "" {
		t.Errorf("got %+v, want empty payload/err", got)
	}
}

func TestFrame_RoundTrip_Err(t *testing.T) {
	f := Frame{ID: 7, Type: MsgRebalance, Err: "boom"}
	got := roundTripFrame(t, f)

	if got.Err != "boom" || len(got.Payload) != 0 {
		t.Errorf("got %+v, want Err=boom", got)
	}
}

func TestFrame_ErrWinsOverPayload(t *testing.T) {
	// A frame cannot carry both — Err takes priority and Payload is dropped.
	f := Frame{ID: 1, Type: MsgForward, Payload: []byte("ignored"), Err: "boom"}
	got := roundTripFrame(t, f)

	if got.Err != "boom" || len(got.Payload) != 0 {
		t.Errorf("got %+v, want Err=boom and empty payload", got)
	}
}

func TestFrame_RoundTrip_LargePayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 1<<20) // 1 MiB
	f := Frame{ID: 99, Type: MsgForward, Payload: payload}
	got := roundTripFrame(t, f)

	if !bytes.Equal(got.Payload, payload) {
		t.Error("large payload mismatch after round trip")
	}
}

func TestReadFrame_TruncatedHeader(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader([]byte{1, 2, 3}))
	if err == nil {
		t.Error("expected error for truncated header")
	}
}

func TestReadFrame_TruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	WriteFrame(&buf, Frame{ID: 1, Type: MsgForward, Payload: []byte("hello world")})
	truncated := buf.Bytes()[:frameHeaderSize+3] // header claims more than we provide

	_, err := ReadFrame(bytes.NewReader(truncated))
	if err == nil {
		t.Error("expected error for truncated payload")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Errorf("expected an EOF-family error, got %v", err)
	}
}

func TestReadFrame_OversizedPayloadLen_RejectedBeforeAlloc(t *testing.T) {
	var hdr [frameHeaderSize]byte
	// PayloadLen far exceeds maxFrameSize; must be rejected without attempting
	// to read/allocate that many bytes.
	hdr[0], hdr[1], hdr[2], hdr[3] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := ReadFrame(bytes.NewReader(hdr[:]))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestWriteFrame_OversizedPayload_Rejected(t *testing.T) {
	f := Frame{ID: 1, Type: MsgForward, Payload: make([]byte, maxFrameSize+1)}
	var buf bytes.Buffer
	err := WriteFrame(&buf, f)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrame_EOF_NoFrames(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Errorf("got %v, want io.EOF", err)
	}
}

func TestFrame_MultipleFramesOnStream(t *testing.T) {
	var buf bytes.Buffer
	WriteFrame(&buf, Frame{ID: 1, Type: MsgForward, Payload: []byte("a")})
	WriteFrame(&buf, Frame{ID: 2, Type: MsgRebalance, Payload: []byte("b")})

	f1, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame 1: %v", err)
	}
	f2, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame 2: %v", err)
	}
	if f1.ID != 1 || string(f1.Payload) != "a" || f2.ID != 2 || string(f2.Payload) != "b" {
		t.Errorf("got f1=%+v f2=%+v", f1, f2)
	}
}
