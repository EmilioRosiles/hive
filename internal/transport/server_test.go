package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"
)

// echoHandler builds a Handler that returns a canned response per MsgType,
// recording every (msgType, payload) it receives.
func echoHandler(t *testing.T, responses map[MsgType][]byte) (Handler, func() [][]byte) {
	t.Helper()
	var received [][]byte
	h := func(msgType MsgType, payload []byte) ([]byte, error) {
		received = append(received, payload)
		return responses[msgType], nil
	}
	return h, func() [][]byte { return received }
}

func startTestServer(t *testing.T, handler Handler) *Server {
	t.Helper()
	s, err := NewServer("127.0.0.1:0", handler, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go s.Serve()
	t.Cleanup(func() { s.Close() })
	return s
}

// rawRoundTrip dials addr directly (bypassing Client/mux) and writes+reads a
// single frame using WriteFrame/ReadFrame. This exercises the server's wire
// framing in isolation, independent of the client-side mux implementation.
// Only safe to call from the goroutine running the test itself — it uses
// t.Fatalf, which must not be called from a spawned goroutine (see
// rawRoundTripErr for that case).
func rawRoundTrip(t *testing.T, addr string, req Frame) Frame {
	t.Helper()
	resp, err := rawRoundTripErr(addr, req)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return resp
}

// rawRoundTripErr is rawRoundTrip without the t.Fatalf calls, so it's safe to
// use from spawned goroutines (Fatalf may only be called from the goroutine
// running the test — calling it elsewhere just terminates that goroutine
// silently, without reporting the failure).
func rawRoundTripErr(addr string, req Frame) (Frame, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return Frame{}, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	if err := WriteFrame(conn, req); err != nil {
		return Frame{}, fmt.Errorf("WriteFrame: %w", err)
	}
	resp, err := ReadFrame(conn)
	if err != nil {
		return Frame{}, fmt.Errorf("ReadFrame: %w", err)
	}
	return resp, nil
}

func TestServer_RoundTrip_MsgForward(t *testing.T) {
	req := ForwardRequest{Op: OpValueSet, Key: "k", Args: [][]byte{[]byte("v")}}
	reqPayload, err := req.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	respWant := ForwardResponse{Results: [][]byte{[]byte("ok")}}
	respPayload, _ := respWant.MarshalBinary()

	handler, received := echoHandler(t, map[MsgType][]byte{MsgForward: respPayload})
	s := startTestServer(t, handler)

	resp := rawRoundTrip(t, s.Addr().String(), Frame{ID: 1, Type: MsgForward, Payload: reqPayload})

	var got ForwardResponse
	if err := got.UnmarshalBinary(resp.Payload); err != nil {
		t.Fatalf("UnmarshalBinary response: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0]) != "ok" {
		t.Errorf("got %+v, want Results=[ok]", got)
	}

	recv := received()
	if len(recv) != 1 {
		t.Fatalf("handler invoked %d times, want 1", len(recv))
	}
	var gotReq ForwardRequest
	if err := gotReq.UnmarshalBinary(recv[0]); err != nil {
		t.Fatalf("UnmarshalBinary request: %v", err)
	}
	if gotReq.Op != req.Op || gotReq.Key != req.Key {
		t.Errorf("server received %+v, want %+v", gotReq, req)
	}
}

func TestServer_RoundTrip_MsgRebalance(t *testing.T) {
	batch := RebalanceBatch{Entries: []RebalanceEntry{
		{Key: "k1", Kind: 1, Data: []byte("d1"), TTL: 1000},
		{Key: "k2", Kind: 2, Data: []byte("d2"), TTL: 0},
	}}
	payload, err := batch.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	handler, received := echoHandler(t, nil) // nil response payload, like handleRebalance
	s := startTestServer(t, handler)

	resp := rawRoundTrip(t, s.Addr().String(), Frame{ID: 1, Type: MsgRebalance, Payload: payload})
	if len(resp.Payload) != 0 || resp.Err != "" {
		t.Errorf("got %+v, want empty response", resp)
	}

	recv := received()
	if len(recv) != 1 {
		t.Fatalf("handler invoked %d times, want 1", len(recv))
	}
	var gotBatch RebalanceBatch
	if err := gotBatch.UnmarshalBinary(recv[0]); err != nil {
		t.Fatalf("UnmarshalBinary batch: %v", err)
	}
	if len(gotBatch.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(gotBatch.Entries))
	}
	for i, want := range batch.Entries {
		got := gotBatch.Entries[i]
		if got.Key != want.Key || got.Kind != want.Kind || got.TTL != want.TTL || !bytes.Equal(got.Data, want.Data) {
			t.Errorf("entry %d: got %+v, want %+v", i, got, want)
		}
	}
}

func TestServer_HandlerError_PropagatesAsErrFrame(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) {
		return nil, errBoom
	}
	s := startTestServer(t, handler)

	resp := rawRoundTrip(t, s.Addr().String(), Frame{ID: 1, Type: MsgForward, Payload: []byte("x")})
	if resp.Err != errBoom.Error() {
		t.Errorf("got Err=%q, want %q", resp.Err, errBoom.Error())
	}
	if len(resp.Payload) != 0 {
		t.Errorf("got Payload=%v, want empty", resp.Payload)
	}
}

func TestServer_ConcurrentConnections_EachHandledIndependently(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) {
		// Echo the payload back so we can verify each response matches its request.
		return payload, nil
	}
	s := startTestServer(t, handler)

	const n = 50
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			req := ForwardRequest{Op: OpValueGet, Key: string(rune('a' + i%26))}
			payload, _ := req.MarshalBinary()
			resp, err := rawRoundTripErr(s.Addr().String(), Frame{ID: uint32(i), Type: MsgForward, Payload: payload})
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(resp.Payload, payload) {
				errCh <- errMismatch
				return
			}
			errCh <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent request failed: %v", err)
		}
	}
}

func TestServer_OversizedPayloadLen_ConnectionRejectedGracefully(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) { return nil, nil }
	s := startTestServer(t, handler)

	conn, err := net.DialTimeout("tcp", s.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var hdr [frameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], 0xFFFFFFFF) // declared payload len far exceeds maxFrameSize
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write crafted header: %v", err)
	}

	// The server must close the connection rather than hang or crash trying
	// to allocate/read an oversized payload.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Error("expected connection to be closed after oversized PayloadLen")
	}
}

var (
	errBoom     = errors.New("boom")
	errMismatch = errors.New("response payload did not match request")
)
