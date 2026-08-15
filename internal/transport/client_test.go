package transport

import (
	"context"
	"errors"
	"testing"
)

// These tests exercise the full Client -> mux -> Server stack (as opposed to
// server_test.go's rawRoundTrip helper, which talks to the server directly to
// isolate its framing from the client-side mux).

func TestClient_RoundTrip_MsgForward(t *testing.T) {
	req := ForwardRequest{Op: OpHSet, Key: "hash", Args: [][]byte{[]byte("field"), []byte("value")}}
	reqPayload, err := req.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	respWant := ForwardResponse{Results: [][]byte{[]byte("done")}}
	respPayload, _ := respWant.MarshalBinary()

	handler, received := echoHandler(t, map[MsgType][]byte{MsgForward: respPayload})
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), nil, 1)
	defer client.Close()

	frame, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: reqPayload})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	var got ForwardResponse
	if err := got.UnmarshalBinary(frame.Payload); err != nil {
		t.Fatalf("UnmarshalBinary response: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0]) != "done" {
		t.Errorf("got %+v, want Results=[done]", got)
	}
	if recv := received(); len(recv) != 1 {
		t.Fatalf("handler invoked %d times, want 1", len(recv))
	}
}

func TestClient_HandlerError_ReturnsErrRejected(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) {
		return nil, errBoom
	}
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), nil, 1)
	defer client.Close()

	_, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")})
	if err == nil {
		t.Fatal("expected error")
	}
	var rejected *ErrRejected
	if !errors.As(err, &rejected) {
		t.Fatalf("got %v (%T), want *ErrRejected", err, err)
	}
	if rejected.Error() != errBoom.Error() {
		t.Errorf("got %q, want %q", rejected.Error(), errBoom.Error())
	}
}

func TestClient_ConcurrentSends_MultiplexedOverOneConnection(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) {
		return payload, nil // echo, so each response can be matched to its request
	}
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), nil, 1)
	defer client.Close()

	const n = 100
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			req := ForwardRequest{Op: OpValueGet, Key: string(rune('a' + i%26))}
			payload, _ := req.MarshalBinary()
			frame, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: payload})
			if err != nil {
				errCh <- err
				return
			}
			var got ForwardRequest
			if err := got.UnmarshalBinary(frame.Payload); err != nil {
				errCh <- err
				return
			}
			if got.Key != req.Key {
				errCh <- errMismatch
				return
			}
			errCh <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent send failed: %v", err)
		}
	}
}

func TestClient_Pool_RoundRobinAcrossDistinctConnections(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) { return payload, nil }
	s := startTestServer(t, handler)

	const poolSize = 3
	client := NewClient(s.Addr().String(), nil, poolSize)
	defer client.Close()

	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	seen := make(map[*mux]bool)
	for i, m := range client.muxes {
		if m == nil {
			t.Fatalf("slot %d: never dialed", i)
		}
		if seen[m] {
			t.Fatalf("slot %d: mux reused from another slot, want %d distinct connections", i, poolSize)
		}
		seen[m] = true
	}
}

func TestClient_Pool_CloseClosesAllConnections(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) { return payload, nil }
	s := startTestServer(t, handler)

	const poolSize = 3
	client := NewClient(s.Addr().String(), nil, poolSize)
	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	muxes := make([]*mux, poolSize)
	copy(muxes, client.muxes)

	client.Close()

	for i, m := range muxes {
		if !m.closed() {
			t.Errorf("slot %d: mux still open after Close", i)
		}
	}
}

func TestClient_Pool_ReconnectsOnlyDeadSlot(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) { return payload, nil }
	s := startTestServer(t, handler)

	const poolSize = 2
	client := NewClient(s.Addr().String(), nil, poolSize)
	defer client.Close()

	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	live := client.muxes[1]
	dead := client.muxes[0]
	dead.shutdown(nil) // simulate slot 0's connection dying

	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send after slot 0 died: %v", err)
		}
	}

	if client.muxes[0] == dead {
		t.Error("slot 0: still pointing at the dead mux, want a fresh redialed one")
	}
	if client.muxes[0] == nil || client.muxes[0].closed() {
		t.Error("slot 0: expected a live redialed connection")
	}
	if client.muxes[1] != live {
		t.Error("slot 1: should be untouched by slot 0's reconnect")
	}
}

func TestClient_RoundTrip_MsgRebalance(t *testing.T) {
	batch := RebalanceBatch{Entries: []RebalanceEntry{
		{Key: "k1", Kind: 1, Data: []byte("d1")},
	}}
	payload, err := batch.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	handler, received := echoHandler(t, nil)
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), nil, 1)
	defer client.Close()

	frame, err := client.Send(context.Background(), Frame{Type: MsgRebalance, Payload: payload})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(frame.Payload) != 0 || frame.Err != "" {
		t.Errorf("got %+v, want empty response", frame)
	}
	if recv := received(); len(recv) != 1 {
		t.Fatalf("handler invoked %d times, want 1", len(recv))
	}
}
