package transport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
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

	client := NewClient(s.Addr().String(), nil, 1, slog.Default())
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

	client := NewClient(s.Addr().String(), nil, 1, slog.Default())
	defer client.Close()

	_, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("got %v, want an error wrapping ErrRejected", err)
	}
	if !strings.Contains(err.Error(), errBoom.Error()) {
		t.Errorf("got %q, want it to contain %q", err.Error(), errBoom.Error())
	}
}

func TestClient_ConcurrentSends_MultiplexedOverOneConnection(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) {
		return payload, nil // echo, so each response can be matched to its request
	}
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), nil, 1, slog.Default())
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
	client := NewClient(s.Addr().String(), nil, poolSize, slog.Default())
	defer client.Close()

	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	seen := make(map[*mux]bool)
	for i := range client.muxes {
		m := client.muxes[i].Load()
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
	client := NewClient(s.Addr().String(), nil, poolSize, slog.Default())
	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	muxes := make([]*mux, poolSize)
	for i := range client.muxes {
		muxes[i] = client.muxes[i].Load()
	}

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
	client := NewClient(s.Addr().String(), nil, poolSize, slog.Default())
	defer client.Close()

	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	live := client.muxes[1].Load()
	dead := client.muxes[0].Load()
	dead.shutdown(nil) // simulate slot 0's connection dying

	for i := 0; i < poolSize; i++ {
		if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send after slot 0 died: %v", err)
		}
	}

	if client.muxes[0].Load() == dead {
		t.Error("slot 0: still pointing at the dead mux, want a fresh redialed one")
	}
	if got := client.muxes[0].Load(); got == nil || got.closed() {
		t.Error("slot 0: expected a live redialed connection")
	}
	if client.muxes[1].Load() != live {
		t.Error("slot 1: should be untouched by slot 0's reconnect")
	}
}

func TestClient_Pool_SendAfterClose_DoesNotRedial(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) { return payload, nil }
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), nil, 1, slog.Default())
	if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	client.Close()

	_, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")})
	if !errors.Is(err, ErrUnsent) {
		t.Errorf("got %v, want an error wrapping ErrUnsent", err)
	}
	if m := client.muxes[0].Load(); m != nil {
		t.Error("Send after Close dialed a new connection")
	}
}

func TestClient_Send_DialFailure_WrapsErrUnsent(t *testing.T) {
	client := NewClient("127.0.0.1:1", nil, 1, slog.Default()) // nothing listens on port 1
	defer client.Close()

	_, err := client.Send(context.Background(), Frame{Type: MsgForward})
	if !errors.Is(err, ErrUnsent) {
		t.Errorf("got %v, want an error wrapping ErrUnsent", err)
	}
}

// The peer drops its first connection after reading the request and answers on
// any later one, so a retry would surface as a successful Send.
func TestClient_Send_ClosedAfterWrite_NotRetried(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var conns atomic.Int32
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			first := conns.Add(1) == 1
			go func() {
				defer nc.Close()
				for {
					f, err := ReadFrame(nc)
					if err != nil || first {
						return
					}
					if err := WriteFrame(nc, Frame{ID: f.ID, Type: f.Type}); err != nil {
						return
					}
				}
			}()
		}
	}()

	client := NewClient(ln.Addr().String(), nil, 1, slog.Default())
	defer client.Close()

	_, err = client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")})
	if err == nil {
		t.Fatal("Send succeeded, want an error: the request was resent after the peer read it")
	}
	if !errors.Is(err, ErrMuxClosed) || errors.Is(err, ErrUnsent) {
		t.Errorf("got %v, want ErrMuxClosed without ErrUnsent", err)
	}
	if errors.Is(err, ErrRejected) {
		t.Errorf("got %v, a lost connection must not be reported as a rejection", err)
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

	client := NewClient(s.Addr().String(), nil, 1, slog.Default())
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
