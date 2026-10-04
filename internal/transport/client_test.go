package transport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

	client := NewClient(s.Addr().String(), "", nil, nil, 1, slog.Default())
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

	client := NewClient(s.Addr().String(), "", nil, nil, 1, slog.Default())
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

	client := NewClient(s.Addr().String(), "", nil, nil, 1, slog.Default())
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

// liveConns counts c's open connections.
func liveConns(c *Client) int {
	n := 0
	for i := range c.conns {
		if cn := c.conns[i].Load(); cn != nil && !cn.closed() {
			n++
		}
	}
	return n
}

// fillPool dials connections into every free slot of c.
func fillPool(t *testing.T, c *Client) []*conn {
	t.Helper()
	for range c.conns {
		if _, err := c.grow(false); err != nil {
			t.Fatalf("grow: %v", err)
		}
	}
	conns := make([]*conn, len(c.conns))
	for i := range c.conns {
		conns[i] = c.conns[i].Load()
	}
	return conns
}

func TestClient_Pool_SequentialSendsUseOneConnection(t *testing.T) {
	s := startTestServer(t, func(_ MsgType, payload []byte) ([]byte, error) { return payload, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 3, slog.Default())
	defer client.Close()

	for range 10 {
		if _, err := client.Send(t.Context(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	if n := liveConns(client); n != 1 {
		t.Errorf("%d connections for sequential sends, want 1", n)
	}
}

func TestClient_Pool_ColdStartDialsOnce(t *testing.T) {
	s := startTestServer(t, func(_ MsgType, payload []byte) ([]byte, error) { return payload, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 3, slog.Default())
	defer client.Close()

	client.mu.Lock()
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.pick(); err != nil {
				t.Errorf("pick: %v", err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	client.mu.Unlock()
	wg.Wait()

	if n := liveConns(client); n != 1 {
		t.Errorf("%d connections dialed by concurrent callers on a cold pool, want 1", n)
	}
}

func TestClient_Pool_GrowsWhileBusy_UpToPoolSize(t *testing.T) {
	gate := make(chan struct{})
	s := startTestServer(t, func(MsgType, []byte) ([]byte, error) {
		<-gate
		return nil, nil
	})
	const poolSize = 3
	client := NewClient(s.Addr().String(), "", nil, nil, poolSize, slog.Default())
	defer client.Close()

	errs := make(chan error, 2*poolSize)
	for range 2 * poolSize {
		go func() {
			_, err := client.Send(t.Context(), Frame{Type: MsgForward})
			errs <- err
		}()
		time.Sleep(20 * time.Millisecond)
	}
	inflight := func() (total int32) {
		for i := range client.conns {
			if cn := client.conns[i].Load(); cn != nil {
				total += cn.inflight.Load()
			}
		}
		return total
	}
	deadline := time.Now().Add(2 * time.Second)
	for inflight() < 2*poolSize && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := liveConns(client); n != poolSize {
		t.Errorf("%d connections while every one is busy, want %d", n, poolSize)
	}
	for i := range client.conns {
		if cn := client.conns[i].Load(); cn == nil || cn.inflight.Load() == 0 {
			t.Errorf("slot %d carries no request: sends should go to the least busy connection", i)
		}
	}
	close(gate)
	for range 2 * poolSize {
		if err := <-errs; err != nil {
			t.Errorf("Send: %v", err)
		}
	}
}

func TestClient_Pool_CloseClosesAllConnections(t *testing.T) {
	s := startTestServer(t, func(_ MsgType, payload []byte) ([]byte, error) { return payload, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 3, slog.Default())
	conns := fillPool(t, client)

	client.Close()

	for i, cn := range conns {
		if !cn.closed() {
			t.Errorf("slot %d: connection still open after Close", i)
		}
	}
}

func TestClient_Pool_DeadConnReplaced(t *testing.T) {
	s := startTestServer(t, func(_ MsgType, payload []byte) ([]byte, error) { return payload, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 2, slog.Default())
	defer client.Close()
	if _, err := client.Send(t.Context(), Frame{Type: MsgForward}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	dead := client.conns[0].Load()
	dead.shutdown(nil)

	if _, err := client.Send(t.Context(), Frame{Type: MsgForward}); err != nil {
		t.Fatalf("Send after the connection died: %v", err)
	}
	if n := liveConns(client); n != 1 {
		t.Errorf("%d live connections, want 1 replacing the dead one", n)
	}
}

func TestConn_ServedRequestCountsAsUse(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	cn := newConn(a, func(MsgType, []byte) ([]byte, error) { return nil, nil }, slog.Default())
	go cn.readLoop()
	defer cn.shutdown(nil)

	if err := WriteFrame(b, Frame{ID: 1, Type: MsgForward}); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := ReadFrame(b); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got := cn.ops.Load(); got != 1 {
		t.Errorf("ops = %d after serving one request, want 1", got)
	}
}

func TestClient_Reap_ClosesIdleKeepsOne(t *testing.T) {
	s := startTestServer(t, func(_ MsgType, payload []byte) ([]byte, error) { return payload, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 3, slog.Default())
	defer client.Close()
	fillPool(t, client)

	client.Reap()

	if n := liveConns(client); n != 1 {
		t.Errorf("%d connections after reaping an idle pool, want 1", n)
	}
}

func TestClient_Reap_KeepsUsedAndBusyConns(t *testing.T) {
	s := startTestServer(t, func(_ MsgType, payload []byte) ([]byte, error) { return payload, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 3, slog.Default())
	defer client.Close()
	conns := fillPool(t, client)
	if _, err := conns[1].send(t.Context(), Frame{Type: MsgForward}); err != nil {
		t.Fatalf("send: %v", err)
	}
	conns[2].inflight.Add(1)
	defer conns[2].inflight.Add(-1)

	client.Reap()

	if !conns[0].closed() {
		t.Error("an idle connection was kept")
	}
	if conns[1].closed() {
		t.Error("a connection used since the last Reap was closed")
	}
	if conns[2].closed() {
		t.Error("a connection with a request in flight was closed")
	}
}

func TestClient_Pool_SendAfterClose_DoesNotRedial(t *testing.T) {
	handler := func(msgType MsgType, payload []byte) ([]byte, error) { return payload, nil }
	s := startTestServer(t, handler)

	client := NewClient(s.Addr().String(), "", nil, nil, 1, slog.Default())
	if _, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	client.Close()

	_, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("x")})
	if !errors.Is(err, ErrUnsent) {
		t.Errorf("got %v, want an error wrapping ErrUnsent", err)
	}
	if m := client.conns[0].Load(); m != nil {
		t.Error("Send after Close dialed a new connection")
	}
}

func TestClient_Send_DialFailure_WrapsErrUnsent(t *testing.T) {
	client := NewClient("127.0.0.1:1", "", nil, nil, 1, slog.Default()) // nothing listens on port 1
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

	client := NewClient(ln.Addr().String(), "", nil, nil, 1, slog.Default())
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

	client := NewClient(s.Addr().String(), "", nil, nil, 1, slog.Default())
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

// TestServer_AdoptsHelloConn_PeerSendsBackOverIt checks that once A dials B,
// B's Client for A sends over that connection instead of dialing, and A's
// connection serves B's request.
func TestServer_AdoptsHelloConn_PeerSendsBackOverIt(t *testing.T) {
	clientB := NewClient("127.0.0.1:1", "B", nil, nil, 1, slog.Default()) // dialing A would fail
	defer clientB.Close()
	srv, err := NewServer("127.0.0.1:0", func(MsgType, []byte) ([]byte, error) { return []byte("from B"), nil },
		func(nodeID string) (*Client, bool) { return clientB, nodeID == "A" }, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	clientA := NewClient(srv.Addr().String(), "A", func(MsgType, []byte) ([]byte, error) { return []byte("from A"), nil }, nil, 1, slog.Default())
	defer clientA.Close()
	if resp, err := clientA.Send(t.Context(), Frame{Type: MsgForward}); err != nil || string(resp.Payload) != "from B" {
		t.Fatalf("A -> B: got %q, %v", resp.Payload, err)
	}
	waitAdopted(t, clientB)

	resp, err := clientB.Send(t.Context(), Frame{Type: MsgForward})
	if err != nil || string(resp.Payload) != "from A" {
		t.Fatalf("B -> A over the adopted connection: got %q, %v", resp.Payload, err)
	}
}

// waitAdopted waits until c holds a connection in its first slot.
func waitAdopted(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.conns[0].Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("connection never adopted")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestServer_HelloWhileClientDials_StillServes checks that adopting a
// connection never blocks its read loop, even while the peer's Client is
// mid-dial and holds its lock.
func TestServer_HelloWhileClientDials_StillServes(t *testing.T) {
	clientB := NewClient("127.0.0.1:1", "B", nil, nil, 1, slog.Default())
	defer clientB.Close()
	srv, err := NewServer("127.0.0.1:0", func(MsgType, []byte) ([]byte, error) { return []byte("from B"), nil },
		func(nodeID string) (*Client, bool) { return clientB, nodeID == "A" }, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	clientB.mu.Lock()
	defer clientB.mu.Unlock()

	clientA := NewClient(srv.Addr().String(), "A", nil, nil, 1, slog.Default())
	defer clientA.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := clientA.Send(ctx, Frame{Type: MsgForward}); err != nil {
		t.Fatalf("A's request stalled behind adoption: %v", err)
	}
}

func TestClient_Adopt_FullPool_KeepsExistingConn(t *testing.T) {
	s := startTestServer(t, func(MsgType, []byte) ([]byte, error) { return nil, nil })
	client := NewClient(s.Addr().String(), "", nil, nil, 1, slog.Default())
	defer client.Close()
	if _, err := client.Send(t.Context(), Frame{Type: MsgForward}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	dialed := client.conns[0].Load()

	a, b := net.Pipe()
	defer b.Close()
	client.adopt(newConn(a, nil, slog.Default()))

	if client.conns[0].Load() != dialed {
		t.Error("adopt replaced a live connection in a full pool")
	}
}
