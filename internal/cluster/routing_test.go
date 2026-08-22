package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// dispatch is always local in these tests because the cluster has only one node
// (self), so responsibleNodes always returns ["self"].

func TestDispatch_Write_ExecutesLocally(t *testing.T) {
	m := newTestCluster("self")

	_, err := m.dispatch(t.Context(), transport.OpValueSet, "key", []byte("value"))
	if err != nil {
		t.Fatalf("dispatch write: %v", err)
	}
	if _, ok := m.store.Get("key"); !ok {
		t.Error("key should be present after local write dispatch")
	}
}

func TestDispatch_Read_ExecutesLocally(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("key", newValueStructure(t, []byte("hello")))

	results, err := m.dispatch(t.Context(), transport.OpValueGet, "key")
	if err != nil {
		t.Fatalf("dispatch read: %v", err)
	}
	if len(results) == 0 || string(results[0]) != "hello" {
		t.Errorf("dispatch read: got %v, want hello", results)
	}
}

func TestDispatch_Read_MissingKey_ReturnsNotFound(t *testing.T) {
	m := newTestCluster("self")

	_, err := m.dispatch(t.Context(), transport.OpValueGet, "missing")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDispatch_Del_RemovesKey(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("key", newValueStructure(t, []byte("v")))

	if _, err := m.dispatch(t.Context(), transport.OpDel, "key"); err != nil {
		t.Fatalf("dispatch del: %v", err)
	}
	if _, ok := m.store.Get("key"); ok {
		t.Error("key should be gone after del dispatch")
	}
}

func TestDispatch_UnknownOp_ReturnsError(t *testing.T) {
	m := newTestCluster("self")
	if _, err := m.dispatch(t.Context(), transport.Op(255), "key"); err == nil {
		t.Error("unknown op should return error")
	}
}

// -- handleForward --

func TestHandleForward_ExecutesOp(t *testing.T) {
	m := newTestCluster("self")

	req := transport.ForwardRequest{
		Op:   transport.OpValueSet,
		Key:  "fwd-key",
		Args: [][]byte{[]byte("fwd-value")},
	}
	payload, err := transport.Encode(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if _, err := m.handleForward(payload); err != nil {
		t.Fatalf("handleForward: %v", err)
	}
	if _, ok := m.store.Get("fwd-key"); !ok {
		t.Error("fwd-key should be present after handleForward")
	}
}

func TestHandleForward_ReturnsResultForReadOp(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("fwd-key", newValueStructure(t, []byte("fwd-value")))

	req := transport.ForwardRequest{Op: transport.OpValueGet, Key: "fwd-key"}
	payload, _ := transport.Encode(req)

	respPayload, err := m.handleForward(payload)
	if err != nil {
		t.Fatalf("handleForward: %v", err)
	}

	var resp transport.ForwardResponse
	if err := transport.Decode(respPayload, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Results) == 0 || string(resp.Results[0]) != "fwd-value" {
		t.Errorf("got %v, want fwd-value", resp.Results)
	}
}

func TestHandleForward_InvalidPayload_ReturnsError(t *testing.T) {
	m := newTestCluster("self")
	if _, err := m.handleForward([]byte("garbage")); err == nil {
		t.Error("invalid payload should return error")
	}
}

func TestHandleForward_UnknownOp_ReturnsError(t *testing.T) {
	m := newTestCluster("self")

	req := transport.ForwardRequest{Op: transport.Op(255), Key: "k"}
	payload, _ := transport.Encode(req)

	if _, err := m.handleForward(payload); err == nil {
		t.Error("unknown op should return error")
	}
}

// -- execOrForward ctx/RoutingTimeout interaction --

// newForwardingTestCluster builds a two-node fixture ("self" + "peer") where
// self owns no vnodes, so every key's primary owner is "peer" — forcing
// dispatch/execOrForward down the forward branch instead of the local fast
// path. peer is a fake in-process transport.Server that blocks every
// MsgForward it receives on gate, standing in for an unresponsive remote
// primary so tests can assert on how long the caller actually waits.
func newForwardingTestCluster(t *testing.T, gate chan struct{}) *Cluster {
	t.Helper()
	srv, err := transport.NewServer("127.0.0.1:0", func(msgType transport.MsgType, payload []byte) ([]byte, error) {
		<-gate
		return nil, errors.New("newForwardingTestCluster: peer should never respond")
	}, nil)
	if err != nil {
		t.Fatalf("newForwardingTestCluster: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	m := newTestClusterRF("self", 1)
	if err := m.addPeer(psRF("peer", srv.Addr().String(), NodeAlive, 1, 1)); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	m.ring.Add("self", 0) // strip self's vnodes: every key now routes to "peer"
	return m
}

// TestExecOrForward_CallerDeadlineShorterThanRoutingTimeout_WinsOut proves
// that execOrForward derives its network-hop deadline from the incoming ctx
// (context.WithTimeout(ctx, RoutingTimeout)), so a caller's tighter deadline
// aborts the wait even when RoutingTimeout is much looser.
func TestExecOrForward_CallerDeadlineShorterThanRoutingTimeout_WinsOut(t *testing.T) {
	gate := make(chan struct{}) // never closed — peer never responds
	m := newForwardingTestCluster(t, gate)
	m.cfg.RoutingTimeout = 2 * time.Second // deliberately looser than the caller's ctx

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		_, err := m.dispatch(ctx, transport.OpValueGet, "key")
		done <- result{err: err, elapsed: time.Since(start)}
	}()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", r.err)
		}
		if r.elapsed > time.Second {
			t.Errorf("caller's 50ms deadline should have won over the 2s RoutingTimeout, took %v", r.elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not honor the caller's shorter deadline")
	}
}

// TestExecOrForward_BareBackgroundContext_StillBoundedByRoutingTimeout is a
// regression test for the pre-ctx behavior: a caller that doesn't set its own
// deadline (context.Background()) must still have the forward bounded by
// RoutingTimeout, not hang forever.
func TestExecOrForward_BareBackgroundContext_StillBoundedByRoutingTimeout(t *testing.T) {
	gate := make(chan struct{}) // never closed — peer never responds
	m := newForwardingTestCluster(t, gate)
	m.cfg.RoutingTimeout = 150 * time.Millisecond

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		_, err := m.dispatch(context.Background(), transport.OpValueGet, "key")
		done <- result{err: err, elapsed: time.Since(start)}
	}()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", r.err)
		}
		if r.elapsed < m.cfg.RoutingTimeout {
			t.Errorf("returned before RoutingTimeout elapsed: took %v, want >= %v", r.elapsed, m.cfg.RoutingTimeout)
		}
		if r.elapsed > time.Second {
			t.Errorf("a bare context.Background() should still be bounded by RoutingTimeout, took %v", r.elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a bare context.Background() must still be bounded by RoutingTimeout, not hang forever")
	}
}
