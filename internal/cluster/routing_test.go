package cluster

import (
	"testing"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// dispatch is always local in these tests because the cluster has only one node
// (self), so responsibleNodes always returns ["self"].

func TestDispatch_Write_ExecutesLocally(t *testing.T) {
	m := newTestCluster("self")

	_, err := m.dispatch(transport.OpValueSet, "key", []byte("value"))
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

	results, err := m.dispatch(transport.OpValueGet, "key")
	if err != nil {
		t.Fatalf("dispatch read: %v", err)
	}
	if len(results) == 0 || string(results[0]) != "hello" {
		t.Errorf("dispatch read: got %v, want hello", results)
	}
}

func TestDispatch_Read_MissingKey_ReturnsNotFound(t *testing.T) {
	m := newTestCluster("self")

	_, err := m.dispatch(transport.OpValueGet, "missing")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDispatch_Del_RemovesKey(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("key", newValueStructure(t, []byte("v")))

	if _, err := m.dispatch(transport.OpDel, "key"); err != nil {
		t.Fatalf("dispatch del: %v", err)
	}
	if _, ok := m.store.Get("key"); ok {
		t.Error("key should be gone after del dispatch")
	}
}

func TestDispatch_UnknownOp_ReturnsError(t *testing.T) {
	m := newTestCluster("self")
	if _, err := m.dispatch(transport.Op(255), "key"); err == nil {
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
