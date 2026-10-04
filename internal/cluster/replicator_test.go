package cluster

import (
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// fakePeer is an in-process transport.Server standing in for a replica.
// It records every ForwardRequest it receives (in arrival order) and can be
// gated to simulate a slow peer, or made to always fail sends.
type fakePeer struct {
	addr    string
	server  *transport.Server
	gate    chan struct{} // handler blocks on this until closed, if non-nil
	fail    bool
	mu      sync.Mutex
	applied []transport.ForwardRequest
}

func newFakePeer(t *testing.T) *fakePeer {
	t.Helper()
	fp := &fakePeer{}
	srv, err := transport.NewServer("127.0.0.1:0", fp.handle, nil, slog.Default())
	if err != nil {
		t.Fatalf("newFakePeer: %v", err)
	}
	fp.server = srv
	fp.addr = srv.Addr().String()
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return fp
}

func (fp *fakePeer) handle(msgType transport.MsgType, payload []byte) ([]byte, error) {
	if fp.gate != nil {
		<-fp.gate
	}
	if fp.fail {
		return nil, errors.New("fakePeer: forced failure")
	}
	var batch transport.ForwardBatch
	if err := transport.Decode(payload, &batch); err != nil {
		return nil, err
	}
	fp.mu.Lock()
	fp.applied = append(fp.applied, batch.Requests...)
	fp.mu.Unlock()
	return nil, nil
}

func (fp *fakePeer) received() []transport.ForwardRequest {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	out := make([]transport.ForwardRequest, len(fp.applied))
	copy(out, fp.applied)
	return out
}

// waitForCond polls cond until it returns true or timeout elapses.
func waitForCond(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// clusterWithPeer builds a two-node test Cluster ("self") with an active
// replicator (RF=2) and a peer at addr, using small queue/batch sizes so
// backpressure and batching behavior are easy to exercise.
func clusterWithPeer(t *testing.T, nodeID, addr string, queueSize, batchSize int) *Cluster {
	t.Helper()
	m := newTestClusterRF(nodeID, 2)
	m.cfg.ReplicationQueueSize = queueSize
	m.cfg.ReplicationBatchSize = batchSize
	m.cfg.RoutingTimeout = 2 * time.Second
	m.replicator.stop()
	m.replicator = newReplicator(m)
	t.Cleanup(m.replicator.stop)
	if err := m.addPeer(psRF("peer", addr, NodeAlive, 1, 2)); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	return m
}

func TestReplicator_AppliesWritesInOrder(t *testing.T) {
	fp := newFakePeer(t)
	m := clusterWithPeer(t, "self", fp.addr, 512, 8)

	const n = 200
	for i := range n {
		m.replicator.enqueue("peer", transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte{byte(i)}}})
	}

	waitForCond(t, 2*time.Second, "all writes applied", func() bool {
		return len(fp.received()) == n
	})

	got := fp.received()
	for i, req := range got {
		if len(req.Args) != 1 || req.Args[0][0] != byte(i) {
			t.Fatalf("write %d applied out of order: got arg %v, want %d", i, req.Args, i)
		}
	}
}

// fillStuckPeer queues writes for a peer whose handler is gated, until the
// next enqueue must block: one batch in flight, a full peer queue and a full
// jobs channel (queue size 1). It returns a channel closed once that blocked
// enqueue returns.
func fillStuckPeer(t *testing.T, m *Cluster) chan struct{} {
	t.Helper()
	req := func(i int) transport.ForwardRequest {
		return transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte{byte(i)}}}
	}
	m.replicator.enqueue("peer", req(1))
	waitForCond(t, time.Second, "first batch in flight", func() bool {
		return len(m.replicator.jobs) == 0
	})
	m.replicator.enqueue("peer", req(2))
	m.replicator.enqueue("peer", req(3))

	done := make(chan struct{})
	go func() {
		m.replicator.enqueue("peer", req(4))
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("enqueue should block while the peer queue is full and the peer is stuck")
	case <-time.After(150 * time.Millisecond):
	}
	return done
}

func TestReplicator_Backpressure_BlocksWhenQueueFull(t *testing.T) {
	fp := newFakePeer(t)
	fp.gate = make(chan struct{})
	m := clusterWithPeer(t, "self", fp.addr, 1, 1)
	done := fillStuckPeer(t, m)

	close(fp.gate)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue should unblock once the queue drains")
	}
	waitForCond(t, 2*time.Second, "all writes applied", func() bool {
		return len(fp.received()) == 4
	})
}

func TestReplicator_SendFailure_MarksDeadAndReleasesBlocked(t *testing.T) {
	fp := newFakePeer(t)
	fp.gate = make(chan struct{})
	fp.fail = true
	m := clusterWithPeer(t, "self", fp.addr, 1, 1)
	done := fillStuckPeer(t, m)

	close(fp.gate)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue should be released once the peer is marked dead")
	}
	if status, _ := m.peerStatus("peer"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", status)
	}
	if _, ok := m.getClient("peer"); ok {
		t.Error("client should be removed once the peer is marked dead")
	}
}

func TestReplicator_StuckPeerDoesNotStallOthers(t *testing.T) {
	stuck := newFakePeer(t)
	stuck.gate = make(chan struct{})
	t.Cleanup(func() { close(stuck.gate) })
	fp := newFakePeer(t)
	m := clusterWithPeer(t, "self", stuck.addr, 64, 4)
	if err := m.addPeer(psRF("other", fp.addr, NodeAlive, 1, 2)); err != nil {
		t.Fatalf("addPeer: %v", err)
	}

	req := transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte("v")}}
	m.replicator.enqueue("peer", req)
	for range 10 {
		m.replicator.enqueue("other", req)
	}

	waitForCond(t, time.Second, "other peer receives writes while peer is stuck", func() bool {
		return len(fp.received()) == 10
	})
}

func TestFanOutReplicas_LocalTarget_ExecutesSynchronously(t *testing.T) {
	m := newTestCluster("self")
	def := opRegistry[transport.OpValueSet]
	req := transport.ForwardRequest{Op: transport.OpValueSet, Key: "local-key", Args: [][]byte{[]byte("v")}}

	m.fanOutReplicas(def, req, []string{"self"})

	if _, ok := m.store.Get("local-key"); !ok {
		t.Error("local replica target should be applied synchronously by fanOutReplicas")
	}
}

func TestFanOutReplicas_RemoteTarget_ReachesPeer(t *testing.T) {
	fp := newFakePeer(t)
	m := clusterWithPeer(t, "self", fp.addr, 64, 16)
	def := opRegistry[transport.OpValueSet]
	req := transport.ForwardRequest{Op: transport.OpValueSet, Key: "remote-key", Args: [][]byte{[]byte("v")}}

	m.fanOutReplicas(def, req, []string{"peer"})

	waitForCond(t, 2*time.Second, "remote peer receives the write", func() bool {
		return len(fp.received()) == 1
	})
}
