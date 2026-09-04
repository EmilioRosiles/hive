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

// clusterWithPeer builds a two-node test Cluster ("self") with a live
// replicator pointed at addr (RF=2, so it's active — see
// TestAddPeer_ReplicationFactorOne_NoopReplicator), using small queue/batch
// sizes so backpressure and batching behavior are easy to exercise.
func clusterWithPeer(t *testing.T, nodeID, addr string, queueSize, batchSize int) *Cluster {
	t.Helper()
	m := newTestClusterRF(nodeID, 2)
	m.cfg.ReplicationQueueSize = queueSize
	m.cfg.ReplicationBatchSize = batchSize
	m.cfg.RoutingTimeout = 2 * time.Second
	if err := m.addPeer(psRF("peer", addr, NodeAlive, 1, 2)); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	return m
}

func TestReplicator_AppliesWritesInOrder(t *testing.T) {
	fp := newFakePeer(t)
	m := clusterWithPeer(t, "self", fp.addr, 512, 8)
	rep, ok := m.getReplicator("peer")
	if !ok {
		t.Fatal("replicator should exist after addPeer")
	}

	const n = 200
	for i := range n {
		rep.enqueue(transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte{byte(i)}}})
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

func TestReplicator_Backpressure_BlocksWhenQueueFull(t *testing.T) {
	fp := newFakePeer(t)
	fp.gate = make(chan struct{})
	m := clusterWithPeer(t, "self", fp.addr, 1, 1)
	rep, _ := m.getReplicator("peer")

	req := func(i int) transport.ForwardRequest {
		return transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte{byte(i)}}}
	}

	// job1 is picked up by the worker immediately and blocks on fp.gate.
	rep.enqueue(req(1))
	// job2 fills the (size-1) queue.
	rep.enqueue(req(2))

	// job3 must block: the worker is stuck sending job1, and job2 already
	// occupies the only queue slot.
	done := make(chan struct{})
	go func() {
		rep.enqueue(req(3))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("enqueue should block while the queue is full and the peer is stuck")
	case <-time.After(150 * time.Millisecond):
	}

	close(fp.gate) // release job1's send; worker moves on to job2, freeing a slot for job3

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue should unblock once the queue drains")
	}
}

func TestReplicator_SendFailure_StopsWorkerAndReleasesBlocked(t *testing.T) {
	fp := newFakePeer(t)
	fp.gate = make(chan struct{})
	fp.fail = true
	m := clusterWithPeer(t, "self", fp.addr, 1, 1)
	rep, _ := m.getReplicator("peer")

	req := func(i int) transport.ForwardRequest {
		return transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte{byte(i)}}}
	}

	rep.enqueue(req(1)) // picked up immediately, blocks on fp.gate before failing
	rep.enqueue(req(2)) // fills the queue

	done := make(chan struct{})
	go func() {
		rep.enqueue(req(3)) // must block until the peer is marked dead
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("enqueue should block while the worker is stuck")
	case <-time.After(150 * time.Millisecond):
	}

	close(fp.gate) // let job1's send fail

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked enqueue should be released once the peer is marked dead")
	}

	waitForCond(t, 2*time.Second, "peer marked dead", func() bool {
		_, ok := m.getReplicator("peer")
		return !ok
	})
	if _, ok := m.getClient("peer"); ok {
		t.Error("client should be removed once the peer is marked dead")
	}
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
