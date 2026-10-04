package cluster

import (
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// fakePeer is an in-process transport.Server standing in for a replica. It
// records every batch it applies, in arrival order, and acks pings. It can be
// gated to simulate a stuck peer, reject batches, or stall them past the
// sender's timeout without applying them.
type fakePeer struct {
	addr     string
	server   *transport.Server
	gate     chan struct{} // handler blocks on this until closed, if non-nil
	stall    time.Duration
	mu       sync.Mutex
	rejects  int // batches to reject before applying
	stalls   int // batches to stall for stall, then drop
	applied  []transport.ForwardRequest
	attempts []uint64 // Seq of every batch received, applied or not
}

func newFakePeer(t *testing.T) *fakePeer {
	t.Helper()
	fp := &fakePeer{}
	srv, err := transport.NewServer("127.0.0.1:0", fp.handle, nil, nil, slog.Default())
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
	if msgType == transport.MsgPing {
		return pingAck, nil
	}
	var batch transport.ForwardBatch
	if err := transport.Decode(payload, &batch); err != nil {
		return nil, err
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.attempts = append(fp.attempts, batch.Seq)
	if fp.stalls > 0 {
		fp.stalls--
		fp.mu.Unlock()
		time.Sleep(fp.stall)
		fp.mu.Lock()
		return nil, errors.New("fakePeer: stalled")
	}
	if fp.rejects > 0 {
		fp.rejects--
		return nil, errors.New("fakePeer: rejected")
	}
	fp.applied = append(fp.applied, batch.Requests...)
	return nil, nil
}

func (fp *fakePeer) seqs() []uint64 {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return append([]uint64(nil), fp.attempts...)
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

func setReq(i int) transport.ForwardRequest {
	return transport.ForwardRequest{Op: transport.OpValueSet, Key: "k", Args: [][]byte{{byte(i)}}}
}

func TestReplicator_FullQueue_DropsWithoutBlocking(t *testing.T) {
	fp := newFakePeer(t)
	m := clusterWithPeer(t, "self", fp.addr, 2, 1)
	setStatus(m, "peer", NodeSuspect)

	done := make(chan struct{})
	go func() {
		for i := range 5 {
			m.replicator.enqueue("peer", setReq(i))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked on a full queue")
	}

	m.markAlive("peer")
	waitForCond(t, 2*time.Second, "held writes flushed", func() bool {
		return len(fp.received()) == 3
	})
	m.replicator.enqueue("peer", setReq(9))
	waitForCond(t, 2*time.Second, "write after the flush applied", func() bool {
		return len(fp.received()) == 4
	})
	var got []byte
	for _, req := range fp.received() {
		got = append(got, req.Args[0][0])
	}
	if string(got) != string([]byte{0, 1, 2, 9}) {
		t.Errorf("applied %v, want [0 1 2 9]: one batch in flight plus a full queue of 2, the rest dropped", got)
	}
}

func TestReplicator_FailedSend_SuspectsThenDead(t *testing.T) {
	fp := newFakePeer(t)
	fp.gate = make(chan struct{})
	t.Cleanup(func() { close(fp.gate) })
	m := clusterWithPeer(t, "self", fp.addr, 64, 1)
	m.cfg.RoutingTimeout = 200 * time.Millisecond
	m.cfg.ProbeTimeout = 50 * time.Millisecond

	m.replicator.enqueue("peer", setReq(1))

	waitForCond(t, 3*time.Second, "peer marked dead by the probe", func() bool {
		status, _ := m.peerStatus("peer")
		return status == NodeDead
	})
}

func TestReplicator_RejectedBatch_NotSuspected(t *testing.T) {
	fp := newFakePeer(t)
	fp.rejects = 1
	m := clusterWithPeer(t, "self", fp.addr, 64, 1)

	m.replicator.enqueue("peer", setReq(1))
	m.replicator.enqueue("peer", setReq(2))

	waitForCond(t, 2*time.Second, "second write applied", func() bool {
		return len(fp.received()) == 1
	})
	if got := fp.received()[0].Args[0][0]; got != 2 {
		t.Errorf("applied write %d, want 2 (the rejected batch must not be retried)", got)
	}
	if status, _ := m.peerStatus("peer"); status != NodeAlive {
		t.Errorf("status: got %v, want NodeAlive", status)
	}
}

func TestReplicator_HoldsWhileSuspect_FlushesInOrderOnAlive(t *testing.T) {
	fp := newFakePeer(t)
	m := clusterWithPeer(t, "self", fp.addr, 64, 2)
	setStatus(m, "peer", NodeSuspect)

	for i := range 5 {
		m.replicator.enqueue("peer", setReq(i))
	}
	time.Sleep(150 * time.Millisecond)
	if n := len(fp.received()); n != 0 {
		t.Fatalf("%d writes sent to a Suspect peer, want 0", n)
	}

	m.markAlive("peer")
	waitForCond(t, 2*time.Second, "held writes flushed", func() bool {
		return len(fp.received()) == 5
	})
	for i, req := range fp.received() {
		if req.Args[0][0] != byte(i) {
			t.Fatalf("write %d flushed out of order: got %d", i, req.Args[0][0])
		}
	}
}

func TestReplicator_FailedSend_RetriedWithSameSeq(t *testing.T) {
	fp := newFakePeer(t)
	fp.stalls = 1
	fp.stall = 500 * time.Millisecond
	m := clusterWithPeer(t, "self", fp.addr, 64, 4)
	m.cfg.RoutingTimeout = 200 * time.Millisecond

	m.replicator.enqueue("peer", setReq(1))

	waitForCond(t, 3*time.Second, "write applied after retry", func() bool {
		return len(fp.received()) == 1
	})
	seqs := fp.seqs()
	if len(seqs) != 2 || seqs[0] != seqs[1] {
		t.Errorf("attempt seqs: got %v, want two attempts with the same seq", seqs)
	}
	if status, _ := m.peerStatus("peer"); status != NodeAlive {
		t.Errorf("status: got %v, want NodeAlive", status)
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

func TestHandleForwardBatch_SkipsAppliedSeq(t *testing.T) {
	m := newTestCluster("self")
	apply := func(from string, seq uint64, v string) {
		payload, err := transport.Encode(transport.ForwardBatch{From: from, Seq: seq, Requests: []transport.ForwardRequest{
			{Op: transport.OpValueSet, Key: "k", Args: [][]byte{[]byte(v)}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.handleForwardBatch(payload); err != nil {
			t.Fatalf("handleForwardBatch: %v", err)
		}
	}
	get := func() string {
		res, err := opRegistry[transport.OpValueGet].Exec(m, "k", nil, 0)
		if err != nil || len(res) == 0 {
			t.Fatalf("get: %v", err)
		}
		return string(res[0])
	}

	apply("a", 5, "first")
	apply("a", 5, "duplicate")
	apply("a", 4, "older")
	if got := get(); got != "first" {
		t.Errorf("after duplicate and older seq: got %q, want %q", got, "first")
	}
	apply("b", 1, "other sender")
	if got := get(); got != "other sender" {
		t.Errorf("another sender's batch: got %q, want %q", got, "other sender")
	}
	apply("a", 6, "newer")
	if got := get(); got != "newer" {
		t.Errorf("newer seq: got %q, want %q", got, "newer")
	}
}

func TestReplicatorApply_DuplicateWaitsForOriginal(t *testing.T) {
	m := newTestCluster("self")
	const n = 2000
	reqs := make([]transport.ForwardRequest, n)
	for i := range reqs {
		reqs[i] = transport.ForwardRequest{Op: transport.OpRPush, Key: "l", Args: [][]byte{{1}}}
	}
	batch := transport.ForwardBatch{From: "a", Seq: 1, Requests: reqs}
	llen := func() int {
		res, _ := opRegistry[transport.OpLLen].Exec(m, "l", nil, 0)
		if len(res) == 0 {
			return 0
		}
		return int(decodeUint64(res[0]))
	}

	go m.replicator.apply(batch)
	for llen() == 0 {
		runtime.Gosched()
	}
	m.replicator.apply(batch)

	if got := llen(); got != n {
		t.Errorf("duplicate returned with %d of %d ops applied, want it to wait for the original", got, n)
	}
}
