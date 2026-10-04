package cluster

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/ring"
	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// -- shared test helpers --

// newTestCluster builds a minimal Cluster for unit tests.
// No TCP server, gossip loop, or janitor is started.
func newTestCluster(nodeID string) *Cluster {
	return newTestClusterRF(nodeID, 1)
}

// newTestClusterRF mirrors newTestCluster with an explicit ReplicationFactor,
// for tests that need an active (non-no-op) replicator — see newReplicator.
func newTestClusterRF(nodeID string, rf int) *Cluster {
	r := ring.New(rf, slog.Default())
	r.Add(nodeID, 100) // arbitrary nonzero vnode count for this no-network fixture
	m := &Cluster{
		cfg: Config{
			NodeID:               nodeID,
			ReplicationFactor:    rf,
			RoutingTimeout:       time.Second,
			RoutingRetryInterval: 10 * time.Millisecond,
			GossipTimeout:        300 * time.Millisecond,
			ProbeTimeout:         300 * time.Millisecond,
			ProbeHelpers:         3,
			ProbeInterval:        100 * time.Millisecond,
			RebalanceBatchSize:   16,
			RebalanceTimeout:     time.Second,
			ReplicationQueueSize: 64,
			ReplicationBatchSize: 16,
			MemLimit:             256 << 20, // nonzero so this fixture's rebalancer isn't a no-op
		},
		ring:    r,
		store:   store.NewDataStore(math.MaxInt64), // capacity is enforced literally; this fixture doesn't want a cap
		peers:   make(map[string]*PeerInfo),
		clients: make(map[string]*transport.Client),
		stopCh:  make(chan struct{}),
		logger:  slog.Default(),
	}
	m.incarnation.Store(uint64(time.Now().UnixNano()))
	m.rebalancer = newRebalancer(0, m)
	m.replicator = newReplicator(m)
	return m
}

// ps builds a PeerState for use in mergeState calls.
func ps(nodeID, addr string, status NodeStatus, incarnation uint64) transport.PeerState {
	return psRF(nodeID, addr, status, incarnation, 1)
}

// psRF mirrors ps with an explicit ReplicationFactor.
func psRF(nodeID, addr string, status NodeStatus, incarnation uint64, rf int) transport.PeerState {
	return transport.PeerState{
		NodeID:            nodeID,
		Addr:              addr,
		Status:            uint8(status),
		Incarnation:       incarnation,
		ReplicationFactor: rf,
		MemLimit:          256 << 20, // realistic nonzero vnode count; 0 now means "owns nothing"
	}
}

// -- addPeer --

func TestAddPeer_New(t *testing.T) {
	m := newTestClusterRF("self", 2)

	if err := m.addPeer(psRF("peer1", "127.0.0.1:1001", NodeAlive, 100, 2)); err != nil {
		t.Fatalf("addPeer: %v", err)
	}

	p, ok := m.getPeer("peer1")
	if !ok {
		t.Fatal("peer1 should exist after addPeer")
	}
	if p.Status != NodeAlive {
		t.Errorf("status: got %v, want NodeAlive", p.Status)
	}
	if p.Incarnation != 100 {
		t.Errorf("incarnation: got %d, want 100", p.Incarnation)
	}
	if _, ok := m.getClient("peer1"); !ok {
		t.Error("client should be registered after addPeer")
	}
}

// TestReplicator_ReplicationFactorOne_Noop verifies RF=1 gets a no-op
// replicator with no jobs channel.
func TestReplicator_ReplicationFactorOne_Noop(t *testing.T) {
	m := newTestCluster("self")
	if m.replicator.jobs != nil {
		t.Error("replicator should be a no-op (no jobs channel) when ReplicationFactor is 1")
	}
}

func TestAddPeer_AlreadyAlive_NoOp(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	ringVersionBefore := m.ring.GetVersion()
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 200))

	if m.ring.GetVersion() != ringVersionBefore {
		t.Error("ring should not change when re-adding an already-alive peer")
	}
}

func TestAddPeer_RevivesDead(t *testing.T) {
	m := newTestClusterRF("self", 2)
	m.addPeer(psRF("peer1", "127.0.0.1:1001", NodeAlive, 100, 2))
	m.markDead("peer1")

	if err := m.addPeer(psRF("peer1", "127.0.0.1:1001", NodeAlive, 200, 2)); err != nil {
		t.Fatalf("addPeer revival: %v", err)
	}

	p, _ := m.getPeer("peer1")
	if p.Status != NodeAlive {
		t.Errorf("status after revival: got %v, want NodeAlive", p.Status)
	}
	if p.Incarnation != 200 {
		t.Errorf("incarnation after revival: got %d, want 200", p.Incarnation)
	}
	if _, ok := m.getClient("peer1"); !ok {
		t.Error("client should be re-registered after revival")
	}
}

func TestAddPeer_ReplicationFactorMismatch(t *testing.T) {
	m := newTestCluster("self")
	bad := transport.PeerState{
		NodeID: "peer1", Addr: "127.0.0.1:1001",
		Status: uint8(NodeAlive), Incarnation: 100,
		ReplicationFactor: 3,
	}
	if err := m.addPeer(bad); err == nil {
		t.Error("addPeer should return error on replication factor mismatch")
	}
}

// -- suspect --

// setStatus sets a peer's status directly, without starting a probe.
func setStatus(m *Cluster, nodeID string, status NodeStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers[nodeID].Status = status
}

func TestSuspect_KeepsRingAndClientWhileProbing(t *testing.T) {
	release := make(chan struct{})
	addr := startPeerServer(t, func(transport.MsgType, []byte) ([]byte, error) {
		<-release
		return pingAck, nil
	})
	m := newTestCluster("self")
	m.cfg.ProbeTimeout = 5 * time.Second
	m.addPeer(ps("peer1", addr, NodeAlive, 100))
	client, _ := m.getClient("peer1")
	ringVersionBefore := m.ring.GetVersion()

	m.markSuspect("peer1")

	if status, _ := m.peerStatus("peer1"); status != NodeSuspect {
		t.Errorf("status while probing: got %v, want NodeSuspect", status)
	}
	if m.ring.GetVersion() != ringVersionBefore {
		t.Error("ring should not change while a peer is suspected")
	}
	if c, ok := m.getClient("peer1"); !ok || c != client {
		t.Error("client should be kept while a peer is suspected")
	}

	close(release)
	waitForCond(t, time.Second, "probe acked", func() bool {
		status, _ := m.peerStatus("peer1")
		return status == NodeAlive
	})
}

func TestSuspect_IgnoresDeadPeer(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("peer1")

	m.markSuspect("peer1")

	if status, _ := m.peerStatus("peer1"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", status)
	}
}

func TestAddPeer_KeepsSuspect(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	client, _ := m.getClient("peer1")
	setStatus(m, "peer1", NodeSuspect)
	ringVersionBefore := m.ring.GetVersion()

	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 101))

	if status, _ := m.peerStatus("peer1"); status != NodeSuspect {
		t.Errorf("status: got %v, want NodeSuspect (only the probe ends a suspicion)", status)
	}
	if m.ring.GetVersion() != ringVersionBefore {
		t.Error("ring should not change for a suspect peer")
	}
	if c, _ := m.getClient("peer1"); c != client {
		t.Error("a suspect peer should keep its existing client")
	}
}

// startPeerServer serves handler on a random local port for the test's duration.
func startPeerServer(t *testing.T, handler transport.Handler) string {
	t.Helper()
	srv, err := transport.NewServer("127.0.0.1:0", handler, nil, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

// startNodeServer serves a test node with nodeID, answering frames like a real peer.
func startNodeServer(t *testing.T, nodeID string) string {
	t.Helper()
	return startPeerServer(t, newTestCluster(nodeID).handleFrame)
}

// pingAck is the payload of an acked MsgPing.
var pingAck = []byte{byte(probeAck)}

func TestProbe_Ack_MarksAlive(t *testing.T) {
	addr := startNodeServer(t, "peer1")
	m := newTestCluster("self")
	m.addPeer(ps("peer1", addr, NodeAlive, 100))
	setStatus(m, "peer1", NodeSuspect)

	m.probe("peer1")

	if status, _ := m.peerStatus("peer1"); status != NodeAlive {
		t.Errorf("status: got %v, want NodeAlive", status)
	}
}

func TestProbe_NoAnswer_MarksDead(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1", NodeAlive, 100)) // nothing listens on port 1
	setStatus(m, "peer1", NodeSuspect)

	m.probe("peer1")

	if status, _ := m.peerStatus("peer1"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", status)
	}
	if _, ok := m.getClient("peer1"); ok {
		t.Error("client should be removed once the peer is Dead")
	}
}

// Y restarted on X's old address with a new node ID: it must not answer for X.
func TestProbe_OtherNodeOnAddress_MarksDead(t *testing.T) {
	addr := startNodeServer(t, "Y")
	m := newTestCluster("self")
	m.addPeer(ps("X", addr, NodeAlive, 100))
	setStatus(m, "X", NodeSuspect)

	m.probe("X")

	if status, _ := m.peerStatus("X"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead (Y answered for X)", status)
	}
}

// The peer turns Dead (e.g. from a Dead rumour) while the ping is in flight.
// Only the status is flipped, so the client stays open and the ack arrives.
func TestProbe_Ack_DoesNotReviveDeadPeer(t *testing.T) {
	m := newTestCluster("self")
	addr := startPeerServer(t, func(transport.MsgType, []byte) ([]byte, error) {
		setStatus(m, "peer1", NodeDead)
		return pingAck, nil
	})
	m.addPeer(ps("peer1", addr, NodeAlive, 100))
	setStatus(m, "peer1", NodeSuspect)

	m.probe("peer1")

	if status, _ := m.peerStatus("peer1"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", status)
	}
}

// helperServer answers relayed pings with result after delay, counting them.
func helperServer(t *testing.T, result probeResult, delay func(n int32) time.Duration) (string, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	addr := startPeerServer(t, func(msgType transport.MsgType, payload []byte) ([]byte, error) {
		if msgType != transport.MsgPing || payload[0] != pingRelay {
			return nil, nil
		}
		time.Sleep(delay(calls.Add(1)))
		return []byte{byte(result)}, nil
	})
	return addr, &calls
}

func noDelay(int32) time.Duration { return 0 }

func TestHandlePing(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("up", startNodeServer(t, "up"), NodeAlive, 100))
	m.addPeer(ps("down", "127.0.0.1:1", NodeAlive, 100)) // nothing listens on port 1

	tests := []struct {
		name   string
		hop    byte
		target string
		want   probeResult
	}{
		{"direct, we are the target", pingDirect, "self", probeAck},
		{"direct, another node", pingDirect, "other", probeNack},
		{"relay, reachable target", pingRelay, "up", probeAck},
		{"relay, unreachable target", pingRelay, "down", probeNack},
		{"relay, unknown target", pingRelay, "unknown", probeNack},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.handlePing(append([]byte{tt.hop}, tt.target...))
			if len(got) != 1 || probeResult(got[0]) != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// A relayed ping must reach the target as a direct ping, or it could be relayed again.
func TestHandlePing_RelaysAsDirectPing(t *testing.T) {
	payloads := make(chan []byte, 1)
	addr := startPeerServer(t, func(_ transport.MsgType, payload []byte) ([]byte, error) {
		payloads <- payload
		return pingAck, nil
	})
	m := newTestCluster("self")
	m.addPeer(ps("target", addr, NodeAlive, 100))

	m.handlePing(append([]byte{pingRelay}, "target"...))

	if got, want := <-payloads, append([]byte{pingDirect}, "target"...); string(got) != string(want) {
		t.Errorf("target received payload %q, want %q", got, want)
	}
}

func TestProbe_HelperAcks_MarksAlive(t *testing.T) {
	helper, _ := helperServer(t, probeAck, noDelay)
	m := newTestCluster("self")
	m.addPeer(ps("target", "127.0.0.1:1", NodeAlive, 100)) // unreachable from us
	m.addPeer(ps("helper", helper, NodeAlive, 100))
	setStatus(m, "target", NodeSuspect)
	ringVersionBefore := m.ring.GetVersion()

	m.probe("target")

	if status, _ := m.peerStatus("target"); status != NodeAlive {
		t.Errorf("status: got %v, want NodeAlive", status)
	}
	if m.ring.GetVersion() != ringVersionBefore {
		t.Error("ring should not change when a helper reaches the peer")
	}
}

func TestProbe_HelperNacks_MarksDead(t *testing.T) {
	helper, _ := helperServer(t, probeNack, noDelay)
	m := newTestCluster("self")
	m.addPeer(ps("target", "127.0.0.1:1", NodeAlive, 100))
	m.addPeer(ps("helper", helper, NodeAlive, 100))
	setStatus(m, "target", NodeSuspect)

	m.probe("target")

	if status, _ := m.peerStatus("target"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", status)
	}
}

// The helper doesn't answer the first round in time, so the probe must keep
// the peer Suspect and retry instead of declaring it Dead.
func TestProbe_NoHelperAnswers_StaysSuspectAndRetries(t *testing.T) {
	helper, calls := helperServer(t, probeAck, func(n int32) time.Duration {
		if n == 1 {
			return 500 * time.Millisecond
		}
		return 0
	})
	m := newTestCluster("self")
	m.cfg.ProbeTimeout = 50 * time.Millisecond
	m.cfg.ProbeInterval = 50 * time.Millisecond
	m.addPeer(ps("target", "127.0.0.1:1", NodeAlive, 100))
	m.addPeer(ps("helper", helper, NodeAlive, 100))
	setStatus(m, "target", NodeSuspect)

	go m.probe("target")

	waitForCond(t, 2*time.Second, "probe retried and helper acked", func() bool {
		status, _ := m.peerStatus("target")
		return calls.Load() >= 2 && status == NodeAlive
	})
}

// -- markDead --

func TestMarkDead_ClosesClient(t *testing.T) {
	fp := newFakePeer(t)
	m := newTestCluster("self")
	m.addPeer(ps("peer1", fp.addr, NodeAlive, 100))
	c, _ := m.getClient("peer1")

	payload, _ := transport.Encode(transport.ForwardBatch{})
	frame := transport.Frame{Type: transport.MsgForwardBatch, Payload: payload}
	if _, err := c.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send before markDead: %v", err)
	}

	m.markDead("peer1")

	waitForCond(t, time.Second, "client closed", func() bool {
		_, err := c.Send(context.Background(), frame)
		return errors.Is(err, transport.ErrUnsent)
	})
}

func TestMarkDead_AlivePeer(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	ringVersionBefore := m.ring.GetVersion()
	m.markDead("peer1")

	p, _ := m.getPeer("peer1")
	if p.Status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", p.Status)
	}
	if m.ring.GetVersion() == ringVersionBefore {
		t.Error("ring should change when a peer is marked dead")
	}
	if _, ok := m.getClient("peer1"); ok {
		t.Error("client should be removed after markDead")
	}
}

func TestMarkDead_AlreadyDead_NoOp(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("peer1")
	ringVersion := m.ring.GetVersion()

	m.markDead("peer1")

	if m.ring.GetVersion() != ringVersion {
		t.Error("second markDead should be a no-op")
	}
}

func TestMarkDead_UnknownPeer_NoOp(t *testing.T) {
	m := newTestCluster("self")
	ringVersion := m.ring.GetVersion()

	m.markDead("nobody")

	if m.ring.GetVersion() != ringVersion {
		t.Error("markDead on unknown peer should be a no-op")
	}
}

// -- evictDeadPeers --

func TestEvictDeadPeers_RemovesDeadKeepsAlive(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("alive", "127.0.0.1:1001", NodeAlive, 100))
	m.addPeer(ps("dead", "127.0.0.1:1002", NodeAlive, 100))
	m.markDead("dead")

	m.evictDeadPeers()

	if _, ok := m.getPeer("dead"); ok {
		t.Error("dead peer tombstone should be evicted")
	}
	if _, ok := m.getPeer("alive"); !ok {
		t.Error("alive peer should not be evicted")
	}
}

func TestEvictDeadPeers_KeepsTombstoneForDeadRetention(t *testing.T) {
	m := newTestCluster("self")
	m.cfg.DeadRetention = time.Hour
	m.addPeer(ps("dead", "127.0.0.1:1002", NodeAlive, 100))
	m.markDead("dead")

	m.evictDeadPeers()
	if _, ok := m.getPeer("dead"); !ok {
		t.Fatal("dead peer evicted before DeadRetention elapsed")
	}

	m.mu.Lock()
	m.peers["dead"].deadAt = time.Now().Add(-time.Hour)
	m.mu.Unlock()
	m.evictDeadPeers()
	if _, ok := m.getPeer("dead"); ok {
		t.Error("dead peer should be evicted once DeadRetention elapsed")
	}
}

func TestEvictDeadPeers_ForgetsDedupState(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("dead", "127.0.0.1:1002", NodeAlive, 100))
	m.replicator.apply(transport.ForwardBatch{From: "dead", Seq: 5})
	m.markDead("dead")

	m.evictDeadPeers()

	if _, ok := m.replicator.applied["dead"]; ok {
		t.Error("dedup state should be forgotten with the tombstone")
	}
}

func TestEvictDeadPeers_EmptyMap_NoOp(t *testing.T) {
	m := newTestCluster("self")
	m.evictDeadPeers() // should not panic
}

// -- randomPeers --

func TestRandomPeers_SkipsDeadAndExcluded(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("alive1", "127.0.0.1:1001", NodeAlive, 100))
	m.addPeer(ps("target", "127.0.0.1:1002", NodeAlive, 100))
	m.addPeer(ps("suspect1", "127.0.0.1:1003", NodeAlive, 100))
	m.addPeer(ps("dead1", "127.0.0.1:1004", NodeAlive, 100))
	setStatus(m, "suspect1", NodeSuspect)
	m.markDead("dead1")

	got := map[string]bool{}
	for _, p := range m.randomPeers(10, "target") {
		got[p.NodeID] = true
	}
	if len(got) != 2 || !got["alive1"] || !got["suspect1"] {
		t.Errorf("got %v, want alive1 and suspect1", got)
	}
}

func TestRandomPeers_CountCapped(t *testing.T) {
	m := newTestCluster("self")
	for i := range 5 {
		m.addPeer(ps(
			string(rune('a'+i)),
			"127.0.0.1:100"+string(rune('0'+i)),
			NodeAlive, 100,
		))
	}

	if got := len(m.randomPeers(2, "")); got != 2 {
		t.Errorf("got %d peers, want 2", got)
	}
}

func TestRandomPeers_NoPeers(t *testing.T) {
	m := newTestCluster("self")
	if peers := m.randomPeers(3, ""); len(peers) != 0 {
		t.Errorf("expected empty slice, got %d peers", len(peers))
	}
}

// -- incarnation initialisation --

func TestIncarnation_InitialisedFromTimestamp(t *testing.T) {
	before := uint64(time.Now().UnixNano())
	m := newTestCluster("self")
	after := uint64(time.Now().UnixNano())

	inc := m.incarnation.Load()
	if inc < before || inc > after {
		t.Errorf("incarnation %d should be between %d and %d", inc, before, after)
	}
}
