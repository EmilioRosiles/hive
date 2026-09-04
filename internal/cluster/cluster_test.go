package cluster

import (
	"log/slog"
	"math"
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
			ReplicationQueueSize: 64,
			ReplicationBatchSize: 16,
			MemLimit:             256 << 20, // nonzero so this fixture's rebalancer isn't a no-op
		},
		ring:        r,
		store:       store.NewDataStore(math.MaxInt64), // capacity is enforced literally; this fixture doesn't want a cap
		peers:       make(map[string]*PeerInfo),
		clients:     make(map[string]*transport.Client),
		replicators: make(map[string]*replicator),
		stopCh:      make(chan struct{}),
		logger:      slog.Default(),
	}
	m.incarnation.Store(uint64(time.Now().UnixNano()))
	m.rebalancer = newRebalancer(0, m)
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
	if _, ok := m.getReplicator("peer1"); !ok {
		t.Error("replicator should be registered after addPeer")
	}
}

// TestAddPeer_ReplicationFactorOne_NoopReplicator verifies RF=1 gets a
// no-op replicator (present in the map, but no jobs channel).
func TestAddPeer_ReplicationFactorOne_NoopReplicator(t *testing.T) {
	m := newTestCluster("self") // RF=1

	if err := m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100)); err != nil {
		t.Fatalf("addPeer: %v", err)
	}
	if _, ok := m.getClient("peer1"); !ok {
		t.Error("client should still be registered — forwarding always needs it")
	}
	rep, ok := m.getReplicator("peer1")
	if !ok {
		t.Fatal("replicator entry should still exist (as a no-op) after addPeer")
	}
	if rep.jobs != nil {
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
	if _, ok := m.getReplicator("peer1"); !ok {
		t.Error("replicator should be re-registered after revival")
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

// -- markDead --

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
	if _, ok := m.getReplicator("peer1"); ok {
		t.Error("replicator should be removed after markDead")
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

func TestEvictDeadPeers_EmptyMap_NoOp(t *testing.T) {
	m := newTestCluster("self")
	m.evictDeadPeers() // should not panic
}

// -- randomAlivePeers --

func TestRandomAlivePeers_OnlyReturnsAlive(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("alive1", "127.0.0.1:1001", NodeAlive, 100))
	m.addPeer(ps("alive2", "127.0.0.1:1002", NodeAlive, 100))
	m.addPeer(ps("dead1", "127.0.0.1:1003", NodeAlive, 100))
	m.markDead("dead1")

	peers := m.randomAlivePeers(10)

	if len(peers) != 2 {
		t.Errorf("got %d alive peers, want 2", len(peers))
	}
	for _, p := range peers {
		if p.Status != NodeAlive {
			t.Errorf("randomAlivePeers returned dead peer %s", p.NodeID)
		}
	}
}

func TestRandomAlivePeers_CountCapped(t *testing.T) {
	m := newTestCluster("self")
	for i := range 5 {
		m.addPeer(ps(
			string(rune('a'+i)),
			"127.0.0.1:100"+string(rune('0'+i)),
			NodeAlive, 100,
		))
	}

	if got := len(m.randomAlivePeers(2)); got != 2 {
		t.Errorf("got %d peers, want 2", got)
	}
}

func TestRandomAlivePeers_NoPeers(t *testing.T) {
	m := newTestCluster("self")
	if peers := m.randomAlivePeers(3); len(peers) != 0 {
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
