package cluster

import (
	"testing"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// -- mergeState --

func TestMergeState_UnknownAlivePeer_Added(t *testing.T) {
	m := newTestCluster("self")

	if err := m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeAlive, 100)}); err != nil {
		t.Fatalf("mergeState: %v", err)
	}
	if _, ok := m.getPeer("peer1"); !ok {
		t.Error("unknown alive peer should be added")
	}
}

func TestMergeState_UnknownDeadPeer_Ignored(t *testing.T) {
	m := newTestCluster("self")

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 100)})

	if _, ok := m.getPeer("peer1"); ok {
		t.Error("unknown dead peer should not be added")
	}
}

func TestMergeState_HigherIncarnation_AliveToDead(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 101)})

	p, _ := m.getPeer("peer1")
	if p.Status != NodeDead {
		t.Errorf("peer1 should be dead after higher-incarnation dead gossip; got %v", p.Status)
	}
}

func TestMergeState_HigherIncarnation_DeadToAlive(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("peer1")

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeAlive, 101)})

	p, _ := m.getPeer("peer1")
	if p.Status != NodeAlive {
		t.Errorf("peer1 should be revived after higher-incarnation alive gossip; got %v", p.Status)
	}
}

func TestMergeState_DeadToDeadHigherIncarnation_NoRevival(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("peer1")

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 200)})

	p, _ := m.getPeer("peer1")
	if p.Status != NodeDead {
		t.Errorf("dead peer should stay dead after dead gossip with higher incarnation; got %v", p.Status)
	}
	if p.Incarnation != 200 {
		t.Errorf("incarnation should update to 200; got %d", p.Incarnation)
	}
}

func TestMergeState_LowerIncarnation_Ignored(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 50)})

	p, _ := m.getPeer("peer1")
	if p.Status != NodeAlive {
		t.Error("lower-incarnation dead gossip should not override alive state")
	}
}

func TestMergeState_EqualIncarnation_Ignored(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 100)})

	p, _ := m.getPeer("peer1")
	if p.Status != NodeAlive {
		t.Error("equal-incarnation dead gossip should not override alive state")
	}
}

func TestMergeState_SelfEntry_Skipped(t *testing.T) {
	m := newTestCluster("self")

	m.mergeState([]transport.PeerState{ps("self", "127.0.0.1:7946", NodeDead, 9999)})

	if _, ok := m.getPeer("self"); ok {
		t.Error("self should never be added to the peer map")
	}
}

func TestMergeState_ReplicationFactorMismatch_ReturnsError(t *testing.T) {
	m := newTestCluster("self")
	bad := transport.PeerState{
		NodeID: "peer1", Addr: "127.0.0.1:1001",
		Status: uint8(NodeAlive), Incarnation: 100,
		ReplicationFactor: 3,
	}
	if err := m.mergeState([]transport.PeerState{bad}); err == nil {
		t.Error("mergeState should return error on replication factor mismatch")
	}
}

// -- buildHeartbeatRequest --

func TestBuildHeartbeatRequest_BumpsIncarnationEachCall(t *testing.T) {
	m := newTestCluster("self")
	before := m.incarnation.Load()

	m.buildHeartbeatRequest()
	after1 := m.incarnation.Load()
	m.buildHeartbeatRequest()
	after2 := m.incarnation.Load()

	if after1 <= before {
		t.Errorf("incarnation should increase after first call: %d → %d", before, after1)
	}
	if after2 <= after1 {
		t.Errorf("incarnation should increase after second call: %d → %d", after1, after2)
	}
}

func TestBuildHeartbeatRequest_IncludesSelfAsAlive(t *testing.T) {
	m := newTestCluster("self")
	req := m.buildHeartbeatRequest()

	var self *transport.PeerState
	for i := range req.Peers {
		if req.Peers[i].NodeID == "self" {
			self = &req.Peers[i]
			break
		}
	}
	if self == nil {
		t.Fatal("self entry missing from heartbeat request")
	}
	if NodeStatus(self.Status) != NodeAlive {
		t.Errorf("self status should be NodeAlive, got %v", self.Status)
	}
	if self.Incarnation != m.incarnation.Load() {
		t.Errorf("self incarnation in payload (%d) should match current (%d)",
			self.Incarnation, m.incarnation.Load())
	}
}

func TestBuildHeartbeatRequest_IncludesAllPeers(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.addPeer(ps("peer2", "127.0.0.1:1002", NodeAlive, 100))

	req := m.buildHeartbeatRequest()

	ids := make(map[string]bool)
	for _, p := range req.Peers {
		ids[p.NodeID] = true
	}
	for _, want := range []string{"self", "peer1", "peer2"} {
		if !ids[want] {
			t.Errorf("heartbeat missing entry for %q", want)
		}
	}
}

func TestBuildHeartbeatRequest_DeadPeersIncluded(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("peer1")

	req := m.buildHeartbeatRequest()

	for _, p := range req.Peers {
		if p.NodeID == "peer1" {
			if NodeStatus(p.Status) != NodeDead {
				t.Errorf("dead peer should be reported as NodeDead in heartbeat; got %v", p.Status)
			}
			return
		}
	}
	t.Error("dead peer should be included in heartbeat so the dead state propagates")
}
