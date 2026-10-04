package cluster

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

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

func TestMergeState_DeadRumour_UnreachablePeer_MarkedDead(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1", NodeAlive, 100)) // nothing listens on port 1

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1", NodeDead, 101)})

	waitForCond(t, time.Second, "peer1 confirmed dead", func() bool {
		status, _ := m.peerStatus("peer1")
		return status == NodeDead
	})
}

func TestMergeState_DeadRumour_ReachablePeer_StaysAlive(t *testing.T) {
	m := newTestCluster("self")
	addr := startNodeServer(t, "peer1")
	m.addPeer(ps("peer1", addr, NodeAlive, 100))
	ringVersionBefore := m.ring.GetVersion()

	m.mergeState([]transport.PeerState{ps("peer1", addr, NodeDead, 101)})

	waitForCond(t, time.Second, "probe acked", func() bool {
		status, _ := m.peerStatus("peer1")
		return status == NodeAlive
	})
	if m.ring.GetVersion() != ringVersionBefore {
		t.Error("a refuted dead rumour should not change the ring")
	}
}

func TestMergeState_DeadRumour_SuspectPeer_LeftToProbe(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	setStatus(m, "peer1", NodeSuspect)

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 101)})

	if status, _ := m.peerStatus("peer1"); status != NodeSuspect {
		t.Errorf("status: got %v, want NodeSuspect (the running probe decides)", status)
	}
}

func TestHeartbeat_Failure_ProbeAcks_StaysAlive(t *testing.T) {
	release := make(chan struct{})
	var pings atomic.Int32
	addr := startPeerServer(t, func(msgType transport.MsgType, _ []byte) ([]byte, error) {
		if msgType == transport.MsgPing {
			pings.Add(1)
			return pingAck, nil
		}
		<-release // heartbeats hang past GossipTimeout
		return nil, nil
	})
	t.Cleanup(func() { close(release) })

	m := newTestCluster("self")
	m.addPeer(ps("peer1", addr, NodeAlive, 100))
	ringVersionBefore := m.ring.GetVersion()

	p, _ := m.getPeer("peer1")
	m.heartbeat(p)

	waitForCond(t, time.Second, "probe acked", func() bool {
		status, _ := m.peerStatus("peer1")
		return pings.Load() > 0 && status == NodeAlive
	})
	if m.ring.GetVersion() != ringVersionBefore {
		t.Error("a failed heartbeat with a successful probe should not change the ring")
	}
}

func TestHeartbeat_Failure_ProbeFails_MarksDead(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1", NodeAlive, 100)) // nothing listens on port 1

	p, _ := m.getPeer("peer1")
	m.heartbeat(p)

	waitForCond(t, time.Second, "peer marked dead", func() bool {
		status, _ := m.peerStatus("peer1")
		return status == NodeDead
	})
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

func TestMergeState_EqualIncarnation_DeadWins(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeDead, 100)})

	waitForCond(t, time.Second, "equal-incarnation dead rumour applied", func() bool {
		status, _ := m.peerStatus("peer1")
		return status == NodeDead
	})
}

func TestMergeState_EqualIncarnation_AliveDoesNotRevive(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("peer1")

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeAlive, 100)})

	if status, _ := m.peerStatus("peer1"); status != NodeDead {
		t.Errorf("status: got %v, want NodeDead", status)
	}
}

func TestMergeState_EqualIncarnation_RefreshesMemUsed(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	update := ps("peer1", "127.0.0.1:1001", NodeAlive, 100)
	update.MemUsed = 42
	m.mergeState([]transport.PeerState{update})

	if p, _ := m.getPeer("peer1"); p.MemUsed != 42 {
		t.Errorf("MemUsed: got %d, want 42", p.MemUsed)
	}
}

func TestMergeState_DeadRumourAboutSelf_Refutes(t *testing.T) {
	m := newTestCluster("self")
	m.incarnation.Store(100)

	m.mergeState([]transport.PeerState{ps("self", "127.0.0.1:7946", NodeDead, 50)})
	if got := m.incarnation.Load(); got != 100 {
		t.Errorf("older rumour: incarnation got %d, want 100 (already beats it)", got)
	}

	m.mergeState([]transport.PeerState{ps("self", "127.0.0.1:7946", NodeDead, 100)})
	if got := m.incarnation.Load(); got != 101 {
		t.Errorf("equal rumour: incarnation got %d, want 101", got)
	}

	m.mergeState([]transport.PeerState{ps("self", "127.0.0.1:7946", NodeAlive, 500)})
	if got := m.incarnation.Load(); got != 101 {
		t.Errorf("alive entry about self: incarnation got %d, want 101 (only Dead is refuted)", got)
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

func TestBuildHeartbeatRequest_DoesNotBumpIncarnation(t *testing.T) {
	m := newTestCluster("self")
	before := m.incarnation.Load()

	m.buildHeartbeatRequest()
	m.buildHeartbeatRequest()

	if got := m.incarnation.Load(); got != before {
		t.Errorf("incarnation: got %d, want %d (only refutations bump it)", got, before)
	}
}

func TestMergeState_UnknownSuspectPeer_Added(t *testing.T) {
	m := newTestCluster("self")

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeSuspect, 100)})

	if status, ok := m.peerStatus("peer1"); !ok || status != NodeAlive {
		t.Errorf("got (%v, %v), want an Alive peer1 (only our own probe can suspect it)", status, ok)
	}
}

func TestMergeState_SuspectRumour_KeepsPeerAlive(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	m.mergeState([]transport.PeerState{ps("peer1", "127.0.0.1:1001", NodeSuspect, 100)})

	if status, _ := m.peerStatus("peer1"); status != NodeAlive {
		t.Errorf("status: got %v, want NodeAlive (another node's suspicion isn't acted on)", status)
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

// countingProxy forwards connections to backend and counts how many it accepted.
func countingProxy(t *testing.T, backend string) (string, *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	var accepts atomic.Int32
	go func() {
		for {
			a, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			b, err := net.Dial("tcp", backend)
			if err != nil {
				a.Close()
				continue
			}
			go func() { io.Copy(b, a); b.Close() }()
			go func() { io.Copy(a, b); a.Close() }()
		}
	}()
	return ln.Addr().String(), &accepts
}

func TestBootstrap_KeepsSeedConnection(t *testing.T) {
	seed := newTestCluster("seed") // its own entry advertises no address
	addr, accepts := countingProxy(t, startPeerServer(t, seed.handleFrame))
	m := newTestCluster("self")

	m.bootstrap(addr)

	if got := m.ping("seed", "seed"); got != probeAck {
		t.Fatalf("ping over the seed's client: got %v, want ack", got)
	}
	if n := accepts.Load(); n != 1 {
		t.Errorf("%d connections to the seed, want 1: the bootstrap connection should be kept", n)
	}
}

func TestAnnounceLeave_UsesPooledClients(t *testing.T) {
	peer := newTestCluster("peer")
	left := make(chan struct{}, 1)
	backend := startPeerServer(t, func(msgType transport.MsgType, payload []byte) ([]byte, error) {
		if msgType == transport.MsgLeave {
			left <- struct{}{}
		}
		return peer.handleFrame(msgType, payload)
	})
	addr, accepts := countingProxy(t, backend)
	m := newTestCluster("self")
	m.addPeer(ps("peer", addr, NodeAlive, 1))
	if got := m.ping("peer", "peer"); got != probeAck {
		t.Fatalf("ping: got %v, want ack", got)
	}

	m.announceLeave()

	select {
	case <-left:
	case <-time.After(time.Second):
		t.Fatal("peer never received MsgLeave")
	}
	if n := accepts.Load(); n != 1 {
		t.Errorf("%d connections to the peer, want 1: the leave should reuse the pooled connection", n)
	}
}
