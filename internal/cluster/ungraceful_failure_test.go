package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// newClusteredTestNode starts a real, network-bound Cluster — unlike
// newTestCluster (no networking, single "self" node), this exercises actual
// gossip/heartbeat traffic over real loopback sockets, which is what the
// failure-detection path below needs.
func newClusteredTestNode(t *testing.T, seeds []string, rf int) *Cluster {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	m, err := NewCluster(Config{
		NodeID:               fmt.Sprintf("node-%d", port),
		BindAddr:             "127.0.0.1",
		BindPort:             port,
		Seeds:                seeds,
		ReplicationFactor:    rf,
		MemLimit:             256 << 20, // realistic capacity/vnode count; 0 now means "owns nothing"
		RoutingTimeout:       time.Second,
		GossipInterval:       100 * time.Millisecond,
		GossipFanout:         3,
		GossipTimeout:        300 * time.Millisecond,
		RebalanceDebounce:    50 * time.Millisecond,
		RebalanceBatchSize:   128,
		ReplicationQueueSize: 64,
		ReplicationBatchSize: 16,
		CleanupInterval:      time.Second,
		Clustered:            true,
	})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	return m
}

func clusteredAddr(m *Cluster) string {
	return fmt.Sprintf("%s:%d", m.cfg.BindAddr, m.cfg.BindPort)
}

// killUngracefully simulates a hard process crash: it stops m's background
// loops and drops every connection it holds — WITHOUT calling announceLeave,
// unlike Shutdown. Peers must discover this the same way they'd discover a
// real crash: their next heartbeat to m fails. Reuses stopOnce so a later
// Shutdown() call (e.g. from deferred test cleanup) is a safe no-op.
func killUngracefully(m *Cluster) {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		if m.server != nil {
			m.server.Close()
		}
	})
}

// TestCluster_UngracefulFailure_DetectedAndDataSurvives verifies the actual
// production failure-detection path — Cluster.heartbeat's real client.Send
// failing over a real dropped connection, not markDead called directly as a
// test shortcut, and not the graceful Shutdown/announceLeave/MsgLeave path
// every other "node leaves" test in this codebase uses.
func TestCluster_UngracefulFailure_DetectedAndDataSurvives(t *testing.T) {
	n1 := newClusteredTestNode(t, nil, 2)
	n2 := newClusteredTestNode(t, []string{clusteredAddr(n1)}, 2)
	n3 := newClusteredTestNode(t, []string{clusteredAddr(n1)}, 2)
	defer n1.Shutdown()
	defer n2.Shutdown()
	defer n3.Shutdown()

	waitForCond(t, 2*time.Second, "cluster formed", func() bool {
		return len(n1.Peers()) == 2 && len(n2.Peers()) == 2 && len(n3.Peers()) == 2
	})

	// Write through n1 while all three nodes are healthy, with RF=2 so at
	// least one other node holds a replica.
	const key = "ungraceful-key"
	if _, err := n1.Exec(transport.OpValueSet, key, []byte("value")); err != nil {
		t.Fatalf("Exec Set: %v", err)
	}
	// Let the write actually finish replicating before we crash anything —
	// killing a node before its replica lands is the already-known,
	// separately-accepted primary-death data-loss window, not what this
	// test is checking.
	waitForCond(t, time.Second, "write replicated before kill", func() bool {
		_, err2 := n2.Exec(transport.OpValueGet, key)
		_, err3 := n3.Exec(transport.OpValueGet, key)
		return err2 == nil || err3 == nil
	})

	killUngracefully(n3)

	// n1 and n2 must detect this via the real heartbeat-timeout path
	// (Cluster.heartbeat -> client.Send error -> markDead).
	waitForCond(t, 3*time.Second, "surviving nodes detect the crash", func() bool {
		s1, ok1 := n1.peerStatus(n3.cfg.NodeID)
		s2, ok2 := n2.peerStatus(n3.cfg.NodeID)
		return ok1 && s1 == NodeDead && ok2 && s2 == NodeDead
	})

	// Rebalance should redistribute n3's share of the keyspace; the key must
	// still be readable from at least one surviving node afterward.
	waitForCond(t, 2*time.Second, "key survives the crash", func() bool {
		_, err1 := n1.Exec(transport.OpValueGet, key)
		_, err2 := n2.Exec(transport.OpValueGet, key)
		return err1 == nil || err2 == nil
	})
}
