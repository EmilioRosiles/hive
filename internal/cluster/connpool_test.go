package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// TestCluster_PooledConnections_PreserveReplicationOrder verifies that
// pooling connections per peer (ConnPoolSize > 1) doesn't reorder replicated writes.
func TestCluster_PooledConnections_PreserveReplicationOrder(t *testing.T) {
	const poolSize = 4
	n1 := newClusteredTestNodeWithPool(t, nil, 2, poolSize)
	n2 := newClusteredTestNodeWithPool(t, []string{clusteredAddr(n1)}, 2, poolSize)
	n3 := newClusteredTestNodeWithPool(t, []string{clusteredAddr(n1)}, 2, poolSize)
	defer n1.Shutdown()
	defer n2.Shutdown()
	defer n3.Shutdown()

	waitForCond(t, 2*time.Second, "cluster formed", func() bool {
		return len(n1.Peers()) == 2 && len(n2.Peers()) == 2 && len(n3.Peers()) == 2
	})

	const key = "ordering-key"
	const writes = 200
	for i := 0; i < writes; i++ {
		if _, err := n1.Exec(t.Context(), transport.OpValueSet, key, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Exec Set #%d: %v", i, err)
		}
	}
	want := fmt.Sprintf("v%d", writes-1)

	// Every node holding a copy must end up with the last-written value.
	waitForCond(t, 2*time.Second, "replication catches up", func() bool {
		for _, n := range []*Cluster{n1, n2, n3} {
			if got, err := n.Exec(t.Context(), transport.OpValueGet, key); err == nil && string(got[0]) != want {
				return false
			}
		}
		return true
	})
	for _, n := range []*Cluster{n1, n2, n3} {
		got, err := n.Exec(t.Context(), transport.OpValueGet, key)
		if err != nil {
			continue // this node holds no copy of the key — not a failure
		}
		if string(got[0]) != want {
			t.Errorf("node %s: got %q, want last-written %q", n.cfg.NodeID, got[0], want)
		}
	}
}
