package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

func TestCluster_Formation(t *testing.T) {
	n1, _ := clusterNode(t, nil, 1)
	n2, _ := clusterNode(t, []string{addr(n1)}, 1)
	n3, _ := clusterNode(t, []string{addr(n1)}, 1)

	waitFor(t, 1*time.Second, "all nodes see 2 peers", func() bool {
		return n1.Status().Size == 3 &&
			n2.Status().Size == 3 &&
			n3.Status().Size == 3
	})
}

func TestCluster_ReadFromAnyNode(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 1)
	n2, c2 := clusterNode(t, []string{addr(n1)}, 1)
	_, c3 := clusterNode(t, []string{addr(n1)}, 1)

	waitFor(t, 1*time.Second, "cluster formed", func() bool {
		return n1.Status().Size == 3 && n2.Status().Size == 3
	})

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	store1 := hive.NewValueStore[Session](c1, "sessions")
	store2 := hive.NewValueStore[Session](c2, "sessions")
	store3 := hive.NewValueStore[Session](c3, "sessions")

	for i, k := range keys {
		if err := store1.Set(k, Session{UserID: i, Token: k}); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	// Every key must be readable from all three nodes via forwarding.
	for _, k := range keys {
		for i, s := range []*hive.ValueStore[Session]{store1, store2, store3} {
			got, err := s.Get(k)
			if err != nil {
				t.Errorf("node %d Get %q: %v", i+1, k, err)
				continue
			}
			if got.Token != k {
				t.Errorf("node %d Get %q: got token %q, want %q", i+1, k, got.Token, k)
			}
		}
	}
}

func TestCluster_SetStoreAcrossNodes(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 1)
	n2, c2 := clusterNode(t, []string{addr(n1)}, 1)

	waitFor(t, 1*time.Second, "cluster formed", func() bool {
		return n1.Status().Size == 2 && n2.Status().Size == 2
	})

	online1 := hive.NewSetStore(c1, "online")
	online2 := hive.NewSetStore(c2, "online")

	online1.SAdd("room:1", "user:1")
	online1.SAdd("room:1", "user:2")

	// Read back from node 2 (may forward).
	members, err := online2.SMembers("room:1")
	if err != nil {
		t.Fatalf("SMembers from node2: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("SMembers: got %d members, want 2", len(members))
	}
}

func TestCluster_RebalanceOnJoin(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 1)
	n2, _ := clusterNode(t, []string{addr(n1)}, 1)

	waitFor(t, 1*time.Second, "2-node cluster formed", func() bool {
		return n1.Status().Size == 2 && n2.Status().Size == 2
	})

	store := hive.NewValueStore[Session](c1, "sessions")
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		store.Set(keys[i], Session{UserID: i})
	}

	// Add a third node and wait for rebalance.
	n3, c3 := clusterNode(t, []string{addr(n1)}, 1)
	waitFor(t, 1*time.Second, "3-node cluster formed", func() bool {
		return n1.Status().Size == 3 && n3.Status().Size == 3
	})
	time.Sleep(300 * time.Millisecond) // let rebalance settle

	// All keys must still be readable from any node.
	store3 := hive.NewValueStore[Session](c3, "sessions")
	for _, k := range keys {
		if _, err := store3.Get(k); err != nil {
			t.Errorf("Get %q from new node after rebalance: %v", k, err)
		}
	}
}

// TestCluster_RebalanceOnJoin_ValueIntegrity verifies that after a new node
// joins and rebalance migrates keys, every node returns the correct values —
// not just a non-error response.
func TestCluster_RebalanceOnJoin_ValueIntegrity(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 1)
	n2, c2 := clusterNode(t, []string{addr(n1)}, 1)

	waitFor(t, 1*time.Second, "2-node cluster formed", func() bool {
		return n1.Status().Size == 2 && n2.Status().Size == 2
	})

	const nkeys = 30
	store1 := hive.NewValueStore[Session](c1, "sessions")
	keys := make([]string, nkeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		if err := store1.Set(keys[i], Session{UserID: i, Token: keys[i]}); err != nil {
			t.Fatalf("Set %q: %v", keys[i], err)
		}
	}

	// n3 joins; triggers rebalance — some keys migrate to n3.
	n3, c3 := clusterNode(t, []string{addr(n1)}, 1)
	waitFor(t, 1*time.Second, "3-node cluster formed", func() bool {
		return n1.Status().Size == 3 && n3.Status().Size == 3
	})
	time.Sleep(300 * time.Millisecond) // let rebalance settle

	stores := []*hive.ValueStore[Session]{
		hive.NewValueStore[Session](c1, "sessions"),
		hive.NewValueStore[Session](c2, "sessions"),
		hive.NewValueStore[Session](c3, "sessions"),
	}
	for i, k := range keys {
		for node, s := range stores {
			got, err := s.Get(k)
			if err != nil {
				t.Errorf("node %d Get %q: %v", node+1, k, err)
				continue
			}
			if got.UserID != i || got.Token != k {
				t.Errorf("node %d Get %q: got %+v, want {UserID:%d Token:%q}", node+1, k, got, i, k)
			}
		}
	}
}

// TestCluster_NodeLeave_RF2 verifies that when a node shuts down gracefully,
// the remaining two nodes retain all data (RF=2 means every key had two copies).
func TestCluster_NodeLeave_RF2(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 2)
	n2, c2 := clusterNode(t, []string{addr(n1)}, 2)
	n3, c3 := clusterNode(t, []string{addr(n1)}, 2)

	waitFor(t, 1*time.Second, "cluster formed", func() bool {
		return n1.Status().Size == 3 && n2.Status().Size == 3 && n3.Status().Size == 3
	})

	// Write enough keys to ensure every node is primary for at least some.
	s1 := hive.NewValueStore[Session](c1, "sessions")
	const nkeys = 30
	keys := make([]string, nkeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("leave-%d", i)
		if err := s1.Set(keys[i], Session{UserID: i, Token: keys[i]}); err != nil {
			t.Fatalf("Set %q: %v", keys[i], err)
		}
	}

	// Give async replica fanout time to settle before n3 departs.
	time.Sleep(100 * time.Millisecond)

	// n3 leaves gracefully; peers immediately evict it and trigger rebalance.
	n1.Shutdown()
	waitFor(t, 2*time.Second, "n1 gone", func() bool {
		return n2.Status().Size == 2 && n3.Status().Size == 2
	})
	time.Sleep(200 * time.Millisecond) // let rebalance settle

	// All keys must be readable with correct values from both remaining nodes.
	s2 := hive.NewValueStore[Session](c2, "sessions")
	s3 := hive.NewValueStore[Session](c3, "sessions")
	for i, k := range keys {
		for node, s := range []*hive.ValueStore[Session]{s2, s3} {
			got, err := s.Get(k)
			if err != nil {
				t.Errorf("after n3 leave, node %d Get %q: %v", node+1, k, err)
				continue
			}
			if got.UserID != i || got.Token != k {
				t.Errorf("after n3 leave, node %d Get %q: got %+v, want {UserID:%d Token:%q}", node+1, k, got, i, k)
			}
		}
	}
}

// TestCluster_SequentialLeaves_DataSurvives verifies that with RF=2, data
// survives two sequential graceful departures as long as rebalance settles
// between each — the last surviving node ends up with everything.
func TestCluster_SequentialLeaves_DataSurvives(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 2)
	n2, _ := clusterNode(t, []string{addr(n1)}, 2)
	n3, _ := clusterNode(t, []string{addr(n1)}, 2)

	waitFor(t, 1*time.Second, "cluster formed", func() bool {
		return n1.Status().Size == 3 && n2.Status().Size == 3 && n3.Status().Size == 3
	})

	s1 := hive.NewValueStore[Session](c1, "sessions")
	const nkeys = 30
	keys := make([]string, nkeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("seq-%d", i)
		if err := s1.Set(keys[i], Session{UserID: i, Token: keys[i]}); err != nil {
			t.Fatalf("Set %q: %v", keys[i], err)
		}
	}

	time.Sleep(100 * time.Millisecond) // let async replicas settle

	// First departure: n3 leaves, rebalance redistributes its keys to n1/n2.
	n3.Shutdown()
	waitFor(t, 2*time.Second, "n3 gone", func() bool {
		return n1.Status().Size == 2 && n2.Status().Size == 2
	})
	time.Sleep(200 * time.Millisecond) // let rebalance settle

	// Second departure: n2 leaves, rebalance moves everything to n1.
	n2.Shutdown()
	waitFor(t, 2*time.Second, "n2 gone", func() bool {
		return n1.Status().Size == 1
	})
	time.Sleep(200 * time.Millisecond) // let rebalance settle

	// n1 alone must have all keys.
	for i, k := range keys {
		got, err := s1.Get(k)
		if err != nil {
			t.Errorf("after n2+n3 leave, Get %q: %v", k, err)
			continue
		}
		if got.UserID != i || got.Token != k {
			t.Errorf("after n2+n3 leave, Get %q: got %+v, want {UserID:%d Token:%q}", k, got, i, k)
		}
	}
}

func TestCluster_ReplicationFactor2(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 2)
	n2, c2 := clusterNode(t, []string{addr(n1)}, 2)
	n3, c3 := clusterNode(t, []string{addr(n1)}, 2)

	waitFor(t, 1*time.Second, "cluster formed", func() bool {
		return n1.Status().Size == 3 &&
			n2.Status().Size == 3 &&
			n3.Status().Size == 3
	})

	store := hive.NewValueStore[Session](c1, "sessions")
	keys := []string{"r1", "r2", "r3", "r4"}
	for i, k := range keys {
		store.Set(k, Session{UserID: i})
	}

	// With RF=2 each key should be readable from every node.
	stores := []*hive.ValueStore[Session]{
		hive.NewValueStore[Session](c1, "sessions"),
		hive.NewValueStore[Session](c2, "sessions"),
		hive.NewValueStore[Session](c3, "sessions"),
	}
	waitFor(t, 1*time.Second, "all keys replicated", func() bool {
		for _, s := range stores {
			for _, k := range keys {
				if _, err := s.Get(k); err != nil {
					return false
				}
			}
		}
		return true
	})
}
