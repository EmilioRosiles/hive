package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// -- helpers --

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func startNode(t *testing.T, seeds []string, rf int) *Manager {
	t.Helper()
	cfg := Config{
		NodeID:            fmt.Sprintf("node-%d", time.Now().UnixNano()),
		BindAddr:          "127.0.0.1",
		BindPort:          freePort(t),
		Seeds:             seeds,
		ReplicationFactor: rf,
		GossipInterval:    100 * time.Millisecond,
		GossipFanout:      3,
		RebalanceDebounce: 50 * time.Millisecond,
		DeadTimeout:       2 * time.Second,
		Clustered:         true,
	}
	m, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("startNode: %v", err)
	}
	t.Cleanup(func() { m.Shutdown() })
	return m
}

func addr(m *Manager) string {
	return fmt.Sprintf("127.0.0.1:%d", m.cfg.BindPort)
}

// waitFor polls condition every 20ms until it returns true or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, msg string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// keyOwners returns the node IDs that directly store key in their local store.
func keyOwners(key string, nodes []*Manager) []string {
	var owners []string
	for _, m := range nodes {
		if _, ok := m.store.Get(key); ok {
			owners = append(owners, m.cfg.NodeID)
		}
	}
	return owners
}

// -- tests --

func TestStandaloneSetGet(t *testing.T) {
	m, err := NewManager(Config{
		NodeID:            "solo",
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()

	if err := m.Set("hello", []byte("world")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, err := m.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("got %q, want %q", val, "world")
	}

	if err := m.Del("hello"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := m.Get("hello"); err != ErrNotFound {
		t.Fatalf("after Del: got err %v, want ErrNotFound", err)
	}
}

func TestClusterFormation(t *testing.T) {
	n1 := startNode(t, nil, 1)
	n2 := startNode(t, []string{addr(n1)}, 1)
	n3 := startNode(t, []string{addr(n1)}, 1)

	waitFor(t, 2*time.Second, "n1 sees 2 peers", func() bool { return len(n1.Peers()) == 2 })
	waitFor(t, 2*time.Second, "n2 sees 2 peers", func() bool { return len(n2.Peers()) == 2 })
	waitFor(t, 2*time.Second, "n3 sees 2 peers", func() bool { return len(n3.Peers()) == 2 })
}

func TestKeyDistributionRF1(t *testing.T) {
	n1 := startNode(t, nil, 1)
	n2 := startNode(t, []string{addr(n1)}, 1)
	n3 := startNode(t, []string{addr(n1)}, 1)

	nodes := []*Manager{n1, n2, n3}
	waitFor(t, 2*time.Second, "cluster formed", func() bool {
		return len(n1.Peers()) == 2 && len(n2.Peers()) == 2 && len(n3.Peers()) == 2
	})

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	for _, k := range keys {
		if err := n1.Set(k, []byte("v:"+k)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	// Wait for any in-flight rebalance to settle.
	time.Sleep(200 * time.Millisecond)

	for _, k := range keys {
		owners := keyOwners(k, nodes)
		if len(owners) != 1 {
			t.Errorf("key %q: want 1 owner, got %d: %v", k, len(owners), owners)
		}
	}
}

func TestGetFromAnyNode(t *testing.T) {
	n1 := startNode(t, nil, 1)
	n2 := startNode(t, []string{addr(n1)}, 1)
	n3 := startNode(t, []string{addr(n1)}, 1)

	waitFor(t, 2*time.Second, "cluster formed", func() bool {
		return len(n1.Peers()) == 2 && len(n2.Peers()) == 2 && len(n3.Peers()) == 2
	})

	keys := []string{"foo", "bar", "baz", "qux"}
	for _, k := range keys {
		if err := n1.Set(k, []byte("val:"+k)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	// Every key must be readable from every node via forwarding.
	for _, k := range keys {
		for _, m := range []*Manager{n1, n2, n3} {
			val, err := m.Get(k)
			if err != nil {
				t.Errorf("Get %q from %s: %v", k, m.cfg.NodeID, err)
				continue
			}
			if string(val) != "val:"+k {
				t.Errorf("Get %q from %s: got %q, want %q", k, m.cfg.NodeID, val, "val:"+k)
			}
		}
	}
}

func TestRebalanceOnJoin(t *testing.T) {
	n1 := startNode(t, nil, 1)
	n2 := startNode(t, []string{addr(n1)}, 1)

	waitFor(t, 2*time.Second, "2-node cluster formed", func() bool {
		return len(n1.Peers()) == 1 && len(n2.Peers()) == 1
	})

	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"}
	for _, k := range keys {
		if err := n1.Set(k, []byte("v:"+k)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	// Add a third node and wait for it to join and rebalance.
	n3 := startNode(t, []string{addr(n1)}, 1)
	nodes := []*Manager{n1, n2, n3}

	waitFor(t, 2*time.Second, "3-node cluster formed", func() bool {
		return len(n1.Peers()) == 2 && len(n2.Peers()) == 2 && len(n3.Peers()) == 2
	})
	// Allow rebalance debounce + propagation.
	time.Sleep(300 * time.Millisecond)

	// Each key must be on exactly one node.
	for _, k := range keys {
		owners := keyOwners(k, nodes)
		if len(owners) != 1 {
			t.Errorf("key %q: want 1 owner after rebalance, got %d: %v", k, len(owners), owners)
		}
	}

	// All keys must still be readable from any node.
	for _, k := range keys {
		for _, m := range nodes {
			if _, err := m.Get(k); err != nil {
				t.Errorf("Get %q from %s after rebalance: %v", k, m.cfg.NodeID, err)
			}
		}
	}
}

func TestReplicationFactor2(t *testing.T) {
	n1 := startNode(t, nil, 2)
	n2 := startNode(t, []string{addr(n1)}, 2)
	n3 := startNode(t, []string{addr(n1)}, 2)

	nodes := []*Manager{n1, n2, n3}
	waitFor(t, 2*time.Second, "cluster formed", func() bool {
		return len(n1.Peers()) == 2 && len(n2.Peers()) == 2 && len(n3.Peers()) == 2
	})

	keys := []string{"r1", "r2", "r3", "r4", "r5", "r6"}
	for _, k := range keys {
		if err := n1.Set(k, []byte("rv:"+k)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	for _, k := range keys {
		kk := k
		waitFor(t, 2*time.Second, fmt.Sprintf("key %q has 2 owners", kk), func() bool {
			return len(keyOwners(kk, nodes)) == 2
		})
	}
}
