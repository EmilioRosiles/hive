package tests

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// -- test types --

type Session struct {
	UserID int
	Token  string
}

type Stream struct {
	BitRate   int
	StartedAt time.Time
}

// -- node helpers --

// standalone returns a cache backed by a single local node with no networking.
func standalone(t *testing.T) *hive.Cache {
	t.Helper()
	node, err := hive.NewNode(hive.Config{})
	if err != nil {
		t.Fatalf("standalone: %v", err)
	}
	t.Cleanup(func() { node.Shutdown() })
	return node.Cache()
}

// clusterNode starts a cluster-mode node and returns its node and cache.
// The node is shut down automatically when the test ends.
func clusterNode(t *testing.T, seeds []string, rf int) (*hive.Node, *hive.Cache) {
	t.Helper()
	node, err := hive.NewNode(hive.Config{
		Mode:              hive.ModeCluster,
		BindAddr:          "127.0.0.1",
		BindPort:          freePort(t),
		Seeds:             seeds,
		ReplicationFactor: rf,
		GossipInterval:    100 * time.Millisecond,
		GossipFanout:      3,
		RebalanceDebounce: 50 * time.Millisecond,
		DeadTimeout:       2 * time.Second,
	})
	if err != nil {
		t.Fatalf("clusterNode: %v", err)
	}
	t.Cleanup(func() { node.Shutdown() })
	return node, node.Cache()
}

// addr returns the bind address of a cluster node.
func addr(n *hive.Node) string {
	s := n.Status()
	return fmt.Sprintf("127.0.0.1:%d", s.BindPort)
}

// freePort returns an available TCP port on localhost.
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
