package tests

import (
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// TestCluster_ZeroOwnershipWorker_RoutesWithoutStoringLocally verifies a
// node configured with hive.Bytes(0) joins the cluster normally and can
// serve as a pure routing/relay point — correctly forwarding both reads and
// writes to the real owner — without ever needing local keyspace of its own.
func TestCluster_ZeroOwnershipWorker_RoutesWithoutStoringLocally(t *testing.T) {
	n1, c1 := clusterNode(t, nil, 2)
	n2, c2 := clusterNode(t, []string{addr(n1)}, 2)
	n3, c3 := clusterNodeWithMemLimit(t, []string{addr(n1)}, 2, hive.Bytes(0))

	waitFor(t, 2*time.Second, "cluster formed", func() bool {
		return n1.Cluster().AliveCount() == 3 && n2.Cluster().AliveCount() == 3 && n3.Cluster().AliveCount() == 3
	})

	store1 := hive.NewValueStore[Session](c1, "sessions")
	store3 := hive.NewValueStore[Session](c3, "sessions")

	// Write via a real storage node, read via the zero-ownership worker —
	// must be forwarded correctly since n3 owns nothing.
	if err := store1.Set("alice", Session{UserID: 1, Token: "alice"}); err != nil {
		t.Fatalf("Set via n1: %v", err)
	}
	waitFor(t, time.Second, "read via worker forwards correctly", func() bool {
		got, err := store3.Get("alice")
		return err == nil && got.Token == "alice"
	})

	// Write via the zero-ownership worker itself — must forward to whichever
	// real node should own the key, not fail or store locally.
	if err := store3.Set("bob", Session{UserID: 2, Token: "bob"}); err != nil {
		t.Fatalf("Set via worker n3: %v", err)
	}
	store2 := hive.NewValueStore[Session](c2, "sessions")
	waitFor(t, time.Second, "write via worker lands on a real owner", func() bool {
		got1, err1 := store1.Get("bob")
		got2, err2 := store2.Get("bob")
		return (err1 == nil && got1.Token == "bob") || (err2 == nil && got2.Token == "bob")
	})
}
