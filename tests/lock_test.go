package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// TestCluster_Lock_WorksAcrossForwarding uses a zero-ownership relay node so
// every op below is forwarded over the wire, exercising
// transport.ForwardRequest.LockToken rather than just the in-process ctx path.
func TestCluster_Lock_WorksAcrossForwarding(t *testing.T) {
	n1, _ := clusterNode(t, nil, 2)
	_, _ = clusterNode(t, []string{addr(n1)}, 2)
	n3, c3 := clusterNodeWithMemLimit(t, []string{addr(n1)}, 2, hive.Bytes(0))

	waitFor(t, 2*time.Second, "cluster formed", func() bool {
		return n1.Cluster().AliveCount() == 3 && n3.Cluster().AliveCount() == 3
	})

	// c3 owns nothing, so every op below is forwarded to whichever other
	// node actually owns "alice".
	store := hive.NewValueStore[Session](c3, "sessions")

	if err := store.Set(t.Context(), "alice", Session{UserID: 1}); err != nil {
		t.Fatalf("forwarded Set (pre-lock): %v", err)
	}

	lock, err := store.Lock(t.Context(), "alice", time.Minute)
	if err != nil {
		t.Fatalf("Lock (forwarded): %v", err)
	}

	// Errors crossing a forward lose Go type identity (transport.ErrRejected
	// carries only the message string), so this checks the message instead
	// of errors.Is — see standalone_test.go for the errors.Is version.
	if _, err := store.Get(t.Context(), "alice"); err == nil || !strings.Contains(err.Error(), hive.ErrKeyLocked.Error()) {
		t.Errorf("forwarded Get without auth: got %v, want an error containing %q", err, hive.ErrKeyLocked.Error())
	}

	// With lock.Context(), the token must ride the forward request and be
	// honored by the remote primary.
	authCtx := lock.Context(t.Context())
	if err := store.Set(authCtx, "alice", Session{UserID: 1, Token: "ok"}); err != nil {
		t.Fatalf("forwarded authorized Set: %v", err)
	}
	got, err := store.Get(authCtx, "alice")
	if err != nil || got.Token != "ok" {
		t.Fatalf("forwarded authorized Get: got (%+v, %v)", got, err)
	}

	if err := lock.Unlock(t.Context()); err != nil {
		t.Fatalf("forwarded Unlock: %v", err)
	}
	if _, err := store.Get(t.Context(), "alice"); err != nil {
		t.Errorf("Get after Unlock should succeed, got %v", err)
	}
}
