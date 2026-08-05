package tests

import (
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
	"github.com/EmilioRosiles/hive/internal/tlstest"
)

// TestCluster_TLS_FormsAndReplicates confirms a TLS-enabled cluster actually
// forms via gossip and replicates a write end-to-end over TLS — not just
// that individual frames round-trip in isolation (covered elsewhere in
// internal/transport and the top-level hive package's TLS tests).
func TestCluster_TLS_FormsAndReplicates(t *testing.T) {
	caPEM, certPEM, keyPEM := tlstest.NewPair(t)
	tlsConfig, err := hive.NewClusterTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig: %v", err)
	}

	n1, c1 := clusterNodeTLS(t, nil, 2, tlsConfig)
	n2, c2 := clusterNodeTLS(t, []string{addr(n1)}, 2, tlsConfig)
	n3, c3 := clusterNodeTLS(t, []string{addr(n1)}, 2, tlsConfig)

	waitFor(t, 2*time.Second, "TLS cluster formed", func() bool {
		return n1.Status().Size == 3 && n2.Status().Size == 3 && n3.Status().Size == 3
	})

	store1 := hive.NewValueStore[Session](c1, "sessions")
	if err := store1.Set("alice", Session{UserID: 1, Token: "alice"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// RF=2, so this key must be readable from at least one other node besides
	// whichever is primary — confirms replication, not just local storage.
	store2 := hive.NewValueStore[Session](c2, "sessions")
	store3 := hive.NewValueStore[Session](c3, "sessions")
	waitFor(t, 2*time.Second, "write replicated over TLS", func() bool {
		got2, err2 := store2.Get("alice")
		got3, err3 := store3.Get("alice")
		return (err2 == nil && got2.Token == "alice") || (err3 == nil && got3.Token == "alice")
	})
}
