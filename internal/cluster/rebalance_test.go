package cluster

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// -- rebalancer creation --

func TestNewCluster_ZeroMemLimit_NoopRebalancer(t *testing.T) {
	m, err := NewCluster(Config{NodeID: "self", ReplicationFactor: 1, MemLimit: 0, CleanupInterval: time.Second, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer m.Shutdown()
	if m.rebalancer == nil {
		t.Fatal("rebalancer should still exist (as a no-op)")
	}
	if m.rebalancer.enabled {
		t.Error("a MemLimit=0 node never owns keys, so its rebalancer should be a no-op")
	}
}

func TestNewCluster_NonZeroMemLimit_ActiveRebalancer(t *testing.T) {
	m, err := NewCluster(Config{NodeID: "self", ReplicationFactor: 1, MemLimit: 256 << 20, CleanupInterval: time.Second, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer m.Shutdown()
	if !m.rebalancer.enabled {
		t.Error("a node that owns keyspace needs an active rebalancer")
	}
}

// -- migrationTargets --

func TestMigrationTargets_ReturnsNewOnly(t *testing.T) {
	targets := migrationTargets([]string{"a", "b"}, []string{"b", "c"})
	if len(targets) != 1 || targets[0] != "c" {
		t.Errorf("got %v, want [c]", targets)
	}
}

func TestMigrationTargets_NoOverlap(t *testing.T) {
	targets := migrationTargets([]string{"a"}, []string{"b", "c"})
	if len(targets) != 2 {
		t.Errorf("got %v, want [b c]", targets)
	}
}

func TestMigrationTargets_FullOverlap_Empty(t *testing.T) {
	targets := migrationTargets([]string{"a", "b"}, []string{"a", "b"})
	if len(targets) != 0 {
		t.Errorf("got %v, want empty", targets)
	}
}

// -- migrationLeader --

func TestMigrationLeader_PrefersThisNode(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("other", "127.0.0.1:1001", NodeAlive, 100))

	if leader := migrationLeader([]string{"self", "other"}, []string{"new"}, m); leader != "self" {
		t.Errorf("got %q, want self", leader)
	}
}

func TestMigrationLeader_FallsBackToAlivePeer(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("peer1", "127.0.0.1:1001", NodeAlive, 100))

	if leader := migrationLeader([]string{"peer1"}, []string{"new"}, m); leader != "peer1" {
		t.Errorf("got %q, want peer1", leader)
	}
}

func TestMigrationLeader_SkipsDeadPeers(t *testing.T) {
	m := newTestCluster("self")
	m.addPeer(ps("dead", "127.0.0.1:1001", NodeAlive, 100))
	m.markDead("dead")
	m.addPeer(ps("alive", "127.0.0.1:1002", NodeAlive, 100))

	if leader := migrationLeader([]string{"dead", "alive"}, []string{"new"}, m); leader != "alive" {
		t.Errorf("got %q, want alive", leader)
	}
}

func TestMigrationLeader_FallsBackToNewOwner(t *testing.T) {
	m := newTestCluster("self")

	if leader := migrationLeader([]string{"unknown"}, []string{"new1", "new2"}, m); leader != "new1" {
		t.Errorf("got %q, want new1", leader)
	}
}

// -- handleRebalance --

func encodeEntry(t *testing.T, key string, value []byte, ttlNanos int64) transport.RebalanceEntry {
	t.Helper()
	vs := store.NewValueStructure(value)
	data, err := vs.Encode()
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}
	return transport.RebalanceEntry{
		Key:  key,
		Kind: uint8(store.KindValue),
		Data: data,
		TTL:  ttlNanos,
	}
}

func TestHandleRebalance_StoresEntry(t *testing.T) {
	m := newTestCluster("self")

	batch := transport.RebalanceBatch{Entries: []transport.RebalanceEntry{
		encodeEntry(t, "k1", []byte("hello"), 0),
	}}
	payload, err := transport.Encode(batch)
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}

	if _, err := m.handleRebalance(payload); err != nil {
		t.Fatalf("handleRebalance: %v", err)
	}

	if _, ok := m.store.Get("k1"); !ok {
		t.Error("k1 should be present after rebalance")
	}
}

func TestHandleRebalance_StoresMultipleEntries(t *testing.T) {
	m := newTestCluster("self")

	batch := transport.RebalanceBatch{Entries: []transport.RebalanceEntry{
		encodeEntry(t, "k1", []byte("v1"), 0),
		encodeEntry(t, "k2", []byte("v2"), 0),
		encodeEntry(t, "k3", []byte("v3"), 0),
	}}
	payload, _ := transport.Encode(batch)
	m.handleRebalance(payload)

	for _, key := range []string{"k1", "k2", "k3"} {
		if _, ok := m.store.Get(key); !ok {
			t.Errorf("%s should be present after rebalance", key)
		}
	}
}

func TestHandleRebalance_WithTTL_SetsExpiry(t *testing.T) {
	m := newTestCluster("self")

	ttl := time.Hour.Nanoseconds()
	batch := transport.RebalanceBatch{Entries: []transport.RebalanceEntry{
		encodeEntry(t, "k1", []byte("v"), ttl),
	}}
	payload, _ := transport.Encode(batch)
	m.handleRebalance(payload)

	e, ok := m.store.Get("k1")
	if !ok {
		t.Fatal("k1 should be present after rebalance with TTL")
	}
	if e.KeyExpiry() == 0 {
		t.Error("key expiry should be set when TTL is provided")
	}
}

func TestHandleRebalance_PreservesLockState(t *testing.T) {
	m := newTestCluster("self")

	// A locked entry is just a ValueStructure with lock fields set.
	vs := store.NewValueStructure([]byte("v"))
	vs.SetLock(42, uint32(time.Now().Add(time.Hour).Unix()))
	data, err := vs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	batch := transport.RebalanceBatch{Entries: []transport.RebalanceEntry{
		{Key: "k1", Kind: uint8(store.KindValue), Data: data},
	}}
	payload, _ := transport.Encode(batch)
	if _, err := m.handleRebalance(payload); err != nil {
		t.Fatalf("handleRebalance: %v", err)
	}

	e, ok := m.store.Get("k1")
	if !ok {
		t.Fatal("k1 should be present after rebalance")
	}
	if e.LockToken() != 42 || e.LockExpiry() == 0 {
		t.Errorf("lock state should have migrated along with the entry: got token=%d expiry=%d", e.LockToken(), e.LockExpiry())
	}
}

func TestHandleRebalance_ExpiredInTransit_Skipped(t *testing.T) {
	m := newTestCluster("self")

	batch := transport.RebalanceBatch{Entries: []transport.RebalanceEntry{
		encodeEntry(t, "k1", []byte("v"), 1), // 1 ns TTL — already expired
	}}
	payload, _ := transport.Encode(batch)
	m.handleRebalance(payload)

	if _, ok := m.store.Get("k1"); ok {
		t.Error("entry with expired TTL should not be stored")
	}
}

func TestHandleRebalance_InvalidPayload_ReturnsError(t *testing.T) {
	m := newTestCluster("self")

	if _, err := m.handleRebalance([]byte("not-msgpack")); err == nil {
		t.Error("invalid payload should return error")
	}
}

// moveKeysToPeer stores keys on m, then gives peer1 (at addr) most of the ring,
// returning the keys peer1 now owns. The ring is changed directly so no
// background rebalance races the test's own run.
func moveKeysToPeer(t *testing.T, m *Cluster, addr string) []string {
	t.Helper()
	for i := range 100 {
		m.store.Set(fmt.Sprintf("k%d", i), newValueStructure(t, []byte("v")))
	}
	m.mu.Lock()
	m.peers["peer1"] = &PeerInfo{NodeID: "peer1", Addr: addr, Status: NodeAlive}
	m.clients["peer1"] = m.newClient(addr)
	m.mu.Unlock()
	m.ring.Add("peer1", 1000)

	var moved []string
	for i := range 100 {
		if key := fmt.Sprintf("k%d", i); m.ring.Get(key)[0] == "peer1" {
			moved = append(moved, key)
		}
	}
	if len(moved) == 0 {
		t.Fatal("no key moved to peer1")
	}
	return moved
}

func TestRebalance_FailedSend_KeepsKeysAndRetries(t *testing.T) {
	m := newTestCluster("self")
	moved := moveKeysToPeer(t, m, "127.0.0.1:1") // nothing listens on port 1
	setStatus(m, "peer1", NodeSuspect)           // no probe, so peer1 stays in the ring

	m.rebalancer.run()

	for _, key := range moved {
		if _, ok := m.store.Get(key); !ok {
			t.Fatalf("key %q deleted although its migration failed", key)
		}
	}
	if m.rebalancer.lastRing.GetVersion() == m.ring.GetVersion() {
		t.Error("a failed migration should leave the ring diff pending for the next run")
	}
}

func TestRebalance_DeliveredSend_DeletesKeys(t *testing.T) {
	m := newTestCluster("self")
	moved := moveKeysToPeer(t, m, startNodeServer(t, "peer1"))

	m.rebalancer.run()

	for _, key := range moved {
		if _, ok := m.store.Get(key); ok {
			t.Fatalf("key %q kept although peer1 received it", key)
		}
	}
	if m.rebalancer.lastRing.GetVersion() != m.ring.GetVersion() {
		t.Error("a completed rebalance should record the new ring")
	}
}

func TestRebalance_HungTarget_TimesOutAndKeepsKeys(t *testing.T) {
	release := make(chan struct{})
	addr := startPeerServer(t, func(msgType transport.MsgType, _ []byte) ([]byte, error) {
		if msgType == transport.MsgRebalance {
			<-release
		}
		return pingAck, nil
	})
	t.Cleanup(func() { close(release) })
	m := newTestCluster("self")
	m.cfg.RebalanceTimeout = 100 * time.Millisecond
	moved := moveKeysToPeer(t, m, addr)

	done := make(chan struct{})
	go func() {
		m.rebalancer.run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("rebalance blocked on a target that never answers")
	}

	for _, key := range moved {
		if _, ok := m.store.Get(key); !ok {
			t.Fatalf("key %q deleted although its migration timed out", key)
		}
	}
}
