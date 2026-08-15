package ring

import (
	"slices"
	"testing"
)

// -- Add / Get --

func TestAdd_NodeAppearsInGet(t *testing.T) {
	r := New(1)
	r.Add("node1", 100)

	owners := r.Get("somekey")
	if len(owners) == 0 {
		t.Fatal("Get returned empty slice after Add")
	}
	if owners[0] != "node1" {
		t.Errorf("expected node1, got %s", owners[0])
	}
}

func TestAdd_Idempotent(t *testing.T) {
	r := New(1)
	r.Add("node1", 100)
	v1 := r.GetVersion()

	r.Add("node1", 100) // re-adding the same (nodeID, count) should be a true no-op
	v2 := r.GetVersion()

	if v1 != v2 {
		t.Errorf("version changed on idempotent re-add: %d -> %d", v1, v2)
	}
	if owners := r.Get("key"); len(owners) != 1 || owners[0] != "node1" {
		t.Errorf("Get after idempotent Add: got %v, want [node1]", owners)
	}
}

func TestAdd_ReplacesVirtualNodes(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	r.Add("b", 100)
	r.Add("c", 100)

	// Re-add "a" with a different vnode count — its old vnodes must be
	// replaced, not left behind alongside the new ones.
	r.Add("a", 200)

	count := 0
	for _, v := range r.vNodes {
		if v.nodeID == "a" {
			count++
		}
	}
	if count != 200 {
		t.Errorf("node a should have exactly 200 vnodes after re-add, got %d (old ones weren't replaced)", count)
	}
	if nodes := r.GetNodes(); len(nodes) != 3 {
		t.Errorf("expected 3 nodes after re-add, got %d", len(nodes))
	}
}

// -- Remove --

func TestRemove_NodeNotReturnedByGet(t *testing.T) {
	r := New(1)
	r.Add("node1", 100)
	r.Add("node2", 100)

	r.Remove("node1")

	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		for _, owner := range r.Get(key) {
			if owner == "node1" {
				t.Errorf("key %q returned removed node1", key)
			}
		}
	}
}

func TestRemove_UnknownNode_NoOp(t *testing.T) {
	r := New(1)
	r.Add("node1", 100)
	v := r.GetVersion()

	r.Remove("nobody")

	if r.GetVersion() != v {
		t.Error("removing unknown node should not change the ring version")
	}
}

func TestRemove_LastNode_EmptyRing(t *testing.T) {
	r := New(1)
	r.Add("node1", 100)
	r.Remove("node1")

	if owners := r.Get("key"); len(owners) != 0 {
		t.Errorf("expected empty result from empty ring, got %v", owners)
	}
}

// -- Get --

func TestGet_EmptyRing_ReturnsEmpty(t *testing.T) {
	r := New(3)
	if owners := r.Get("key"); len(owners) != 0 {
		t.Errorf("expected empty, got %v", owners)
	}
}

func TestGet_CapsAtReplicas(t *testing.T) {
	r := New(2)
	r.Add("a", 100)
	r.Add("b", 100)
	r.Add("c", 100)

	owners := r.Get("key")
	if len(owners) != 2 {
		t.Errorf("expected 2 owners (replicas=2), got %d", len(owners))
	}
}

func TestGet_FewerNodesThanReplicas(t *testing.T) {
	r := New(3)
	r.Add("only", 100)

	owners := r.Get("key")
	if len(owners) != 1 {
		t.Errorf("expected 1 owner (only 1 node), got %d: %v", len(owners), owners)
	}
}

func TestGet_ReturnsUniqueNodes(t *testing.T) {
	r := New(3)
	r.Add("a", 100)
	r.Add("b", 100)
	r.Add("c", 100)

	owners := r.Get("key")
	seen := make(map[string]bool)
	for _, o := range owners {
		if seen[o] {
			t.Errorf("duplicate node %q in Get result %v", o, owners)
		}
		seen[o] = true
	}
}

func TestGet_Deterministic(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	r.Add("b", 100)
	r.Add("c", 100)

	for range 10 {
		first := r.Get("consistent-key")
		again := r.Get("consistent-key")
		if !slices.Equal(first, again) {
			t.Fatalf("Get is not deterministic: %v vs %v", first, again)
		}
	}
}

func TestGet_DistributesAcrossNodes(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	r.Add("b", 100)
	r.Add("c", 100)

	counts := make(map[string]int)
	for i := range 300 {
		key := "key-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		owners := r.Get(key)
		if len(owners) > 0 {
			counts[owners[0]]++
		}
	}
	// Each node should own at least some keys.
	for _, node := range []string{"a", "b", "c"} {
		if counts[node] == 0 {
			t.Errorf("node %q received no keys — distribution is broken", node)
		}
	}
}

// -- GetNodes --

func TestGetNodes_ReturnsAllNodes(t *testing.T) {
	r := New(1)
	r.Add("x", 100)
	r.Add("y", 100)
	r.Add("z", 100)

	nodes := r.GetNodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d: %v", len(nodes), nodes)
	}
	slices.Sort(nodes)
	for i, want := range []string{"x", "y", "z"} {
		if nodes[i] != want {
			t.Errorf("nodes[%d]: got %q, want %q", i, nodes[i], want)
		}
	}
}

func TestGetNodes_EmptyRing(t *testing.T) {
	r := New(1)
	if nodes := r.GetNodes(); len(nodes) != 0 {
		t.Errorf("expected empty, got %v", nodes)
	}
}

// -- GetVersion --

func TestGetVersion_ChangesOnAdd(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	v1 := r.GetVersion()

	r.Add("b", 100)
	v2 := r.GetVersion()

	if v1 == v2 {
		t.Error("version should change after adding a new node")
	}
}

func TestGetVersion_ChangesOnRemove(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	r.Add("b", 100)
	v1 := r.GetVersion()

	r.Remove("a")
	v2 := r.GetVersion()

	if v1 == v2 {
		t.Error("version should change after removing a node")
	}
}

func TestGetVersion_StableWithNoChanges(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	v := r.GetVersion()

	for range 5 {
		if r.GetVersion() != v {
			t.Error("version should be stable when ring is unchanged")
		}
	}
}

// -- Copy --

func TestCopy_IsIndependentSnapshot(t *testing.T) {
	r := New(1)
	r.Add("a", 100)
	r.Add("b", 100)

	snap := r.Copy()

	// Mutate original after copying.
	r.Remove("a")
	r.Add("c", 100)

	// Snapshot should still see the original state.
	snapNodes := snap.GetNodes()
	slices.Sort(snapNodes)
	if len(snapNodes) != 2 || snapNodes[0] != "a" || snapNodes[1] != "b" {
		t.Errorf("snapshot was mutated by original changes; got %v", snapNodes)
	}
}

func TestCopy_PreservesReplicas(t *testing.T) {
	r := New(3)
	r.Add("a", 100)
	r.Add("b", 100)
	r.Add("c", 100)

	snap := r.Copy()

	if snap.Replicas != r.Replicas {
		t.Errorf("copy Replicas: got %d, want %d", snap.Replicas, r.Replicas)
	}
}

func TestCopy_PreservesVersion(t *testing.T) {
	r := New(1)
	r.Add("a", 100)

	snap := r.Copy()

	if snap.GetVersion() != r.GetVersion() {
		t.Errorf("copy version %d differs from original %d", snap.GetVersion(), r.GetVersion())
	}
}
