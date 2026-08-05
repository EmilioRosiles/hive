package cluster

import "testing"

func TestComputeVNodes_Zero_OwnsNothing(t *testing.T) {
	if got := computeVNodes(0); got != 0 {
		t.Errorf("computeVNodes(0): got %d, want 0", got)
	}
}

func TestComputeVNodes_TinyNonzero_FloorsToOne(t *testing.T) {
	// Smaller than one unitSize: must not silently truncate to 0 just
	// because it's less than a full unit — only a true 0 should do that.
	if got := computeVNodes(1); got != 1 {
		t.Errorf("computeVNodes(1): got %d, want 1", got)
	}
}

func TestComputeVNodes_ScalesWithUnits(t *testing.T) {
	cases := []struct {
		memLimit uint64
		want     int
	}{
		{unitSize, vNodesPerUnit},
		{4 * unitSize, 4 * vNodesPerUnit},
		{16 * unitSize, 16 * vNodesPerUnit},
	}
	for _, tc := range cases {
		if got := computeVNodes(tc.memLimit); got != tc.want {
			t.Errorf("computeVNodes(%d): got %d, want %d", tc.memLimit, got, tc.want)
		}
	}
}
