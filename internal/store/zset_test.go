package store

import (
	"testing"
	"time"
)

func TestZSetZAddAndZScore(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 10.0)
	z.ZAdd("bob", 20.0)

	if s, ok := z.ZScore("alice"); !ok || s != 10.0 {
		t.Errorf("ZScore(alice): got %v ok=%v, want 10.0 true", s, ok)
	}
	if s, ok := z.ZScore("bob"); !ok || s != 20.0 {
		t.Errorf("ZScore(bob): got %v ok=%v, want 20.0 true", s, ok)
	}
	if _, ok := z.ZScore("missing"); ok {
		t.Error("ZScore(missing): expected false")
	}
}

func TestZSetZAddUpdateScore(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 5.0)
	z.ZAdd("alice", 99.0)

	if s, ok := z.ZScore("alice"); !ok || s != 99.0 {
		t.Errorf("ZScore after update: got %v ok=%v, want 99.0 true", s, ok)
	}
	if z.ZCard() != 1 {
		t.Errorf("ZCard after update: got %d, want 1", z.ZCard())
	}
}

func TestZSetZRem(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 1.0)
	z.ZRem("alice")

	if _, ok := z.ZScore("alice"); ok {
		t.Error("ZScore after ZRem: alice should be gone")
	}
	if z.ZCard() != 0 {
		t.Errorf("ZCard after ZRem: got %d, want 0", z.ZCard())
	}
}

func TestZSetZRemMissingIsNoOp(t *testing.T) {
	z := NewZSetStructure()
	z.ZRem("nonexistent") // should not panic
}

func TestZSetZCard(t *testing.T) {
	z := NewZSetStructure()
	if z.ZCard() != 0 {
		t.Errorf("ZCard empty: got %d, want 0", z.ZCard())
	}
	z.ZAdd("a", 1.0)
	z.ZAdd("b", 2.0)
	if z.ZCard() != 2 {
		t.Errorf("ZCard: got %d, want 2", z.ZCard())
	}
}

func TestZSetZRankAscending(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 30.0)
	z.ZAdd("bob", 10.0)
	z.ZAdd("carol", 20.0)
	// Ascending: bob(10)=0, carol(20)=1, alice(30)=2

	cases := []struct {
		member string
		rank   int
	}{
		{"bob", 0},
		{"carol", 1},
		{"alice", 2},
	}
	for _, tc := range cases {
		got, ok := z.ZRank(tc.member)
		if !ok {
			t.Errorf("ZRank(%s): not found", tc.member)
			continue
		}
		if got != tc.rank {
			t.Errorf("ZRank(%s): got %d, want %d", tc.member, got, tc.rank)
		}
	}
}

func TestZSetZRankMissing(t *testing.T) {
	z := NewZSetStructure()
	if _, ok := z.ZRank("missing"); ok {
		t.Error("ZRank(missing): expected false")
	}
}

func TestZSetZRevRank(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 30.0)
	z.ZAdd("bob", 10.0)
	z.ZAdd("carol", 20.0)
	// Descending: alice(30)=0, carol(20)=1, bob(10)=2

	cases := []struct {
		member  string
		revRank int
	}{
		{"alice", 0},
		{"carol", 1},
		{"bob", 2},
	}
	for _, tc := range cases {
		got, ok := z.ZRevRank(tc.member)
		if !ok {
			t.Errorf("ZRevRank(%s): not found", tc.member)
			continue
		}
		if got != tc.revRank {
			t.Errorf("ZRevRank(%s): got %d, want %d", tc.member, got, tc.revRank)
		}
	}
}

func TestZSetZRankTieBrokenLexicographically(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("b", 5.0)
	z.ZAdd("a", 5.0)
	z.ZAdd("c", 5.0)
	// Same score → lex order: a=0, b=1, c=2

	if r, ok := z.ZRank("a"); !ok || r != 0 {
		t.Errorf("ZRank(a) tie: got %d ok=%v, want 0 true", r, ok)
	}
	if r, ok := z.ZRank("b"); !ok || r != 1 {
		t.Errorf("ZRank(b) tie: got %d ok=%v, want 1 true", r, ok)
	}
	if r, ok := z.ZRank("c"); !ok || r != 2 {
		t.Errorf("ZRank(c) tie: got %d ok=%v, want 2 true", r, ok)
	}
}

func TestZSetZRange(t *testing.T) {
	z := NewZSetStructure()
	for _, m := range []string{"a", "b", "c", "d", "e"} {
		z.ZAdd(m, float64(len(m))) // all score 1, lex order
	}
	// Score is 1 for all, so lex: a=0, b=1, c=2, d=3, e=4

	got := z.ZRange(1, 3)
	if len(got) != 3 {
		t.Fatalf("ZRange(1,3): got %d elements, want 3", len(got))
	}
	for i, want := range []string{"b", "c", "d"} {
		if got[i].Member != want {
			t.Errorf("ZRange(1,3)[%d]: got %q, want %q", i, got[i].Member, want)
		}
	}
}

func TestZSetZRangeNegative(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("a", 1.0)
	z.ZAdd("b", 2.0)
	z.ZAdd("c", 3.0)

	got := z.ZRange(-2, -1)
	if len(got) != 2 {
		t.Fatalf("ZRange(-2,-1): got %d elements, want 2", len(got))
	}
	if got[0].Member != "b" || got[1].Member != "c" {
		t.Errorf("ZRange(-2,-1): got [%s %s], want [b c]", got[0].Member, got[1].Member)
	}
}

func TestZSetZRangeEmpty(t *testing.T) {
	z := NewZSetStructure()
	if got := z.ZRange(0, 1); got != nil {
		t.Errorf("ZRange on empty: got %v, want nil", got)
	}
}

func TestZSetZRangeByScore(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("a", 1.0)
	z.ZAdd("b", 2.0)
	z.ZAdd("c", 3.0)
	z.ZAdd("d", 4.0)

	got := z.ZRangeByScore(2.0, 3.0)
	if len(got) != 2 {
		t.Fatalf("ZRangeByScore(2,3): got %d elements, want 2", len(got))
	}
	if got[0].Member != "b" || got[1].Member != "c" {
		t.Errorf("ZRangeByScore(2,3): got [%s %s], want [b c]", got[0].Member, got[1].Member)
	}
}

func TestZSetZRangeByScoreInclusive(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("x", 5.0)

	got := z.ZRangeByScore(5.0, 5.0)
	if len(got) != 1 || got[0].Member != "x" {
		t.Errorf("ZRangeByScore(5,5): got %v, want [{x 5}]", got)
	}
}

func TestZSetZRangeByScoreNoResults(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("x", 10.0)

	if got := z.ZRangeByScore(0, 5); got != nil {
		t.Errorf("ZRangeByScore: got %v, want nil", got)
	}
}

func TestZSetZRangeByScoreInvertedRange(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("x", 1.0)

	if got := z.ZRangeByScore(5, 1); got != nil {
		t.Errorf("ZRangeByScore inverted: got %v, want nil", got)
	}
}

func TestZSetByteSize(t *testing.T) {
	z := NewZSetStructure()
	emptyWant := int64(mtimeSize + keyExpirySize)
	if got := z.ByteSize(); got != emptyWant {
		t.Errorf("ByteSize empty: got %d, want %d", got, emptyWant)
	}

	member := "alice"
	z.ZAdd(member, 1.0)
	want := int64(len(member)) + mapEntryOverhead + zsetEntryOverhead + mtimeSize + keyExpirySize
	if got := z.ByteSize(); got != want {
		t.Errorf("ByteSize with one member: got %d, want %d", got, want)
	}
}

func TestZSetEncodeDecodeRoundTrip(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 10.5)
	z.ZAdd("bob", 3.0)
	z.ZAdd("carol", 7.25)
	z.SetKeyExpiry(8888)
	z.mtime = 99

	data, err := z.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeZSetStructure(data)
	if err != nil {
		t.Fatalf("DecodeZSetStructure: %v", err)
	}
	if decoded.KeyExpiry() != 8888 {
		t.Errorf("round-trip: KeyExpiry got %d, want 8888", decoded.KeyExpiry())
	}
	if decoded.MTime() != 99 {
		t.Errorf("round-trip: MTime got %d, want 99", decoded.MTime())
	}
	if decoded.ZCard() != 3 {
		t.Fatalf("round-trip: ZCard got %d, want 3", decoded.ZCard())
	}

	// Verify scores.
	for _, tc := range []struct {
		m string
		s float64
	}{{"alice", 10.5}, {"bob", 3.0}, {"carol", 7.25}} {
		got, ok := decoded.ZScore(tc.m)
		if !ok || got != tc.s {
			t.Errorf("round-trip ZScore(%s): got %v ok=%v, want %v true", tc.m, got, ok, tc.s)
		}
	}

	// Verify order is preserved: bob(3) < carol(7.25) < alice(10.5).
	entries := decoded.ZRange(0, -1)
	order := []string{"bob", "carol", "alice"}
	for i, want := range order {
		if entries[i].Member != want {
			t.Errorf("round-trip order[%d]: got %s, want %s", i, entries[i].Member, want)
		}
	}
}

func TestZSetCleanupIsNoOp(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("x", 1.0)
	if z.Cleanup(time.Now()) {
		t.Error("Cleanup: should always return false for zsets")
	}
	if z.ZCard() != 1 {
		t.Error("Cleanup: should not remove zset members")
	}
}

func TestZSetSortedOrderAfterUpdates(t *testing.T) {
	z := NewZSetStructure()
	z.ZAdd("alice", 50.0)
	z.ZAdd("bob", 20.0)
	z.ZAdd("carol", 80.0)
	z.ZAdd("alice", 10.0) // lower alice's score below bob

	// Expected order: alice(10) < bob(20) < carol(80)
	entries := z.ZRange(0, -1)
	want := []string{"alice", "bob", "carol"}
	if len(entries) != len(want) {
		t.Fatalf("ZRange after update: got %d entries, want %d", len(entries), len(want))
	}
	for i, w := range want {
		if entries[i].Member != w {
			t.Errorf("ZRange after update[%d]: got %s, want %s", i, entries[i].Member, w)
		}
	}
}
