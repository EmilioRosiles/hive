package store

import (
	"fmt"
	"strconv"
	"testing"
)

// zsetPreloaded returns a ZSetStructure with n members already inserted
// (score = index), and the list of member names in insertion order.
func zsetPreloaded(n int) (*ZSetStructure, []string) {
	z := NewZSetStructure()
	members := make([]string, n)
	for i := range n {
		m := fmt.Sprintf("member-%d", i)
		members[i] = m
		z.ZAdd(m, float64(i))
	}
	return z, members
}

// BenchmarkZSetZAdd_IntoExisting measures inserting one more member into a
// zset that already holds n members, at a score landing in the middle of the
// existing range — the representative case for a sorted-slice's O(n)
// tail-shift (an insert past the max score would be an O(1) append, not
// representative). The skip list should stay close to flat (O(log n)) as n
// grows.
func BenchmarkZSetZAdd_IntoExisting(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, _ := zsetPreloaded(n)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				z.ZAdd(fmt.Sprintf("bench-%d", i), float64(n)/2+0.5)
				z.ZRem(fmt.Sprintf("bench-%d", i)) // undo, so n stays constant across iterations
				i++
			}
		})
	}
}

// BenchmarkZSetZRem measures removing a member from a zset with n members.
// Old: O(n) tail-shift. New: O(log n).
func BenchmarkZSetZRem(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				z, members := zsetPreloaded(n)
				b.StartTimer()
				z.ZRem(members[n/2])
			}
		})
	}
}

// BenchmarkZSetZRank measures ranking a middle member in a zset with n
// members. Old: O(log n) via binary search on the slice (already fast).
// New: O(log n) via skip-list span walk — expected to be roughly comparable,
// included as a regression check rather than an expected win.
func BenchmarkZSetZRank(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, members := zsetPreloaded(n)
			target := members[n/2]
			b.ReportAllocs()
			for b.Loop() {
				if _, ok := z.ZRank(target); !ok {
					b.Fatal("member not found")
				}
			}
		})
	}
}

// BenchmarkZSetZScore / ZCard are pure map operations in both versions —
// included as a sanity check that they're genuinely unaffected.
func BenchmarkZSetZScore(b *testing.B) {
	for _, n := range []int{100, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, members := zsetPreloaded(n)
			target := members[n/2]
			b.ReportAllocs()
			for b.Loop() {
				z.ZScore(target)
			}
		})
	}
}

func BenchmarkZSetZCard(b *testing.B) {
	for _, n := range []int{100, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, _ := zsetPreloaded(n)
			b.ReportAllocs()
			for b.Loop() {
				z.ZCard()
			}
		})
	}
}

// BenchmarkZSetZRevRank mirrors ZRank (it's ZRank plus a subtraction).
func BenchmarkZSetZRevRank(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, members := zsetPreloaded(n)
			target := members[n/2]
			b.ReportAllocs()
			for b.Loop() {
				if _, ok := z.ZRevRank(target); !ok {
					b.Fatal("member not found")
				}
			}
		})
	}
}

// BenchmarkZSetZRange measures a mid-sized rank-range scan. Old: O(log n)
// find + a single contiguous slice copy. New: O(log n) find + a
// pointer-chasing walk across heap-allocated nodes — a real constant-factor
// question, not just an asymptotic one, since the elements aren't contiguous.
func BenchmarkZSetZRange(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, _ := zsetPreloaded(n)
			b.ReportAllocs()
			for b.Loop() {
				z.ZRange(0, n/2)
			}
		})
	}
}

// BenchmarkZSetZRangeByScore mirrors ZRange's walk cost, entered via a score
// descent instead of a rank descent.
func BenchmarkZSetZRangeByScore(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			z, _ := zsetPreloaded(n)
			b.ReportAllocs()
			for b.Loop() {
				z.ZRangeByScore(0, float64(n)/2)
			}
		})
	}
}
