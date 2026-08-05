package store

import (
	"strconv"
	"testing"
)

// BenchmarkListLPush_Sustained pushes N items via LPush into a single list,
// measuring the amortized per-push cost. With the ring-buffer backing this
// should stay flat (O(1) amortized) regardless of N; the old append-based
// implementation was O(n) per push, so its per-op cost grew linearly with N.
func BenchmarkListLPush_Sustained(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				l := NewListStructure()
				for range n {
					l.LPush([]byte("x"))
				}
			}
		})
	}
}

func listPreloaded(n int) *ListStructure {
	l := NewListStructure()
	for range n {
		l.RPush([]byte("x"))
	}
	return l
}

// BenchmarkListRPush_Sustained mirrors LPush_Sustained for the tail-push
// path, which was already O(1) amortized in the old append-based
// implementation — expected to stay roughly comparable, not a regression.
func BenchmarkListRPush_Sustained(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				l := NewListStructure()
				for range n {
					l.RPush([]byte("x"))
				}
			}
		})
	}
}

// BenchmarkListLPop / RPop measure single-pop cost against a preloaded list
// of size n — both were already O(1) in the old implementation.
func BenchmarkListLPop(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				l := listPreloaded(n)
				b.StartTimer()
				l.LPop()
			}
		})
	}
}

func BenchmarkListRPop(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				l := listPreloaded(n)
				b.StartTimer()
				l.RPop()
			}
		})
	}
}

// BenchmarkListIndex / Set measure O(1) random access — the ring buffer adds
// a modulo operation versus the old direct slice index, a constant-factor
// question rather than a complexity one.
func BenchmarkListIndex(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			l := listPreloaded(n)
			b.ReportAllocs()
			for b.Loop() {
				l.Index(n / 2)
			}
		})
	}
}

func BenchmarkListSet(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			l := listPreloaded(n)
			b.ReportAllocs()
			for b.Loop() {
				l.Set(n/2, []byte("y"))
			}
		})
	}
}

// BenchmarkListRange measures a mid-sized range scan. The old implementation
// used a single builtin copy() over a contiguous slice; the ring buffer must
// walk element-by-element to handle wraparound, even when a given range
// doesn't actually wrap — a real constant-factor cost to check honestly.
func BenchmarkListRange(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			l := listPreloaded(n)
			b.ReportAllocs()
			for b.Loop() {
				l.Range(0, n/2)
			}
		})
	}
}
