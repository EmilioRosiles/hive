package tests

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// benchClusterNodes starts an n-node cluster with the given replication
// factor and waits for full membership before returning one Cluster per node.
//
// Run with a fixed iteration count (-benchtime=Nx) rather than the default
// auto-ranging duration: Go's benchmark runner otherwise re-invokes this
// function (and re-forms the whole cluster) multiple times while calibrating
// b.N, which is both slow and adds setup noise to the measurement.
func benchClusterNodes(b *testing.B, n, rf int) []*hive.Cluster {
	b.Helper()
	nodes := make([]*hive.Node, n)
	clusters := make([]*hive.Cluster, n)
	var seed string
	for i := range n {
		var seeds []string
		if seed != "" {
			seeds = []string{seed}
		}
		node, cluster := clusterNode(b, seeds, rf)
		nodes[i] = node
		clusters[i] = cluster
		if i == 0 {
			seed = addr(node)
		}
	}
	waitFor(b, 5*time.Second, "cluster formed", func() bool {
		for _, node := range nodes {
			if node.Cluster().AliveCount() != n {
				return false
			}
		}
		return true
	})
	return clusters
}

// benchKeySpace bounds the number of distinct keys written per run, so the
// benchmark measures steady-state write/replication throughput against a
// stable working set instead of unbounded store growth.
const benchKeySpace = 256

// BenchmarkCluster_ConcurrentWrites simulates concurrent clients spread
// across every node in a 3-node cluster, each writing to a shared key space.
// Comparing RF=1 (no replication) against RF=2/3 isolates the cost of
// replication fan-out itself.
func BenchmarkCluster_ConcurrentWrites(b *testing.B) {
	for _, rf := range []int{1, 2, 3} {
		b.Run(fmt.Sprintf("RF=%d", rf), func(b *testing.B) {
			caches := benchClusterNodes(b, 3, rf)
			stores := make([]*hive.ValueStore[Session], len(caches))
			for i, c := range caches {
				stores[i] = hive.NewValueStore[Session](c, "bench")
			}

			var counter atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := counter.Add(1)
					store := stores[n%uint64(len(stores))]
					key := fmt.Sprintf("k-%d", n%benchKeySpace)
					if err := store.Set(b.Context(), key, Session{UserID: int(n)}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// benchForwardingCluster starts a 3-node RF=2 cluster where node 0 is a pure
// relay (MemLimit: hive.Bytes(0)) and returns its Cluster handle, so every
// operation the benchmark issues is forwarded cross-node.
func benchForwardingCluster(b *testing.B) *hive.Cluster {
	b.Helper()
	relay, relayCluster := clusterNodeWithMemLimit(b, nil, 2, hive.Bytes(0))
	_, c2 := clusterNode(b, []string{addr(relay)}, 2)
	_, c3 := clusterNode(b, []string{addr(relay)}, 2)
	waitFor(b, 5*time.Second, "cluster formed", func() bool {
		return relayCluster.AliveCount() == 3 && c2.AliveCount() == 3 && c3.AliveCount() == 3
	})
	return relayCluster
}

// BenchmarkCluster_ForwardedSet measures Set latency/throughput when every
// operation must be routed cross-node — see benchForwardingCluster.
func BenchmarkCluster_ForwardedSet(b *testing.B) {
	cache := benchForwardingCluster(b)
	store := hive.NewValueStore[Session](cache, "bench")
	v := Session{UserID: 1, Token: "bench-token"}

	b.Run("SingleThreaded", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			if err := store.Set(b.Context(), fmt.Sprintf("k-%d", i%benchKeySpace), v); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Concurrent", func(b *testing.B) {
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n := counter.Add(1)
				key := fmt.Sprintf("k-%d", n%benchKeySpace)
				if err := store.Set(b.Context(), key, v); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// BenchmarkCluster_ForwardedGet mirrors BenchmarkCluster_ForwardedSet for
// Get, pre-populating the key space before timing starts.
func BenchmarkCluster_ForwardedGet(b *testing.B) {
	cache := benchForwardingCluster(b)
	store := hive.NewValueStore[Session](cache, "bench")
	v := Session{UserID: 1, Token: "bench-token"}
	for i := range benchKeySpace {
		if err := store.Set(b.Context(), fmt.Sprintf("k-%d", i), v); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("SingleThreaded", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			if _, err := store.Get(b.Context(), fmt.Sprintf("k-%d", i%benchKeySpace)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Concurrent", func(b *testing.B) {
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n := counter.Add(1)
				key := fmt.Sprintf("k-%d", n%benchKeySpace)
				if _, err := store.Get(b.Context(), key); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// BenchmarkCluster_ForwardedLock measures a full Lock+Unlock round trip when
// every op must be routed cross-node — see benchForwardingCluster. Each call
// uses a fresh key so it never contends with a prior or concurrent one.
func BenchmarkCluster_ForwardedLock(b *testing.B) {
	cache := benchForwardingCluster(b)
	store := hive.NewValueStore[Session](cache, "bench")
	v := Session{UserID: 1, Token: "bench-token"}

	b.Run("SingleThreaded", func(b *testing.B) {
		// Lock now requires the key to already exist, so pre-create every
		// key this benchmark will touch before timing starts.
		for i := range b.N {
			store.Set(b.Context(), fmt.Sprintf("lock-%d", i), v)
		}

		b.ResetTimer()
		for i := range b.N {
			lock, err := store.Lock(b.Context(), fmt.Sprintf("lock-%d", i), time.Minute)
			if err != nil {
				b.Fatal(err)
			}
			if err := lock.Unlock(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Concurrent", func(b *testing.B) {
		var counter atomic.Uint64
		for i := 1; i <= b.N; i++ {
			store.Set(b.Context(), fmt.Sprintf("lock-%d", i), v)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n := counter.Add(1)
				lock, err := store.Lock(b.Context(), fmt.Sprintf("lock-%d", n), time.Minute)
				if err != nil {
					b.Fatal(err)
				}
				if err := lock.Unlock(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
