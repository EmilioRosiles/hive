package tests

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// benchClusterNodes starts an n-node cluster with the given replication
// factor and waits for full membership before returning one Cache per node.
//
// Run with a fixed iteration count (-benchtime=Nx) rather than the default
// auto-ranging duration: Go's benchmark runner otherwise re-invokes this
// function (and re-forms the whole cluster) multiple times while calibrating
// b.N, which is both slow and adds setup noise to the measurement.
func benchClusterNodes(b *testing.B, n, rf int) []*hive.Cache {
	b.Helper()
	nodes := make([]*hive.Node, n)
	caches := make([]*hive.Cache, n)
	var seed string
	for i := range n {
		var seeds []string
		if seed != "" {
			seeds = []string{seed}
		}
		node, cache := clusterNode(b, seeds, rf)
		nodes[i] = node
		caches[i] = cache
		if i == 0 {
			seed = addr(node)
		}
	}
	waitFor(b, 5*time.Second, "cluster formed", func() bool {
		for _, node := range nodes {
			if node.Status().Size != n {
				return false
			}
		}
		return true
	})
	return caches
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
					if err := store.Set(key, Session{UserID: int(n)}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
