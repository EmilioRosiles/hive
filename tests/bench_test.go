package tests

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// Run with: go test ./tests/ -bench=. -benchmem

// -- ValueStore --

func BenchmarkValueStore_Set(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	v := Session{UserID: 1, Token: "bench-token"}

	b.ResetTimer()
	for i := range b.N {
		store.Set(b.Context(), fmt.Sprintf("key-%d", i), v)
	}
}

func BenchmarkValueStore_Get(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	store.Set(b.Context(), "key", Session{UserID: 1, Token: "bench-token"})

	for b.Loop() {
		store.Get(b.Context(), "key")
	}
}

func BenchmarkValueStore_Set_Parallel(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	v := Session{UserID: 1, Token: "bench-token"}
	var counter atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			store.Set(b.Context(), fmt.Sprintf("key-%d", n%1000), v)
		}
	})
}

func BenchmarkValueStore_Get_Parallel(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	store.Set(b.Context(), "key", Session{UserID: 1, Token: "bench-token"})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Get(b.Context(), "key")
		}
	})
}

// BenchmarkValueStore_SetGet_Parallel measures a 50/50 mixed read/write
// workload, unlike the pure Set/Get_Parallel benchmarks above.
func BenchmarkValueStore_SetGet_Parallel(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	v := Session{UserID: 1, Token: "bench-token"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			if i%2 == 0 {
				store.Set(b.Context(), key, v)
			} else {
				store.Get(b.Context(), key)
			}
			i++
		}
	})
}

// -- SetStore --

func BenchmarkSetStore_SAdd(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewSetStore(cache, "online")

	b.ResetTimer()
	for i := range b.N {
		store.SAdd(b.Context(), "room:1", fmt.Sprintf("user:%d", i))
	}
}

func BenchmarkSetStore_SIsMember(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewSetStore(cache, "online")
	for i := range 100 {
		store.SAdd(b.Context(), "room:1", fmt.Sprintf("user:%d", i))
	}

	b.ResetTimer()
	for i := range b.N {
		store.SIsMember(b.Context(), "room:1", fmt.Sprintf("user:%d", i%100))
	}
}

// -- HashStore --

func BenchmarkHashStore_HSet(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewHashStore[Stream](cache, "streams")
	v := Stream{BitRate: 1080, StartedAt: time.Now()}

	b.ResetTimer()
	for i := range b.N {
		store.HSet(b.Context(), "user:1", fmt.Sprintf("stream:%d", i), v)
	}
}

func BenchmarkHashStore_HGet(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewHashStore[Stream](cache, "streams")
	v := Stream{BitRate: 1080, StartedAt: time.Now()}
	store.HSet(b.Context(), "user:1", "stream:a", v)

	for b.Loop() {
		store.HGet(b.Context(), "user:1", "stream:a")
	}
}

func BenchmarkHashStore_HGetAll_10Fields(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewHashStore[Stream](cache, "streams")
	v := Stream{BitRate: 1080, StartedAt: time.Now()}
	for i := range 10 {
		store.HSet(b.Context(), "user:1", fmt.Sprintf("stream:%d", i), v)
	}

	for b.Loop() {
		store.HGetAll(b.Context(), "user:1")
	}
}

// -- Lock --

// BenchmarkValueStore_Lock measures the cost of a full Lock+Unlock round
// trip. Each iteration uses a fresh key so calls never contend with each
// other — this measures the mechanism's own overhead, not lock contention.
func BenchmarkValueStore_Lock(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	v := Session{UserID: 1, Token: "bench-token"}

	// Lock now requires the key to already exist, so pre-create every key
	// this benchmark will touch before timing starts.
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
}

func BenchmarkValueStore_Lock_Parallel(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	v := Session{UserID: 1, Token: "bench-token"}
	var counter atomic.Uint64

	// Lock now requires the key to already exist, so pre-create every key
	// this benchmark will touch before timing starts.
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
}

// -- value sizes --

func BenchmarkValueStore_Set_SmallValue(b *testing.B)  { benchSetSize(b, 16) }
func BenchmarkValueStore_Set_MediumValue(b *testing.B) { benchSetSize(b, 256) }
func BenchmarkValueStore_Set_LargeValue(b *testing.B)  { benchSetSize(b, 4096) }

func benchSetSize(b *testing.B, size int) {
	b.Helper()
	cache := benchStandalone(b)
	store := hive.NewValueStore[[]byte](cache, "blobs")
	v := make([]byte, size)

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := range b.N {
		store.Set(b.Context(), fmt.Sprintf("key-%d", i%1000), v)
	}
}

// -- helpers --

func benchStandalone(b *testing.B) *hive.Cluster {
	b.Helper()
	node, err := hive.NewNode(hive.Config{})
	if err != nil {
		b.Fatalf("benchStandalone: %v", err)
	}
	b.Cleanup(func() { node.Shutdown() })
	return node.Cluster()
}
