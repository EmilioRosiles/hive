package tests

import (
	"fmt"
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
		store.Set(fmt.Sprintf("key-%d", i), v)
	}
}

func BenchmarkValueStore_Get(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewValueStore[Session](cache, "sessions")
	store.Set("key", Session{UserID: 1, Token: "bench-token"})

	for b.Loop() {
		store.Get("key")
	}
}

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
				store.Set(key, v)
			} else {
				store.Get(key)
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
		store.SAdd("room:1", fmt.Sprintf("user:%d", i))
	}
}

func BenchmarkSetStore_SIsMember(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewSetStore(cache, "online")
	for i := range 100 {
		store.SAdd("room:1", fmt.Sprintf("user:%d", i))
	}

	b.ResetTimer()
	for i := range b.N {
		store.SIsMember("room:1", fmt.Sprintf("user:%d", i%100))
	}
}

// -- HashStore --

func BenchmarkHashStore_HSet(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewHashStore[Stream](cache, "streams")
	v := Stream{BitRate: 1080, StartedAt: time.Now()}

	b.ResetTimer()
	for i := range b.N {
		store.HSet("user:1", fmt.Sprintf("stream:%d", i), v)
	}
}

func BenchmarkHashStore_HGet(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewHashStore[Stream](cache, "streams")
	v := Stream{BitRate: 1080, StartedAt: time.Now()}
	store.HSet("user:1", "stream:a", v)

	for b.Loop() {
		store.HGet("user:1", "stream:a")
	}
}

func BenchmarkHashStore_HGetAll_10Fields(b *testing.B) {
	cache := benchStandalone(b)
	store := hive.NewHashStore[Stream](cache, "streams")
	v := Stream{BitRate: 1080, StartedAt: time.Now()}
	for i := range 10 {
		store.HSet("user:1", fmt.Sprintf("stream:%d", i), v)
	}

	for b.Loop() {
		store.HGetAll("user:1")
	}
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
		store.Set(fmt.Sprintf("key-%d", i%1000), v)
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
