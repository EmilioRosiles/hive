package transport

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// See the caveat in forward_bench_test.go about msgpack v5's auto-detection
// of encoding.BinaryMarshaler/BinaryUnmarshaler — it applies here too.

// benchRebalanceBatch builds a 100-entry batch matching the real batchSize
// used by cluster/rebalance.go's sendRebalanceBatch.
func benchRebalanceBatch() RebalanceBatch {
	entries := make([]RebalanceEntry, 100)
	for i := range entries {
		entries[i] = RebalanceEntry{
			Key:  "key-000000",
			Kind: 1,
			Data: make([]byte, 64),
			TTL:  3600_000_000_000,
		}
	}
	return RebalanceBatch{Entries: entries}
}

func BenchmarkRebalanceBatch_MarshalBinary(b *testing.B) {
	batch := benchRebalanceBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := batch.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebalanceBatch_MarshalMsgpack(b *testing.B) {
	batch := benchRebalanceBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := msgpack.Marshal(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebalanceBatch_UnmarshalBinary(b *testing.B) {
	data, _ := benchRebalanceBatch().MarshalBinary()
	b.ReportAllocs()
	for b.Loop() {
		var batch RebalanceBatch
		if err := batch.UnmarshalBinary(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebalanceBatch_UnmarshalMsgpack(b *testing.B) {
	data, _ := msgpack.Marshal(benchRebalanceBatch())
	b.ReportAllocs()
	for b.Loop() {
		var batch RebalanceBatch
		if err := msgpack.Unmarshal(data, &batch); err != nil {
			b.Fatal(err)
		}
	}
}
