package transport

import "testing"

func FuzzRebalanceBatchUnmarshal(f *testing.F) {
	seed, _ := (RebalanceBatch{Entries: []RebalanceEntry{
		{Key: "k1", Kind: 1, Data: []byte("d1"), TTL: 100},
	}}).MarshalBinary()
	f.Add(seed)
	f.Add([]byte("not-msgpack"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		var batch RebalanceBatch
		_ = batch.UnmarshalBinary(data)
	})
}
