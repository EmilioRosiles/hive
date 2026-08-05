package transport

import "testing"

func FuzzForwardRequestUnmarshal(f *testing.F) {
	seed, _ := (ForwardRequest{Op: OpValueSet, Key: "k", Args: [][]byte{[]byte("v")}}).MarshalBinary()
	f.Add(seed)
	f.Add([]byte("garbage"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		var req ForwardRequest
		_ = req.UnmarshalBinary(data) // only requirement: must not panic
	})
}

func FuzzForwardResponseUnmarshal(f *testing.F) {
	seed, _ := (ForwardResponse{Results: [][]byte{[]byte("a"), []byte("b")}}).MarshalBinary()
	f.Add(seed)
	f.Add([]byte("garbage"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		var resp ForwardResponse
		_ = resp.UnmarshalBinary(data)
	})
}

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
