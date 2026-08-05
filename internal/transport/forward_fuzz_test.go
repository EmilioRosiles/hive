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

func FuzzForwardBatchUnmarshal(f *testing.F) {
	seed, _ := (ForwardBatch{Requests: []ForwardRequest{
		{Op: OpValueSet, Key: "k", Args: [][]byte{[]byte("v")}},
		{Op: OpValueGet, Key: "k2"},
	}}).MarshalBinary()
	f.Add(seed)
	f.Add([]byte("garbage"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		var batch ForwardBatch
		_ = batch.UnmarshalBinary(data) // only requirement: must not panic
	})
}
