package transport

import (
	"bytes"
	"testing"
)

// -- ForwardRequest --

func TestForwardRequest_RoundTrip_NilArgs(t *testing.T) {
	req := ForwardRequest{Op: OpValueGet, Key: "k"}
	data, err := req.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got ForwardRequest
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if got.Op != req.Op || got.Key != req.Key || len(got.Args) != 0 {
		t.Errorf("got %+v, want %+v", got, req)
	}
}

func TestForwardRequest_RoundTrip_EmptyArgsSlice(t *testing.T) {
	req := ForwardRequest{Op: OpValueGet, Key: "k", Args: [][]byte{}}
	data, _ := req.MarshalBinary()
	var got ForwardRequest
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Args) != 0 {
		t.Errorf("got %v args, want 0", len(got.Args))
	}
}

func TestForwardRequest_RoundTrip_MultipleArgs(t *testing.T) {
	req := ForwardRequest{
		Op:   OpHSet,
		Key:  "hash-key",
		Args: [][]byte{[]byte("field"), []byte("value"), {}},
	}
	data, err := req.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got ForwardRequest
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if got.Op != req.Op || got.Key != req.Key || len(got.Args) != len(req.Args) {
		t.Fatalf("got %+v, want %+v", got, req)
	}
	for i := range req.Args {
		if !bytes.Equal(got.Args[i], req.Args[i]) {
			t.Errorf("arg %d: got %v, want %v", i, got.Args[i], req.Args[i])
		}
	}
}

func TestForwardRequest_RoundTrip_UnicodeKey(t *testing.T) {
	req := ForwardRequest{Op: OpValueSet, Key: "日本語キー", Args: [][]byte{[]byte("value")}}
	data, _ := req.MarshalBinary()
	var got ForwardRequest
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if got.Key != req.Key {
		t.Errorf("got key %q, want %q", got.Key, req.Key)
	}
}

func TestForwardRequest_RoundTrip_LargeArg(t *testing.T) {
	large := bytes.Repeat([]byte{0x42}, 1<<20) // 1 MiB
	req := ForwardRequest{Op: OpValueSet, Key: "big", Args: [][]byte{large}}
	data, err := req.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got ForwardRequest
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if !bytes.Equal(got.Args[0], large) {
		t.Error("large arg mismatch after round trip")
	}
}

// -- ForwardResponse --

func TestForwardResponse_RoundTrip_NilResults(t *testing.T) {
	resp := ForwardResponse{}
	data, _ := resp.MarshalBinary()
	var got ForwardResponse
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Results) != 0 {
		t.Errorf("got %v results, want 0", len(got.Results))
	}
}

func TestForwardResponse_RoundTrip_MultipleResults(t *testing.T) {
	resp := ForwardResponse{Results: [][]byte{[]byte("a"), {}, []byte("ccc")}}
	data, err := resp.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got ForwardResponse
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Results) != len(resp.Results) {
		t.Fatalf("got %d results, want %d", len(got.Results), len(resp.Results))
	}
	for i := range resp.Results {
		if !bytes.Equal(got.Results[i], resp.Results[i]) {
			t.Errorf("result %d: got %v, want %v", i, got.Results[i], resp.Results[i])
		}
	}
}

// -- ForwardBatch --

func TestForwardBatch_RoundTrip_Empty(t *testing.T) {
	batch := ForwardBatch{}
	data, err := batch.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got ForwardBatch
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Requests) != 0 {
		t.Errorf("got %d requests, want 0", len(got.Requests))
	}
}

func TestForwardBatch_RoundTrip_MultipleRequests(t *testing.T) {
	batch := ForwardBatch{Requests: []ForwardRequest{
		{Op: OpValueSet, Key: "a", Args: [][]byte{[]byte("1")}},
		{Op: OpHSet, Key: "b", Args: [][]byte{[]byte("field"), []byte("value")}},
		{Op: OpValueGet, Key: "c"},
	}}
	data, err := batch.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got ForwardBatch
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Requests) != len(batch.Requests) {
		t.Fatalf("got %d requests, want %d", len(got.Requests), len(batch.Requests))
	}
	for i := range batch.Requests {
		want, got := batch.Requests[i], got.Requests[i]
		if got.Op != want.Op || got.Key != want.Key || len(got.Args) != len(want.Args) {
			t.Fatalf("request %d: got %+v, want %+v", i, got, want)
		}
		for j := range want.Args {
			if !bytes.Equal(got.Args[j], want.Args[j]) {
				t.Errorf("request %d arg %d: got %v, want %v", i, j, got.Args[j], want.Args[j])
			}
		}
	}
}

func TestForwardBatch_Unmarshal_EmptyInput(t *testing.T) {
	var batch ForwardBatch
	if err := batch.UnmarshalBinary(nil); err == nil {
		t.Error("expected error for empty input")
	}
}

func TestForwardBatch_Unmarshal_TruncatedMidElement(t *testing.T) {
	full := ForwardBatch{Requests: []ForwardRequest{
		{Op: OpValueSet, Key: "a", Args: [][]byte{[]byte("1")}},
		{Op: OpValueSet, Key: "b", Args: [][]byte{[]byte("2")}},
	}}
	data, _ := full.MarshalBinary()
	truncated := data[:len(data)-2]

	var batch ForwardBatch
	if err := batch.UnmarshalBinary(truncated); err == nil {
		t.Error("expected error for truncated payload")
	}
}

func TestForwardBatch_Unmarshal_TrailingGarbage(t *testing.T) {
	full := ForwardBatch{Requests: []ForwardRequest{{Op: OpValueSet, Key: "a"}}}
	data, _ := full.MarshalBinary()
	data = append(data, 0xFF, 0xFF)

	var batch ForwardBatch
	if err := batch.UnmarshalBinary(data); err == nil {
		t.Error("expected error for trailing garbage")
	}
}

func TestForwardBatch_Unmarshal_HugeRequestCount_RejectedFast(t *testing.T) {
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	var batch ForwardBatch
	if err := batch.UnmarshalBinary(data); err == nil {
		t.Error("expected error for oversized request count")
	}
}

// -- malformed/truncated input --

func TestForwardRequest_Unmarshal_EmptyInput(t *testing.T) {
	var req ForwardRequest
	if err := req.UnmarshalBinary(nil); err == nil {
		t.Error("expected error for empty input")
	}
}

func TestForwardRequest_Unmarshal_SingleByte(t *testing.T) {
	var req ForwardRequest
	if err := req.UnmarshalBinary([]byte{1}); err == nil {
		t.Error("expected error for single-byte input")
	}
}

func TestForwardRequest_Unmarshal_TruncatedMidElement(t *testing.T) {
	full := ForwardRequest{Op: OpValueSet, Key: "key", Args: [][]byte{[]byte("value")}}
	data, _ := full.MarshalBinary()
	truncated := data[:len(data)-2]

	var req ForwardRequest
	if err := req.UnmarshalBinary(truncated); err == nil {
		t.Error("expected error for truncated payload")
	}
}

func TestForwardRequest_Unmarshal_TrailingGarbage(t *testing.T) {
	full := ForwardRequest{Op: OpValueSet, Key: "key"}
	data, _ := full.MarshalBinary()
	data = append(data, 0xFF, 0xFF)

	var req ForwardRequest
	if err := req.UnmarshalBinary(data); err == nil {
		t.Error("expected error for trailing garbage")
	}
}

func TestForwardRequest_Unmarshal_HugeArgCount_RejectedFast(t *testing.T) {
	// Op(1) + KeyLen=0(4) + huge ArgCount, with only a few bytes remaining.
	data := []byte{0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}
	var req ForwardRequest
	if err := req.UnmarshalBinary(data); err == nil {
		t.Error("expected error for oversized arg count")
	}
}

func TestForwardResponse_Unmarshal_HugeResultCount_RejectedFast(t *testing.T) {
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	var resp ForwardResponse
	if err := resp.UnmarshalBinary(data); err == nil {
		t.Error("expected error for oversized result count")
	}
}

func TestForwardResponse_Unmarshal_EmptyInput(t *testing.T) {
	var resp ForwardResponse
	if err := resp.UnmarshalBinary(nil); err == nil {
		t.Error("expected error for empty input")
	}
}
