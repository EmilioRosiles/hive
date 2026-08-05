package transport

import (
	"bytes"
	"testing"
)

func TestRebalanceBatch_RoundTrip_ZeroEntries(t *testing.T) {
	batch := RebalanceBatch{}
	data, _ := batch.MarshalBinary()
	var got RebalanceBatch
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Entries) != 0 {
		t.Errorf("got %d entries, want 0", len(got.Entries))
	}
}

func TestRebalanceBatch_RoundTrip_MultipleEntries(t *testing.T) {
	batch := RebalanceBatch{Entries: []RebalanceEntry{
		{Key: "k1", Kind: 1, Data: []byte("d1"), TTL: 0},
		{Key: "日本語", Kind: 2, Data: []byte{}, TTL: 123456789},
		{Key: "k3", Kind: 3, Data: bytes.Repeat([]byte{0x7}, 4096), TTL: -1},
	}}
	data, err := batch.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var got RebalanceBatch
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Entries) != len(batch.Entries) {
		t.Fatalf("got %d entries, want %d", len(got.Entries), len(batch.Entries))
	}
	for i := range batch.Entries {
		want := batch.Entries[i]
		g := got.Entries[i]
		if g.Key != want.Key || g.Kind != want.Kind || g.TTL != want.TTL || !bytes.Equal(g.Data, want.Data) {
			t.Errorf("entry %d: got %+v, want %+v", i, g, want)
		}
	}
}

func TestRebalanceBatch_RoundTrip_ZeroLengthData(t *testing.T) {
	batch := RebalanceBatch{Entries: []RebalanceEntry{{Key: "k", Kind: 0, Data: nil, TTL: 0}}}
	data, _ := batch.MarshalBinary()
	var got RebalanceBatch
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if len(got.Entries[0].Data) != 0 {
		t.Errorf("got Data %v, want empty", got.Entries[0].Data)
	}
}

// -- malformed/truncated input --

func TestRebalanceBatch_Unmarshal_HugeEntryCount_RejectedFast(t *testing.T) {
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	var batch RebalanceBatch
	if err := batch.UnmarshalBinary(data); err == nil {
		t.Error("expected error for oversized entry count")
	}
}

func TestRebalanceBatch_Unmarshal_TrailingGarbage(t *testing.T) {
	full := RebalanceBatch{Entries: []RebalanceEntry{{Key: "k", Kind: 1, Data: []byte("d")}}}
	data, _ := full.MarshalBinary()
	data = append(data, 0x1)

	var got RebalanceBatch
	if err := got.UnmarshalBinary(data); err == nil {
		t.Error("expected error for trailing garbage")
	}
}

func TestRebalanceBatch_Unmarshal_EmptyInput(t *testing.T) {
	var batch RebalanceBatch
	if err := batch.UnmarshalBinary(nil); err == nil {
		t.Error("expected error for empty input")
	}
}
