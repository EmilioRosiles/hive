package store

import (
	"sort"
	"testing"
	"time"
)

func TestHashHSetAndHGet(t *testing.T) {
	h := NewHashStructure()
	h.HSet("f", []byte("value"))
	data, ok := h.HGet("f")
	if !ok {
		t.Fatal("HGet: field should exist")
	}
	if string(data) != "value" {
		t.Errorf("HGet: got %q, want %q", data, "value")
	}
}

func TestHashHGetMissingField(t *testing.T) {
	h := NewHashStructure()
	if _, ok := h.HGet("missing"); ok {
		t.Error("HGet: should return false for missing field")
	}
}

func TestHashHSetClearsTTL(t *testing.T) {
	h := NewHashStructure()
	h.HSetWithTTL("f", []byte("v"), 10*time.Millisecond)
	h.HSet("f", []byte("v")) // re-set without TTL
	time.Sleep(20 * time.Millisecond)
	if _, ok := h.HGet("f"); !ok {
		t.Error("HSet after HSetWithTTL should clear TTL; field should still exist")
	}
}

func TestHashHSetWithTTLExpires(t *testing.T) {
	h := NewHashStructure()
	h.HSetWithTTL("f", []byte("v"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := h.HGet("f"); ok {
		t.Error("HGet: field should have expired")
	}
}

func TestHashHDel(t *testing.T) {
	h := NewHashStructure()
	h.HSet("f", []byte("v"))
	h.HDel("f")
	if _, ok := h.HGet("f"); ok {
		t.Error("HGet: field should be gone after HDel")
	}
}

func TestHashHDelMissingIsNoOp(t *testing.T) {
	h := NewHashStructure()
	h.HDel("nonexistent") // should not panic
}

func TestHashFields(t *testing.T) {
	h := NewHashStructure()
	h.HSet("a", []byte("1"))
	h.HSet("b", []byte("2"))
	h.HSetWithTTL("expired", []byte("x"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	fields := h.Fields()
	sort.Strings(fields)
	if len(fields) != 2 || fields[0] != "a" || fields[1] != "b" {
		t.Errorf("Fields: got %v, want [a b]", fields)
	}
}

func TestHashAppendAll(t *testing.T) {
	h := NewHashStructure()
	h.HSet("a", []byte("1"))
	h.HSet("b", []byte("2"))
	h.HSetWithTTL("expired", []byte("x"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	pairs := h.AppendAll(nil)
	if len(pairs) != 4 { // 2 live fields × (name + value)
		t.Errorf("AppendAll: got %d elements, want 4", len(pairs))
	}
	// Build a map from the pairs to check values regardless of iteration order.
	got := make(map[string]string)
	for i := 0; i+1 < len(pairs); i += 2 {
		got[string(pairs[i])] = string(pairs[i+1])
	}
	if got["a"] != "1" || got["b"] != "2" {
		t.Errorf("AppendAll: got %v, want {a:1 b:2}", got)
	}
}

func TestHashExpireField(t *testing.T) {
	h := NewHashStructure()
	h.HSet("f", []byte("v"))
	h.ExpireField("f", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := h.HGet("f"); ok {
		t.Error("ExpireField: field should have expired")
	}
}

func TestHashExpireFieldMissingIsNoOp(t *testing.T) {
	h := NewHashStructure()
	h.ExpireField("nonexistent", time.Second) // should not panic
}

func TestHashLen(t *testing.T) {
	h := NewHashStructure()
	h.HSet("a", []byte("1"))
	h.HSet("b", []byte("2"))
	if h.Len() != 2 {
		t.Errorf("Len: got %d, want 2", h.Len())
	}
}

func TestHashCleanupRemovesExpiredFields(t *testing.T) {
	h := NewHashStructure()
	h.HSet("live", []byte("v"))
	h.HSetWithTTL("dead", []byte("v"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	empty := h.Cleanup(time.Now())
	if empty {
		t.Error("Cleanup: hash should not be empty — live field remains")
	}
	if _, ok := h.HGet("dead"); ok {
		t.Error("Cleanup: expired field should be removed")
	}
}

func TestHashCleanupReportsEmptyWhenAllExpired(t *testing.T) {
	h := NewHashStructure()
	h.HSetWithTTL("a", []byte("v"), 10*time.Millisecond)
	h.HSetWithTTL("b", []byte("v"), 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if empty := h.Cleanup(time.Now()); !empty {
		t.Error("Cleanup: should report empty when all fields have expired")
	}
}

func TestHashByteSize(t *testing.T) {
	h := NewHashStructure()
	emptyWant := int64(mtimeSize + keyExpirySize)
	if got := h.ByteSize(); got != emptyWant {
		t.Errorf("ByteSize: empty hash should be %d, got %d", emptyWant, got)
	}
	h.HSet("field", []byte("data"))
	want := int64(len("field")+len("data")) + mapEntryOverhead + mtimeSize + keyExpirySize
	if got := h.ByteSize(); got != want {
		t.Errorf("ByteSize: got %d, want %d", got, want)
	}
}

func TestHashEncodeDecodeRoundTrip(t *testing.T) {
	h := NewHashStructure()
	h.HSet("f1", []byte("v1"))
	h.HSet("f2", []byte("v2"))
	h.SetKeyExpiry(9999)

	data, err := h.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeHashStructure(data)
	if err != nil {
		t.Fatalf("DecodeHashStructure: %v", err)
	}
	if decoded.KeyExpiry() != 9999 {
		t.Errorf("round-trip: KeyExpiry got %d, want 9999", decoded.KeyExpiry())
	}
	if v, ok := decoded.HGet("f1"); !ok || string(v) != "v1" {
		t.Errorf("round-trip: f1 got %q ok=%v, want v1 true", v, ok)
	}
	if v, ok := decoded.HGet("f2"); !ok || string(v) != "v2" {
		t.Errorf("round-trip: f2 got %q ok=%v, want v2 true", v, ok)
	}
}
