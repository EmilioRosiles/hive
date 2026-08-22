package store

import (
	"testing"
	"time"
)

func TestValueByteSize(t *testing.T) {
	data := []byte("hello world")
	v := NewValueStructure(data)
	want := int64(len(data)) + mtimeSize + keyExpirySize
	if got := v.ByteSize(); got != want {
		t.Errorf("ByteSize: got %d, want %d", got, want)
	}
}

func TestValueByteSizeEmpty(t *testing.T) {
	v := NewValueStructure(nil)
	want := int64(mtimeSize + keyExpirySize)
	if got := v.ByteSize(); got != want {
		t.Errorf("ByteSize(nil): got %d, want %d", got, want)
	}
}

func TestValueEncode(t *testing.T) {
	data := []byte("payload")
	v := NewValueStructure(data)
	v.setMTime(42)
	enc, err := v.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Decode the round-trip and verify the fields are preserved.
	got, err := DecodeValueStructure(enc)
	if err != nil {
		t.Fatalf("DecodeValueStructure: %v", err)
	}
	if string(got.Data) != string(data) {
		t.Errorf("Data: got %q, want %q", got.Data, data)
	}
	if got.MTime() != 42 {
		t.Errorf("MTime: got %d, want 42", got.MTime())
	}
}

func TestValueKeyExpiry(t *testing.T) {
	v := NewValueStructure([]byte("v"))
	if v.KeyExpiry() != 0 {
		t.Error("KeyExpiry: new value should have no expiry")
	}
	v.SetKeyExpiry(12345)
	if v.KeyExpiry() != 12345 {
		t.Errorf("SetKeyExpiry: got %d, want 12345", v.KeyExpiry())
	}
	v.SetKeyExpiry(0)
	if v.KeyExpiry() != 0 {
		t.Error("SetKeyExpiry(0): should clear expiry")
	}
}

func TestNewValueStructureWithTTL(t *testing.T) {
	v := NewValueStructureWithTTL([]byte("v"), time.Hour)
	if v.KeyExpiry() == 0 {
		t.Error("NewValueStructureWithTTL: expiry should be set")
	}
	if v.KeyExpiry() <= uint32(time.Now().Unix()) {
		t.Error("NewValueStructureWithTTL: expiry should be in the future")
	}
}
