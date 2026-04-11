package store

import (
	"testing"
	"time"
)

func TestValueByteSize(t *testing.T) {
	data := []byte("hello world")
	v := NewValueStructure(data)
	if got := v.ByteSize(); got != int64(len(data)) {
		t.Errorf("ByteSize: got %d, want %d", got, len(data))
	}
}

func TestValueByteSizeEmpty(t *testing.T) {
	v := NewValueStructure(nil)
	if got := v.ByteSize(); got != 0 {
		t.Errorf("ByteSize(nil): got %d, want 0", got)
	}
}

func TestValueEncode(t *testing.T) {
	data := []byte("payload")
	v := NewValueStructure(data)
	enc, err := v.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(enc) != string(data) {
		t.Errorf("Encode: got %q, want %q", enc, data)
	}
}

func TestValueCleanupAlwaysFalse(t *testing.T) {
	v := NewValueStructure([]byte("v"))
	if v.Cleanup(time.Now()) {
		t.Error("Cleanup: plain value should never report empty")
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
	if v.KeyExpiry() <= time.Now().Unix() {
		t.Error("NewValueStructureWithTTL: expiry should be in the future")
	}
}
