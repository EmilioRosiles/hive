package store

import (
	"bytes"
	"testing"
	"time"
)

func TestListLPushRPush(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))
	l.LPush([]byte("z"))
	// Expected order: z, a, b
	if l.Len() != 3 {
		t.Fatalf("Len: got %d, want 3", l.Len())
	}
	for i, want := range []string{"z", "a", "b"} {
		v, ok := l.Index(i)
		if !ok {
			t.Fatalf("Index(%d): not found", i)
		}
		if string(v) != want {
			t.Errorf("Index(%d): got %q, want %q", i, v, want)
		}
	}
}

func TestListLPop(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("first"))
	l.RPush([]byte("second"))

	v, ok := l.LPop()
	if !ok {
		t.Fatal("LPop: expected element, got none")
	}
	if string(v) != "first" {
		t.Errorf("LPop: got %q, want %q", v, "first")
	}
	if l.Len() != 1 {
		t.Errorf("Len after LPop: got %d, want 1", l.Len())
	}
}

func TestListRPop(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("first"))
	l.RPush([]byte("second"))

	v, ok := l.RPop()
	if !ok {
		t.Fatal("RPop: expected element, got none")
	}
	if string(v) != "second" {
		t.Errorf("RPop: got %q, want %q", v, "second")
	}
	if l.Len() != 1 {
		t.Errorf("Len after RPop: got %d, want 1", l.Len())
	}
}

func TestListPopEmpty(t *testing.T) {
	l := NewListStructure()
	if _, ok := l.LPop(); ok {
		t.Error("LPop on empty list: expected false")
	}
	if _, ok := l.RPop(); ok {
		t.Error("RPop on empty list: expected false")
	}
}

func TestListIndexNegative(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))
	l.RPush([]byte("c"))

	v, ok := l.Index(-1)
	if !ok || string(v) != "c" {
		t.Errorf("Index(-1): got %q ok=%v, want c true", v, ok)
	}
	v, ok = l.Index(-3)
	if !ok || string(v) != "a" {
		t.Errorf("Index(-3): got %q ok=%v, want a true", v, ok)
	}
}

func TestListIndexOutOfBounds(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("x"))

	if _, ok := l.Index(1); ok {
		t.Error("Index(1) on single-element list: expected false")
	}
	if _, ok := l.Index(-2); ok {
		t.Error("Index(-2) on single-element list: expected false")
	}
}

func TestListSet(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("old"))
	if !l.Set(0, []byte("new")) {
		t.Fatal("Set(0): returned false on valid index")
	}
	v, _ := l.Index(0)
	if string(v) != "new" {
		t.Errorf("Set(0): got %q, want %q", v, "new")
	}
}

func TestListSetOutOfBounds(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("x"))
	if l.Set(5, []byte("y")) {
		t.Error("Set(5): expected false on out-of-bounds index")
	}
}

func TestListRange(t *testing.T) {
	l := NewListStructure()
	for _, s := range []string{"a", "b", "c", "d", "e"} {
		l.RPush([]byte(s))
	}

	got := l.Range(1, 3)
	if len(got) != 3 {
		t.Fatalf("Range(1,3): got %d elements, want 3", len(got))
	}
	for i, want := range []string{"b", "c", "d"} {
		if string(got[i]) != want {
			t.Errorf("Range(1,3)[%d]: got %q, want %q", i, got[i], want)
		}
	}
}

func TestListRangeNegative(t *testing.T) {
	l := NewListStructure()
	for _, s := range []string{"a", "b", "c"} {
		l.RPush([]byte(s))
	}

	got := l.Range(-2, -1)
	if len(got) != 2 {
		t.Fatalf("Range(-2,-1): got %d elements, want 2", len(got))
	}
	if string(got[0]) != "b" || string(got[1]) != "c" {
		t.Errorf("Range(-2,-1): got [%q %q], want [b c]", got[0], got[1])
	}
}

func TestListRangeClipped(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))

	got := l.Range(0, 100)
	if len(got) != 2 {
		t.Errorf("Range(0,100) with 2 elements: got %d, want 2", len(got))
	}
}

func TestListRangeEmpty(t *testing.T) {
	l := NewListStructure()
	if got := l.Range(0, 1); got != nil {
		t.Errorf("Range on empty list: got %v, want nil", got)
	}
}

func TestListRangeInverted(t *testing.T) {
	l := NewListStructure()
	for _, s := range []string{"a", "b", "c"} {
		l.RPush([]byte(s))
	}
	// After normalization: start=2 (clamps to 2), stop=0 → s > e → nil.
	if got := l.Range(2, 0); got != nil {
		t.Errorf("Range with normalized start > stop: got %v, want nil", got)
	}
}

func TestListByteSize(t *testing.T) {
	l := NewListStructure()
	emptyWant := int64(mtimeSize + keyExpirySize)
	if got := l.ByteSize(); got != emptyWant {
		t.Errorf("ByteSize: empty list should be %d, got %d", emptyWant, got)
	}

	data := []byte("hello")
	l.RPush(data)
	want := int64(len(data)) + sliceItemOverhead + mtimeSize + keyExpirySize
	if got := l.ByteSize(); got != want {
		t.Errorf("ByteSize: got %d, want %d", got, want)
	}
}

func TestListEncodeDecodeRoundTrip(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("alpha"))
	l.RPush([]byte("beta"))
	l.SetKeyExpiry(1234)
	l.mtime = 42

	data, err := l.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeListStructure(data)
	if err != nil {
		t.Fatalf("DecodeListStructure: %v", err)
	}
	if decoded.KeyExpiry() != 1234 {
		t.Errorf("round-trip: KeyExpiry got %d, want 1234", decoded.KeyExpiry())
	}
	if decoded.MTime() != 42 {
		t.Errorf("round-trip: MTime got %d, want 42", decoded.MTime())
	}
	if decoded.Len() != 2 {
		t.Fatalf("round-trip: Len got %d, want 2", decoded.Len())
	}
	if v, _ := decoded.Index(0); string(v) != "alpha" {
		t.Errorf("round-trip: Index(0) got %q, want alpha", v)
	}
	if v, _ := decoded.Index(1); string(v) != "beta" {
		t.Errorf("round-trip: Index(1) got %q, want beta", v)
	}
}

func TestListDecodeEmpty(t *testing.T) {
	l := NewListStructure()
	data, err := l.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeListStructure(data)
	if err != nil {
		t.Fatalf("DecodeListStructure: %v", err)
	}
	if decoded.Len() != 0 {
		t.Errorf("DecodeListStructure empty: Len got %d, want 0", decoded.Len())
	}
}

func TestListCleanupIsNoOp(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("x"))
	if l.Cleanup(time.Now()) {
		t.Error("Cleanup: should always return false for lists")
	}
	if l.Len() != 1 {
		t.Error("Cleanup: should not remove list elements")
	}
}

func TestListRangeReturnsIndependentCopy(t *testing.T) {
	l := NewListStructure()
	l.RPush([]byte("original"))
	got := l.Range(0, 0)
	got[0] = []byte("mutated")
	v, _ := l.Index(0)
	if !bytes.Equal(v, []byte("original")) {
		t.Error("Range: mutating result slice should not affect the list")
	}
}
