package store

import "testing"

func TestBitmapSetAndGetBit(t *testing.T) {
	b := NewBitmapStructure()
	b.SetBit(0, true)
	b.SetBit(9, true)
	b.SetBit(123456, true)
	for _, off := range []uint32{0, 9, 123456} {
		if !b.GetBit(off) {
			t.Errorf("GetBit(%d): want set", off)
		}
	}
	for _, off := range []uint32{1, 8, 10, 123455, 123457, 1 << 30} {
		if b.GetBit(off) {
			t.Errorf("GetBit(%d): want clear", off)
		}
	}
}

func TestBitmapClearBit(t *testing.T) {
	b := NewBitmapStructure()
	b.SetBit(7, true)
	b.SetBit(7, false)
	if b.GetBit(7) {
		t.Error("GetBit(7): want clear after SetBit false")
	}
}

func TestBitmapClearPastEndDoesNotGrow(t *testing.T) {
	b := NewBitmapStructure()
	b.SetBit(1000, false)
	if n := len(b.bits); n != 0 {
		t.Errorf("len(bits) after clearing past end: got %d, want 0", n)
	}
}

func TestBitmapGrowsToFitOffset(t *testing.T) {
	b := NewBitmapStructure()
	b.SetBit(8, true)
	if n := len(b.bits); n != 2 {
		t.Errorf("len(bits) after SetBit(8): got %d, want 2", n)
	}
	b.SetBit(3, true)
	if n := len(b.bits); n != 2 {
		t.Errorf("len(bits) after SetBit(3): got %d, want 2", n)
	}
}

func TestBitmapCount(t *testing.T) {
	b := NewBitmapStructure()
	if n := b.Count(); n != 0 {
		t.Errorf("Count empty: got %d, want 0", n)
	}
	for _, off := range []uint32{0, 1, 2, 63, 64, 5000} {
		b.SetBit(off, true)
	}
	b.SetBit(1, true)
	if n := b.Count(); n != 6 {
		t.Errorf("Count: got %d, want 6", n)
	}
	b.SetBit(63, false)
	if n := b.Count(); n != 5 {
		t.Errorf("Count after clear: got %d, want 5", n)
	}
}

func TestBitmapByteSize(t *testing.T) {
	b := NewBitmapStructure()
	if got, want := b.ByteSize(), int64(mtimeSize+keyExpirySize); got != want {
		t.Errorf("ByteSize empty: got %d, want %d", got, want)
	}
	b.SetBit(8*99, true)
	if got, want := b.ByteSize(), int64(100+mtimeSize+keyExpirySize); got != want {
		t.Errorf("ByteSize: got %d, want %d", got, want)
	}
}

func TestBitmapEncodeDecodeRoundTrip(t *testing.T) {
	b := NewBitmapStructure()
	b.SetBit(3, true)
	b.SetBit(700, true)
	b.SetKeyExpiry(9999)
	b.SetLock(42, 8888)

	data, err := b.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeBitmapStructure(data)
	if err != nil {
		t.Fatalf("DecodeBitmapStructure: %v", err)
	}
	if decoded.KeyExpiry() != 9999 {
		t.Errorf("round-trip: KeyExpiry got %d, want 9999", decoded.KeyExpiry())
	}
	if decoded.LockToken() != 42 || decoded.LockExpiry() != 8888 {
		t.Errorf("round-trip: lock got (%d, %d), want (42, 8888)", decoded.LockToken(), decoded.LockExpiry())
	}
	if !decoded.GetBit(3) || !decoded.GetBit(700) || decoded.Count() != 2 {
		t.Errorf("round-trip: bits not preserved, Count=%d", decoded.Count())
	}
}

func TestBitmapDecodeEntry(t *testing.T) {
	b := NewBitmapStructure()
	b.SetBit(5, true)
	data, _ := b.Encode()

	e, err := NewDataStore(unlimited).DecodeEntry(KindBitmap, data)
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	if got, ok := e.(*BitmapStructure); !ok || !got.GetBit(5) {
		t.Errorf("DecodeEntry: got %T, want *BitmapStructure with bit 5 set", e)
	}
}
