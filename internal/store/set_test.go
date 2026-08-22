package store

import (
	"slices"
	"sort"
	"testing"
)

func TestSetAddAndIsMember(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	if !ss.IsMember("alice") {
		t.Error("IsMember: alice should be a member")
	}
	if ss.IsMember("bob") {
		t.Error("IsMember: bob should not be a member")
	}
}

func TestSetAddIsIdempotent(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	ss.Add("alice")
	if n := ss.Card(); n != 1 {
		t.Errorf("Card after duplicate Add: got %d, want 1", n)
	}
}

func TestSetRemove(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	ss.Remove("alice")
	if ss.IsMember("alice") {
		t.Error("IsMember: alice should be removed")
	}
}

func TestSetRemoveMissingIsNoOp(t *testing.T) {
	ss := NewSetStructure()
	ss.Remove("nonexistent") // should not panic
}

func TestSetMembers(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	ss.Add("bob")

	members := ss.Members()
	sort.Strings(members)
	if !slices.Equal(members, []string{"alice", "bob"}) {
		t.Errorf("Members: got %v, want [alice bob]", members)
	}
}

func TestSetCard(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	ss.Add("bob")
	if n := ss.Card(); n != 2 {
		t.Errorf("Card: got %d, want 2", n)
	}
}

func TestSetByteSize(t *testing.T) {
	ss := NewSetStructure()
	emptyWant := int64(mtimeSize + keyExpirySize)
	if got := ss.ByteSize(); got != emptyWant {
		t.Errorf("ByteSize: empty set should be %d, got %d", emptyWant, got)
	}
	ss.Add("alice")
	ss.Add("bob")
	want := int64(len("alice")+mapBucketOverhead) + int64(len("bob")+mapBucketOverhead) + mtimeSize + keyExpirySize
	if got := ss.ByteSize(); got != want {
		t.Errorf("ByteSize: got %d, want %d", got, want)
	}
}

func TestSetEncodeDecodeRoundTrip(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	ss.Add("bob")
	ss.SetKeyExpiry(9999)

	data, err := ss.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeSetStructure(data)
	if err != nil {
		t.Fatalf("DecodeSetStructure: %v", err)
	}
	if decoded.KeyExpiry() != 9999 {
		t.Errorf("round-trip: KeyExpiry got %d, want 9999", decoded.KeyExpiry())
	}
	members := decoded.Members()
	sort.Strings(members)
	if !slices.Equal(members, []string{"alice", "bob"}) {
		t.Errorf("round-trip: Members got %v, want [alice bob]", members)
	}
}
