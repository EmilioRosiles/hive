package store

import (
	"slices"
	"sort"
	"testing"
	"time"
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

func TestSetAddClearsTTL(t *testing.T) {
	ss := NewSetStructure()
	ss.AddWithTTL("alice", 10*time.Millisecond)
	ss.Add("alice") // re-add with no TTL
	time.Sleep(20 * time.Millisecond)
	if !ss.IsMember("alice") {
		t.Error("Add after AddWithTTL should clear TTL; alice should still be a member")
	}
}

func TestSetAddWithTTLExpires(t *testing.T) {
	ss := NewSetStructure()
	ss.AddWithTTL("alice", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if ss.IsMember("alice") {
		t.Error("IsMember: alice should have expired")
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
	ss.AddWithTTL("expired", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

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
	ss.AddWithTTL("expired", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if n := ss.Card(); n != 2 {
		t.Errorf("Card: got %d, want 2", n)
	}
}

func TestSetExpireMember(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("alice")
	ss.ExpireMember("alice", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if ss.IsMember("alice") {
		t.Error("ExpireMember: alice should have expired")
	}
}

func TestSetExpireMemberMissingIsNoOp(t *testing.T) {
	ss := NewSetStructure()
	ss.ExpireMember("nonexistent", time.Second) // should not panic
}

func TestSetCleanupRemovesExpiredMembers(t *testing.T) {
	ss := NewSetStructure()
	ss.Add("live")
	ss.AddWithTTL("dead", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	empty := ss.Cleanup(time.Now())
	if empty {
		t.Error("Cleanup: set should not be empty — live member remains")
	}
	if ss.IsMember("dead") {
		t.Error("Cleanup: expired member should be removed")
	}
}

func TestSetCleanupReportsEmptyWhenAllExpired(t *testing.T) {
	ss := NewSetStructure()
	ss.AddWithTTL("a", 10*time.Millisecond)
	ss.AddWithTTL("b", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if empty := ss.Cleanup(time.Now()); !empty {
		t.Error("Cleanup: should report empty when all members have expired")
	}
}

func TestSetByteSize(t *testing.T) {
	ss := NewSetStructure()
	emptyWant := int64(writeAtSize + keyExpirySize)
	if got := ss.ByteSize(); got != emptyWant {
		t.Errorf("ByteSize: empty set should be %d, got %d", emptyWant, got)
	}
	ss.Add("alice")
	ss.Add("bob")
	want := int64(len("alice")+mapEntryOverhead) + int64(len("bob")+mapEntryOverhead) + writeAtSize + keyExpirySize
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
