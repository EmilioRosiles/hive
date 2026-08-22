package tests

import (
	"errors"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive"
)

// -- ValueStore --

func TestValueStore_SetGet(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	want := Session{UserID: 1, Token: "abc"}
	if err := store.Set(t.Context(), "s1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := store.Get(t.Context(), "s1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestValueStore_Del(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	store.Set(t.Context(), "s1", Session{UserID: 1})
	if err := store.Del(t.Context(), "s1"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	_, err := store.Get(t.Context(), "s1")
	if !errors.Is(err, hive.ErrNotFound) {
		t.Fatalf("after Del: got %v, want ErrNotFound", err)
	}
}

func TestValueStore_GetMissing(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	_, err := store.Get(t.Context(), "missing")
	if !errors.Is(err, hive.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestValueStore_Expire(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	store.Set(t.Context(), "s1", Session{UserID: 1})
	store.Expire(t.Context(), "s1", 1100*time.Millisecond)

	time.Sleep(1300 * time.Millisecond)

	_, err := store.Get(t.Context(), "s1")
	if !errors.Is(err, hive.ErrNotFound) {
		t.Fatalf("after expiry: got %v, want ErrNotFound", err)
	}
}

func TestValueStore_Overwrite(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	store.Set(t.Context(), "s1", Session{UserID: 1, Token: "old"})
	store.Set(t.Context(), "s1", Session{UserID: 1, Token: "new"})

	got, err := store.Get(t.Context(), "s1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Token != "new" {
		t.Fatalf("got token %q, want %q", got.Token, "new")
	}
}

// -- SetStore --

func TestSetStore_AddMembers(t *testing.T) {
	cache := standalone(t)
	store := hive.NewSetStore(cache, "online")

	store.SAdd(t.Context(), "room:1", "user:1")
	store.SAdd(t.Context(), "room:1", "user:2")
	store.SAdd(t.Context(), "room:1", "user:3")

	n, err := store.SCard(t.Context(), "room:1")
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if n != 3 {
		t.Fatalf("SCard: got %d, want 3", n)
	}
}

func TestSetStore_IsMember(t *testing.T) {
	cache := standalone(t)
	store := hive.NewSetStore(cache, "online")

	store.SAdd(t.Context(), "room:1", "user:1")

	ok, err := store.SIsMember(t.Context(), "room:1", "user:1")
	if err != nil || !ok {
		t.Fatalf("SIsMember existing: got (%v, %v)", ok, err)
	}

	ok, err = store.SIsMember(t.Context(), "room:1", "user:99")
	if err != nil || ok {
		t.Fatalf("SIsMember missing: got (%v, %v)", ok, err)
	}
}

func TestSetStore_Remove(t *testing.T) {
	cache := standalone(t)
	store := hive.NewSetStore(cache, "online")

	store.SAdd(t.Context(), "room:1", "user:1")
	store.SAdd(t.Context(), "room:1", "user:2")
	store.SRem(t.Context(), "room:1", "user:1")

	members, err := store.SMembers(t.Context(), "room:1")
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 1 || members[0] != "user:2" {
		t.Fatalf("SMembers: got %v, want [user:2]", members)
	}
}

func TestSetStore_MemberExpiry(t *testing.T) {
	cache := standalone(t)
	store := hive.NewSetStore(cache, "online")

	store.SAdd(t.Context(), "room:1", "user:1")
	store.SAdd(t.Context(), "room:1", "user:2")
	store.SExpireMember(t.Context(), "room:1", "user:1", 1100*time.Millisecond)

	time.Sleep(1300 * time.Millisecond)

	// user:1 expired, user:2 still alive
	ok, _ := store.SIsMember(t.Context(), "room:1", "user:1")
	if ok {
		t.Fatal("expired member still present")
	}
	ok, _ = store.SIsMember(t.Context(), "room:1", "user:2")
	if !ok {
		t.Fatal("non-expired member missing")
	}
}

func TestSetStore_KeyExpiry(t *testing.T) {
	cache := standalone(t)
	store := hive.NewSetStore(cache, "online")

	store.SAdd(t.Context(), "room:1", "user:1")
	store.Expire(t.Context(), "room:1", 1100*time.Millisecond)

	time.Sleep(1300 * time.Millisecond)

	n, err := store.SCard(t.Context(), "room:1")
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if n != 0 {
		t.Fatalf("SCard after key expiry: got %d, want 0", n)
	}
}

// -- HashStore --

func TestHashStore_SetGet(t *testing.T) {
	cache := standalone(t)
	store := hive.NewHashStore[Stream](cache, "streams")

	want := Stream{BitRate: 1080, StartedAt: time.Now().Round(time.Millisecond)}
	if err := store.HSet(t.Context(), "user:1", "stream:a", want); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	got, err := store.HGet(t.Context(), "user:1", "stream:a")
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestHashStore_MultipleFields(t *testing.T) {
	cache := standalone(t)
	store := hive.NewHashStore[Stream](cache, "streams")

	store.HSet(t.Context(), "user:1", "stream:a", Stream{BitRate: 720})
	store.HSet(t.Context(), "user:1", "stream:b", Stream{BitRate: 1080})
	store.HSet(t.Context(), "user:1", "stream:c", Stream{BitRate: 480})

	keys, err := store.HKeys(t.Context(), "user:1")
	if err != nil {
		t.Fatalf("HKeys: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("HKeys: got %d fields, want 3", len(keys))
	}

	all, err := store.HGetAll(t.Context(), "user:1")
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("HGetAll: got %d entries, want 3", len(all))
	}
}

func TestHashStore_DelField(t *testing.T) {
	cache := standalone(t)
	store := hive.NewHashStore[Stream](cache, "streams")

	store.HSet(t.Context(), "user:1", "stream:a", Stream{BitRate: 720})
	store.HSet(t.Context(), "user:1", "stream:b", Stream{BitRate: 1080})
	store.HDel(t.Context(), "user:1", "stream:a")

	_, err := store.HGet(t.Context(), "user:1", "stream:a")
	if !errors.Is(err, hive.ErrNotFound) {
		t.Fatalf("after HDel: got %v, want ErrNotFound", err)
	}
	// stream:b should still be there
	if _, err := store.HGet(t.Context(), "user:1", "stream:b"); err != nil {
		t.Fatalf("HGet remaining field: %v", err)
	}
}

func TestHashStore_FieldExpiry(t *testing.T) {
	cache := standalone(t)
	store := hive.NewHashStore[Stream](cache, "streams")

	store.HSet(t.Context(), "user:1", "stream:a", Stream{BitRate: 720})
	store.HSet(t.Context(), "user:1", "stream:b", Stream{BitRate: 1080})
	store.HExpireField(t.Context(), "user:1", "stream:a", 1100*time.Millisecond)

	time.Sleep(1300 * time.Millisecond)

	_, err := store.HGet(t.Context(), "user:1", "stream:a")
	if !errors.Is(err, hive.ErrNotFound) {
		t.Fatalf("expired field still present: %v", err)
	}
	// stream:b unaffected
	if _, err := store.HGet(t.Context(), "user:1", "stream:b"); err != nil {
		t.Fatalf("non-expired field missing: %v", err)
	}
}

func TestHashStore_HSetWithTTL(t *testing.T) {
	cache := standalone(t)
	store := hive.NewHashStore[Stream](cache, "streams")

	store.HSetWithTTL(t.Context(), "user:1", "stream:a", Stream{BitRate: 720}, 1100*time.Millisecond)

	time.Sleep(1300 * time.Millisecond)

	_, err := store.HGet(t.Context(), "user:1", "stream:a")
	if !errors.Is(err, hive.ErrNotFound) {
		t.Fatalf("field with TTL still present: %v", err)
	}
}

// -- Namespace isolation --

func TestNamespaceIsolation(t *testing.T) {
	cache := standalone(t)

	sessions := hive.NewValueStore[Session](cache, "sessions")
	counters := hive.NewValueStore[int](cache, "counters")

	sessions.Set(t.Context(), "key", Session{UserID: 1})
	counters.Set(t.Context(), "key", 42)

	s, err := sessions.Get(t.Context(), "key")
	if err != nil || s.UserID != 1 {
		t.Fatalf("sessions.Get: got (%+v, %v)", s, err)
	}
	n, err := counters.Get(t.Context(), "key")
	if err != nil || n != 42 {
		t.Fatalf("counters.Get: got (%d, %v)", n, err)
	}
}
