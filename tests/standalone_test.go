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

// -- Lock --

func TestLock_AcquireAndUnlock(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	lock, err := store.Lock(t.Context(), "s1", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := lock.Unlock(t.Context()); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// Unlocked now — a second Lock should succeed.
	if _, err := store.Lock(t.Context(), "s1", time.Minute); err != nil {
		t.Errorf("Lock after Unlock should succeed, got %v", err)
	}
}

func TestLock_AlreadyLocked_ReturnsErrKeyLocked(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	if _, err := store.Lock(t.Context(), "s1", time.Minute); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if _, err := store.Lock(t.Context(), "s1", time.Minute); !errors.Is(err, hive.ErrKeyLocked) {
		t.Errorf("second Lock: got %v, want ErrKeyLocked", err)
	}
}

func TestLock_BlocksOrdinaryOpsForEveryoneIncludingHolder(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	store.Set(t.Context(), "s1", Session{UserID: 1})
	if _, err := store.Lock(t.Context(), "s1", time.Minute); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	if _, err := store.Get(t.Context(), "s1"); !errors.Is(err, hive.ErrKeyLocked) {
		t.Errorf("Get on locked key (no auth): got %v, want ErrKeyLocked", err)
	}
	if err := store.Set(t.Context(), "s1", Session{UserID: 2}); !errors.Is(err, hive.ErrKeyLocked) {
		t.Errorf("Set on locked key (no auth): got %v, want ErrKeyLocked", err)
	}
	if err := store.Del(t.Context(), "s1"); !errors.Is(err, hive.ErrKeyLocked) {
		t.Errorf("Del on locked key (no auth): got %v, want ErrKeyLocked", err)
	}
}

func TestLock_ContextAuthorizesHolderOps(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	lock, err := store.Lock(t.Context(), "s1", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	authCtx := lock.Context()
	if err := store.Set(authCtx, "s1", Session{UserID: 1, Token: "in-critical-section"}); err != nil {
		t.Fatalf("Set with lock.Context(): %v", err)
	}
	got, err := store.Get(authCtx, "s1")
	if err != nil {
		t.Fatalf("Get with lock.Context(): %v", err)
	}
	if got.Token != "in-critical-section" {
		t.Errorf("got %+v, want Token=in-critical-section", got)
	}

	// The lock must still be held after an authorized Set — Set must not
	// silently clear it as a side effect of overwriting the entry.
	if _, err := store.Get(t.Context(), "s1"); !errors.Is(err, hive.ErrKeyLocked) {
		t.Errorf("Get without auth after authorized Set: got %v, want ErrKeyLocked (lock should survive)", err)
	}

	if err := lock.Unlock(t.Context()); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if _, err := store.Get(t.Context(), "s1"); err != nil {
		t.Errorf("Get after Unlock should succeed, got %v", err)
	}
}

func TestLock_Renew(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	lock, err := store.Lock(t.Context(), "s1", 1100*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := lock.Renew(t.Context(), time.Minute); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	time.Sleep(1300 * time.Millisecond) // past the original short TTL

	// Still locked — Renew should have extended it well past the original TTL.
	if _, err := store.Get(t.Context(), "s1"); !errors.Is(err, hive.ErrKeyLocked) {
		t.Errorf("Get after Renew: got %v, want ErrKeyLocked (lock should still be held)", err)
	}
	if err := lock.Unlock(t.Context()); err != nil {
		t.Errorf("Unlock: %v", err)
	}
}

func TestLock_UnlockWrongHolder_ReturnsErrLockNotHeld(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	lock, err := store.Lock(t.Context(), "s1", 1100*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	time.Sleep(1300 * time.Millisecond) // let it expire

	newLock, err := store.Lock(t.Context(), "s1", time.Minute)
	if err != nil {
		t.Fatalf("re-Lock after expiry: %v", err)
	}

	// The original (now-stale) handle must not be able to release the new holder's lock.
	if err := lock.Unlock(t.Context()); !errors.Is(err, hive.ErrLockNotHeld) {
		t.Errorf("stale Unlock: got %v, want ErrLockNotHeld", err)
	}

	if err := newLock.Unlock(t.Context()); err != nil {
		t.Errorf("current holder's Unlock: %v", err)
	}
}

func TestLock_AutoExpires(t *testing.T) {
	cache := standalone(t)
	store := hive.NewValueStore[Session](cache, "sessions")

	if _, err := store.Lock(t.Context(), "s1", 1100*time.Millisecond); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	time.Sleep(1300 * time.Millisecond)

	if _, err := store.Lock(t.Context(), "s1", time.Minute); err != nil {
		t.Errorf("Lock after expiry should succeed, got %v", err)
	}
}
