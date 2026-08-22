package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
)

// newValueStructure is a test helper to avoid importing store in routing_test.go.
func newValueStructure(t *testing.T, data []byte) *store.ValueStructure {
	t.Helper()
	return store.NewValueStructure(data)
}

// ttlArg encodes a duration as an 8-byte big-endian nanosecond arg slot.
func ttlArg(d time.Duration) []byte {
	return encodeUint64(uint64(d.Nanoseconds()))
}

// lockArgs builds the args slice execLock expects: ttl then a caller-chosen token.
func lockArgs(ttl time.Duration, token uint32) [][]byte {
	return [][]byte{ttlArg(ttl), encodeUint32(token)}
}

// -- value ops --

func TestExecValueSet_StoresValue(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execValueSet(m, "k", [][]byte{[]byte("hello")}, 0); err != nil {
		t.Fatalf("execValueSet: %v", err)
	}
	if _, ok := m.store.Get("k"); !ok {
		t.Error("key should be present after execValueSet")
	}
}

func TestExecValueGet_ReturnsValue(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("k", store.NewValueStructure([]byte("world")))

	results, err := execValueGet(m, "k", nil, 0)
	if err != nil {
		t.Fatalf("execValueGet: %v", err)
	}
	if string(results[0]) != "world" {
		t.Errorf("got %q, want world", results[0])
	}
}

func TestExecValueGet_MissingKey_ReturnsNotFound(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execValueGet(m, "missing", nil, 0); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// -- shared ops --

func TestExecDel_RemovesKey(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("k", store.NewValueStructure([]byte("v")))

	execDel(m, "k", nil, 0)

	if _, ok := m.store.Get("k"); ok {
		t.Error("key should be removed after execDel")
	}
}

func TestExecExpire_SetsExpiry(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("k", store.NewValueStructure([]byte("v")))

	execExpire(m, "k", [][]byte{ttlArg(50 * time.Millisecond)}, 0)

	time.Sleep(80 * time.Millisecond)
	if _, ok := m.store.Get("k"); ok {
		t.Error("key should have expired after execExpire TTL elapsed")
	}
}

// -- set ops --

func TestExecSAdd_AddsMember(t *testing.T) {
	m := newTestCluster("self")

	execSAdd(m, "s", [][]byte{[]byte("alice")}, 0)

	results, _ := execSMembers(m, "s", nil, 0)
	if len(results) != 1 || string(results[0]) != "alice" {
		t.Errorf("got %v, want [alice]", results)
	}
}

func TestExecSRem_RemovesMember(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("alice")}, 0)
	execSAdd(m, "s", [][]byte{[]byte("bob")}, 0)

	execSRem(m, "s", [][]byte{[]byte("alice")}, 0)

	results, _ := execSMembers(m, "s", nil, 0)
	for _, r := range results {
		if string(r) == "alice" {
			t.Error("alice should have been removed by execSRem")
		}
	}
}

func TestExecSIsMember_Present(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("alice")}, 0)

	results, _ := execSIsMember(m, "s", [][]byte{[]byte("alice")}, 0)
	if len(results) == 0 || results[0][0] != 1 {
		t.Error("alice should be a member")
	}
}

func TestExecSIsMember_Absent(t *testing.T) {
	m := newTestCluster("self")

	results, _ := execSIsMember(m, "s", [][]byte{[]byte("alice")}, 0)
	if len(results) == 0 || results[0][0] != 0 {
		t.Error("alice should not be a member of an empty set")
	}
}

func TestExecSCard_ReturnsCount(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("a")}, 0)
	execSAdd(m, "s", [][]byte{[]byte("b")}, 0)
	execSAdd(m, "s", [][]byte{[]byte("c")}, 0)

	results, _ := execSCard(m, "s", nil, 0)
	count := decodeUint64(results[0])
	if count != 3 {
		t.Errorf("got %d, want 3", count)
	}
}

// -- hash ops --

func TestExecHSet_StoresField(t *testing.T) {
	m := newTestCluster("self")

	execHSet(m, "h", [][]byte{[]byte("field"), []byte("value")}, 0)

	results, err := execHGet(m, "h", [][]byte{[]byte("field")}, 0)
	if err != nil {
		t.Fatalf("execHGet: %v", err)
	}
	if string(results[0]) != "value" {
		t.Errorf("got %q, want value", results[0])
	}
}

func TestExecHGet_MissingField_ReturnsNotFound(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execHGet(m, "h", [][]byte{[]byte("missing")}, 0); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestExecHDel_RemovesField(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("f"), []byte("v")}, 0)

	execHDel(m, "h", [][]byte{[]byte("f")}, 0)

	if _, err := execHGet(m, "h", [][]byte{[]byte("f")}, 0); err != ErrNotFound {
		t.Error("field should be gone after execHDel")
	}
}

func TestExecHGetAll_ReturnsAllFields(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("f1"), []byte("v1")}, 0)
	execHSet(m, "h", [][]byte{[]byte("f2"), []byte("v2")}, 0)

	results, err := execHGetAll(m, "h", nil, 0)
	if err != nil {
		t.Fatalf("execHGetAll: %v", err)
	}
	// results are interleaved field/value pairs
	if len(results) != 4 {
		t.Errorf("expected 4 results (2 field+value pairs), got %d", len(results))
	}
}

func TestExecHKeys_ReturnsFieldNames(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("alpha"), []byte("1")}, 0)
	execHSet(m, "h", [][]byte{[]byte("beta"), []byte("2")}, 0)

	results, err := execHKeys(m, "h", nil, 0)
	if err != nil {
		t.Fatalf("execHKeys: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 field names, got %d", len(results))
	}
}

// -- decodeUint64 is the inverse of encodeUint64, used to read SCard results --

func decodeUint64(b []byte) uint64 {
	var n uint64
	for _, v := range b {
		n = n<<8 | uint64(v)
	}
	return n
}

// -- lock ops --

func TestExecLock_Acquires(t *testing.T) {
	m := newTestCluster("self")

	results, err := execLock(m, "k", lockArgs(time.Minute, 42), 0)
	if err != nil {
		t.Fatalf("execLock: %v", err)
	}
	if results != nil {
		t.Errorf("execLock: expected no results, got %v", results)
	}

	e, ok := m.store.Get("k")
	if !ok {
		t.Fatal("execLock: placeholder entry should exist")
	}
	if e.LockToken() != 42 || e.LockExpiry() == 0 {
		t.Error("execLock: stored lock state should match the caller-provided token")
	}
}

// TestExecLock_ReplayingSameArgs_ProducesIdenticalToken confirms replaying
// identical Lock args on a second, independent Cluster (what a replica does)
// produces the identical stored token.
func TestExecLock_ReplayingSameArgs_ProducesIdenticalToken(t *testing.T) {
	primary := newTestCluster("primary")
	replica := newTestCluster("replica")

	args := lockArgs(time.Minute, 12345)
	if _, err := execLock(primary, "k", args, 0); err != nil {
		t.Fatalf("execLock on primary: %v", err)
	}
	if _, err := execLock(replica, "k", args, 0); err != nil {
		t.Fatalf("execLock on replica: %v", err)
	}

	pe, _ := primary.store.Get("k")
	re, _ := replica.store.Get("k")
	if pe.LockToken() != re.LockToken() {
		t.Errorf("primary token=%d, replica token=%d — replaying identical args must produce identical lock state", pe.LockToken(), re.LockToken())
	}
}

func TestExecLock_AlreadyLocked_ReturnsErrKeyLocked(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execLock(m, "k", lockArgs(time.Minute, 1), 0); err != nil {
		t.Fatalf("first execLock: %v", err)
	}
	if _, err := execLock(m, "k", lockArgs(time.Minute, 2), 0); !errors.Is(err, ErrKeyLocked) {
		t.Errorf("second execLock: got %v, want ErrKeyLocked", err)
	}
}

func TestExecLock_AfterExpiry_CanReacquire(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execLock(m, "k", lockArgs(20*time.Millisecond, 1), 0); err != nil {
		t.Fatalf("first execLock: %v", err)
	}
	time.Sleep(1100 * time.Millisecond) // KeyExpiry/LockExpiry is second-precision

	if _, err := execLock(m, "k", lockArgs(time.Minute, 2), 0); err != nil {
		t.Errorf("execLock after expiry should succeed, got %v", err)
	}
}

func TestExecLock_ConcurrentCallers_OnlyOneSucceeds(t *testing.T) {
	m := newTestCluster("self")

	const n = 50
	var wg sync.WaitGroup
	successes := make([]bool, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := execLock(m, "k", lockArgs(time.Minute, uint32(i+1)), 0)
			successes[i] = err == nil
		}(i)
	}
	wg.Wait()

	count := 0
	for _, ok := range successes {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 successful Lock out of %d concurrent callers, got %d", n, count)
	}
}

func TestExecUnlock_ReleasesLock(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execLock(m, "k", lockArgs(time.Minute, 7), 0); err != nil {
		t.Fatalf("execLock: %v", err)
	}

	if _, err := execUnlock(m, "k", [][]byte{encodeUint32(7)}, 0); err != nil {
		t.Fatalf("execUnlock: %v", err)
	}

	e, ok := m.store.Get("k")
	if !ok {
		t.Fatal("key should still exist after unlock (placeholder entry stays)")
	}
	if e.LockExpiry() != 0 {
		t.Error("execUnlock: lock should be cleared")
	}

	// Re-locking should now succeed since the key is unlocked.
	if _, err := execLock(m, "k", lockArgs(time.Minute, 8), 0); err != nil {
		t.Errorf("execLock after unlock should succeed, got %v", err)
	}
}

func TestExecUnlock_WrongToken_ReturnsErrLockNotHeld(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execLock(m, "k", lockArgs(time.Minute, 7), 0); err != nil {
		t.Fatalf("execLock: %v", err)
	}

	if _, err := execUnlock(m, "k", [][]byte{encodeUint32(999999)}, 0); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("execUnlock with wrong token: got %v, want ErrLockNotHeld", err)
	}
}

func TestExecUnlock_NeverLocked_ReturnsErrLockNotHeld(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execUnlock(m, "k", [][]byte{encodeUint32(1)}, 0); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("execUnlock on never-locked key: got %v, want ErrLockNotHeld", err)
	}
}

func TestExecRenew_ExtendsTTL(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execLock(m, "k", lockArgs(50*time.Millisecond, 7), 0); err != nil {
		t.Fatalf("execLock: %v", err)
	}

	if _, err := execRenew(m, "k", [][]byte{encodeUint32(7), ttlArg(time.Minute)}, 0); err != nil {
		t.Fatalf("execRenew: %v", err)
	}

	e, ok := m.store.Get("k")
	if !ok {
		t.Fatal("key should exist")
	}
	if e.LockExpiry() <= uint32(time.Now().Add(30*time.Second).Unix()) {
		t.Error("execRenew: expiry should have been extended well past the original short TTL")
	}
}

func TestExecRenew_WrongToken_ReturnsErrLockNotHeld(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execLock(m, "k", lockArgs(time.Minute, 7), 0); err != nil {
		t.Fatalf("execLock: %v", err)
	}

	if _, err := execRenew(m, "k", [][]byte{encodeUint32(999999), ttlArg(time.Minute)}, 0); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("execRenew with wrong token: got %v, want ErrLockNotHeld", err)
	}
}

// TestConcurrentReadAndLockMutation_NoRace runs a reader concurrently with
// Lock/Unlock on the same key so -race can catch any regression; it asserts
// nothing about outcomes.
func TestConcurrentReadAndLockMutation_NoRace(t *testing.T) {
	m := newTestCluster("self")
	if _, err := execValueSet(m, "k", [][]byte{[]byte("v")}, 0); err != nil {
		t.Fatalf("execValueSet: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				execValueGet(m, "k", nil, 0)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		const token = 7
		for range 200 {
			execLock(m, "k", lockArgs(time.Minute, token), 0)
			execUnlock(m, "k", [][]byte{encodeUint32(token)}, 0)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
