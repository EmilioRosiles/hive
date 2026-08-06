package store

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

// -- helpers --

// unlimited is passed to NewDataStore in tests that don't care about
// capacity enforcement — capacity is enforced literally (0 means zero,
// not unlimited), so a real, effectively-uncapped value is needed instead.
const unlimited = math.MaxInt64

func val(s string) *ValueStructure { return NewValueStructure([]byte(s)) }

func mustSet(t *testing.T, ds *DataStore, key string, e DataStructure) {
	t.Helper()
	if err := ds.Set(key, e); err != nil {
		t.Fatalf("Set(%q): unexpected error: %v", key, err)
	}
}

// totalEntries counts entries across all shards.
func totalEntries(ds *DataStore) int {
	n := 0
	for i := range ds.shardsCount {
		s := ds.shards[i]
		s.mu.RLock()
		n += len(s.data)
		s.mu.RUnlock()
	}
	return n
}

// entrySize returns the accounting cost for key + value string v.
func entrySize(key, v string) int64 {
	return int64(len(key)) + int64(len(v)) + mtimeSize + keyExpirySize + entryOverhead
}

// -- Set / Get --

func TestSetAndGet(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("hello"))

	e, ok := ds.Get("k")
	if !ok {
		t.Fatal("Get: key should exist")
	}
	if string(e.(*ValueStructure).Data) != "hello" {
		t.Errorf("Get: got %q, want %q", e.(*ValueStructure).Data, "hello")
	}
}

func TestGetMissingKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	if _, ok := ds.Get("missing"); ok {
		t.Error("Get: expected false for missing key")
	}
}

func TestGetExpiredKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	e := NewValueStructureWithTTL([]byte("bye"), 10*time.Millisecond)
	mustSet(t, ds, "k", e)
	time.Sleep(20 * time.Millisecond)
	if _, ok := ds.Get("k"); ok {
		t.Error("Get: expected false for expired key")
	}
}

func TestSetOverwrite(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("first"))
	mustSet(t, ds, "k", val("second"))

	e, ok := ds.Get("k")
	if !ok {
		t.Fatal("Get after overwrite: key should exist")
	}
	if string(e.(*ValueStructure).Data) != "second" {
		t.Errorf("overwrite: got %q, want %q", e.(*ValueStructure).Data, "second")
	}
	if totalEntries(ds) != 1 {
		t.Errorf("overwrite: expected 1 entry, got %d", totalEntries(ds))
	}
}

// -- Del --

func TestDel(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("v"))
	ds.Del("k")
	if _, ok := ds.Get("k"); ok {
		t.Error("Del: key should be gone")
	}
}

func TestDelMissingKeyIsNoOp(t *testing.T) {
	ds := NewDataStore(unlimited)
	before := ds.used.Load()
	ds.Del("nonexistent")
	if ds.used.Load() != before {
		t.Error("Del on missing key should not change used counter")
	}
}

func TestDelDecrementsUsed(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("v"))
	before := ds.used.Load()
	ds.Del("k")
	if ds.used.Load() >= before {
		t.Errorf("Del: used should decrease; before=%d after=%d", before, ds.used.Load())
	}
	if ds.used.Load() != 0 {
		t.Errorf("Del: used should be 0 after deleting only entry, got %d", ds.used.Load())
	}
}

// -- Expire --

func TestExpireTTL(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("v"))
	ds.Expire("k", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := ds.Get("k"); ok {
		t.Error("Expire: key should have expired")
	}
}

func TestExpireMissingKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	if ds.Expire("missing", time.Second) {
		t.Error("Expire: should return false for missing key")
	}
}

func TestExpireClearTTL(t *testing.T) {
	ds := NewDataStore(unlimited)
	e := NewValueStructureWithTTL([]byte("v"), 20*time.Millisecond)
	mustSet(t, ds, "k", e)
	ds.Expire("k", 0) // clear TTL
	time.Sleep(30 * time.Millisecond)
	if _, ok := ds.Get("k"); !ok {
		t.Error("Expire(0): key should still exist after clearing TTL")
	}
}

// -- Read --

func TestReadCallsFnForExistingKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("v"))
	called := false
	ds.Read("k", func(e DataStructure) { called = true })
	if !called {
		t.Error("Read: fn should be called for existing key")
	}
}

func TestReadSkipsMissingKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	ds.Read("missing", func(DataStructure) { t.Error("Read: fn should not be called for missing key") })
}

func TestReadSkipsExpiredKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	e := NewValueStructureWithTTL([]byte("v"), 10*time.Millisecond)
	mustSet(t, ds, "k", e)
	time.Sleep(20 * time.Millisecond)
	ds.Read("k", func(DataStructure) { t.Error("Read: fn should not be called for expired key") })
}

// -- Apply --

func TestApplyCreatesEntry(t *testing.T) {
	ds := NewDataStore(unlimited)
	err := ds.Apply("k", func(e DataStructure) (DataStructure, error) {
		return val("created"), nil
	})
	if err != nil {
		t.Fatalf("Apply: unexpected error: %v", err)
	}
	if _, ok := ds.Get("k"); !ok {
		t.Error("Apply: key should exist after creation")
	}
}

func TestApplyUpdatesEntry(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("old"))
	ds.Apply("k", func(e DataStructure) (DataStructure, error) {
		return val("new"), nil
	})
	e, _ := ds.Get("k")
	if string(e.(*ValueStructure).Data) != "new" {
		t.Errorf("Apply: got %q, want %q", e.(*ValueStructure).Data, "new")
	}
}

func TestApplyNilDeletesEntry(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("v"))
	usedBefore := ds.used.Load()
	ds.Apply("k", func(DataStructure) (DataStructure, error) { return nil, nil })
	if _, ok := ds.Get("k"); ok {
		t.Error("Apply(nil): key should be deleted")
	}
	if ds.used.Load() >= usedBefore {
		t.Error("Apply(nil): used should decrease")
	}
}

func TestApplyPropagatesFnError(t *testing.T) {
	ds := NewDataStore(unlimited)
	sentinel := errors.New("fn error")
	err := ds.Apply("k", func(DataStructure) (DataStructure, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Errorf("Apply: expected sentinel error, got %v", err)
	}
}

// -- Scan --

func TestScanIteratesAllLiveEntries(t *testing.T) {
	ds := NewDataStore(unlimited)
	for i := range 10 {
		mustSet(t, ds, fmt.Sprintf("k%d", i), val("v"))
	}
	seen := map[string]bool{}
	ds.Scan(-1, 0, func(key string, _ DataStructure) { seen[key] = true })
	if len(seen) != 10 {
		t.Errorf("Scan: expected 10 entries, got %d", len(seen))
	}
}

func TestScanSkipsExpiredEntries(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "live", val("v"))
	e := NewValueStructureWithTTL([]byte("v"), 10*time.Millisecond)
	mustSet(t, ds, "dead", e)
	time.Sleep(20 * time.Millisecond)

	seen := map[string]bool{}
	ds.Scan(-1, 0, func(key string, _ DataStructure) { seen[key] = true })
	if seen["dead"] {
		t.Error("Scan: should not yield expired entries")
	}
	if !seen["live"] {
		t.Error("Scan: should yield live entries")
	}
}

// -- capacity enforcement --

func TestCapacityRejectsFreshInsert(t *testing.T) {
	key := "only"
	ds := NewDataStore(uint64(entrySize(key, "v")))
	mustSet(t, ds, key, val("v"))

	err := ds.Set("overflow", val("v"))
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Errorf("expected ErrCapacityExceeded, got %v", err)
	}
	if _, ok := ds.Get("overflow"); ok {
		t.Error("rejected key must not be present")
	}
}

func TestCapacityRejectsGrowingOverwrite(t *testing.T) {
	key := "k"
	ds := NewDataStore(uint64(entrySize(key, "small")))
	mustSet(t, ds, key, val("small"))

	err := ds.Set(key, val("much-larger-value-here"))
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Errorf("expected ErrCapacityExceeded on growing overwrite, got %v", err)
	}
	// original value must be intact
	e, ok := ds.Get(key)
	if !ok {
		t.Fatal("original entry should still exist after rejected overwrite")
	}
	if string(e.(*ValueStructure).Data) != "small" {
		t.Errorf("original value corrupted: got %q", e.(*ValueStructure).Data)
	}
}

func TestCapacityAllowsShrinkingOverwrite(t *testing.T) {
	key := "k"
	ds := NewDataStore(uint64(entrySize(key, "big-value")))
	mustSet(t, ds, key, val("big-value"))

	if err := ds.Set(key, val("v")); err != nil {
		t.Errorf("shrinking overwrite should always succeed, got %v", err)
	}
}

func TestCapacityUnlimitedNeverRejects(t *testing.T) {
	ds := NewDataStore(unlimited)
	for i := range 10000 {
		if err := ds.Set(fmt.Sprintf("k%d", i), val("v")); err != nil {
			t.Fatalf("unlimited store rejected write at i=%d: %v", i, err)
		}
	}
}

// -- used accounting --

func TestUsedAccountingFreshInsert(t *testing.T) {
	ds := NewDataStore(unlimited)
	key, v := "k", "hello"
	mustSet(t, ds, key, val(v))
	want := entrySize(key, v)
	if got := ds.used.Load(); got != want {
		t.Errorf("used after insert: got %d, want %d", got, want)
	}
}

func TestUsedAccountingOverwrite(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("short"))
	mustSet(t, ds, "k", val("longer-value"))
	want := entrySize("k", "longer-value")
	if got := ds.used.Load(); got != want {
		t.Errorf("used after overwrite: got %d, want %d", got, want)
	}
}

func TestUsedAccountingRepeatedOverwrite(t *testing.T) {
	ds := NewDataStore(unlimited)
	for range 1000 {
		mustSet(t, ds, "k", val("v"))
	}
	want := entrySize("k", "v")
	if got := ds.used.Load(); got != want {
		t.Errorf("used after 1000 overwrites: got %d, want %d", got, want)
	}
}

func TestUsedAccountingDel(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("v"))
	ds.Del("k")
	if got := ds.used.Load(); got != 0 {
		t.Errorf("used after Del: got %d, want 0", got)
	}
}

// -- DeleteExpired --

func TestDeleteExpiredRemovesExpiredEntries(t *testing.T) {
	ds := NewDataStore(unlimited)

	e := NewValueStructureWithTTL([]byte("v"), 10*time.Millisecond)
	mustSet(t, ds, "k", e)
	usedBefore := ds.used.Load()

	time.Sleep(20 * time.Millisecond)
	ds.DeleteExpired()

	if _, ok := ds.Get("k"); ok {
		t.Error("DeleteExpired: expired key should be gone")
	}
	if ds.used.Load() >= usedBefore {
		t.Errorf("DeleteExpired: used should decrease; before=%d after=%d", usedBefore, ds.used.Load())
	}
}

func TestDeleteExpiredRemovesEmptySet(t *testing.T) {
	ds := NewDataStore(unlimited)

	err := ds.Apply("s", func(_ DataStructure) (DataStructure, error) {
		ss := NewSetStructure()
		ss.AddWithTTL("member", 10*time.Millisecond)
		return ss, nil
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	ds.DeleteExpired()

	if _, ok := ds.Get("s"); ok {
		t.Error("DeleteExpired: set with all-expired members should be removed")
	}
}

// -- ApplyIfNewer --

func TestApplyIfNewer_NoExisting_Stores(t *testing.T) {
	ds := NewDataStore(unlimited)
	v := val("hello")
	v.setMTime(uint32(time.Now().Unix()))

	if err := ds.ApplyIfNewer("k", v); err != nil {
		t.Fatalf("ApplyIfNewer: %v", err)
	}
	if _, ok := ds.Get("k"); !ok {
		t.Error("ApplyIfNewer: key should be present when no existing entry")
	}
}

func TestApplyIfNewer_NewerWins(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("old"))

	newer := val("new")
	newer.setMTime(uint32(time.Now().Add(time.Hour).Unix()))

	if err := ds.ApplyIfNewer("k", newer); err != nil {
		t.Fatalf("ApplyIfNewer: %v", err)
	}
	e, _ := ds.Get("k")
	if string(e.(*ValueStructure).Data) != "new" {
		t.Errorf("newer entry should win; got %q", e.(*ValueStructure).Data)
	}
}

func TestApplyIfNewer_OlderLoses(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("current"))

	stale := val("stale")
	stale.setMTime(1) // second 1 — older than any real write

	if err := ds.ApplyIfNewer("k", stale); err != nil {
		t.Fatalf("ApplyIfNewer: %v", err)
	}
	e, _ := ds.Get("k")
	if string(e.(*ValueStructure).Data) != "current" {
		t.Errorf("stale entry should not overwrite; got %q", e.(*ValueStructure).Data)
	}
}

func TestApplyIfNewer_SameTimestamp_KeepsExisting(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("first"))

	existing, _ := ds.Get("k")
	ts := existing.MTime()

	second := val("second")
	second.setMTime(ts) // identical timestamp

	ds.ApplyIfNewer("k", second) //nolint
	e, _ := ds.Get("k")
	if string(e.(*ValueStructure).Data) != "first" {
		t.Errorf("same-timestamp should keep existing; got %q", e.(*ValueStructure).Data)
	}
}

func TestApplyIfNewer_UsedAccountingNewKey(t *testing.T) {
	ds := NewDataStore(unlimited)
	v := val("hello")
	v.setMTime(uint32(time.Now().Unix()))

	ds.ApplyIfNewer("k", v) //nolint
	want := entrySize("k", "hello")
	if got := ds.used.Load(); got != want {
		t.Errorf("used after ApplyIfNewer insert: got %d, want %d", got, want)
	}
}

func TestApplyIfNewer_UsedAccountingReplace(t *testing.T) {
	ds := NewDataStore(unlimited)
	mustSet(t, ds, "k", val("short"))

	longer := val("much-longer")
	longer.setMTime(uint32(time.Now().Add(time.Hour).Unix()))

	ds.ApplyIfNewer("k", longer) //nolint
	want := entrySize("k", "much-longer")
	if got := ds.used.Load(); got != want {
		t.Errorf("used after ApplyIfNewer replace: got %d, want %d", got, want)
	}
}

func TestApplyIfNewer_CapacityEnforced(t *testing.T) {
	key := "k"
	ds := NewDataStore(uint64(entrySize(key, "v")))
	mustSet(t, ds, key, val("v"))

	// A newer but larger value must be rejected when over capacity.
	big := val("this-value-is-too-large")
	big.setMTime(uint32(time.Now().Add(time.Hour).Unix()))

	if err := ds.ApplyIfNewer(key, big); !errors.Is(err, ErrCapacityExceeded) {
		t.Errorf("expected ErrCapacityExceeded, got %v", err)
	}
	// Original value must be intact.
	e, _ := ds.Get(key)
	if string(e.(*ValueStructure).Data) != "v" {
		t.Errorf("original value corrupted after rejected ApplyIfNewer: got %q", e.(*ValueStructure).Data)
	}
}
