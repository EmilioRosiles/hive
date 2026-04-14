package cluster

import (
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

// -- value ops --

func TestExecValueSet_StoresValue(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execValueSet(m, "k", [][]byte{[]byte("hello")}); err != nil {
		t.Fatalf("execValueSet: %v", err)
	}
	if _, ok := m.store.Get("k"); !ok {
		t.Error("key should be present after execValueSet")
	}
}

func TestExecValueGet_ReturnsValue(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("k", store.NewValueStructure([]byte("world")))

	results, err := execValueGet(m, "k", nil)
	if err != nil {
		t.Fatalf("execValueGet: %v", err)
	}
	if string(results[0]) != "world" {
		t.Errorf("got %q, want world", results[0])
	}
}

func TestExecValueGet_MissingKey_ReturnsNotFound(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execValueGet(m, "missing", nil); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// -- shared ops --

func TestExecDel_RemovesKey(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("k", store.NewValueStructure([]byte("v")))

	execDel(m, "k", nil)

	if _, ok := m.store.Get("k"); ok {
		t.Error("key should be removed after execDel")
	}
}

func TestExecExpire_SetsExpiry(t *testing.T) {
	m := newTestCluster("self")
	m.store.Set("k", store.NewValueStructure([]byte("v")))

	execExpire(m, "k", [][]byte{ttlArg(50 * time.Millisecond)})

	time.Sleep(80 * time.Millisecond)
	if _, ok := m.store.Get("k"); ok {
		t.Error("key should have expired after execExpire TTL elapsed")
	}
}

// -- set ops --

func TestExecSAdd_AddsMember(t *testing.T) {
	m := newTestCluster("self")

	execSAdd(m, "s", [][]byte{[]byte("alice"), nil})

	results, _ := execSMembers(m, "s", nil)
	if len(results) != 1 || string(results[0]) != "alice" {
		t.Errorf("got %v, want [alice]", results)
	}
}

func TestExecSAdd_WithTTL_MemberExpires(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("bob"), ttlArg(50 * time.Millisecond)})

	time.Sleep(80 * time.Millisecond)
	results, _ := execSMembers(m, "s", nil)
	for _, r := range results {
		if string(r) == "bob" {
			t.Error("TTL-expired member should not be returned")
		}
	}
}

func TestExecSRem_RemovesMember(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("alice"), nil})
	execSAdd(m, "s", [][]byte{[]byte("bob"), nil})

	execSRem(m, "s", [][]byte{[]byte("alice")})

	results, _ := execSMembers(m, "s", nil)
	for _, r := range results {
		if string(r) == "alice" {
			t.Error("alice should have been removed by execSRem")
		}
	}
}

func TestExecSIsMember_Present(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("alice"), nil})

	results, _ := execSIsMember(m, "s", [][]byte{[]byte("alice")})
	if len(results) == 0 || results[0][0] != 1 {
		t.Error("alice should be a member")
	}
}

func TestExecSIsMember_Absent(t *testing.T) {
	m := newTestCluster("self")

	results, _ := execSIsMember(m, "s", [][]byte{[]byte("alice")})
	if len(results) == 0 || results[0][0] != 0 {
		t.Error("alice should not be a member of an empty set")
	}
}

func TestExecSCard_ReturnsCount(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("a"), nil})
	execSAdd(m, "s", [][]byte{[]byte("b"), nil})
	execSAdd(m, "s", [][]byte{[]byte("c"), nil})

	results, _ := execSCard(m, "s", nil)
	count := decodeUint64(results[0])
	if count != 3 {
		t.Errorf("got %d, want 3", count)
	}
}

func TestExecSExpireMember_ExpiresMember(t *testing.T) {
	m := newTestCluster("self")
	execSAdd(m, "s", [][]byte{[]byte("alice"), nil})

	execSExpireMember(m, "s", [][]byte{[]byte("alice"), ttlArg(50 * time.Millisecond)})

	time.Sleep(80 * time.Millisecond)
	results, _ := execSIsMember(m, "s", [][]byte{[]byte("alice")})
	if results[0][0] != 0 {
		t.Error("alice should have expired after execSExpireMember TTL")
	}
}

// -- hash ops --

func TestExecHSet_StoresField(t *testing.T) {
	m := newTestCluster("self")

	execHSet(m, "h", [][]byte{[]byte("field"), []byte("value"), nil})

	results, err := execHGet(m, "h", [][]byte{[]byte("field")})
	if err != nil {
		t.Fatalf("execHGet: %v", err)
	}
	if string(results[0]) != "value" {
		t.Errorf("got %q, want value", results[0])
	}
}

func TestExecHSet_WithTTL_FieldExpires(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("f"), []byte("v"), ttlArg(50 * time.Millisecond)})

	time.Sleep(80 * time.Millisecond)
	if _, err := execHGet(m, "h", [][]byte{[]byte("f")}); err != ErrNotFound {
		t.Error("TTL-expired field should not be retrievable")
	}
}

func TestExecHGet_MissingField_ReturnsNotFound(t *testing.T) {
	m := newTestCluster("self")

	if _, err := execHGet(m, "h", [][]byte{[]byte("missing")}); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestExecHDel_RemovesField(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("f"), []byte("v"), nil})

	execHDel(m, "h", [][]byte{[]byte("f")})

	if _, err := execHGet(m, "h", [][]byte{[]byte("f")}); err != ErrNotFound {
		t.Error("field should be gone after execHDel")
	}
}

func TestExecHGetAll_ReturnsAllFields(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("f1"), []byte("v1"), nil})
	execHSet(m, "h", [][]byte{[]byte("f2"), []byte("v2"), nil})

	results, err := execHGetAll(m, "h", nil)
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
	execHSet(m, "h", [][]byte{[]byte("alpha"), []byte("1"), nil})
	execHSet(m, "h", [][]byte{[]byte("beta"), []byte("2"), nil})

	results, err := execHKeys(m, "h", nil)
	if err != nil {
		t.Fatalf("execHKeys: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 field names, got %d", len(results))
	}
}

func TestExecHExpireField_ExpiresField(t *testing.T) {
	m := newTestCluster("self")
	execHSet(m, "h", [][]byte{[]byte("f"), []byte("v"), nil})

	execHExpireField(m, "h", [][]byte{[]byte("f"), ttlArg(50 * time.Millisecond)})

	time.Sleep(80 * time.Millisecond)
	if _, err := execHGet(m, "h", [][]byte{[]byte("f")}); err != ErrNotFound {
		t.Error("field should have expired after execHExpireField TTL")
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
