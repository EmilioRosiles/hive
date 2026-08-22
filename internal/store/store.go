// Package store provides a thread-safe, sharded in-memory store with TTL support.
package store

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ErrCapacityExceeded is returned by write operations when the node has reached
// its configured memory limit. Add more nodes to the cluster to increase capacity.
var ErrCapacityExceeded = errors.New("hive: node memory limit reached")

// Kind identifies the type of data structure stored under a key.
type Kind uint8

const (
	KindValue Kind = iota
	KindSet
	KindHash
	KindList
	KindZSet
)

// DecodeFunc deserializes a DataStructure from bytes produced by its Encode method.
type DecodeFunc func(data []byte) (DataStructure, error)

// DataStructure is the common interface for all stored data types.
type DataStructure interface {
	Kind() Kind
	// Encode serializes the full state for rebalance migration.
	Encode() ([]byte, error)
	// KeyExpiry returns the key-level expiry as a Unix timestamp in seconds,
	// or 0 if there is no expiry.
	KeyExpiry() int64
	// SetKeyExpiry sets the key-level expiry as a Unix timestamp in seconds.
	// Pass 0 to clear the expiry.
	SetKeyExpiry(unixSec int64)
	// ByteSize returns the estimated in-memory byte cost of this value (excluding
	// the key and per-entry overhead, which are added by the DataStore).
	ByteSize() int64
	// MTime returns the Unix second timestamp of the last write.
	// Used by ApplyIfNewer to resolve rebalance conflicts (LWW).
	MTime() uint32
	// cachedSize / setCachedSize / setMTime are package-internal bookkeeping.
	cachedSize() int64
	setCachedSize(int64)
	setMTime(uint32)
}

// sizeBase holds the cached byte cost for a stored entry. Embed it in each
// concrete DataStructure type to satisfy the unexported interface methods above.
type sizeBase struct {
	size int64
}

func (b *sizeBase) cachedSize() int64     { return b.size }
func (b *sizeBase) setCachedSize(n int64) { b.size = n }

// mtimeBase holds the last-write timestamp for LWW conflict resolution.
// Embed it in each concrete DataStructure type alongside sizeBase.
type mtimeBase struct {
	mtime uint32
}

func (b *mtimeBase) MTime() uint32     { return b.mtime }
func (b *mtimeBase) setMTime(t uint32) { b.mtime = t }

// -- DataStore --

// Byte-size constants for struct fields shared across all DataStructure types.
const (
	entryOverhead     = 64 // per-entry cost added to ByteSize() to account for the map bucket allocation
	mtimeSize         = 4  // mtimeBase.mtime uint32
	keyExpirySize     = 8  // key-level expiresAt int64
	mapEntryOverhead  = 16 // per-item cost in the zset scores map: float64 score (8) + map bucket (8)
	mapBucketOverhead = 8  // per-item map bucket cost for maps
	sliceItemOverhead = 24 // per-element cost in [][]byte slices: slice header (pointer+len+cap = 24)
	zsetNodeOverhead  = 56 // zsetNode struct: member string header(16) + score(8) + backward ptr(8) + level slice header(24)
	zsetLevelSize     = 16 // one zsetLevel: forward pointer (8) + span int (8)
)

type shard struct {
	mu   sync.RWMutex
	data map[string]DataStructure
}

// DataStore is a thread-safe, sharded in-memory store.
type DataStore struct {
	shards      []*shard
	shardsCount uint64
	used        atomic.Int64 // global byte estimate across all shards
	capacity    int64        // global byte cap, enforced literally; read-only after init
	decoders    map[Kind]DecodeFunc
}

func NewDataStore(memLimit uint64) *DataStore {
	n := shardCount()
	ds := &DataStore{
		shards:      make([]*shard, n),
		shardsCount: n,
		capacity:    int64(memLimit),
		decoders: map[Kind]DecodeFunc{
			KindValue: func(data []byte) (DataStructure, error) { return DecodeValueStructure(data) },
			KindSet:   func(data []byte) (DataStructure, error) { return DecodeSetStructure(data) },
			KindHash:  func(data []byte) (DataStructure, error) { return DecodeHashStructure(data) },
			KindList:  func(data []byte) (DataStructure, error) { return DecodeListStructure(data) },
			KindZSet:  func(data []byte) (DataStructure, error) { return DecodeZSetStructure(data) },
		},
	}
	for i := range n {
		ds.shards[i] = &shard{data: make(map[string]DataStructure)}
	}
	return ds
}

// Used returns the current estimated byte usage across all shards.
func (ds *DataStore) Used() int64 {
	return ds.used.Load()
}

// Len returns the number of live entries across all shards. Entries that
// have expired but not yet been swept by the janitor are counted as live.
func (ds *DataStore) Len() int {
	n := 0
	for _, s := range ds.shards {
		s.mu.RLock()
		n += len(s.data)
		s.mu.RUnlock()
	}
	return n
}

// DecodeEntry deserializes an entry of the given Kind using the registered decoder.
func (ds *DataStore) DecodeEntry(kind Kind, data []byte) (DataStructure, error) {
	fn, ok := ds.decoders[kind]
	if !ok {
		return nil, fmt.Errorf("store: no decoder registered for kind %d", kind)
	}
	return fn(data)
}

// fnv1a64 constants match the stdlib hash/fnv implementation.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

func (ds *DataStore) getShard(key string) *shard {
	h := fnvOffset64
	for i := range len(key) {
		h ^= uint64(key[i])
		h *= fnvPrime64
	}
	return ds.shards[h&(ds.shardsCount-1)]
}

// removeEntry removes key from s, decrementing the global used counter.
// Must be called with s's write lock held.
func (ds *DataStore) removeEntry(s *shard, key string) {
	e, ok := s.data[key]
	if !ok {
		return
	}
	ds.used.Add(-e.cachedSize())
	delete(s.data, key)
}

// upsertEntry inserts or replaces key in s, updating the global used counter.
// Returns ErrCapacityExceeded if the write would push used above capacity.
// Overwrites that reduce or maintain entry size always succeed.
// Must be called with s's write lock held.
func (ds *DataStore) upsertEntry(s *shard, key string, e DataStructure) error {
	size := int64(len(key)) + e.ByteSize() + entryOverhead

	if old, ok := s.data[key]; ok {
		delta := size - old.cachedSize()
		if delta > 0 && ds.used.Load()+delta > ds.capacity {
			return ErrCapacityExceeded
		}
		ds.used.Add(delta)
	} else {
		if ds.used.Load()+size > ds.capacity {
			return ErrCapacityExceeded
		}
		ds.used.Add(size)
	}

	e.setMTime(uint32(time.Now().Unix()))
	e.setCachedSize(size)
	s.data[key] = e
	return nil
}

func (ds *DataStore) Get(key string) (DataStructure, bool) {
	s := ds.getShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return nil, false
	}
	if exp := e.KeyExpiry(); exp != 0 && time.Now().Unix() >= exp {
		return nil, false
	}
	return e, true
}

func (ds *DataStore) Set(key string, e DataStructure) error {
	s := ds.getShard(key)
	s.mu.Lock()
	err := ds.upsertEntry(s, key, e)
	s.mu.Unlock()
	return err
}

func (ds *DataStore) Del(key string) {
	s := ds.getShard(key)
	s.mu.Lock()
	ds.removeEntry(s, key)
	s.mu.Unlock()
}

func (ds *DataStore) Expire(key string, ttl time.Duration) bool {
	s := ds.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		return false
	}
	if ttl == 0 {
		e.SetKeyExpiry(0)
	} else {
		e.SetKeyExpiry(time.Now().Add(ttl).Unix())
	}
	return true
}

// Read runs fn under the shard read lock for key.
// fn is only called if the key exists and has not expired.
// fn must not block or write.
func (ds *DataStore) Read(key string, fn func(DataStructure)) {
	s := ds.getShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return
	}
	if exp := e.KeyExpiry(); exp != 0 && time.Now().Unix() >= exp {
		return
	}
	fn(e)
}

// Apply runs fn under the shard write lock for key, replacing the stored entry
// with the value fn returns. fn receives nil if the key does not exist.
// If fn returns (nil, nil), the key is deleted.
func (ds *DataStore) Apply(key string, fn func(DataStructure) (DataStructure, error)) error {
	s := ds.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := fn(s.data[key])
	if err != nil {
		return err
	}
	if next == nil {
		ds.removeEntry(s, key)
		return nil
	}
	return ds.upsertEntry(s, key, next)
}

// ApplyIfNewer stores incoming only when its MTime exceeds the currently
// stored entry's MTime (or no entry exists). Last-Write-Wins conflict
// resolution for rebalancing keys written on both sides of a network partition.
func (ds *DataStore) ApplyIfNewer(key string, incoming DataStructure) error {
	s := ds.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.data[key]
	if exists && existing.MTime() >= incoming.MTime() {
		return nil
	}

	size := int64(len(key)) + incoming.ByteSize() + entryOverhead
	if exists {
		delta := size - existing.cachedSize()
		if delta > 0 && ds.used.Load()+delta > ds.capacity {
			return ErrCapacityExceeded
		}
		ds.used.Add(delta)
	} else {
		if ds.used.Load()+size > ds.capacity {
			return ErrCapacityExceeded
		}
		ds.used.Add(size)
	}

	incoming.setCachedSize(size)
	s.data[key] = incoming
	return nil
}

// Scan iterates over all live (non-expired) entries. Pass cursor=-1 to scan all shards.
func (ds *DataStore) Scan(cursor, count int, fn func(key string, e DataStructure)) {
	now := time.Now().Unix()
	start, end := cursor, cursor+count
	if cursor == -1 {
		start, end = 0, int(ds.shardsCount)
	}
	if start >= int(ds.shardsCount) {
		return
	}
	if end > int(ds.shardsCount) {
		end = int(ds.shardsCount)
	}
	for i := start; i < end; i++ {
		s := ds.shards[i]
		s.mu.RLock()
		for key, e := range s.data {
			if exp := e.KeyExpiry(); exp != 0 && now >= exp {
				continue
			}
			fn(key, e)
		}
		s.mu.RUnlock()
	}
}

// shardCount returns the next power of 2 >= NumCPU*4.
func shardCount() uint64 {
	n := runtime.NumCPU() * 4
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return uint64(n)
}

// DeleteExpired sweeps all shards, removing entries whose key-level TTL has
// elapsed. Called by the cluster janitor on each cleanup tick.
func (ds *DataStore) DeleteExpired() {
	nowUnix := time.Now().Unix()
	for i := range ds.shardsCount {
		s := ds.shards[i]
		s.mu.Lock()
		for key, e := range s.data {
			if exp := e.KeyExpiry(); exp != 0 && nowUnix >= exp {
				ds.removeEntry(s, key)
			}
		}
		s.mu.Unlock()
	}
}
