// Package store provides a thread-safe, sharded in-memory store with TTL support.
package store

import (
	"hash/fnv"
	"runtime"
	"sync"
	"time"
)

// Kind identifies the type of data structure stored under a key.
type Kind uint8

const (
	KindValue Kind = iota
	KindSet
	KindHash
)

// DataStructure is the common interface for all stored data types.
type DataStructure interface {
	Kind() Kind
	// Cleanup removes any expired sub-fields and reports whether the entry
	// itself has expired and should be deleted by the janitor.
	Cleanup(now time.Time) bool
	// Encode serializes the full state for rebalance migration.
	Encode() ([]byte, error)
	// KeyExpiry returns the key-level expiry as a Unix timestamp in seconds,
	// or 0 if there is no expiry.
	KeyExpiry() int64
	// SetKeyExpiry sets the key-level expiry as a Unix timestamp in seconds.
	// Pass 0 to clear the expiry.
	SetKeyExpiry(unixSec int64)
}

// -- DataStore --

type shard struct {
	mu   sync.RWMutex
	data map[string]DataStructure
}

// DataStore is a thread-safe, sharded in-memory store.
type DataStore struct {
	shards      []*shard
	shardsCount uint64
	janitor     *janitor
}

func NewDataStore(cleanupInterval time.Duration) *DataStore {
	n := shardCount()
	ds := &DataStore{
		shards:      make([]*shard, n),
		shardsCount: n,
	}
	for i := range n {
		ds.shards[i] = &shard{data: make(map[string]DataStructure)}
	}
	if cleanupInterval > 0 {
		j := &janitor{interval: cleanupInterval, stop: make(chan struct{})}
		ds.janitor = j
		go j.run(ds)
		runtime.SetFinalizer(ds, stopJanitor)
	}
	return ds
}

func (ds *DataStore) getShard(key string) *shard {
	h := fnv.New64a()
	h.Write([]byte(key))
	return ds.shards[h.Sum64()&(ds.shardsCount-1)]
}

func (ds *DataStore) Get(key string) (DataStructure, bool) {
	s := ds.getShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return nil, false
	}
	if exp := e.KeyExpiry(); exp != 0 && time.Now().Unix() > exp {
		return nil, false
	}
	return e, true
}

func (ds *DataStore) Set(key string, e DataStructure) {
	s := ds.getShard(key)
	s.mu.Lock()
	s.data[key] = e
	s.mu.Unlock()
}

func (ds *DataStore) Del(key string) {
	s := ds.getShard(key)
	s.mu.Lock()
	delete(s.data, key)
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
// Multiple readers can proceed concurrently; fn must not block or write.
func (ds *DataStore) Read(key string, fn func(DataStructure)) {
	s := ds.getShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return
	}
	if exp := e.KeyExpiry(); exp != 0 && time.Now().Unix() > exp {
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
		delete(s.data, key)
	} else {
		s.data[key] = next
	}
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
			if exp := e.KeyExpiry(); exp != 0 && now > exp {
				continue
			}
			fn(key, e)
		}
		s.mu.RUnlock()
	}
}

func (ds *DataStore) Stop() {
	if ds.janitor != nil {
		ds.janitor.stop <- struct{}{}
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

// -- janitor --

type janitor struct {
	interval time.Duration
	stop     chan struct{}
}

func (j *janitor) run(ds *DataStore) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ds.deleteExpired()
		case <-j.stop:
			return
		}
	}
}

func (ds *DataStore) deleteExpired() {
	now := time.Now()
	nowUnix := now.Unix()
	for i := range ds.shardsCount {
		s := ds.shards[i]
		s.mu.Lock()
		for key, e := range s.data {
			// Phase 1: key-level TTL — always evicts the whole entry.
			if exp := e.KeyExpiry(); exp != 0 && nowUnix > exp {
				delete(s.data, key)
				continue
			}
			// Phase 2: field-level cleanup (Sets, Hashes).
			// Cleanup removes expired sub-fields and returns true if the
			// structure is now empty and should itself be deleted.
			if e.Cleanup(now) {
				delete(s.data, key)
			}
		}
		s.mu.Unlock()
	}
}

func stopJanitor(ds *DataStore) {
	ds.janitor.stop <- struct{}{}
}
