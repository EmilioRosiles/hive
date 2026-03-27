// Package store provides a thread-safe, sharded in-memory key-value store with TTL support.
package store

import (
	"hash/fnv"
	"runtime"
	"sync"
	"time"
)

// Entry holds a raw byte payload with an optional expiry.
type Entry struct {
	mu        sync.RWMutex
	Data      []byte
	expiresAt int64
}

func NewEntry(data []byte) *Entry {
	return &Entry{Data: data}
}

func (e *Entry) ExpiresAt() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.expiresAt
}

func (e *Entry) SetTTL(ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ttl == 0 {
		e.expiresAt = 0
	} else {
		e.expiresAt = time.Now().Add(ttl).Unix()
	}
}

// -- DataStore --

type shard struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

// DataStore is a thread-safe, sharded in-memory key-value store.
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
		ds.shards[i] = &shard{data: make(map[string]*Entry)}
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

// GetEntry returns the entry directly (used by rebalance to read Data + TTL together).
func (ds *DataStore) GetEntry(key string) *Entry {
	e, ok := ds.Get(key)
	if !ok {
		return nil
	}
	return e
}

func (ds *DataStore) Get(key string) (*Entry, bool) {
	s := ds.getShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return nil, false
	}
	if exp := e.ExpiresAt(); exp != 0 && time.Now().Unix() > exp {
		return nil, false
	}
	return e, true
}

func (ds *DataStore) Set(key string, e *Entry) {
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
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	e.SetTTL(ttl)
	return true
}

// Scan iterates over all non-expired entries. Pass cursor=-1 to scan all shards.
func (ds *DataStore) Scan(cursor, count int, fn func(key string, e *Entry)) {
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
			if exp := e.ExpiresAt(); exp != 0 && now > exp {
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
	now := time.Now().Unix()
	for i := range ds.shardsCount {
		s := ds.shards[i]
		s.mu.Lock()
		for key, e := range s.data {
			if exp := e.ExpiresAt(); exp != 0 && now > exp {
				delete(s.data, key)
			}
		}
		s.mu.Unlock()
	}
}

func stopJanitor(ds *DataStore) {
	ds.janitor.stop <- struct{}{}
}
