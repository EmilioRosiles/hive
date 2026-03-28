package store

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// SetStructure is a set of unique string members with optional per-member TTL.
// The shard lock in DataStore protects all field access — no internal lock needed.
type SetStructure struct {
	members   map[string]int64 // member → expiresAt unix nanoseconds, 0 = no expiry
	expiresAt int64            // key-level expiry, unix seconds, 0 = no expiry
}

func NewSetStructure() *SetStructure {
	return &SetStructure{members: make(map[string]int64)}
}

func (s *SetStructure) Kind() Kind           { return KindSet }
func (s *SetStructure) KeyExpiry() int64     { return s.expiresAt }
func (s *SetStructure) SetKeyExpiry(t int64) { s.expiresAt = t }

// Add adds a member with no expiry. If the member already exists its TTL is cleared.
func (s *SetStructure) Add(member string) {
	s.members[member] = 0
}

// AddWithTTL adds a member that expires after ttl.
func (s *SetStructure) AddWithTTL(member string, ttl time.Duration) {
	s.members[member] = time.Now().Add(ttl).UnixNano()
}

// Remove deletes a member. No-op if it does not exist.
func (s *SetStructure) Remove(member string) {
	delete(s.members, member)
}

// IsMember reports whether member exists and has not expired.
func (s *SetStructure) IsMember(member string) bool {
	exp, ok := s.members[member]
	if !ok {
		return false
	}
	return exp == 0 || time.Now().UnixNano() <= exp
}

// Members returns all live (non-expired) members.
func (s *SetStructure) Members() []string {
	now := time.Now().UnixNano()
	out := make([]string, 0, len(s.members))
	for m, exp := range s.members {
		if exp == 0 || now <= exp {
			out = append(out, m)
		}
	}
	return out
}

// Card returns the number of live members.
func (s *SetStructure) Card() int {
	now := time.Now().UnixNano()
	n := 0
	for _, exp := range s.members {
		if exp == 0 || now <= exp {
			n++
		}
	}
	return n
}

// ExpireMember sets a TTL on an existing member. No-op if member does not exist.
func (s *SetStructure) ExpireMember(member string, ttl time.Duration) {
	if _, ok := s.members[member]; ok {
		s.members[member] = time.Now().Add(ttl).UnixNano()
	}
}

// Cleanup removes expired members and reports whether the set is empty.
// Called by the DataStore janitor while the shard write lock is held.
func (s *SetStructure) Cleanup(now time.Time) bool {
	nowNs := now.UnixNano()
	for m, exp := range s.members {
		if exp != 0 && nowNs > exp {
			delete(s.members, m)
		}
	}
	return len(s.members) == 0
}

// -- serialization for rebalance --

// wireSet is the msgpack-serializable form of SetStructure.
type wireSet struct {
	Members   map[string]int64 `msgpack:"m"`
	ExpiresAt int64            `msgpack:"e"`
}

func (s *SetStructure) Encode() ([]byte, error) {
	return msgpack.Marshal(wireSet{Members: s.members, ExpiresAt: s.expiresAt})
}

func DecodeSetStructure(data []byte) (*SetStructure, error) {
	var w wireSet
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Members == nil {
		w.Members = make(map[string]int64)
	}
	return &SetStructure{members: w.Members, expiresAt: w.ExpiresAt}, nil
}
