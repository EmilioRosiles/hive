package store

import (
	"github.com/vmihailenco/msgpack/v5"
)

// SetStructure is a set of unique string members. There is no per-member
// TTL — only a key-level expiry applies.
// The shard lock in DataStore protects all field access — no internal lock needed.
type SetStructure struct {
	sizeBase
	mtimeBase
	members   map[string]struct{}
	expiresAt int64 // key-level expiry, unix seconds, 0 = no expiry
}

func NewSetStructure() *SetStructure {
	return &SetStructure{members: make(map[string]struct{})}
}

func (s *SetStructure) Kind() Kind           { return KindSet }
func (s *SetStructure) KeyExpiry() int64     { return s.expiresAt }
func (s *SetStructure) SetKeyExpiry(t int64) { s.expiresAt = t }

func (s *SetStructure) ByteSize() int64 {
	var n int64
	for m := range s.members {
		n += int64(len(m)) + mapBucketOverhead
	}
	return n + mtimeSize + keyExpirySize
}

// Add adds a member. No-op if it already exists.
func (s *SetStructure) Add(member string) {
	s.members[member] = struct{}{}
}

// Remove deletes a member. No-op if it does not exist.
func (s *SetStructure) Remove(member string) {
	delete(s.members, member)
}

// IsMember reports whether member exists.
func (s *SetStructure) IsMember(member string) bool {
	_, ok := s.members[member]
	return ok
}

// Members returns all members.
func (s *SetStructure) Members() []string {
	out := make([]string, 0, len(s.members))
	for m := range s.members {
		out = append(out, m)
	}
	return out
}

// Card returns the number of members.
func (s *SetStructure) Card() int {
	return len(s.members)
}

// -- serialization for rebalance --

// wireSet is the msgpack-serializable form of SetStructure.
type wireSet struct {
	Members   map[string]struct{} `msgpack:"m"`
	ExpiresAt int64               `msgpack:"e"`
	MTime     uint32              `msgpack:"mt"`
}

func (s *SetStructure) Encode() ([]byte, error) {
	return msgpack.Marshal(wireSet{Members: s.members, ExpiresAt: s.expiresAt, MTime: s.mtime})
}

func DecodeSetStructure(data []byte) (*SetStructure, error) {
	var w wireSet
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Members == nil {
		w.Members = make(map[string]struct{})
	}
	ss := &SetStructure{members: w.Members, expiresAt: w.ExpiresAt}
	ss.mtime = w.MTime
	return ss, nil
}
