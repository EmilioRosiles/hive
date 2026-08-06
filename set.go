package hive

import (
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// SetStore is a distributed string set backed by a Cluster. Keys are namespaced
// as {name}:s:{key} to prevent collisions with other stores on the same node.
//
//	online := hive.NewSetStore(cluster, "online_users")
//	online.SAdd("room:1", "user:123")
//	online.SExpireMember("room:1", "user:123", 30*time.Second)
type SetStore struct {
	cluster *Cluster
	prefix  string
}

// NewSetStore creates a set store backed by cluster.
// name is used as the namespace — use a distinct name per set.
func NewSetStore(cluster *Cluster, name string) *SetStore {
	return &SetStore{cluster: cluster, prefix: name + ":s:"}
}

// SAdd adds member to the set at key with no expiry.
func (s *SetStore) SAdd(key, member string) error {
	_, err := s.cluster.exec(transport.OpSAdd, s.prefix+key, []byte(member))
	return err
}

// SRem removes member from the set at key.
func (s *SetStore) SRem(key, member string) error {
	_, err := s.cluster.exec(transport.OpSRem, s.prefix+key, []byte(member))
	return err
}

// SIsMember reports whether member exists in the set at key and has not expired.
func (s *SetStore) SIsMember(key, member string) (bool, error) {
	results, err := s.cluster.exec(transport.OpSIsMember, s.prefix+key, []byte(member))
	if err != nil {
		return false, err
	}
	return results[0][0] == 1, nil
}

// SMembers returns all live members of the set at key.
func (s *SetStore) SMembers(key string) ([]string, error) {
	results, err := s.cluster.exec(transport.OpSMembers, s.prefix+key)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(results))
	for i, b := range results {
		out[i] = string(b)
	}
	return out, nil
}

// SCard returns the number of live members in the set at key.
func (s *SetStore) SCard(key string) (int, error) {
	results, err := s.cluster.exec(transport.OpSCard, s.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// Del removes the entire set at key.
func (s *SetStore) Del(key string) error {
	_, err := s.cluster.exec(transport.OpDel, s.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire set is deleted after ttl elapses.
func (s *SetStore) Expire(key string, ttl time.Duration) error {
	_, err := s.cluster.exec(transport.OpExpire, s.prefix+key, encodeTTL(ttl))
	return err
}

// SExpireMember sets a TTL on a single member. The member is evicted after ttl
// elapses without affecting other members or the key itself.
func (s *SetStore) SExpireMember(key, member string, ttl time.Duration) error {
	_, err := s.cluster.exec(transport.OpSExpireMember, s.prefix+key, []byte(member), encodeTTL(ttl))
	return err
}
