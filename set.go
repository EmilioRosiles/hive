package hive

import "time"

// SetStore is a distributed string set backed by a Node. Keys are namespaced
// as {name}:s:{key} to prevent collisions with other stores on the same node.
//
// Members can carry an optional TTL that evicts them independently of the key:
//
//	online := hive.NewSetStore(node, "online_users")
//	online.SAdd("room:1", "user:123")
//	online.SExpireMember("room:1", "user:123", 30*time.Second)
//	members, _ := online.SMembers("room:1")
type SetStore struct {
	node   *Node
	prefix string
}

// NewSetStore creates a set store backed by node.
// name is used as the namespace — use a distinct name per set.
func NewSetStore(node *Node, name string) *SetStore {
	return &SetStore{node: node, prefix: name + ":s:"}
}

// Add adds member to the set at key with no expiry.
// If member already exists its TTL is cleared.
func (s *SetStore) SAdd(key, member string) error {
	return s.node.cluster.SAdd(s.prefix+key, member, 0)
}

// Rem removes member from the set at key.
func (s *SetStore) SRem(key, member string) error {
	return s.node.cluster.SRem(s.prefix+key, member)
}

// IsMember reports whether member exists in the set at key and has not expired.
func (s *SetStore) SIsMember(key, member string) (bool, error) {
	return s.node.cluster.SIsMember(s.prefix+key, member)
}

// Members returns all live members of the set at key.
func (s *SetStore) SMembers(key string) ([]string, error) {
	return s.node.cluster.SMembers(s.prefix + key)
}

// Card returns the number of live members in the set at key.
func (s *SetStore) SCard(key string) (int, error) {
	return s.node.cluster.SCard(s.prefix + key)
}

// Del removes the entire set at key.
func (s *SetStore) Del(key string) error {
	return s.node.cluster.Del(s.prefix + key)
}

// Expire sets a key-level TTL. The entire set is deleted after ttl elapses.
// A ttl of 0 removes any existing expiry.
func (s *SetStore) Expire(key string, ttl time.Duration) error {
	return s.node.cluster.Expire(s.prefix+key, ttl)
}

// ExpireMember sets a TTL on a single member. The member is evicted after ttl
// elapses without affecting other members or the key itself.
func (s *SetStore) SExpireMember(key, member string, ttl time.Duration) error {
	return s.node.cluster.SExpireMember(s.prefix+key, member, ttl)
}
