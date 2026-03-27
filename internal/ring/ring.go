// Package ring implements a consistent hash ring with virtual nodes.
// Ported from gokv's hashring package.
package ring

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"log/slog"
	"sort"
	"strconv"
	"sync"
)

type vNode struct {
	hash   uint32
	nodeID string
}

// Ring is a thread-safe consistent hash ring.
type Ring struct {
	mu         sync.RWMutex
	hash       hash.Hash32
	vNodeCount int
	Replicas   int
	vNodes     []vNode
}

func New(vNodeCount int, replicas int) *Ring {
	return &Ring{
		vNodeCount: vNodeCount,
		Replicas:   replicas,
		hash:       fnv.New32a(),
	}
}

// Add inserts one or more nodes into the ring.
func (r *Ring) Add(nodeIDs ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, node := range nodeIDs {
		for i := 0; i < r.vNodeCount; i++ {
			r.hash.Write([]byte(strconv.Itoa(i) + node))
			h := r.hash.Sum32()
			r.hash.Reset()
			r.vNodes = append(r.vNodes, vNode{hash: h, nodeID: node})
		}
	}
	sort.Slice(r.vNodes, func(i, j int) bool { return r.vNodes[i].hash < r.vNodes[j].hash })
}

// Remove deletes a node and all its virtual nodes from the ring.
func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	newVNodes := make([]vNode, 0, len(r.vNodes))
	for _, v := range r.vNodes {
		if v.nodeID != nodeID {
			newVNodes = append(newVNodes, v)
		}
	}
	r.vNodes = newVNodes
}

// Get returns the node IDs responsible for key. The first ID is the primary owner.
// Returns up to Replicas+1 unique node IDs.
func (r *Ring) Get(key string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vNodes) == 0 {
		return []string{}
	}

	r.hash.Write([]byte(key))
	h := r.hash.Sum32()
	r.hash.Reset()

	idx := sort.Search(len(r.vNodes), func(i int) bool {
		return r.vNodes[i].hash >= h
	})
	if idx == len(r.vNodes) {
		idx = 0
	}

	uniqueNodes := make([]string, 0, r.Replicas+1)
	seen := make(map[string]struct{})

	i := idx
	for len(uniqueNodes) < r.Replicas+1 && len(seen) < len(r.vNodes)/r.vNodeCount {
		v := r.vNodes[i]
		if _, exists := seen[v.nodeID]; !exists {
			seen[v.nodeID] = struct{}{}
			uniqueNodes = append(uniqueNodes, v.nodeID)
		}
		i++
		if i == len(r.vNodes) {
			i = 0
		}
	}

	return uniqueNodes
}

// GetNodes returns all unique node IDs currently in the ring.
func (r *Ring) GetNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vNodes) == 0 {
		return []string{}
	}

	nodeCount := len(r.vNodes) / r.vNodeCount
	uniqueNodes := make([]string, 0, nodeCount)
	seen := make(map[string]struct{}, nodeCount)

	for _, v := range r.vNodes {
		if _, exists := seen[v.nodeID]; !exists {
			seen[v.nodeID] = struct{}{}
			uniqueNodes = append(uniqueNodes, v.nodeID)
		}
	}

	return uniqueNodes
}

// GetVersion returns a hash of the current ring topology.
// Changes whenever nodes are added or removed.
func (r *Ring) GetVersion() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vNodes) == 0 {
		slog.Warn("ring: error computing version: no nodes")
		return 0
	}

	hasher := fnv.New64a()
	for _, v := range r.vNodes {
		if err := binary.Write(hasher, binary.BigEndian, int64(v.hash)); err != nil {
			slog.Warn(fmt.Sprintf("ring: error computing version: %v", err))
			return 0
		}
	}

	return hasher.Sum64()
}

// Copy returns a point-in-time snapshot of the ring.
func (r *Ring) Copy() *Ring {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c := &Ring{
		hash:       r.hash,
		vNodeCount: r.vNodeCount,
		Replicas:   r.Replicas,
		vNodes:     make([]vNode, len(r.vNodes)),
	}
	copy(c.vNodes, r.vNodes)
	return c
}
