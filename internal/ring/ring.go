// Package ring implements a consistent hash ring with virtual nodes.
// Ported from gokv's hashring package.
package ring

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log/slog"
	"maps"
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
	mu       sync.RWMutex
	Replicas int
	nodes    map[string]int // nodeID -> vNodeCount
	vNodes   []vNode
}

func New(replicas int) *Ring {
	return &Ring{
		Replicas: replicas,
		nodes:    make(map[string]int),
	}
}

// Add inserts a node into the ring with the given virtual node count.
// Safe to call on an already-present node — existing vNodes are replaced.
func (r *Ring) Add(nodeID string, count int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		filtered := r.vNodes[:0]
		for _, v := range r.vNodes {
			if v.nodeID != nodeID {
				filtered = append(filtered, v)
			}
		}
		r.vNodes = filtered
	}

	r.nodes[nodeID] = count
	for i := range count {
		r.vNodes = append(r.vNodes, vNode{hash: hashKey(strconv.Itoa(i) + nodeID), nodeID: nodeID})
	}
	sort.Slice(r.vNodes, func(i, j int) bool { return r.vNodes[i].hash < r.vNodes[j].hash })
}

// Remove deletes a node and all its virtual nodes from the ring.
func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodes, nodeID)
	newVNodes := make([]vNode, 0, len(r.vNodes))
	for _, v := range r.vNodes {
		if v.nodeID != nodeID {
			newVNodes = append(newVNodes, v)
		}
	}
	r.vNodes = newVNodes
}

// Get returns the node IDs responsible for key. The first ID is the primary owner.
// Returns up to Replicas unique node IDs.
func (r *Ring) Get(key string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vNodes) == 0 {
		return []string{}
	}

	h := hashKey(key)
	idx := sort.Search(len(r.vNodes), func(i int) bool {
		return r.vNodes[i].hash >= h
	})
	if idx == len(r.vNodes) {
		idx = 0
	}

	uniqueNodes := make([]string, 0, r.Replicas)
	seen := make(map[string]struct{})

	i := idx
	for len(uniqueNodes) < r.Replicas && len(seen) < len(r.nodes) {
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

	uniqueNodes := make([]string, 0, len(r.nodes))
	for nodeID := range r.nodes {
		uniqueNodes = append(uniqueNodes, nodeID)
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

// hashKey returns the FNV-1a 32-bit hash of s.
func hashKey(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Copy returns a point-in-time snapshot of the ring.
func (r *Ring) Copy() *Ring {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make(map[string]int, len(r.nodes))
	maps.Copy(nodes, r.nodes)
	c := &Ring{
		Replicas: r.Replicas,
		nodes:    nodes,
		vNodes:   make([]vNode, len(r.vNodes)),
	}
	copy(c.vNodes, r.vNodes)
	return c
}
