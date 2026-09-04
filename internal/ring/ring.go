// Package ring implements a consistent hash ring with virtual nodes.
// Ported from gokv's hashring package.
package ring

import (
	"encoding/binary"
	"hash/fnv"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type vNode struct {
	hash   uint32
	nodeID string
}

// ringState is an immutable snapshot of the ring's topology.
type ringState struct {
	nodes  map[string]int // nodeID -> vNodeCount
	vNodes []vNode
}

// Ring is a thread-safe consistent hash ring with virtual nodes.
type Ring struct {
	mu       sync.Mutex
	Replicas int
	state    atomic.Pointer[ringState]
	logger   *slog.Logger
}

func New(replicas int, logger *slog.Logger) *Ring {
	r := &Ring{Replicas: replicas, logger: logger}
	r.state.Store(&ringState{nodes: make(map[string]int)})
	return r
}

// Add inserts a node into the ring with the given virtual node count.
// Safe to call on an already-present node — existing vNodes are replaced.
func (r *Ring) Add(nodeID string, count int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing := r.state.Load()
	nodes := maps.Clone(existing.nodes)
	vNodes := make([]vNode, 0, len(existing.vNodes)+count)
	for _, v := range existing.vNodes {
		if v.nodeID != nodeID {
			vNodes = append(vNodes, v)
		}
	}

	nodes[nodeID] = count
	for i := range count {
		vNodes = append(vNodes, vNode{hash: hashKey(strconv.Itoa(i) + nodeID), nodeID: nodeID})
	}
	sort.Slice(vNodes, func(i, j int) bool { return vNodes[i].hash < vNodes[j].hash })

	r.state.Store(&ringState{nodes: nodes, vNodes: vNodes})
}

// Remove deletes a node and all its virtual nodes from the ring.
func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing := r.state.Load()
	nodes := maps.Clone(existing.nodes)
	delete(nodes, nodeID)

	vNodes := make([]vNode, 0, len(existing.vNodes))
	for _, v := range existing.vNodes {
		if v.nodeID != nodeID {
			vNodes = append(vNodes, v)
		}
	}

	r.state.Store(&ringState{nodes: nodes, vNodes: vNodes})
}

// Get returns the node IDs responsible for key. The first ID is the primary owner.
// Returns up to Replicas unique node IDs.
func (r *Ring) Get(key string) []string {
	st := r.state.Load()
	if len(st.vNodes) == 0 {
		return []string{}
	}

	h := hashKey(key)
	idx := sort.Search(len(st.vNodes), func(i int) bool {
		return st.vNodes[i].hash >= h
	})
	if idx == len(st.vNodes) {
		idx = 0
	}

	uniqueNodes := make([]string, 0, r.Replicas)

	i := idx
	for len(uniqueNodes) < r.Replicas && len(uniqueNodes) < len(st.nodes) {
		nodeID := st.vNodes[i].nodeID
		if !slices.Contains(uniqueNodes, nodeID) {
			uniqueNodes = append(uniqueNodes, nodeID)
		}
		i++
		if i == len(st.vNodes) {
			i = 0
		}
	}

	return uniqueNodes
}

// GetNodes returns all unique node IDs currently in the ring.
func (r *Ring) GetNodes() []string {
	st := r.state.Load()
	uniqueNodes := make([]string, 0, len(st.nodes))
	for nodeID := range st.nodes {
		uniqueNodes = append(uniqueNodes, nodeID)
	}
	return uniqueNodes
}

// GetVersion returns a hash of the current ring topology.
// Changes whenever nodes are added or removed.
func (r *Ring) GetVersion() uint64 {
	st := r.state.Load()
	if len(st.vNodes) == 0 {
		r.logger.Warn("ring: error computing version: no nodes")
		return 0
	}

	hasher := fnv.New64a()
	for _, v := range st.vNodes {
		if err := binary.Write(hasher, binary.BigEndian, int64(v.hash)); err != nil {
			r.logger.Warn("ring: error computing version", "err", err)
			return 0
		}
	}

	return hasher.Sum64()
}

const (
	fnvOffset32 uint32 = 2166136261
	fnvPrime32  uint32 = 16777619
)

// hashKey returns the FNV-1a 32-bit hash of s.
func hashKey(s string) uint32 {
	h := fnvOffset32
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= fnvPrime32
	}
	return h
}

// Copy returns a point-in-time snapshot of the ring.
func (r *Ring) Copy() *Ring {
	c := &Ring{Replicas: r.Replicas}
	c.state.Store(r.state.Load())
	return c
}
