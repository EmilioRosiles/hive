// Package cluster manages the node's membership in the Hive cluster,
// including peer tracking, consistent hashing, data routing, and rebalancing.
package cluster

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EmilioRosiles/hive/internal/ring"
	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// NodeStatus represents a peer's liveness state in the cluster.
type NodeStatus uint8

const (
	NodeAlive NodeStatus = 0
	NodeDead  NodeStatus = 1
)

// Config holds all configuration for the cluster manager.
type Config struct {
	NodeID               string
	BindAddr             string
	BindPort             int
	Seeds                []string
	RoutingTimeout       time.Duration
	ReplicationFactor    int
	MemLimit             uint64
	GossipInterval       time.Duration
	GossipFanout         int
	GossipTimeout        time.Duration
	RebalanceDebounce    time.Duration
	RebalanceBatchSize   int
	ReplicationQueueSize int
	ReplicationBatchSize int
	CleanupInterval      time.Duration
	Clustered            bool
	TLSConfig            *tls.Config
}

// PeerInfo is the canonical peer representation used both as internal mutable
// state and as the read-only snapshot returned by Peers().
type PeerInfo struct {
	NodeID            string
	Addr              string
	Status            NodeStatus
	Incarnation       uint64
	ReplicationFactor int
	MemLimit          uint64
	MemUsed           uint64
}

// Cluster owns the cluster state for this node.
type Cluster struct {
	mu          sync.RWMutex
	cfg         Config
	ring        *ring.Ring
	store       *store.DataStore
	peers       map[string]*PeerInfo
	clients     map[string]*transport.Client
	replicators map[string]*replicator
	rebalancer  *rebalancer
	server      *transport.Server
	stopCh      chan struct{}
	stopOnce    sync.Once
	incarnation atomic.Uint64
}

// NewCluster creates and starts a cluster Cluster.
// In clustered mode it binds a TCP server and contacts Seeds to join.
func NewCluster(cfg Config) (*Cluster, error) {
	r := ring.New(cfg.ReplicationFactor)
	ds := store.NewDataStore(cfg.MemLimit)
	vNodeCount := computeVNodes(cfg.MemLimit)

	m := &Cluster{
		cfg:         cfg,
		ring:        r,
		store:       ds,
		peers:       make(map[string]*PeerInfo),
		clients:     make(map[string]*transport.Client),
		replicators: make(map[string]*replicator),
		stopCh:      make(chan struct{}),
	}
	m.incarnation.Store(uint64(time.Now().UnixNano()))
	m.ring.Add(cfg.NodeID, vNodeCount)
	m.rebalancer = newRebalancer(cfg.RebalanceDebounce, m)
	go m.startJanitor()

	if cfg.Clustered {
		addr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)
		srv, err := transport.NewServer(addr, m.handleFrame, cfg.TLSConfig)
		if err != nil {
			return nil, err
		}
		m.server = srv
		go srv.Serve()
		for _, seed := range cfg.Seeds {
			m.bootstrap(seed)
		}
		go m.startGossip()
	}

	slog.Info("hive: node started", "node", cfg.NodeID, "clustered", cfg.Clustered)
	return m, nil
}

// Shutdown gracefully stops the node. Safe to call more than once.
func (m *Cluster) Shutdown() error {
	var err error
	m.stopOnce.Do(func() {
		close(m.stopCh)
		if m.server != nil {
			m.announceLeave()
			err = m.server.Close()
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		for _, rep := range m.replicators {
			rep.stop()
		}

		for _, c := range m.clients {
			c.Close()
		}
	})
	return err
}

// Used returns this node's current estimated local byte usage.
func (m *Cluster) Used() int64 {
	return m.store.Used()
}

// KeyCount returns the number of live keys held locally by this node.
func (m *Cluster) KeyCount() int {
	return m.store.Len()
}

// Peers returns a snapshot of all known peers.
func (m *Cluster) Peers() []PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		out = append(out, *p)
	}
	return out
}

// addPeer registers a peer and opens a connection to it.
// Returns an error if the peer's ReplicationFactor conflicts with ours.
func (m *Cluster) addPeer(ps transport.PeerState) error {
	if ps.ReplicationFactor != 0 && ps.ReplicationFactor != m.cfg.ReplicationFactor {
		return fmt.Errorf("replication factor mismatch: local=%d peer=%d (addr=%s) — all nodes must be configured with the same ReplicationFactor",
			m.cfg.ReplicationFactor, ps.ReplicationFactor, ps.Addr)
	}
	vNodeCount := computeVNodes(ps.MemLimit)

	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.peers[ps.NodeID]; ok {
		p.MemUsed = ps.MemUsed
		if p.Status != NodeAlive {
			p.Status = NodeAlive
			p.Incarnation = ps.Incarnation
			m.ring.Add(ps.NodeID, vNodeCount)
			m.clients[ps.NodeID] = m.newClient(ps.Addr)
			m.replicators[ps.NodeID] = newReplicator(ps.NodeID, m)
			go m.rebalancer.schedule()
		}
		return nil
	}

	m.peers[ps.NodeID] = &PeerInfo{
		NodeID:            ps.NodeID,
		Addr:              ps.Addr,
		Status:            NodeAlive,
		Incarnation:       ps.Incarnation,
		ReplicationFactor: ps.ReplicationFactor,
		MemLimit:          ps.MemLimit,
		MemUsed:           ps.MemUsed,
	}
	m.ring.Add(ps.NodeID, vNodeCount)
	m.clients[ps.NodeID] = m.newClient(ps.Addr)
	m.replicators[ps.NodeID] = newReplicator(ps.NodeID, m)
	go m.rebalancer.schedule()

	slog.Info("cluster: added peer", "nodeID", ps.NodeID, "addr", ps.Addr)
	return nil
}

// markDead promotes a peer to the Dead state.
func (m *Cluster) markDead(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[nodeID]
	if !ok || p.Status == NodeDead {
		return
	}
	p.Status = NodeDead
	m.ring.Remove(nodeID)
	delete(m.clients, nodeID)
	if rep, ok := m.replicators[nodeID]; ok {
		rep.stop()
		delete(m.replicators, nodeID)
	}
	go m.rebalancer.schedule()
	slog.Warn("cluster: peer marked dead", "node", nodeID)
}

// startJanitor runs the cleanup loop until the node shuts down.
// On each tick it removes expired store entries and evicts dead-peer tombstones.
func (m *Cluster) startJanitor() {
	ticker := time.NewTicker(m.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.store.DeleteExpired()
			m.evictDeadPeers()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Cluster) evictDeadPeers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for nodeID, p := range m.peers {
		if p.Status == NodeDead {
			delete(m.peers, nodeID)
			slog.Info("cluster: evicted dead peer tombstone", "node", nodeID)
		}
	}
}

// getPeer returns a peer by node ID. The returned *PeerInfo is the live,
// mutable entry shared with other goroutines — callers must hold m.mu before
// touching its fields. Prefer peerStatus for a simple status check.
func (m *Cluster) getPeer(nodeID string) (*PeerInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	return p, ok
}

// peerStatus returns nodeID's current status as a locked, point-in-time
// snapshot, safe to read without further synchronization.
func (m *Cluster) peerStatus(nodeID string) (NodeStatus, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return 0, false
	}
	return p.Status, true
}

// newClient builds a transport client for addr, applying this node's TLS
// config (nil means plaintext).
func (m *Cluster) newClient(addr string) *transport.Client {
	return transport.NewClient(addr, m.cfg.TLSConfig)
}

// getClient returns the transport client for a peer node ID.
func (m *Cluster) getClient(nodeID string) (*transport.Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.clients[nodeID]
	return c, ok
}

// getReplicator returns the replication queue worker for a peer node ID.
func (m *Cluster) getReplicator(nodeID string) (*replicator, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.replicators[nodeID]
	return r, ok
}

// randomAlivePeers returns up to n randomly selected alive peers.
func (m *Cluster) randomAlivePeers(n int) []*PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	alive := make([]*PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Status == NodeAlive {
			alive = append(alive, p)
		}
	}
	rand.Shuffle(len(alive), func(i, j int) { alive[i], alive[j] = alive[j], alive[i] })
	if n > len(alive) {
		n = len(alive)
	}
	return alive[:n]
}

// responsibleNodes returns the node IDs responsible for a key.
func (m *Cluster) responsibleNodes(key string) []string {
	return m.ring.Get(key)
}

// vNode count constants for weighted consistent hashing.
const (
	vNodesPerUnit = 32        // virtual nodes per unitSize of memory
	unitSize      = 256 << 20 // 256 MiB
)

// computeVNodes derives the virtual node count from a memory limit in bytes.
// A memLimit of exactly 0 yields 0 vnodes — no keyspace ownership, used for
// a pure routing/relay node. Any positive memLimit yields at least 1 vnode,
// even if it's smaller than unitSize, so a small-but-nonzero limit is never
// silently rounded down to zero ownership by integer truncation.
func computeVNodes(memLimit uint64) int {
	if memLimit == 0 {
		return 0
	}
	return max(1, int(memLimit/unitSize)*vNodesPerUnit)
}
