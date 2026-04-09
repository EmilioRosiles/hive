// Package cluster manages the node's membership in the Hive cluster,
// including peer tracking, consistent hashing, data routing, and rebalancing.
package cluster

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/EmilioRosiles/hive/internal/ring"
	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// Config holds all configuration for the cluster manager.
type Config struct {
	NodeID            string
	BindAddr          string
	BindPort          int
	Seeds             []string
	ReplicationFactor int
	MemLimit          uint64
	GossipInterval    time.Duration
	GossipFanout      int
	RebalanceDebounce time.Duration
	DeadTimeout       time.Duration
	Clustered         bool
}

// PeerInfo is the canonical peer representation used both as internal mutable
// state and as the read-only snapshot returned by Peers().
type PeerInfo struct {
	NodeID            string
	Addr              string
	Alive             bool
	LastSeen          time.Time
	ReplicationFactor int
	MemLimit          uint64
}

// Manager owns the cluster state for this node.
type Manager struct {
	mu         sync.RWMutex
	cfg        Config
	ring       *ring.Ring
	store      *store.DataStore
	peers      map[string]*PeerInfo
	clients    map[string]*transport.Client
	rebalancer *rebalancer
	server     *transport.Server
	stopCh     chan struct{}
}

// NewManager creates and starts a cluster Manager.
// In clustered mode it binds a TCP server and contacts Seeds to join.
func NewManager(cfg Config) (*Manager, error) {
	r := ring.New(cfg.ReplicationFactor)
	ds := store.NewDataStore(30 * time.Second)
	vNodeCount := computeVNodes(cfg.MemLimit)

	m := &Manager{
		cfg:     cfg,
		ring:    r,
		store:   ds,
		peers:   make(map[string]*PeerInfo),
		clients: make(map[string]*transport.Client),
		stopCh:  make(chan struct{}),
	}

	m.ring.Add(cfg.NodeID, vNodeCount)
	m.rebalancer = newRebalancer(cfg.RebalanceDebounce, m)
	m.rebalancer.lastRing = r.Copy()

	if cfg.Clustered {
		addr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)
		srv, err := transport.NewServer(addr, m.handleFrame)
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

// Shutdown gracefully stops the node.
func (m *Manager) Shutdown() error {
	close(m.stopCh)
	m.store.Stop()

	if m.server != nil {
		m.announceLeave()
		return m.server.Close()
	}
	return nil
}

// Peers returns a snapshot of all known peers.
func (m *Manager) Peers() []PeerInfo {
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
// No-op if the peer is already known and alive.
// ps must have a non-empty NodeID — bootstrap ensures this before any peer
// is inserted into the ring.
func (m *Manager) addPeer(ps transport.PeerState) error {
	if ps.ReplicationFactor != 0 && ps.ReplicationFactor != m.cfg.ReplicationFactor {
		return fmt.Errorf("replication factor mismatch: local=%d peer=%d (addr=%s) — all nodes must be configured with the same ReplicationFactor",
			m.cfg.ReplicationFactor, ps.ReplicationFactor, ps.Addr)
	}
	vNodeCount := computeVNodes(ps.MemLimit)

	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.peers[ps.NodeID]; ok {
		if !p.Alive {
			p.Alive = true
			p.LastSeen = time.Now()
			m.ring.Add(ps.NodeID, vNodeCount)
			m.clients[ps.NodeID] = transport.NewClient(ps.Addr)
			go m.rebalancer.schedule()
		}
		return nil
	}

	m.peers[ps.NodeID] = &PeerInfo{
		NodeID:            ps.NodeID,
		Addr:              ps.Addr,
		Alive:             true,
		LastSeen:          time.Now(),
		ReplicationFactor: ps.ReplicationFactor,
		MemLimit:          ps.MemLimit,
	}
	m.ring.Add(ps.NodeID, vNodeCount)
	m.clients[ps.NodeID] = transport.NewClient(ps.Addr)
	go m.rebalancer.schedule()

	slog.Info("cluster: added peer", "nodeID", ps.NodeID, "addr", ps.Addr)
	return nil
}

// markDead flags a peer as unreachable and drops its client connection,
// but leaves it in the ring until DeadTimeout elapses.
func (m *Manager) markDead(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[nodeID]
	if !ok || !p.Alive {
		return
	}
	p.Alive = false
	p.LastSeen = time.Now()
	delete(m.clients, nodeID)

	slog.Warn("cluster: peer marked dead", "node", nodeID)
}

// evictPeer removes a peer from the ring and triggers rebalance.
// Called only after DeadTimeout has elapsed since the peer was marked dead.
func (m *Manager) evictPeer(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[nodeID]
	if !ok {
		return
	}
	delete(m.peers, nodeID)
	m.ring.Remove(nodeID)
	go m.rebalancer.schedule()

	slog.Warn("cluster: peer evicted", "addr", p.Addr)
}

// getPeer returns a peer by node ID.
func (m *Manager) getPeer(nodeID string) (*PeerInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	return p, ok
}

// getClient returns the transport client for a peer node ID.
func (m *Manager) getClient(nodeID string) (*transport.Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.clients[nodeID]
	return c, ok
}

// randomAlivePeers returns up to n randomly selected alive peers.
func (m *Manager) randomAlivePeers(n int) []*PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	alive := make([]*PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Alive {
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
func (m *Manager) responsibleNodes(key string) []string {
	return m.ring.Get(key)
}

// vNode count constants for weighted consistent hashing.
const (
	vNodesPerUnit = 100       // virtual nodes per unitSize of memory
	unitSize      = 256 << 20 // 256 MiB
	defaultVNodes = 100
)

// computeVNodes derives the virtual node count from a memory limit in bytes.
// A node with no MemLimit gets defaultVNodes, preserving previous behaviour.
func computeVNodes(memLimit uint64) int {
	return max(defaultVNodes, int(memLimit/unitSize)*vNodesPerUnit)
}
