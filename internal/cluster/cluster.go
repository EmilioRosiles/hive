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
	VNodes            int
	GossipInterval    time.Duration
	GossipFanout      int
	RebalanceDebounce time.Duration
	DeadTimeout       time.Duration
	Clustered         bool
}

// PeerInfo is a read-only snapshot of a peer's state.
type PeerInfo struct {
	NodeID   string
	Addr     string
	Alive    bool
	LastSeen time.Time
}

type peer struct {
	nodeID   string
	addr     string
	alive    bool
	lastSeen time.Time
}

// Manager owns the cluster state for this node.
type Manager struct {
	mu        sync.RWMutex
	cfg       Config
	ring      *ring.Ring
	store     *store.DataStore
	peers     map[string]*peer
	clients   map[string]*transport.Client
	rebalance *rebalanceManager
	server    *transport.Server
	stopCh    chan struct{}
}

// NewManager creates and starts a cluster Manager.
// In clustered mode it binds a TCP server and contacts Seeds to join.
func NewManager(cfg Config) (*Manager, error) {
	r := ring.New(cfg.VNodes, cfg.ReplicationFactor)
	ds := store.NewDataStore(30 * time.Second)

	m := &Manager{
		cfg:     cfg,
		ring:    r,
		store:   ds,
		peers:   make(map[string]*peer),
		clients: make(map[string]*transport.Client),
		stopCh:  make(chan struct{}),
	}

	m.ring.Add(cfg.NodeID)
	m.rebalance = newRebalanceManager(cfg.RebalanceDebounce, m)
	m.rebalance.lastRing = r.Copy()

	if cfg.Clustered {
		addr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)
		srv, err := transport.NewServer(addr, m.handleFrame)
		if err != nil {
			return nil, err
		}
		m.server = srv
		go srv.Serve()

		for _, seed := range cfg.Seeds {
			m.addPeer(seed)
		}

		go m.startGossip()
	}

	slog.Info(fmt.Sprintf("hive: node %s started (clustered=%v)", cfg.NodeID, cfg.Clustered))
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
		out = append(out, PeerInfo{
			NodeID:   p.nodeID,
			Addr:     p.addr,
			Alive:    p.alive,
			LastSeen: p.lastSeen,
		})
	}
	return out
}

// addPeer registers a peer by address and connects to it.
// No-op if the address belongs to this node.
func (m *Manager) addPeer(addr string) {
	selfAddr := fmt.Sprintf("%s:%d", m.cfg.BindAddr, m.cfg.BindPort)
	if addr == selfAddr {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we already know this peer by address.
	for _, p := range m.peers {
		if p.addr == addr {
			if !p.alive {
				p.alive = true
				p.lastSeen = time.Now()
				m.ring.Add(p.nodeID)
				m.clients[p.nodeID] = transport.NewClient(addr)
				go m.rebalance.schedule()
			}
			return
		}
	}

	// New peer — use addr as a temporary ID until gossip tells us the real NodeID.
	p := &peer{nodeID: "", addr: addr, alive: true, lastSeen: time.Now()}
	m.peers[addr] = p
	m.ring.Add(addr)
	m.clients[addr] = transport.NewClient(addr)
	go m.rebalance.schedule()

	slog.Info(fmt.Sprintf("cluster: added peer %s", addr))
}

// markDead flags a peer as unreachable and drops its client connection,
// but leaves it in the ring until DeadTimeout elapses.
func (m *Manager) markDead(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[nodeID]
	if !ok || !p.alive {
		return
	}
	p.alive = false
	p.lastSeen = time.Now()
	delete(m.clients, nodeID)

	slog.Warn(fmt.Sprintf("cluster: peer %s marked dead", nodeID))
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
	go m.rebalance.schedule()

	slog.Warn(fmt.Sprintf("cluster: peer %s evicted (was dead for > DeadTimeout)", p.addr))
}

// getPeer returns a peer by node ID.
func (m *Manager) getPeer(nodeID string) (*peer, bool) {
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
func (m *Manager) randomAlivePeers(n int) []*peer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	alive := make([]*peer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.alive {
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
