package cluster

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// startGossip runs the heartbeat loop until the node shuts down.
func (m *Manager) startGossip() {
	interval := m.cfg.GossipInterval
	jitterRange := time.Duration(float64(interval) * 0.25)

	for {
		jitter := time.Duration(rand.Int63n(int64(jitterRange)*2)) - jitterRange
		select {
		case <-m.stopCh:
			return
		case <-time.After(interval + jitter):
		}

		targets := m.randomAlivePeers(m.cfg.GossipFanout)
		m.heartbeat(targets...)
		m.evictDeadPeers()
		go m.rebalance.schedule()
	}
}

// evictDeadPeers removes peers that have been dead longer than DeadTimeout.
func (m *Manager) evictDeadPeers() {
	m.mu.RLock()
	var toEvict []string
	for nodeID, p := range m.peers {
		if !p.alive && time.Since(p.lastSeen) > m.cfg.DeadTimeout {
			toEvict = append(toEvict, nodeID)
		}
	}
	m.mu.RUnlock()

	for _, nodeID := range toEvict {
		m.evictPeer(nodeID)
	}
}

// heartbeat sends this node's view of the cluster to each target peer.
// Peers that fail to respond are removed from the cluster.
func (m *Manager) heartbeat(targets ...*peer) {
	if len(targets) == 0 {
		return
	}

	payload, err := transport.Encode(m.buildHeartbeatRequest())
	if err != nil {
		slog.Warn(fmt.Sprintf("gossip: encode heartbeat: %v", err))
		return
	}
	frame := transport.Frame{Type: transport.MsgHeartbeat, Payload: payload}

	for _, p := range targets {
		client, ok := m.getClient(p.nodeID)
		if !ok {
			m.markDead(p.nodeID)
			continue
		}

		resp, err := client.Send(frame)
		if err != nil {
			slog.Warn(fmt.Sprintf("gossip: heartbeat failed for %s: %v", p.nodeID, err))
			m.markDead(p.nodeID)
			continue
		}

		var hbResp transport.HeartbeatResponse
		if err := transport.Decode(resp.Payload, &hbResp); err != nil {
			slog.Warn(fmt.Sprintf("gossip: decode response from %s: %v", p.nodeID, err))
			continue
		}

		m.mergeState(hbResp.Peers)
	}
}

// mergeState reconciles a peer's view of the cluster with our own.
func (m *Manager) mergeState(remote []transport.PeerState) {
	for _, rs := range remote {
		if rs.NodeID == m.cfg.NodeID {
			continue
		}

		local, exists := m.getPeer(rs.NodeID)

		if !exists {
			if rs.Alive {
				m.addPeer(rs.Addr)
			}
			continue
		}

		if rs.LastSeen.After(local.lastSeen) {
			m.mu.Lock()
			local.lastSeen = rs.LastSeen
			local.addr = rs.Addr
			m.mu.Unlock()

			if !rs.Alive && local.alive {
				m.markDead(rs.NodeID)
			} else if rs.Alive && !local.alive {
				m.addPeer(rs.Addr)
			}
		}
	}
}

// buildHeartbeatRequest assembles the current node's peer list for gossip.
func (m *Manager) buildHeartbeatRequest() transport.HeartbeatRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]transport.PeerState, 0, len(m.peers)+1)

	// Include self.
	peers = append(peers, transport.PeerState{
		NodeID:   m.cfg.NodeID,
		Addr:     fmt.Sprintf("%s:%d", m.cfg.BindAddr, m.cfg.BindPort),
		Alive:    true,
		LastSeen: time.Now(),
	})

	for _, p := range m.peers {
		peers = append(peers, transport.PeerState{
			NodeID:   p.nodeID,
			Addr:     p.addr,
			Alive:    p.alive,
			LastSeen: p.lastSeen,
		})
	}

	return transport.HeartbeatRequest{Peers: peers}
}

// announceLeave notifies alive peers that this node is departing.
func (m *Manager) announceLeave() {
	payload, err := transport.Encode(transport.LeaveRequest{NodeID: m.cfg.NodeID})
	if err != nil {
		return
	}
	frame := transport.Frame{Type: transport.MsgLeave, Payload: payload}
	for _, p := range m.randomAlivePeers(len(m.peers)) {
		if client, ok := m.getClient(p.nodeID); ok {
			client.Send(frame) //nolint:errcheck
		}
	}
}
