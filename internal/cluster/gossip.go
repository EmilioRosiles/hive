package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// startGossip runs the heartbeat loop until the node shuts down.
func (m *Manager) startGossip() {
	interval := m.cfg.GossipInterval
	jitterRange := time.Duration(float64(interval) * 0.25)

	for {
		jitter := time.Duration(rand.Int64N(int64(jitterRange)*2)) - jitterRange
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
		if !p.Alive && time.Since(p.LastSeen) > m.cfg.DeadTimeout {
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
func (m *Manager) heartbeat(targets ...*PeerInfo) {
	if len(targets) == 0 {
		return
	}

	payload, err := transport.Encode(m.buildHeartbeatRequest())
	if err != nil {
		slog.Warn("gossip: encode heartbeat failed", "err", err)
		return
	}
	frame := transport.Frame{Type: transport.MsgHeartbeat, Payload: payload}

	for _, p := range targets {
		client, ok := m.getClient(p.NodeID)
		if !ok {
			m.markDead(p.NodeID)
			continue
		}

		resp, err := client.Send(frame)
		if err != nil {
			slog.Warn("gossip: heartbeat failed", "node", p.NodeID, "err", err)
			m.markDead(p.NodeID)
			continue
		}

		var hbResp transport.HeartbeatResponse
		if err := transport.Decode(resp.Payload, &hbResp); err != nil {
			slog.Warn("gossip: decode response failed", "node", p.NodeID, "err", err)
			continue
		}

		if err := m.mergeState(hbResp.Peers); err != nil {
			slog.Warn("gossip: merge state failed", "node", p.NodeID, "err", err)
		}
	}
}

// bootstrap sends a heartbeat to addr and merges the response into our cluster
// view. This is called once per seed at startup so the ring is populated with
// real NodeIDs before the gossip loop begins. Unreachable seeds are skipped —
// at least one must succeed for the node to join the cluster.
// If the seed rejects the join (e.g. replication factor mismatch), the node halts.
func (m *Manager) bootstrap(addr string) {
	payload, err := transport.Encode(m.buildHeartbeatRequest())
	if err != nil {
		return
	}
	client := transport.NewClient(addr)
	resp, err := client.Send(transport.Frame{Type: transport.MsgHeartbeat, Payload: payload})
	if err != nil {
		var rejected *transport.ErrRejected
		if errors.As(err, &rejected) {
			slog.Error("hive: cluster rejected join", "addr", addr, "reason", rejected.Error())
			os.Exit(1)
		}
		slog.Warn("bootstrap: seed unreachable", "addr", addr, "err", err)
		return
	}
	var hbResp transport.HeartbeatResponse
	if err := transport.Decode(resp.Payload, &hbResp); err != nil {
		slog.Warn("bootstrap: decode failed", "addr", addr, "err", err)
		return
	}
	if err := m.mergeState(hbResp.Peers); err != nil {
		slog.Error("hive: cluster rejected join", "addr", addr, "reason", err)
		os.Exit(1)
	}
	slog.Info("bootstrap: joined via seed", "addr", addr, "peers", len(hbResp.Peers))
}

// mergeState reconciles a peer's view of the cluster with our own.
// Returns the first error encountered, e.g. a replication factor mismatch.
func (m *Manager) mergeState(remote []transport.PeerState) error {
	for _, rs := range remote {
		if rs.NodeID == "" || rs.NodeID == m.cfg.NodeID {
			continue
		}

		local, exists := m.getPeer(rs.NodeID)

		if !exists {
			if rs.Alive {
				if err := m.addPeer(rs); err != nil {
					return err
				}
			}
			continue
		}

		if rs.LastSeen.After(local.LastSeen) {
			m.mu.Lock()
			local.LastSeen = rs.LastSeen
			local.Addr = rs.Addr
			m.mu.Unlock()

			if !rs.Alive && local.Alive {
				m.markDead(rs.NodeID)
			} else if rs.Alive && !local.Alive {
				if err := m.addPeer(rs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// buildHeartbeatRequest assembles the current node's peer list for gossip.
func (m *Manager) buildHeartbeatRequest() transport.HeartbeatRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]transport.PeerState, 0, len(m.peers)+1)

	// Include self.
	peers = append(peers, transport.PeerState{
		NodeID:            m.cfg.NodeID,
		Addr:              fmt.Sprintf("%s:%d", m.cfg.BindAddr, m.cfg.BindPort),
		Alive:             true,
		LastSeen:          time.Now(),
		ReplicationFactor: m.cfg.ReplicationFactor,
	})

	for _, p := range m.peers {
		peers = append(peers, transport.PeerState{
			NodeID:            p.NodeID,
			Addr:              p.Addr,
			Alive:             p.Alive,
			LastSeen:          p.LastSeen,
			ReplicationFactor: p.ReplicationFactor,
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
		if client, ok := m.getClient(p.NodeID); ok {
			client.Send(frame)
		}
	}
}
