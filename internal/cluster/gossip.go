package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// handleHeartbeat merges incoming peer state and returns this node's view.
func (m *Cluster) handleHeartbeat(payload []byte) ([]byte, error) {
	var req transport.HeartbeatRequest
	if err := transport.Decode(payload, &req); err != nil {
		return nil, fmt.Errorf("handler: decode heartbeat: %w", err)
	}
	if err := m.mergeState(req.Peers); err != nil {
		return nil, err
	}
	resp := transport.HeartbeatResponse{Peers: m.buildHeartbeatRequest().Peers}
	return transport.Encode(resp)
}

// handleLeave removes a peer that has announced a graceful departure.
func (m *Cluster) handleLeave(payload []byte) error {
	var req transport.LeaveRequest
	if err := transport.Decode(payload, &req); err != nil {
		return fmt.Errorf("handler: decode leave: %w", err)
	}
	m.markDead(req.NodeID)
	return nil
}

// startGossip runs the heartbeat loop until the node shuts down.
func (m *Cluster) startGossip() {
	for {
		select {
		case <-m.stopCh:
			return
		case <-time.After(Jitter(m.cfg.GossipInterval, 0.25)):
		}

		targets := m.randomPeers(m.cfg.GossipFanout, "")
		m.heartbeat(targets...)
		go m.rebalancer.schedule()
	}
}

// heartbeat sends this node's view of the cluster to each target peer.
// Peers that fail to respond are suspected and probed.
func (m *Cluster) heartbeat(targets ...*PeerInfo) {
	if len(targets) == 0 {
		return
	}

	payload, err := transport.Encode(m.buildHeartbeatRequest())
	if err != nil {
		m.logger.Warn("gossip: encode heartbeat failed", "err", err)
		return
	}
	frame := transport.Frame{Type: transport.MsgHeartbeat, Payload: payload}

	for _, p := range targets {
		client, ok := m.getClient(p.NodeID)
		if !ok {
			m.markDead(p.NodeID)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.GossipTimeout)
		resp, err := client.Send(ctx, frame)
		cancel()
		if err != nil {
			m.logger.Warn("gossip: heartbeat failed", "node", p.NodeID, "err", err)
			m.markSuspect(p.NodeID)
			continue
		}

		var hbResp transport.HeartbeatResponse
		if err := transport.Decode(resp.Payload, &hbResp); err != nil {
			m.logger.Warn("gossip: decode response failed", "node", p.NodeID, "err", err)
			continue
		}

		if err := m.mergeState(hbResp.Peers); err != nil {
			m.logger.Warn("gossip: merge state failed", "node", p.NodeID, "err", err)
		}
	}
}

// bootstrap sends a heartbeat to addr and merges the response into our cluster
// view. This is called once per seed at startup so the ring is populated with
// real NodeIDs before the gossip loop begins. Unreachable seeds are skipped —
// at least one must succeed for the node to join the cluster. The bootstrap
// client is kept as the seed's pooled client, so its connection stays in use.
// If the seed rejects the join (e.g. replication factor mismatch), the node halts.
func (m *Cluster) bootstrap(addr string) {
	payload, err := transport.Encode(m.buildHeartbeatRequest())
	if err != nil {
		return
	}
	client := m.newClient(addr)
	resp, err := client.Send(context.Background(), transport.Frame{Type: transport.MsgHeartbeat, Payload: payload})
	if err != nil {
		client.Close()
		if errors.Is(err, transport.ErrRejected) {
			m.logger.Error("hive: cluster rejected join", "addr", addr, "reason", err)
			os.Exit(1)
		}
		m.logger.Warn("bootstrap: seed unreachable", "addr", addr, "err", err)
		return
	}
	var hbResp transport.HeartbeatResponse
	if err := transport.Decode(resp.Payload, &hbResp); err != nil || len(hbResp.Peers) == 0 {
		client.Close()
		m.logger.Warn("bootstrap: decode failed", "addr", addr, "err", err)
		return
	}
	seed := hbResp.Peers[0].NodeID // the responder lists itself first
	_, known := m.getPeer(seed)
	if err := m.mergeState(hbResp.Peers); err != nil {
		client.Close()
		m.logger.Error("hive: cluster rejected join", "addr", addr, "reason", err)
		os.Exit(1)
	}

	m.mu.Lock()
	if fresh, ok := m.clients[seed]; ok && !known {
		m.clients[seed], client = client, fresh
	}
	m.mu.Unlock()
	client.Close()
	m.logger.Info("bootstrap: joined via seed", "addr", addr, "peers", len(hbResp.Peers))
}

// mergeState reconciles a peer's view of the cluster with our own, applying
// each entry that takes precedence (see applyIncarnation) and refuting Dead
// rumours about ourselves. A Dead rumour about a peer is verified with our own
// probe before acting on it. Returns the first error, e.g. a replication factor mismatch.
func (m *Cluster) mergeState(remote []transport.PeerState) error {
	for _, rs := range remote {
		if rs.NodeID == m.cfg.NodeID {
			m.refute(rs)
			continue
		}

		local, exists := m.getPeer(rs.NodeID)

		if !exists {
			if NodeStatus(rs.Status) != NodeDead {
				if err := m.addPeer(rs); err != nil {
					return err
				}
			}
			continue
		}

		if m.applyIncarnation(local, rs) {
			switch NodeStatus(rs.Status) {
			case NodeDead:
				m.markSuspect(rs.NodeID)
			case NodeAlive:
				if err := m.addPeer(rs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// applyIncarnation reports whether rs takes precedence over local and, if so,
// records its incarnation: a higher incarnation wins, and at equal incarnation
// the worse status wins (Dead > Suspect > Alive). MemUsed is refreshed from any
// entry that isn't older.
func (m *Cluster) applyIncarnation(local *PeerInfo, rs transport.PeerState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rs.Incarnation < local.Incarnation {
		return false
	}
	local.MemUsed = rs.MemUsed
	if rs.Incarnation == local.Incarnation && NodeStatus(rs.Status) <= local.Status {
		return false
	}
	local.Incarnation = rs.Incarnation
	return true
}

// refute bumps our incarnation past a Dead rumour about us, so our next
// heartbeat overrides it. A node that announced its leave doesn't refute.
func (m *Cluster) refute(rs transport.PeerState) {
	if NodeStatus(rs.Status) != NodeDead || rs.Incarnation == math.MaxUint64 {
		return
	}
	for {
		cur := m.incarnation.Load()
		if rs.Incarnation < cur || cur == math.MaxUint64 {
			return
		}
		if m.incarnation.CompareAndSwap(cur, rs.Incarnation+1) {
			m.logger.Warn("gossip: refuting dead rumour about this node", "incarnation", rs.Incarnation+1)
			return
		}
	}
}

// buildHeartbeatRequest assembles the current node's peer list for gossip.
func (m *Cluster) buildHeartbeatRequest() transport.HeartbeatRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]transport.PeerState, 0, len(m.peers)+1)

	// Include self — always alive from our own perspective.
	peers = append(peers, transport.PeerState{
		NodeID:            m.cfg.NodeID,
		Addr:              m.cfg.AdvertiseAddr,
		Status:            uint8(NodeAlive),
		Incarnation:       m.incarnation.Load(),
		ReplicationFactor: m.cfg.ReplicationFactor,
		MemLimit:          m.cfg.MemLimit,
		MemUsed:           uint64(m.store.Used()),
	})

	for _, p := range m.peers {
		peers = append(peers, transport.PeerState{
			NodeID:            p.NodeID,
			Addr:              p.Addr,
			Status:            uint8(p.Status),
			Incarnation:       p.Incarnation,
			ReplicationFactor: p.ReplicationFactor,
			MemLimit:          p.MemLimit,
			MemUsed:           p.MemUsed,
		})
	}

	return transport.HeartbeatRequest{Peers: peers}
}

// announceLeave notifies every peer we hold a client for (all but the Dead
// ones) that this node is departing.
func (m *Cluster) announceLeave() {
	m.incarnation.Store(math.MaxUint64)
	payload, err := transport.Encode(transport.LeaveRequest{NodeID: m.cfg.NodeID})
	if err != nil {
		return
	}
	frame := transport.Frame{Type: transport.MsgLeave, Payload: payload}

	m.mu.RLock()
	var wg sync.WaitGroup
	for _, c := range m.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Send(context.Background(), frame)
		}()
	}
	m.mu.RUnlock()

	wg.Wait()
}
