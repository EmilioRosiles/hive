package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

		targets := m.randomAlivePeers(m.cfg.GossipFanout)
		m.heartbeat(targets...)
		go m.rebalancer.schedule()
	}
}

// heartbeat sends this node's view of the cluster to each target peer.
// Peers that fail to respond are removed from the cluster.
func (m *Cluster) heartbeat(targets ...*PeerInfo) {
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

		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.GossipTimeout)
		resp, err := client.Send(ctx, frame)
		cancel()
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
func (m *Cluster) bootstrap(addr string) {
	payload, err := transport.Encode(m.buildHeartbeatRequest())
	if err != nil {
		return
	}
	client := m.newClient(addr)
	resp, err := client.Send(context.Background(), transport.Frame{Type: transport.MsgHeartbeat, Payload: payload})
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
// Incarnation is the authoritative ordering key. A remote state update is
// applied only when its Incarnation greater than what we hold locally.
func (m *Cluster) mergeState(remote []transport.PeerState) error {
	for _, rs := range remote {
		if rs.NodeID == m.cfg.NodeID {
			continue // our own state is authoritative; skip
		}

		local, exists := m.getPeer(rs.NodeID)

		if !exists {
			if NodeStatus(rs.Status) == NodeAlive {
				if err := m.addPeer(rs); err != nil {
					return err
				}
			}
			continue
		}

		if localStatus, ok := m.applyIncarnation(local, rs.Incarnation); ok {
			switch NodeStatus(rs.Status) {
			case NodeDead:
				if localStatus == NodeAlive {
					m.markDead(rs.NodeID)
				}
			case NodeAlive:
				if err := m.addPeer(rs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// applyIncarnation atomically checks whether incarnation is newer than
// local's current value and, if so, updates it. local is a live pointer into
// m.peers shared with concurrent goroutines, so the check and the write must
// happen under the same lock.
func (m *Cluster) applyIncarnation(local *PeerInfo, incarnation uint64) (status NodeStatus, applied bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if incarnation <= local.Incarnation {
		return 0, false
	}
	local.Incarnation = incarnation
	return NodeStatus(local.Status), true
}

// buildHeartbeatRequest assembles the current node's peer list for gossip.
// Incarnation is bumped on every call so each outgoing heartbeat carries a
// strictly higher value than the previous one, ensuring our alive state beats
// any stale dead rumour without needing an explicit refutation step.
func (m *Cluster) buildHeartbeatRequest() transport.HeartbeatRequest {
	m.incarnation.Add(1)
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]transport.PeerState, 0, len(m.peers)+1)

	// Include self — always alive from our own perspective.
	peers = append(peers, transport.PeerState{
		NodeID:            m.cfg.NodeID,
		Addr:              fmt.Sprintf("%s:%d", m.cfg.BindAddr, m.cfg.BindPort),
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

// announceLeave notifies all known peers that this node is departing.
func (m *Cluster) announceLeave() {
	m.incarnation.Store(math.MaxUint64)
	payload, err := transport.Encode(transport.LeaveRequest{NodeID: m.cfg.NodeID})
	if err != nil {
		return
	}
	frame := transport.Frame{Type: transport.MsgLeave, Payload: payload}

	m.mu.RLock()
	var wg sync.WaitGroup
	for _, p := range m.peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			c := m.newClient(addr)
			c.Send(context.Background(), frame)
		}(p.Addr)
	}
	m.mu.RUnlock()

	wg.Wait()
}
