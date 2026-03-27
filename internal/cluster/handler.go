package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// handleFrame is the transport.Handler registered with the TCP server.
// It dispatches inbound frames to the appropriate handler.
func (m *Manager) handleFrame(msgType transport.MsgType, payload []byte) ([]byte, error) {
	switch msgType {
	case transport.MsgHeartbeat:
		return m.handleHeartbeat(payload)
	case transport.MsgForward:
		return m.handleForward(payload)
	case transport.MsgRebalance:
		return m.handleRebalance(payload)
	case transport.MsgLeave:
		return nil, m.handleLeave(payload)
	default:
		return nil, fmt.Errorf("handler: unknown message type %d", msgType)
	}
}

func (m *Manager) handleHeartbeat(payload []byte) ([]byte, error) {
	var req transport.HeartbeatRequest
	if err := transport.Decode(payload, &req); err != nil {
		return nil, fmt.Errorf("handler: decode heartbeat: %w", err)
	}

	m.mergeState(req.Peers)

	resp := transport.HeartbeatResponse{Peers: m.buildHeartbeatRequest().Peers}
	return transport.Encode(resp)
}

func (m *Manager) handleForward(payload []byte) ([]byte, error) {
	var req transport.ForwardRequest
	if err := transport.Decode(payload, &req); err != nil {
		return nil, fmt.Errorf("handler: decode forward: %w", err)
	}

	switch req.Op {
	case transport.OpSet:
		m.store.Set(req.Key, store.NewEntry(req.Data))
		return nil, nil

	case transport.OpGet:
		e, ok := m.store.Get(req.Key)
		if !ok {
			return nil, errors.New("not found")
		}
		return transport.Encode(transport.ForwardResponse{Data: e.Data})

	case transport.OpDel:
		m.store.Del(req.Key)
		return nil, nil

	case transport.OpExpire:
		ttl := time.Duration(req.TTL)
		m.store.Expire(req.Key, ttl)
		return nil, nil

	default:
		return nil, fmt.Errorf("handler: unknown op %d", req.Op)
	}
}

func (m *Manager) handleRebalance(payload []byte) ([]byte, error) {
	var batch transport.RebalanceBatch
	if err := transport.Decode(payload, &batch); err != nil {
		return nil, fmt.Errorf("handler: decode rebalance: %w", err)
	}

	now := time.Now()
	for _, re := range batch.Entries {
		var e *store.Entry
		if re.TTL > 0 {
			e = store.NewEntryWithTTL(re.Data, time.Duration(re.TTL)-time.Since(now))
		} else {
			e = store.NewEntry(re.Data)
		}
		m.store.Set(re.Key, e)
	}
	return nil, nil
}

func (m *Manager) handleLeave(payload []byte) error {
	var req transport.LeaveRequest
	if err := transport.Decode(payload, &req); err != nil {
		return fmt.Errorf("handler: decode leave: %w", err)
	}
	// Graceful departure: evict immediately rather than waiting for DeadTimeout.
	m.markDead(req.NodeID)
	m.evictPeer(req.NodeID)
	return nil
}
