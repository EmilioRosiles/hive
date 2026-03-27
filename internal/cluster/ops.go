package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// Set stores data under key, forwarding to the responsible node if necessary.
func (m *Manager) Set(key string, data []byte) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		m.store.Set(key, store.NewEntry(data))
		return m.replicate(key, data, nodes)
	}
	return m.forward(nodes[0], transport.ForwardRequest{Op: transport.OpSet, Key: key, Data: data})
}

// Get retrieves raw bytes for key, forwarding to the responsible node if necessary.
func (m *Manager) Get(key string) ([]byte, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		e, ok := m.store.Get(key)
		if !ok {
			return nil, errors.New("not found")
		}
		return e.Data, nil
	}
	resp, err := m.forwardWithResponse(nodes[0], transport.ForwardRequest{Op: transport.OpGet, Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// Del removes key from the cluster, forwarding if necessary, then replicating.
func (m *Manager) Del(key string) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		m.store.Del(key)
		return m.replicateDel(key, nodes)
	}
	return m.forward(nodes[0], transport.ForwardRequest{Op: transport.OpDel, Key: key})
}

// Expire sets a TTL on key, forwarding if necessary.
func (m *Manager) Expire(key string, ttl time.Duration) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		m.store.Expire(key, ttl)
		return m.replicateExpire(key, ttl, nodes)
	}
	req := transport.ForwardRequest{Op: transport.OpExpire, Key: key, TTL: ttl.Nanoseconds()}
	return m.forward(nodes[0], req)
}

// -- replication helpers --

// replicate writes data to replica nodes (nodes[1:]) in the background.
func (m *Manager) replicate(key string, data []byte, nodes []string) error {
	for _, nodeID := range nodes[1:] {
		nodeID := nodeID
		go func() {
			if err := m.forward(nodeID, transport.ForwardRequest{Op: transport.OpSet, Key: key, Data: data}); err != nil {
				m.handlePeerError(nodeID, err)
			}
		}()
	}
	return nil
}

func (m *Manager) replicateDel(key string, nodes []string) error {
	for _, nodeID := range nodes[1:] {
		nodeID := nodeID
		go func() {
			if err := m.forward(nodeID, transport.ForwardRequest{Op: transport.OpDel, Key: key}); err != nil {
				m.handlePeerError(nodeID, err)
			}
		}()
	}
	return nil
}

func (m *Manager) replicateExpire(key string, ttl time.Duration, nodes []string) error {
	for _, nodeID := range nodes[1:] {
		nodeID := nodeID
		go func() {
			req := transport.ForwardRequest{Op: transport.OpExpire, Key: key, TTL: ttl.Nanoseconds()}
			if err := m.forward(nodeID, req); err != nil {
				m.handlePeerError(nodeID, err)
			}
		}()
	}
	return nil
}

// -- transport helpers --

func (m *Manager) forward(nodeID string, req transport.ForwardRequest) error {
	_, err := m.forwardWithResponse(nodeID, req)
	return err
}

func (m *Manager) forwardWithResponse(nodeID string, req transport.ForwardRequest) (transport.ForwardResponse, error) {
	client, ok := m.getClient(nodeID)
	if !ok {
		return transport.ForwardResponse{}, fmt.Errorf("cluster: no client for node %s", nodeID)
	}
	payload, err := transport.Encode(req)
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	respFrame, err := client.Send(transport.Frame{Type: transport.MsgForward, Payload: payload})
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	var resp transport.ForwardResponse
	if len(respFrame.Payload) > 0 {
		if err := transport.Decode(respFrame.Payload, &resp); err != nil {
			return transport.ForwardResponse{}, err
		}
	}
	return resp, nil
}

func (m *Manager) handlePeerError(nodeID string, err error) {
	if err != nil {
		m.markDead(nodeID)
	}
}
