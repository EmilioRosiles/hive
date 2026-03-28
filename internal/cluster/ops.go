package cluster

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// -- Value ops --

func (m *Manager) Set(key string, data []byte) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		m.store.Set(key, store.NewValueStructure(data))
		return m.replicateOp(transport.OpValueSet, key, transport.ValueSetPayload{Data: data}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpValueSet, key, transport.ValueSetPayload{Data: data})
}

func (m *Manager) Get(key string) ([]byte, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		e, ok := m.store.Get(key)
		if !ok {
			return nil, errors.New("not found")
		}
		v, ok := e.(*store.ValueStructure)
		if !ok {
			return nil, errors.New("type mismatch")
		}
		return v.Data, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpValueGet, key, nil)
	if err != nil {
		return nil, err
	}
	var dr transport.DataResponse
	if err := transport.Decode(resp.Payload, &dr); err != nil {
		return nil, err
	}
	return dr.Data, nil
}

func (m *Manager) Del(key string) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		m.store.Del(key)
		return m.replicateOp(transport.OpDel, key, nil, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpDel, key, nil)
}

func (m *Manager) Expire(key string, ttl time.Duration) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		m.store.Expire(key, ttl)
		return m.replicateOp(transport.OpExpire, key, transport.ExpirePayload{TTLNs: ttl.Nanoseconds()}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpExpire, key, transport.ExpirePayload{TTLNs: ttl.Nanoseconds()})
}

// -- Set ops --

func (m *Manager) SAdd(key, member string, ttl time.Duration) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		if err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
			return applyAdd(ds, member, ttl)
		}); err != nil {
			return err
		}
		return m.replicateOp(transport.OpSAdd, key, transport.SAddPayload{Member: member, TTLNs: ttl.Nanoseconds()}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpSAdd, key, transport.SAddPayload{Member: member, TTLNs: ttl.Nanoseconds()})
}

func (m *Manager) SRem(key, member string) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		if err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
			if ds == nil {
				return nil, nil
			}
			ss, ok := ds.(*store.SetStructure)
			if !ok {
				return nil, errors.New("type mismatch: not a set")
			}
			ss.Remove(member)
			return ss, nil
		}); err != nil {
			return err
		}
		return m.replicateOp(transport.OpSRem, key, transport.SRemPayload{Member: member}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpSRem, key, transport.SRemPayload{Member: member})
}

func (m *Manager) SIsMember(key, member string) (bool, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		var result bool
		m.store.Read(key, func(ds store.DataStructure) {
			if ss, ok := ds.(*store.SetStructure); ok {
				result = ss.IsMember(member)
			}
		})
		return result, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpSIsMember, key, transport.SIsMemberPayload{Member: member})
	if err != nil {
		return false, err
	}
	var br transport.BoolResponse
	if err := transport.Decode(resp.Payload, &br); err != nil {
		return false, err
	}
	return br.Value, nil
}

func (m *Manager) SMembers(key string) ([]string, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		var result []string
		m.store.Read(key, func(ds store.DataStructure) {
			if ss, ok := ds.(*store.SetStructure); ok {
				result = ss.Members()
			}
		})
		return result, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpSMembers, key, nil)
	if err != nil {
		return nil, err
	}
	var sr transport.StringsResponse
	if err := transport.Decode(resp.Payload, &sr); err != nil {
		return nil, err
	}
	return sr.Values, nil
}

func (m *Manager) SCard(key string) (int, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		var result int
		m.store.Read(key, func(ds store.DataStructure) {
			if ss, ok := ds.(*store.SetStructure); ok {
				result = ss.Card()
			}
		})
		return result, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpSCard, key, nil)
	if err != nil {
		return 0, err
	}
	var ir transport.IntResponse
	if err := transport.Decode(resp.Payload, &ir); err != nil {
		return 0, err
	}
	return ir.Value, nil
}

func (m *Manager) SExpireMember(key, member string, ttl time.Duration) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		if err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
			if ds == nil {
				return nil, nil
			}
			ss, ok := ds.(*store.SetStructure)
			if !ok {
				return nil, errors.New("type mismatch: not a set")
			}
			ss.ExpireMember(member, ttl)
			return ss, nil
		}); err != nil {
			return err
		}
		return m.replicateOp(transport.OpSExpireMember, key, transport.SExpireMemberPayload{Member: member, TTLNs: ttl.Nanoseconds()}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpSExpireMember, key, transport.SExpireMemberPayload{Member: member, TTLNs: ttl.Nanoseconds()})
}

// -- Hash ops --

func (m *Manager) HSet(key, field string, data []byte, ttl time.Duration) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		if err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
			return applyHSet(ds, field, data, ttl)
		}); err != nil {
			return err
		}
		return m.replicateOp(transport.OpHSet, key, transport.HSetPayload{Field: field, Data: data, TTLNs: ttl.Nanoseconds()}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpHSet, key, transport.HSetPayload{Field: field, Data: data, TTLNs: ttl.Nanoseconds()})
}

func (m *Manager) HGet(key, field string) ([]byte, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		var data []byte
		m.store.Read(key, func(ds store.DataStructure) {
			if h, ok := ds.(*store.HashStructure); ok {
				data, _ = h.HGet(field)
			}
		})
		if data == nil {
			return nil, errors.New("not found")
		}
		return data, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpHGet, key, transport.HGetPayload{Field: field})
	if err != nil {
		return nil, err
	}
	var dr transport.DataResponse
	if err := transport.Decode(resp.Payload, &dr); err != nil {
		return nil, err
	}
	return dr.Data, nil
}

func (m *Manager) HDel(key, field string) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		if err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
			if ds == nil {
				return nil, nil
			}
			h, ok := ds.(*store.HashStructure)
			if !ok {
				return nil, errors.New("type mismatch: not a hash")
			}
			h.HDel(field)
			return h, nil
		}); err != nil {
			return err
		}
		return m.replicateOp(transport.OpHDel, key, transport.HDelPayload{Field: field}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpHDel, key, transport.HDelPayload{Field: field})
}

func (m *Manager) HGetAll(key string) (map[string][]byte, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		var result map[string][]byte
		m.store.Read(key, func(ds store.DataStructure) {
			if h, ok := ds.(*store.HashStructure); ok {
				result = h.GetAll()
			}
		})
		return result, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpHGetAll, key, nil)
	if err != nil {
		return nil, err
	}
	var mr transport.MapResponse
	if err := transport.Decode(resp.Payload, &mr); err != nil {
		return nil, err
	}
	return mr.Values, nil
}

func (m *Manager) HKeys(key string) ([]string, error) {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || slices.Contains(nodes, m.cfg.NodeID) {
		var result []string
		m.store.Read(key, func(ds store.DataStructure) {
			if h, ok := ds.(*store.HashStructure); ok {
				result = h.Fields()
			}
		})
		return result, nil
	}
	resp, err := m.forwardOpWithResponse(nodes[0], transport.OpHKeys, key, nil)
	if err != nil {
		return nil, err
	}
	var sr transport.StringsResponse
	if err := transport.Decode(resp.Payload, &sr); err != nil {
		return nil, err
	}
	return sr.Values, nil
}

func (m *Manager) HExpireField(key, field string, ttl time.Duration) error {
	nodes := m.responsibleNodes(key)
	if len(nodes) == 0 || nodes[0] == m.cfg.NodeID {
		if err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
			if ds == nil {
				return nil, nil
			}
			h, ok := ds.(*store.HashStructure)
			if !ok {
				return nil, errors.New("type mismatch: not a hash")
			}
			h.ExpireField(field, ttl)
			return h, nil
		}); err != nil {
			return err
		}
		return m.replicateOp(transport.OpHExpireField, key, transport.HExpireFieldPayload{Field: field, TTLNs: ttl.Nanoseconds()}, nodes)
	}
	return m.forwardOp(nodes[0], transport.OpHExpireField, key, transport.HExpireFieldPayload{Field: field, TTLNs: ttl.Nanoseconds()})
}

// -- shared helpers --

// applyHSet is used by HSet handler and local execution path.
func applyHSet(ds store.DataStructure, field string, data []byte, ttl time.Duration) (store.DataStructure, error) {
	var h *store.HashStructure
	if ds == nil {
		h = store.NewHashStructure()
	} else {
		var ok bool
		h, ok = ds.(*store.HashStructure)
		if !ok {
			return nil, errors.New("type mismatch: not a hash")
		}
	}
	if ttl > 0 {
		h.HSetWithTTL(field, data, ttl)
	} else {
		h.HSet(field, data)
	}
	return h, nil
}

// applyAdd is used by SAdd handler and local execution path.
func applyAdd(ds store.DataStructure, member string, ttl time.Duration) (store.DataStructure, error) {
	var ss *store.SetStructure
	if ds == nil {
		ss = store.NewSetStructure()
	} else {
		var ok bool
		ss, ok = ds.(*store.SetStructure)
		if !ok {
			return nil, errors.New("type mismatch: not a set")
		}
	}
	if ttl > 0 {
		ss.AddWithTTL(member, ttl)
	} else {
		ss.Add(member)
	}
	return ss, nil
}

// -- transport helpers --

// buildReq encodes payload into a ForwardRequest ready to send.
func buildReq(op transport.Op, key string, payload any) (transport.ForwardRequest, error) {
	req := transport.ForwardRequest{Op: op, Key: key}
	if payload != nil {
		var err error
		req.Payload, err = transport.Encode(payload)
		if err != nil {
			return transport.ForwardRequest{}, err
		}
	}
	return req, nil
}

// forwardOp sends req to nodeID, ignoring any response payload.
func (m *Manager) forwardOp(nodeID string, op transport.Op, key string, payload any) error {
	req, err := buildReq(op, key, payload)
	if err != nil {
		return err
	}
	_, err = m.sendReq(nodeID, req)
	return err
}

// forwardOpWithResponse sends req to nodeID and returns the decoded response.
func (m *Manager) forwardOpWithResponse(nodeID string, op transport.Op, key string, payload any) (transport.ForwardResponse, error) {
	req, err := buildReq(op, key, payload)
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	return m.sendReq(nodeID, req)
}

// sendReq encodes and sends a ForwardRequest, returning the ForwardResponse.
func (m *Manager) sendReq(nodeID string, req transport.ForwardRequest) (transport.ForwardResponse, error) {
	client, ok := m.getClient(nodeID)
	if !ok {
		return transport.ForwardResponse{}, fmt.Errorf("cluster: no client for node %s", nodeID)
	}
	framePayload, err := transport.Encode(req)
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	respFrame, err := client.Send(transport.Frame{Type: transport.MsgForward, Payload: framePayload})
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

// replicateOp fans out a pre-built request to replica nodes (nodes[1:]) in the background.
func (m *Manager) replicateOp(op transport.Op, key string, payload any, nodes []string) error {
	if len(nodes) <= 1 {
		return nil
	}
	req, err := buildReq(op, key, payload)
	if err != nil {
		return err
	}
	for _, nodeID := range nodes[1:] {
		nodeID := nodeID
		go func() {
			if _, err := m.sendReq(nodeID, req); err != nil {
				m.handlePeerError(nodeID, err)
			}
		}()
	}
	return nil
}

func (m *Manager) handlePeerError(nodeID string, err error) {
	m.markDead(nodeID)
}
