package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// opFn is the signature for all op registry handlers.
// It receives the manager, the target key, and the msgpack-encoded op payload.
// It returns an optional msgpack-encoded response payload.
type opFn func(m *Manager, key string, payload []byte) ([]byte, error)

// opRegistry maps each Op to its handler. Add new ops here — handleForward never changes.
var opRegistry = map[transport.Op]opFn{
	transport.OpDel:    execDel,
	transport.OpExpire: execExpire,

	transport.OpValueSet: execValueSet,
	transport.OpValueGet: execValueGet,

	transport.OpSAdd:          execSAdd,
	transport.OpSRem:          execSRem,
	transport.OpSIsMember:     execSIsMember,
	transport.OpSMembers:      execSMembers,
	transport.OpSCard:         execSCard,
	transport.OpSExpireMember: execSExpireMember,

	transport.OpHSet:         execHSet,
	transport.OpHGet:         execHGet,
	transport.OpHDel:         execHDel,
	transport.OpHGetAll:      execHGetAll,
	transport.OpHFields:      execHFields,
	transport.OpHExpireField: execHExpireField,
}

// handleFrame is the transport.Handler registered with the TCP server.
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
	fn, ok := opRegistry[req.Op]
	if !ok {
		return nil, fmt.Errorf("handler: unknown op %d", req.Op)
	}
	respPayload, err := fn(m, req.Key, req.Payload)
	if err != nil {
		return nil, err
	}
	if respPayload == nil {
		return nil, nil
	}
	return transport.Encode(transport.ForwardResponse{Payload: respPayload})
}

func (m *Manager) handleRebalance(payload []byte) ([]byte, error) {
	var batch transport.RebalanceBatch
	if err := transport.Decode(payload, &batch); err != nil {
		return nil, fmt.Errorf("handler: decode rebalance: %w", err)
	}
	now := time.Now()
	for _, re := range batch.Entries {
		ttl := time.Duration(re.TTL) - time.Since(now)
		switch store.Kind(re.Kind) {
		case store.KindValue:
			var e *store.ValueStructure
			e = store.NewValueStructureWithTTL(re.Data, ttl)
			m.store.Set(re.Key, e)
		case store.KindSet:
			ss, err := store.DecodeSetStructure(re.Data)
			if err != nil {
				continue
			}
			if re.TTL > 0 {
				ss.SetKeyExpiry(now.Add(ttl).Unix())
			}
			m.store.Set(re.Key, ss)
		case store.KindHash:
			hs, err := store.DecodeHashStructure(re.Data)
			if err != nil {
				continue
			}
			if re.TTL > 0 {
				hs.SetKeyExpiry(now.Add(ttl).Unix())
			}
			m.store.Set(re.Key, hs)
		}
	}
	return nil, nil
}

func (m *Manager) handleLeave(payload []byte) error {
	var req transport.LeaveRequest
	if err := transport.Decode(payload, &req); err != nil {
		return fmt.Errorf("handler: decode leave: %w", err)
	}
	m.markDead(req.NodeID)
	m.evictPeer(req.NodeID)
	return nil
}

// -- shared op handlers --

func execDel(m *Manager, key string, _ []byte) ([]byte, error) {
	m.store.Del(key)
	return nil, nil
}

func execExpire(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.ExpirePayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	m.store.Expire(key, time.Duration(p.TTLNs))
	return nil, nil
}

// -- value op handlers --

func execValueSet(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.ValueSetPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	m.store.Set(key, store.NewValueStructure(p.Data))
	return nil, nil
}

func execValueGet(m *Manager, key string, _ []byte) ([]byte, error) {
	e, ok := m.store.Get(key)
	if !ok {
		return nil, errors.New("not found")
	}
	v, ok := e.(*store.ValueStructure)
	if !ok {
		return nil, errors.New("type mismatch")
	}
	resp, err := transport.Encode(transport.DataResponse{Data: v.Data})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// -- set op handlers --

func execSAdd(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.SAddPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		return applyAdd(ds, p.Member, time.Duration(p.TTLNs))
	})
}

func execSRem(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.SRemPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		ss, ok := ds.(*store.SetStructure)
		if !ok {
			return nil, errors.New("type mismatch: not a set")
		}
		ss.Remove(p.Member)
		return ss, nil
	})
}

func execSIsMember(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.SIsMemberPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	var result bool
	m.store.Read(key, func(ds store.DataStructure) {
		if ss, ok := ds.(*store.SetStructure); ok {
			result = ss.IsMember(p.Member)
		}
	})
	return transport.Encode(transport.BoolResponse{Value: result})
}

func execSMembers(m *Manager, key string, _ []byte) ([]byte, error) {
	var members []string
	m.store.Read(key, func(ds store.DataStructure) {
		if ss, ok := ds.(*store.SetStructure); ok {
			members = ss.Members()
		}
	})
	return transport.Encode(transport.StringsResponse{Values: members})
}

func execSCard(m *Manager, key string, _ []byte) ([]byte, error) {
	var count int
	m.store.Read(key, func(ds store.DataStructure) {
		if ss, ok := ds.(*store.SetStructure); ok {
			count = ss.Card()
		}
	})
	return transport.Encode(transport.IntResponse{Value: count})
}

func execSExpireMember(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.SExpireMemberPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		ss, ok := ds.(*store.SetStructure)
		if !ok {
			return nil, errors.New("type mismatch: not a set")
		}
		ss.ExpireMember(p.Member, time.Duration(p.TTLNs))
		return ss, nil
	})
}

// -- hash op handlers --

func execHSet(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.HSetPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		return applyHSet(ds, p.Field, p.Data, time.Duration(p.TTLNs))
	})
}

func execHGet(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.HGetPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	var data []byte
	m.store.Read(key, func(ds store.DataStructure) {
		if h, ok := ds.(*store.HashStructure); ok {
			data, _ = h.HGet(p.Field)
		}
	})
	if data == nil {
		return nil, errors.New("not found")
	}
	return transport.Encode(transport.DataResponse{Data: data})
}

func execHDel(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.HDelPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		h, ok := ds.(*store.HashStructure)
		if !ok {
			return nil, errors.New("type mismatch: not a hash")
		}
		h.HDel(p.Field)
		return h, nil
	})
}

func execHGetAll(m *Manager, key string, _ []byte) ([]byte, error) {
	var result map[string][]byte
	m.store.Read(key, func(ds store.DataStructure) {
		if h, ok := ds.(*store.HashStructure); ok {
			result = h.GetAll()
		}
	})
	return transport.Encode(transport.MapResponse{Values: result})
}

func execHFields(m *Manager, key string, _ []byte) ([]byte, error) {
	var fields []string
	m.store.Read(key, func(ds store.DataStructure) {
		if h, ok := ds.(*store.HashStructure); ok {
			fields = h.Fields()
		}
	})
	return transport.Encode(transport.StringsResponse{Values: fields})
}

func execHExpireField(m *Manager, key string, payload []byte) ([]byte, error) {
	var p transport.HExpireFieldPayload
	if err := transport.Decode(payload, &p); err != nil {
		return nil, err
	}
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		h, ok := ds.(*store.HashStructure)
		if !ok {
			return nil, errors.New("type mismatch: not a hash")
		}
		h.ExpireField(p.Field, time.Duration(p.TTLNs))
		return h, nil
	})
}
