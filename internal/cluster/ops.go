package cluster

import (
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// This file exposes the public data-operation API of the cluster Manager.
// Each method builds a typed payload and delegates to dispatch — no routing
// logic lives here. See routing.go for dispatch, exec.go for local execution.

// -- Value ops --

func (m *Manager) Set(key string, data []byte) error {
	_, err := m.dispatch(transport.OpValueSet, key, transport.ValueSetPayload{Data: data})
	return err
}

func (m *Manager) Get(key string) ([]byte, error) {
	raw, err := m.dispatch(transport.OpValueGet, key, nil)
	if err != nil {
		return nil, err
	}
	var dr transport.DataResponse
	if err := transport.Decode(raw, &dr); err != nil {
		return nil, err
	}
	return dr.Data, nil
}

func (m *Manager) Del(key string) error {
	_, err := m.dispatch(transport.OpDel, key, nil)
	return err
}

func (m *Manager) Expire(key string, ttl time.Duration) error {
	_, err := m.dispatch(transport.OpExpire, key, transport.ExpirePayload{TTLNs: ttl.Nanoseconds()})
	return err
}

// -- Set ops --

func (m *Manager) SAdd(key, member string, ttl time.Duration) error {
	_, err := m.dispatch(transport.OpSAdd, key, transport.SAddPayload{Member: member, TTLNs: ttl.Nanoseconds()})
	return err
}

func (m *Manager) SRem(key, member string) error {
	_, err := m.dispatch(transport.OpSRem, key, transport.SRemPayload{Member: member})
	return err
}

func (m *Manager) SIsMember(key, member string) (bool, error) {
	raw, err := m.dispatch(transport.OpSIsMember, key, transport.SIsMemberPayload{Member: member})
	if err != nil {
		return false, err
	}
	var br transport.BoolResponse
	if err := transport.Decode(raw, &br); err != nil {
		return false, err
	}
	return br.Value, nil
}

func (m *Manager) SMembers(key string) ([]string, error) {
	raw, err := m.dispatch(transport.OpSMembers, key, nil)
	if err != nil {
		return nil, err
	}
	var sr transport.StringsResponse
	if err := transport.Decode(raw, &sr); err != nil {
		return nil, err
	}
	return sr.Values, nil
}

func (m *Manager) SCard(key string) (int, error) {
	raw, err := m.dispatch(transport.OpSCard, key, nil)
	if err != nil {
		return 0, err
	}
	var ir transport.IntResponse
	if err := transport.Decode(raw, &ir); err != nil {
		return 0, err
	}
	return ir.Value, nil
}

func (m *Manager) SExpireMember(key, member string, ttl time.Duration) error {
	_, err := m.dispatch(transport.OpSExpireMember, key, transport.SExpireMemberPayload{Member: member, TTLNs: ttl.Nanoseconds()})
	return err
}

// -- Hash ops --

func (m *Manager) HSet(key, field string, data []byte, ttl time.Duration) error {
	_, err := m.dispatch(transport.OpHSet, key, transport.HSetPayload{Field: field, Data: data, TTLNs: ttl.Nanoseconds()})
	return err
}

func (m *Manager) HGet(key, field string) ([]byte, error) {
	raw, err := m.dispatch(transport.OpHGet, key, transport.HGetPayload{Field: field})
	if err != nil {
		return nil, err
	}
	var dr transport.DataResponse
	if err := transport.Decode(raw, &dr); err != nil {
		return nil, err
	}
	return dr.Data, nil
}

func (m *Manager) HDel(key, field string) error {
	_, err := m.dispatch(transport.OpHDel, key, transport.HDelPayload{Field: field})
	return err
}

func (m *Manager) HGetAll(key string) (map[string][]byte, error) {
	raw, err := m.dispatch(transport.OpHGetAll, key, nil)
	if err != nil {
		return nil, err
	}
	var mr transport.MapResponse
	if err := transport.Decode(raw, &mr); err != nil {
		return nil, err
	}
	return mr.Values, nil
}

func (m *Manager) HKeys(key string) ([]string, error) {
	raw, err := m.dispatch(transport.OpHKeys, key, nil)
	if err != nil {
		return nil, err
	}
	var sr transport.StringsResponse
	if err := transport.Decode(raw, &sr); err != nil {
		return nil, err
	}
	return sr.Values, nil
}

func (m *Manager) HExpireField(key, field string, ttl time.Duration) error {
	_, err := m.dispatch(transport.OpHExpireField, key, transport.HExpireFieldPayload{Field: field, TTLNs: ttl.Nanoseconds()})
	return err
}
