package cluster

import (
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// This file contains the local execution functions for each cluster op.
// Each function sits at the msgpack boundary: it decodes a raw payload,
// applies the operation to the local store, and returns an optional encoded response.
// No routing logic belongs here — by the time these are called, routing is done.

// -- shared ops --

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

// -- value ops --

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
		return nil, ErrNotFound
	}
	v, ok := e.(*store.ValueStructure)
	if !ok {
		return nil, errTypeMismatch
	}
	return transport.Encode(transport.DataResponse{Data: v.Data})
}

// -- set ops --

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
			return nil, errNotASet
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
			return nil, errNotASet
		}
		ss.ExpireMember(p.Member, time.Duration(p.TTLNs))
		return ss, nil
	})
}

// -- hash ops --

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
		return nil, ErrNotFound
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
			return nil, errNotAHash
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

func execHKeys(m *Manager, key string, _ []byte) ([]byte, error) {
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
			return nil, errNotAHash
		}
		h.ExpireField(p.Field, time.Duration(p.TTLNs))
		return h, nil
	})
}

// -- apply helpers --

// applyHSet upserts field into a HashStructure, creating one if ds is nil.
func applyHSet(ds store.DataStructure, field string, data []byte, ttl time.Duration) (store.DataStructure, error) {
	var h *store.HashStructure
	if ds == nil {
		h = store.NewHashStructure()
	} else {
		var ok bool
		h, ok = ds.(*store.HashStructure)
		if !ok {
			return nil, errNotAHash
		}
	}
	if ttl > 0 {
		h.HSetWithTTL(field, data, ttl)
	} else {
		h.HSet(field, data)
	}
	return h, nil
}

// applyAdd adds member to a SetStructure, creating one if ds is nil.
func applyAdd(ds store.DataStructure, member string, ttl time.Duration) (store.DataStructure, error) {
	var ss *store.SetStructure
	if ds == nil {
		ss = store.NewSetStructure()
	} else {
		var ok bool
		ss, ok = ds.(*store.SetStructure)
		if !ok {
			return nil, errNotASet
		}
	}
	if ttl > 0 {
		ss.AddWithTTL(member, ttl)
	} else {
		ss.Add(member)
	}
	return ss, nil
}
