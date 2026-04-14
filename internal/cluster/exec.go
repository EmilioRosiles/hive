package cluster

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
)

// This file contains the local execution functions for each cluster op.
// Each function receives pre-decoded [][]byte arg slots and works directly
// on the local store. No serialization happens here.
// Arg slot positions are documented by the constants below.

// -- arg index constants --

const (
	argExpireTTL = 0 // int64 ns big-endian
)

const (
	argLPushData = 0 // []byte
	argRPushData = 0 // []byte

	argLIndexIndex = 0 // int64 big-endian

	argLRangeStart = 0 // int64 big-endian
	argLRangeStop  = 1 // int64 big-endian

	argLSetIndex = 0 // int64 big-endian
	argLSetData  = 1 // []byte
)

const (
	argZAddScore  = 0 // float64 8-byte IEEE 754 big-endian
	argZAddMember = 1 // string

	argZRemMember = 0 // string

	argZScoreMember = 0 // string

	argZRankMember    = 0 // string
	argZRevRankMember = 0 // string

	argZRangeStart = 0 // int64 big-endian
	argZRangeStop  = 1 // int64 big-endian

	argZRangeByScoreMin = 0 // float64 big-endian
	argZRangeByScoreMax = 1 // float64 big-endian
)

const (
	argValueSetData = 0 // []byte
)

const (
	argSAddMember = 0 // string
	argSAddTTL    = 1 // int64 ns big-endian; absent = no expiry

	argSRemMember = 0 // string

	argSIsMemberMember = 0 // string

	argSExpireMember = 0 // string
	argSExpireTTL    = 1 // int64 ns big-endian
)

const (
	argHSetField = 0 // string
	argHSetData  = 1 // []byte
	argHSetTTL   = 2 // int64 ns big-endian; absent = no expiry

	argHGetField = 0 // string

	argHDelField = 0 // string

	argHExpireField    = 0 // string
	argHExpireFieldTTL = 1 // int64 ns big-endian
)

// -- shared ops --

func execDel(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	m.store.Del(key)
	return nil, nil
}

func execExpire(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	m.store.Expire(key, decodeTTL(args, argExpireTTL))
	return nil, nil
}

// -- value ops --

func execValueSet(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	return nil, m.store.Set(key, store.NewValueStructure(args[argValueSetData]))
}

func execValueGet(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	e, ok := m.store.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	v, ok := e.(*store.ValueStructure)
	if !ok {
		return nil, errTypeMismatch
	}
	return [][]byte{v.Data}, nil
}

// -- set ops --

func execSAdd(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argSAddMember])
	ttl := decodeTTL(args, argSAddTTL)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		return applyAdd(ds, member, ttl)
	})
}

func execSRem(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argSRemMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		ss, ok := ds.(*store.SetStructure)
		if !ok {
			return nil, errNotASet
		}
		ss.Remove(member)
		return ss, nil
	})
}

func execSIsMember(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argSIsMemberMember])
	var result bool
	m.store.Read(key, func(ds store.DataStructure) {
		if ss, ok := ds.(*store.SetStructure); ok {
			result = ss.IsMember(member)
		}
	})
	if result {
		return [][]byte{{1}}, nil
	}
	return [][]byte{{0}}, nil
}

func execSMembers(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var members []string
	m.store.Read(key, func(ds store.DataStructure) {
		if ss, ok := ds.(*store.SetStructure); ok {
			members = ss.Members()
		}
	})
	out := make([][]byte, len(members))
	for i, m := range members {
		out[i] = []byte(m)
	}
	return out, nil
}

func execSCard(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var count int
	m.store.Read(key, func(ds store.DataStructure) {
		if ss, ok := ds.(*store.SetStructure); ok {
			count = ss.Card()
		}
	})
	return [][]byte{encodeUint64(uint64(count))}, nil
}

func execSExpireMember(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argSExpireMember])
	ttl := decodeTTL(args, argSExpireTTL)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		ss, ok := ds.(*store.SetStructure)
		if !ok {
			return nil, errNotASet
		}
		ss.ExpireMember(member, ttl)
		return ss, nil
	})
}

// -- hash ops --

func execHSet(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	field := string(args[argHSetField])
	data := args[argHSetData]
	ttl := decodeTTL(args, argHSetTTL)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		return applyHSet(ds, field, data, ttl)
	})
}

func execHGet(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	field := string(args[argHGetField])
	var data []byte
	m.store.Read(key, func(ds store.DataStructure) {
		if h, ok := ds.(*store.HashStructure); ok {
			data, _ = h.HGet(field)
		}
	})
	if data == nil {
		return nil, ErrNotFound
	}
	return [][]byte{data}, nil
}

func execHDel(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	field := string(args[argHDelField])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		h, ok := ds.(*store.HashStructure)
		if !ok {
			return nil, errNotAHash
		}
		h.HDel(field)
		return h, nil
	})
}

func execHGetAll(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var out [][]byte
	m.store.Read(key, func(ds store.DataStructure) {
		if h, ok := ds.(*store.HashStructure); ok {
			out = h.AppendAll(make([][]byte, 0, h.Len()*2))
		}
	})
	return out, nil
}

func execHKeys(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var fields []string
	m.store.Read(key, func(ds store.DataStructure) {
		if h, ok := ds.(*store.HashStructure); ok {
			fields = h.Fields()
		}
	})
	out := make([][]byte, len(fields))
	for i, f := range fields {
		out[i] = []byte(f)
	}
	return out, nil
}

func execHExpireField(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	field := string(args[argHExpireField])
	ttl := decodeTTL(args, argHExpireFieldTTL)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		h, ok := ds.(*store.HashStructure)
		if !ok {
			return nil, errNotAHash
		}
		h.ExpireField(field, ttl)
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

// -- primitive encoding helpers --

// decodeTTL reads an int64 nanosecond TTL from args[idx].
// Returns 0 (no expiry) if the slot is absent or empty.
func decodeTTL(args [][]byte, idx int) time.Duration {
	if idx >= len(args) || len(args[idx]) == 0 {
		return 0
	}
	return time.Duration(int64(binary.BigEndian.Uint64(args[idx])))
}

// encodeUint64 encodes n as an 8-byte big-endian slice.
func encodeUint64(n uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, n)
}

// encodeFloat64 encodes f as an 8-byte IEEE 754 big-endian slice.
func encodeFloat64(f float64) []byte {
	return binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
}

// decodeFloat64 decodes an 8-byte IEEE 754 big-endian slice.
func decodeFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

// decodeInt64 decodes an 8-byte big-endian int64 from args[idx].
// Returns 0 if the slot is absent or too short.
func decodeInt64(args [][]byte, idx int) int64 {
	if idx >= len(args) || len(args[idx]) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(args[idx]))
}

// -- list ops --

func execLPush(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		l := asOrNewList(ds)
		if l == nil {
			return nil, errNotAList
		}
		l.LPush(args[argLPushData])
		return l, nil
	})
}

func execRPush(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		l := asOrNewList(ds)
		if l == nil {
			return nil, errNotAList
		}
		l.RPush(args[argRPushData])
		return l, nil
	})
}

func execLPop(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var popped []byte
	err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		l, ok := ds.(*store.ListStructure)
		if !ok {
			return nil, errNotAList
		}
		v, ok := l.LPop()
		if !ok {
			return l, nil
		}
		popped = v
		return l, nil
	})
	if err != nil {
		return nil, err
	}
	if popped == nil {
		return nil, ErrNotFound
	}
	return [][]byte{popped}, nil
}

func execRPop(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var popped []byte
	err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		l, ok := ds.(*store.ListStructure)
		if !ok {
			return nil, errNotAList
		}
		v, ok := l.RPop()
		if !ok {
			return l, nil
		}
		popped = v
		return l, nil
	})
	if err != nil {
		return nil, err
	}
	if popped == nil {
		return nil, ErrNotFound
	}
	return [][]byte{popped}, nil
}

func execLLen(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var count int
	m.store.Read(key, func(ds store.DataStructure) {
		if l, ok := ds.(*store.ListStructure); ok {
			count = l.Len()
		}
	})
	return [][]byte{encodeUint64(uint64(count))}, nil
}

func execLIndex(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	idx := int(decodeInt64(args, argLIndexIndex))
	var data []byte
	m.store.Read(key, func(ds store.DataStructure) {
		if l, ok := ds.(*store.ListStructure); ok {
			data, _ = l.Index(idx)
		}
	})
	if data == nil {
		return nil, ErrNotFound
	}
	return [][]byte{data}, nil
}

func execLRange(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	start := int(decodeInt64(args, argLRangeStart))
	stop := int(decodeInt64(args, argLRangeStop))
	var items [][]byte
	m.store.Read(key, func(ds store.DataStructure) {
		if l, ok := ds.(*store.ListStructure); ok {
			items = l.Range(start, stop)
		}
	})
	return items, nil
}

func execLSet(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	idx := int(decodeInt64(args, argLSetIndex))
	data := args[argLSetData]
	var found bool
	err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		l, ok := ds.(*store.ListStructure)
		if !ok {
			return nil, errNotAList
		}
		found = l.Set(idx, data)
		return l, nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return nil, nil
}

// asOrNewList returns ds cast to *store.ListStructure, or a new one if ds is nil.
// Returns nil if ds is a different type.
func asOrNewList(ds store.DataStructure) *store.ListStructure {
	if ds == nil {
		return store.NewListStructure()
	}
	l, ok := ds.(*store.ListStructure)
	if !ok {
		return nil
	}
	return l
}

// -- zset ops --

func execZAdd(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	score := decodeFloat64(args[argZAddScore])
	member := string(args[argZAddMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		z := asOrNewZSet(ds)
		if z == nil {
			return nil, errNotAZSet
		}
		z.ZAdd(member, score)
		return z, nil
	})
}

func execZRem(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argZRemMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, nil
		}
		z, ok := ds.(*store.ZSetStructure)
		if !ok {
			return nil, errNotAZSet
		}
		z.ZRem(member)
		return z, nil
	})
}

func execZScore(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argZScoreMember])
	var score float64
	var found bool
	m.store.Read(key, func(ds store.DataStructure) {
		if z, ok := ds.(*store.ZSetStructure); ok {
			score, found = z.ZScore(member)
		}
	})
	if !found {
		return nil, ErrNotFound
	}
	return [][]byte{encodeFloat64(score)}, nil
}

func execZRank(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argZRankMember])
	var rank int
	var found bool
	m.store.Read(key, func(ds store.DataStructure) {
		if z, ok := ds.(*store.ZSetStructure); ok {
			rank, found = z.ZRank(member)
		}
	})
	if !found {
		return nil, ErrNotFound
	}
	return [][]byte{encodeUint64(uint64(rank))}, nil
}

func execZRevRank(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	member := string(args[argZRevRankMember])
	var rank int
	var found bool
	m.store.Read(key, func(ds store.DataStructure) {
		if z, ok := ds.(*store.ZSetStructure); ok {
			rank, found = z.ZRevRank(member)
		}
	})
	if !found {
		return nil, ErrNotFound
	}
	return [][]byte{encodeUint64(uint64(rank))}, nil
}

func execZCard(m *Cluster, key string, _ [][]byte) ([][]byte, error) {
	var count int
	m.store.Read(key, func(ds store.DataStructure) {
		if z, ok := ds.(*store.ZSetStructure); ok {
			count = z.ZCard()
		}
	})
	return [][]byte{encodeUint64(uint64(count))}, nil
}

func execZRange(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	start := int(decodeInt64(args, argZRangeStart))
	stop := int(decodeInt64(args, argZRangeStop))
	var out [][]byte
	m.store.Read(key, func(ds store.DataStructure) {
		if z, ok := ds.(*store.ZSetStructure); ok {
			for _, e := range z.ZRange(start, stop) {
				out = append(out, []byte(e.Member), encodeFloat64(e.Score))
			}
		}
	})
	return out, nil
}

func execZRangeByScore(m *Cluster, key string, args [][]byte) ([][]byte, error) {
	min := decodeFloat64(args[argZRangeByScoreMin])
	max := decodeFloat64(args[argZRangeByScoreMax])
	var out [][]byte
	m.store.Read(key, func(ds store.DataStructure) {
		if z, ok := ds.(*store.ZSetStructure); ok {
			for _, e := range z.ZRangeByScore(min, max) {
				out = append(out, []byte(e.Member), encodeFloat64(e.Score))
			}
		}
	})
	return out, nil
}

// asOrNewZSet returns ds cast to *store.ZSetStructure, or a new one if ds is nil.
// Returns nil if ds is a different type.
func asOrNewZSet(ds store.DataStructure) *store.ZSetStructure {
	if ds == nil {
		return store.NewZSetStructure()
	}
	z, ok := ds.(*store.ZSetStructure)
	if !ok {
		return nil
	}
	return z
}
