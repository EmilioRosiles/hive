package cluster

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// This file contains the local execution functions for each cluster op.
// Each function receives pre-decoded [][]byte arg slots and works directly
// on the local store. No serialization happens here.
// Arg slot positions are documented by the constants below.
//
// Each function also receives the caller's lock-authorization token, and
// (except for Lock/Unlock/Renew) checks it via store.CheckLock inside its
// existing Read/Apply callback.

// OpScope describes how a cluster op is routed and replicated.
type OpScope uint8

const (
	ScopeWrite OpScope = iota // route to primary owner, replicate to replicas
	ScopeRead                 // route to primary owner, no replication
	ScopeLocal                // always execute on the receiving node
)

// opDef pairs an op's local execution function with its routing scope.
type opDef struct {
	Exec  func(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error)
	Scope OpScope
}

// opRegistry maps each Op to its definition.
// To add a new op: register it here and add its exec function below.
// routing.go's dispatch and handleForward never need to change.
var opRegistry = map[transport.Op]opDef{
	transport.OpDel:    {Exec: execDel, Scope: ScopeWrite},
	transport.OpExpire: {Exec: execExpire, Scope: ScopeWrite},
	transport.OpLock:   {Exec: execLock, Scope: ScopeWrite},
	transport.OpUnlock: {Exec: execUnlock, Scope: ScopeWrite},
	transport.OpRenew:  {Exec: execRenew, Scope: ScopeWrite},

	transport.OpValueSet: {Exec: execValueSet, Scope: ScopeWrite},
	transport.OpValueGet: {Exec: execValueGet, Scope: ScopeRead},

	transport.OpSAdd:      {Exec: execSAdd, Scope: ScopeWrite},
	transport.OpSRem:      {Exec: execSRem, Scope: ScopeWrite},
	transport.OpSIsMember: {Exec: execSIsMember, Scope: ScopeRead},
	transport.OpSMembers:  {Exec: execSMembers, Scope: ScopeRead},
	transport.OpSCard:     {Exec: execSCard, Scope: ScopeRead},

	transport.OpHSet:    {Exec: execHSet, Scope: ScopeWrite},
	transport.OpHGet:    {Exec: execHGet, Scope: ScopeRead},
	transport.OpHDel:    {Exec: execHDel, Scope: ScopeWrite},
	transport.OpHGetAll: {Exec: execHGetAll, Scope: ScopeRead},
	transport.OpHKeys:   {Exec: execHKeys, Scope: ScopeRead},

	transport.OpLPush:  {Exec: execLPush, Scope: ScopeWrite},
	transport.OpRPush:  {Exec: execRPush, Scope: ScopeWrite},
	transport.OpLPop:   {Exec: execLPop, Scope: ScopeWrite},
	transport.OpRPop:   {Exec: execRPop, Scope: ScopeWrite},
	transport.OpLLen:   {Exec: execLLen, Scope: ScopeRead},
	transport.OpLIndex: {Exec: execLIndex, Scope: ScopeRead},
	transport.OpLRange: {Exec: execLRange, Scope: ScopeRead},
	transport.OpLSet:   {Exec: execLSet, Scope: ScopeWrite},

	transport.OpZAdd:          {Exec: execZAdd, Scope: ScopeWrite},
	transport.OpZRem:          {Exec: execZRem, Scope: ScopeWrite},
	transport.OpZScore:        {Exec: execZScore, Scope: ScopeRead},
	transport.OpZRank:         {Exec: execZRank, Scope: ScopeRead},
	transport.OpZCard:         {Exec: execZCard, Scope: ScopeRead},
	transport.OpZRange:        {Exec: execZRange, Scope: ScopeRead},
	transport.OpZRangeByScore: {Exec: execZRangeByScore, Scope: ScopeRead},
	transport.OpZRevRank:      {Exec: execZRevRank, Scope: ScopeRead},
}

// -- arg index constants --

const (
	argExpireTTL = 0 // int64 ns big-endian
)

const (
	argLockTTL   = 0 // int64 ns big-endian
	argLockToken = 1 // uint32 big-endian; client-generated

	argUnlockToken = 0 // uint32 big-endian

	argRenewToken = 0 // uint32 big-endian
	argRenewTTL   = 1 // int64 ns big-endian
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

	argSRemMember = 0 // string

	argSIsMemberMember = 0 // string
)

const (
	argHSetField = 0 // string
	argHSetData  = 1 // []byte

	argHGetField = 0 // string

	argHDelField = 0 // string
)

// -- shared ops --

func execDel(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		return nil, nil
	})
}

func execExpire(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		if ds == nil {
			return nil, nil
		}
		if ttl := decodeTTL(args, argExpireTTL); ttl == 0 {
			ds.SetKeyExpiry(0)
		} else {
			ds.SetKeyExpiry(uint32(time.Now().Add(ttl).Unix()))
		}
		return ds, nil
	})
}

// -- lock ops --

// execLock creates a lock on key with the caller-provided token, failing
// with ErrNotFound if key doesn't exist and ErrKeyLocked if already locked.
// The check-and-set is atomic since both happen inside the same Apply call.
func execLock(m *Cluster, key string, args [][]byte, _ uint32) ([][]byte, error) {
	ttl := decodeTTL(args, argLockTTL)
	token := decodeUint32(args, argLockToken)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil {
			return nil, ErrNotFound
		}
		if exp := ds.LockExpiry(); exp != 0 && exp > uint32(time.Now().Unix()) {
			return ds, ErrKeyLocked
		}
		ds.SetLock(token, uint32(time.Now().Add(ttl).Unix()))
		return ds, nil
	})
}

// execUnlock releases the lock on key, provided token matches the current holder.
func execUnlock(m *Cluster, key string, args [][]byte, _ uint32) ([][]byte, error) {
	token := decodeUint32(args, argUnlockToken)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil || ds.LockExpiry() == 0 || ds.LockToken() != token {
			return ds, ErrLockNotHeld
		}
		ds.SetLock(0, 0)
		return ds, nil
	})
}

// execRenew extends the lock's TTL, provided token matches the current holder.
func execRenew(m *Cluster, key string, args [][]byte, _ uint32) ([][]byte, error) {
	token := decodeUint32(args, argRenewToken)
	ttl := decodeTTL(args, argRenewTTL)
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if ds == nil || ds.LockExpiry() == 0 || ds.LockToken() != token {
			return ds, ErrLockNotHeld
		}
		ds.SetLock(token, uint32(time.Now().Add(ttl).Unix()))
		return ds, nil
	})
}

// -- value ops --

// execValueSet replaces key's value, clearing any TTL but carrying over any
// existing lock state onto the new entry.
func execValueSet(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		v := store.NewValueStructure(args[argValueSetData])
		if ds != nil {
			v.SetLock(ds.LockToken(), ds.LockExpiry())
		}
		return v, nil
	})
}

func execValueGet(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var data []byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		v, ok := ds.(*store.ValueStructure)
		if !ok {
			return errTypeMismatch
		}
		data = v.Data
		return nil
	})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, ErrNotFound
	}
	return [][]byte{data}, nil
}

// -- set ops --

func execSAdd(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argSAddMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		return applyAdd(ds, member)
	})
}

func execSRem(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argSRemMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
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

func execSIsMember(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argSIsMemberMember])
	var result bool
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if ss, ok := ds.(*store.SetStructure); ok {
			result = ss.IsMember(member)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result {
		return [][]byte{{1}}, nil
	}
	return [][]byte{{0}}, nil
}

func execSMembers(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var members []string
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if ss, ok := ds.(*store.SetStructure); ok {
			members = ss.Members()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(members))
	for i, m := range members {
		out[i] = []byte(m)
	}
	return out, nil
}

func execSCard(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var count int
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if ss, ok := ds.(*store.SetStructure); ok {
			count = ss.Card()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{encodeUint64(uint64(count))}, nil
}

// -- hash ops --

func execHSet(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	field := string(args[argHSetField])
	data := args[argHSetData]
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		return applyHSet(ds, field, data)
	})
}

func execHGet(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	field := string(args[argHGetField])
	var data []byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if h, ok := ds.(*store.HashStructure); ok {
			data, _ = h.HGet(field)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, ErrNotFound
	}
	return [][]byte{data}, nil
}

func execHDel(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	field := string(args[argHDelField])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
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

func execHGetAll(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var out [][]byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if h, ok := ds.(*store.HashStructure); ok {
			out = h.AppendAll(make([][]byte, 0, h.Len()*2))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func execHKeys(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var fields []string
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if h, ok := ds.(*store.HashStructure); ok {
			fields = h.Fields()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(fields))
	for i, f := range fields {
		out[i] = []byte(f)
	}
	return out, nil
}

// -- apply helpers --

// applyHSet upserts field into a HashStructure, creating one if ds is nil.
func applyHSet(ds store.DataStructure, field string, data []byte) (store.DataStructure, error) {
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
	h.HSet(field, data)
	return h, nil
}

// applyAdd adds member to a SetStructure, creating one if ds is nil.
func applyAdd(ds store.DataStructure, member string) (store.DataStructure, error) {
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
	ss.Add(member)
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

// encodeUint32 encodes n as a 4-byte big-endian slice.
func encodeUint32(n uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, n)
}

// decodeUint32 decodes a 4-byte big-endian uint32 from args[idx].
// Returns 0 if the slot is absent or too short.
func decodeUint32(args [][]byte, idx int) uint32 {
	if idx >= len(args) || len(args[idx]) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(args[idx])
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

func execLPush(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		l := asOrNewList(ds)
		if l == nil {
			return nil, errNotAList
		}
		l.LPush(args[argLPushData])
		return l, nil
	})
}

func execRPush(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		l := asOrNewList(ds)
		if l == nil {
			return nil, errNotAList
		}
		l.RPush(args[argRPushData])
		return l, nil
	})
}

func execLPop(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var popped []byte
	err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
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

func execRPop(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var popped []byte
	err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
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

func execLLen(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var count int
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if l, ok := ds.(*store.ListStructure); ok {
			count = l.Len()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{encodeUint64(uint64(count))}, nil
}

func execLIndex(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	idx := int(decodeInt64(args, argLIndexIndex))
	var data []byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if l, ok := ds.(*store.ListStructure); ok {
			data, _ = l.Index(idx)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, ErrNotFound
	}
	return [][]byte{data}, nil
}

func execLRange(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	start := int(decodeInt64(args, argLRangeStart))
	stop := int(decodeInt64(args, argLRangeStop))
	var items [][]byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if l, ok := ds.(*store.ListStructure); ok {
			items = l.Range(start, stop)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func execLSet(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	idx := int(decodeInt64(args, argLSetIndex))
	data := args[argLSetData]
	var found bool
	err := m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
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

func execZAdd(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	score := decodeFloat64(args[argZAddScore])
	member := string(args[argZAddMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
		z := asOrNewZSet(ds)
		if z == nil {
			return nil, errNotAZSet
		}
		z.ZAdd(member, score)
		return z, nil
	})
}

func execZRem(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argZRemMember])
	return nil, m.store.Apply(key, func(ds store.DataStructure) (store.DataStructure, error) {
		if err := store.CheckLock(ds, token); err != nil {
			return ds, err
		}
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

func execZScore(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argZScoreMember])
	var score float64
	var found bool
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if z, ok := ds.(*store.ZSetStructure); ok {
			score, found = z.ZScore(member)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return [][]byte{encodeFloat64(score)}, nil
}

func execZRank(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argZRankMember])
	var rank int
	var found bool
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if z, ok := ds.(*store.ZSetStructure); ok {
			rank, found = z.ZRank(member)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return [][]byte{encodeUint64(uint64(rank))}, nil
}

func execZRevRank(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	member := string(args[argZRevRankMember])
	var rank int
	var found bool
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if z, ok := ds.(*store.ZSetStructure); ok {
			rank, found = z.ZRevRank(member)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return [][]byte{encodeUint64(uint64(rank))}, nil
}

func execZCard(m *Cluster, key string, _ [][]byte, token uint32) ([][]byte, error) {
	var count int
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if z, ok := ds.(*store.ZSetStructure); ok {
			count = z.ZCard()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{encodeUint64(uint64(count))}, nil
}

func execZRange(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	start := int(decodeInt64(args, argZRangeStart))
	stop := int(decodeInt64(args, argZRangeStop))
	var out [][]byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if z, ok := ds.(*store.ZSetStructure); ok {
			for _, e := range z.ZRange(start, stop) {
				out = append(out, []byte(e.Member), encodeFloat64(e.Score))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func execZRangeByScore(m *Cluster, key string, args [][]byte, token uint32) ([][]byte, error) {
	min := decodeFloat64(args[argZRangeByScoreMin])
	max := decodeFloat64(args[argZRangeByScoreMax])
	var out [][]byte
	err := m.store.Read(key, func(ds store.DataStructure) error {
		if err := store.CheckLock(ds, token); err != nil {
			return err
		}
		if z, ok := ds.(*store.ZSetStructure); ok {
			for _, e := range z.ZRangeByScore(min, max) {
				out = append(out, []byte(e.Member), encodeFloat64(e.Score))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
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
