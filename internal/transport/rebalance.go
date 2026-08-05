package transport

import (
	"encoding/binary"
	"fmt"
)

// RebalanceEntry is a single key migrated to a new owner node.
// RebalanceEntry never travels alone on the wire, only nested in a
// RebalanceBatch, so it has no MarshalBinary/UnmarshalBinary of its own —
// its layout is inlined directly into RebalanceBatch's.
type RebalanceEntry struct {
	Key  string
	Kind uint8 // store.Kind value
	Data []byte
	TTL  int64 // nanoseconds until expiry from time of send; 0 means no TTL
}

type RebalanceBatch struct {
	Entries []RebalanceEntry
}

// -- RebalanceBatch wire codec --
//
// Wire layout:
//
//	EntryCount uint32
//	repeated EntryCount times:
//	  KeyLen  uint32
//	  Key     [KeyLen]byte
//	  Kind    uint8
//	  DataLen uint32
//	  Data    [DataLen]byte
//	  TTL     int64

// rebalanceEntryMinSize is each entry's minimum possible encoded size
// (KeyLen + Kind + DataLen + TTL, with zero-length Key/Data) — used to bound
// EntryCount against the remaining buffer before allocating the entries slice.
const rebalanceEntryMinSize = 4 + 1 + 4 + 8

func (b RebalanceBatch) MarshalBinary() ([]byte, error) {
	size := 4
	for _, e := range b.Entries {
		size += 4 + len(e.Key) + 1 + 4 + len(e.Data) + 8
	}
	buf := make([]byte, size)
	i := 0
	binary.BigEndian.PutUint32(buf[i:], uint32(len(b.Entries)))
	i += 4
	for _, e := range b.Entries {
		binary.BigEndian.PutUint32(buf[i:], uint32(len(e.Key)))
		i += 4
		i += copy(buf[i:], e.Key)
		buf[i] = e.Kind
		i++
		binary.BigEndian.PutUint32(buf[i:], uint32(len(e.Data)))
		i += 4
		i += copy(buf[i:], e.Data)
		binary.BigEndian.PutUint64(buf[i:], uint64(e.TTL))
		i += 8
	}
	return buf, nil
}

func (b *RebalanceBatch) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	count, err := br.count(rebalanceEntryMinSize)
	if err != nil {
		return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
	}
	entries := make([]RebalanceEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		key, err := br.string()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		kind, err := br.uint8()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		blob, err := br.bytes()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		ttl, err := br.int64()
		if err != nil {
			return fmt.Errorf("transport: decode RebalanceBatch: %w", err)
		}
		entries = append(entries, RebalanceEntry{Key: key, Kind: kind, Data: blob, TTL: ttl})
	}
	if !br.done() {
		return fmt.Errorf("transport: decode RebalanceBatch: trailing data")
	}
	b.Entries = entries
	return nil
}
