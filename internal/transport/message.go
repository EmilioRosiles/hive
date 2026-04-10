// Package transport handles peer-to-peer communication between Hive nodes.
package transport

import "time"

// MsgType identifies the type of message in a frame.
type MsgType uint8

const (
	MsgHeartbeat MsgType = iota + 1 // gossip state sync
	MsgForward                      // route a command to the responsible node
	MsgRebalance                    // bulk key migration during rebalance
	MsgLeave                        // graceful departure announcement
)

// Frame is the envelope wrapping every message on the wire.
type Frame struct {
	ID      uint32 // request ID used to match responses on a multiplexed connection
	Type    MsgType
	Payload []byte // msgpack-encoded message body
	Err     string // non-empty if the handler returned an error
}

// -- Heartbeat --

type PeerState struct {
	NodeID            string
	Addr              string
	Alive             bool
	LastSeen          time.Time
	ReplicationFactor int
	MemLimit          uint64
}

type HeartbeatRequest struct {
	Peers []PeerState
}

type HeartbeatResponse struct {
	Peers []PeerState
}

// -- Forward --

// Op identifies the operation being forwarded.
// Shared ops (Del, Expire) work for any DataStructure kind.
// Kind-specific ops are grouped in ranges: Value=10-19, Set=50-59, Hash=100-109.
type Op uint8

const (
	// Shared ops — apply to any DataStructure kind.
	OpDel    Op = 1
	OpExpire Op = 2

	// Value ops.
	OpValueSet Op = 10
	OpValueGet Op = 11

	// Set ops.
	OpSAdd          Op = 50
	OpSRem          Op = 51
	OpSIsMember     Op = 52
	OpSMembers      Op = 53
	OpSCard         Op = 54
	OpSExpireMember Op = 55

	// Hash ops.
	OpHSet         Op = 100
	OpHGet         Op = 101
	OpHDel         Op = 102
	OpHGetAll      Op = 103
	OpHKeys        Op = 104
	OpHExpireField Op = 105
)

// ForwardRequest asks the receiving node to execute an operation locally.
// Args are positional raw byte slots — no intermediate struct encoding.
type ForwardRequest struct {
	Op   Op
	Key  string
	Args [][]byte
}

// ForwardResponse carries the result of a forwarded operation.
// Results are positional raw byte slots matching the op's return layout.
// Write ops return nil. Read ops return one or more slots.
type ForwardResponse struct {
	Results [][]byte
}

// -- Rebalance --

// RebalanceEntry is a single key migrated to a new owner node.
type RebalanceEntry struct {
	Key  string
	Kind uint8 // store.Kind value
	Data []byte
	TTL  int64 // nanoseconds until expiry from time of send; 0 means no TTL
}

type RebalanceBatch struct {
	Entries []RebalanceEntry
}

// -- Leave --

type LeaveRequest struct {
	NodeID string
}
