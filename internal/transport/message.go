// Package transport handles peer-to-peer communication between Hive nodes.
package transport

import "time"

// MsgType identifies the type of message in a frame.
type MsgType uint8

const (
	MsgHeartbeat MsgType = iota + 1 // gossip state sync
	MsgForward                       // route a command to the responsible node
	MsgRebalance                     // bulk key migration during rebalance
	MsgLeave                         // graceful departure announcement
)

// Frame is the envelope wrapping every message on the wire.
type Frame struct {
	ID      uint32  // request ID used to match responses on a multiplexed connection
	Type    MsgType
	Payload []byte // msgpack-encoded message body
	Err     string // non-empty if the handler returned an error
}

// -- Heartbeat --

type PeerState struct {
	NodeID   string
	Addr     string
	Alive    bool
	LastSeen time.Time
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
	OpHFields      Op = 104
	OpHExpireField Op = 105
)

// ForwardRequest asks the receiving node to execute an operation locally.
// Payload is a msgpack-encoded op-specific struct.
type ForwardRequest struct {
	Op      Op
	Key     string
	Payload []byte
}

// ForwardResponse carries the result of a forwarded operation.
// Payload is a msgpack-encoded op-specific response struct.
type ForwardResponse struct {
	Payload []byte
}

// -- Shared op payloads --

type ExpirePayload struct{ TTLNs int64 }

// -- Value op payloads / responses --

type ValueSetPayload struct{ Data []byte }

type DataResponse struct{ Data []byte }

// -- Set op payloads / responses --

type SAddPayload struct {
	Member string
	TTLNs  int64 // 0 = no expiry
}

type SRemPayload struct{ Member string }

type SIsMemberPayload struct{ Member string }

type SExpireMemberPayload struct {
	Member string
	TTLNs  int64
}

// -- Hash op payloads / responses --

type HSetPayload struct {
	Field string
	Data  []byte
	TTLNs int64 // 0 = no expiry
}

type HGetPayload struct{ Field string }

type HDelPayload struct{ Field string }

type HExpireFieldPayload struct {
	Field string
	TTLNs int64
}

type MapResponse struct{ Values map[string][]byte }

// -- Generic response types --

type BoolResponse struct{ Value bool }

type StringsResponse struct{ Values []string }

type IntResponse struct{ Value int }

// -- Rebalance --

// RebalanceEntry is a single key migrated to a new owner node.
type RebalanceEntry struct {
	Key  string
	Kind uint8  // store.Kind value
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
