// Package transport handles peer-to-peer communication between Hive nodes
// using a simple TCP protocol with gob encoding. No external dependencies.
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
	Type    MsgType
	Payload []byte // gob-encoded message body
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

// Op identifies the operation being forwarded or migrated.
type Op uint8

const (
	OpSet    Op = iota + 1
	OpGet
	OpDel
	OpExpire
)

// ForwardRequest asks the receiving node to execute an operation locally.
type ForwardRequest struct {
	Op   Op
	Key  string
	Data []byte // gob-encoded value; only used by OpSet
	TTL  int64  // nanoseconds; only used by OpExpire; 0 means remove TTL
}

// ForwardResponse carries the result of a forwarded operation.
type ForwardResponse struct {
	Data []byte
}

// -- Rebalance --

// RebalanceEntry is a single key migrated to a new owner node.
type RebalanceEntry struct {
	Key  string
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
