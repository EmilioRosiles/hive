// Package transport handles peer-to-peer communication between Hive nodes.
package transport

// MsgType identifies the type of message in a frame.
type MsgType uint8

const (
	MsgHeartbeat MsgType = iota + 1 // gossip state sync
	MsgForward                      // route a command to the responsible node
	MsgRebalance                    // bulk key migration during rebalance
	MsgLeave                        // graceful departure announcement
	MsgProbe                        // indirect reachability probe
)

// Frame is the envelope wrapping every message on the wire. It is written and
// read via WriteFrame/ReadFrame (see frame.go) as a fixed 10-byte binary
// header followed by the payload bytes — there is no version/magic byte,
// since this is a private protocol between nodes of one cluster running the
// same build, not a format shared with external or mixed-version clients.
//
// A single frame can carry Payload or Err, never both: if Err is set when
// writing, its bytes are sent as the payload and Payload is ignored. This
// holds for every handler in this package today (a non-nil error always
// comes with a nil response payload).
type Frame struct {
	ID   uint32 // request ID used to match responses on a multiplexed connection
	Type MsgType
	// Payload is the message body — binary-encoded for MsgForward/MsgRebalance
	// (see binary.go), msgpack-encoded for MsgHeartbeat/MsgLeave (see codec.go).
	Payload []byte
	Err     string // non-empty if the handler returned an error
}

// -- Heartbeat --

// PeerState is the wire representation of a node's view of a cluster member.
// Incarnation is the authoritative ordering key: a remote update is applied
// only when its Incarnation strictly exceeds the locally held value.
type PeerState struct {
	NodeID            string
	Addr              string
	Status            uint8
	Incarnation       uint64
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

	// List ops.
	OpLPush  Op = 150
	OpRPush  Op = 151
	OpLPop   Op = 152
	OpRPop   Op = 153
	OpLLen   Op = 154
	OpLIndex Op = 155
	OpLRange Op = 156
	OpLSet   Op = 157

	// ZSet ops.
	OpZAdd          Op = 170
	OpZRem          Op = 171
	OpZScore        Op = 172
	OpZRank         Op = 173
	OpZCard         Op = 174
	OpZRange        Op = 175
	OpZRangeByScore Op = 176
	OpZRevRank      Op = 177
)

// ForwardRequest asks the receiving node to execute an operation locally.
// Args are positional raw byte slots — no intermediate struct encoding.
// Wire encoding: see MarshalBinary/UnmarshalBinary in binary.go.
type ForwardRequest struct {
	Op   Op
	Key  string
	Args [][]byte
}

// ForwardResponse carries the result of a forwarded operation.
// Results are positional raw byte slots matching the op's return layout.
// Write ops return nil. Read ops return one or more slots.
// Wire encoding: see MarshalBinary/UnmarshalBinary in binary.go.
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

// Wire encoding: see MarshalBinary/UnmarshalBinary in binary.go. RebalanceEntry
// never travels alone on the wire, so it has no encoding methods of its own —
// its layout is inlined directly into RebalanceBatch's.
type RebalanceBatch struct {
	Entries []RebalanceEntry
}

// -- Leave --

type LeaveRequest struct {
	NodeID string
}
