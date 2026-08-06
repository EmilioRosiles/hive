package transport

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
	MemUsed           uint64
}

type HeartbeatRequest struct {
	Peers []PeerState
}

type HeartbeatResponse struct {
	Peers []PeerState
}

// LeaveRequest announces a node's graceful departure from the cluster.
type LeaveRequest struct {
	NodeID string
}
