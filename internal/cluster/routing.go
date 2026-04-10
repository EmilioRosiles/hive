package cluster

import (
	"fmt"
	"slices"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// OpScope describes how a cluster op is routed and replicated.
type OpScope uint8

const (
	ScopeWrite OpScope = iota // route to primary owner, replicate to replicas
	ScopeRead                 // route to primary owner, no replication
	ScopeLocal                // always execute on the receiving node
)

// opDef pairs an op's local execution function with its routing scope.
type opDef struct {
	Exec  func(m *Manager, key string, payload []byte) ([]byte, error)
	Scope OpScope
}

// opRegistry maps each Op to its definition.
// To add a new op: register it here and add its exec function in exec.go.
// dispatch and handleForward never need to change.
var opRegistry = map[transport.Op]opDef{
	transport.OpDel:    {Exec: execDel, Scope: ScopeWrite},
	transport.OpExpire: {Exec: execExpire, Scope: ScopeWrite},

	transport.OpValueSet: {Exec: execValueSet, Scope: ScopeWrite},
	transport.OpValueGet: {Exec: execValueGet, Scope: ScopeRead},

	transport.OpSAdd:          {Exec: execSAdd, Scope: ScopeWrite},
	transport.OpSRem:          {Exec: execSRem, Scope: ScopeWrite},
	transport.OpSIsMember:     {Exec: execSIsMember, Scope: ScopeRead},
	transport.OpSMembers:      {Exec: execSMembers, Scope: ScopeRead},
	transport.OpSCard:         {Exec: execSCard, Scope: ScopeRead},
	transport.OpSExpireMember: {Exec: execSExpireMember, Scope: ScopeWrite},

	transport.OpHSet:         {Exec: execHSet, Scope: ScopeWrite},
	transport.OpHGet:         {Exec: execHGet, Scope: ScopeRead},
	transport.OpHDel:         {Exec: execHDel, Scope: ScopeWrite},
	transport.OpHGetAll:      {Exec: execHGetAll, Scope: ScopeRead},
	transport.OpHKeys:        {Exec: execHKeys, Scope: ScopeRead},
	transport.OpHExpireField: {Exec: execHExpireField, Scope: ScopeWrite},
}

// handleFrame is the transport.Handler registered with the TCP server.
// It fans incoming frames out to their respective handlers.
func (m *Manager) handleFrame(msgType transport.MsgType, payload []byte) ([]byte, error) {
	switch msgType {
	case transport.MsgHeartbeat:
		return m.handleHeartbeat(payload)
	case transport.MsgForward:
		return m.handleForward(payload)
	case transport.MsgRebalance:
		return m.handleRebalance(payload)
	case transport.MsgLeave:
		return nil, m.handleLeave(payload)
	default:
		return nil, fmt.Errorf("handler: unknown message type %d", msgType)
	}
}

// handleForward decodes an incoming ForwardRequest and executes the op locally.
func (m *Manager) handleForward(payload []byte) ([]byte, error) {
	var req transport.ForwardRequest
	if err := transport.Decode(payload, &req); err != nil {
		return nil, fmt.Errorf("handler: decode forward: %w", err)
	}
	def, ok := opRegistry[req.Op]
	if !ok {
		return nil, fmt.Errorf("handler: unknown op %d", req.Op)
	}
	respPayload, err := def.Exec(m, req.Key, req.Payload)
	if err != nil {
		return nil, err
	}
	if respPayload == nil {
		return nil, nil
	}
	return transport.Encode(transport.ForwardResponse{Payload: respPayload})
}

// dispatch routes op to the correct node(s) and executes it.
//
//   - ScopeRead:  execute locally if responsible, otherwise forward to primary.
//   - ScopeWrite: execute locally if responsible, otherwise forward to primary;
//     then replicate to replicas asynchronously.
//   - ScopeLocal: always execute on this node regardless of ring ownership.
func (m *Manager) dispatch(op transport.Op, key string, payload any) ([]byte, error) {
	def, ok := opRegistry[op]
	if !ok {
		return nil, fmt.Errorf("cluster: unknown op %d", op)
	}

	var encoded []byte
	if payload != nil {
		var err error
		encoded, err = transport.Encode(payload)
		if err != nil {
			return nil, err
		}
	}

	nodes := m.responsibleNodes(key)

	switch def.Scope {
	case ScopeRead:
		if m.localIsResponsible(nodes) {
			return def.Exec(m, key, encoded)
		}
		resp, err := m.sendReq(nodes[0], transport.ForwardRequest{Op: op, Key: key, Payload: encoded})
		if err != nil {
			return nil, err
		}
		return resp.Payload, nil

	case ScopeWrite:
		req := transport.ForwardRequest{Op: op, Key: key, Payload: encoded}
		if m.localIsResponsible(nodes) {
			if _, err := def.Exec(m, key, encoded); err != nil {
				return nil, err
			}
		} else if len(nodes) > 0 {
			if _, err := m.sendReq(nodes[0], req); err != nil {
				return nil, err
			}
		}
		m.fanOutReplicas(req, nodes)
		return nil, nil

	case ScopeLocal:
		return def.Exec(m, key, encoded)
	}

	return nil, fmt.Errorf("cluster: unhandled scope for op %d", op)
}

// fanOutReplicas sends req to nodes[1:] asynchronously, skipping this node.
func (m *Manager) fanOutReplicas(req transport.ForwardRequest, nodes []string) {
	if len(nodes) <= 1 {
		return
	}
	for _, nodeID := range nodes[1:] {
		if nodeID == m.cfg.NodeID {
			continue
		}
		go func() {
			if _, err := m.sendReq(nodeID, req); err != nil {
				m.handlePeerError(nodeID)
			}
		}()
	}
}

// localIsResponsible returns true if this node should store the key locally —
// i.e. it is the primary or one of the replicas.
func (m *Manager) localIsResponsible(nodes []string) bool {
	if len(nodes) == 0 {
		return true
	}
	return slices.Contains(nodes, m.cfg.NodeID)
}

// sendReq encodes and sends a ForwardRequest to nodeID, returning the response.
func (m *Manager) sendReq(nodeID string, req transport.ForwardRequest) (transport.ForwardResponse, error) {
	client, ok := m.getClient(nodeID)
	if !ok {
		return transport.ForwardResponse{}, fmt.Errorf("cluster: no client for node %s", nodeID)
	}
	framePayload, err := transport.Encode(req)
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	respFrame, err := client.Send(transport.Frame{Type: transport.MsgForward, Payload: framePayload})
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	var resp transport.ForwardResponse
	if len(respFrame.Payload) > 0 {
		if err := transport.Decode(respFrame.Payload, &resp); err != nil {
			return transport.ForwardResponse{}, err
		}
	}
	return resp, nil
}

func (m *Manager) handlePeerError(nodeID string) {
	m.markDead(nodeID)
}
