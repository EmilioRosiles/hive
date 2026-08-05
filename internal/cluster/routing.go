package cluster

import (
	"context"
	"fmt"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// handleFrame is the transport.Handler registered with the TCP server.
// It fans incoming frames out to their respective handlers.
func (m *Cluster) handleFrame(msgType transport.MsgType, payload []byte) ([]byte, error) {
	switch msgType {
	case transport.MsgHeartbeat:
		return m.handleHeartbeat(payload)
	case transport.MsgForward:
		return m.handleForward(payload)
	case transport.MsgForwardBatch:
		return nil, m.handleForwardBatch(payload)
	case transport.MsgRebalance:
		return m.handleRebalance(payload)
	case transport.MsgLeave:
		return nil, m.handleLeave(payload)
	default:
		return nil, fmt.Errorf("handler: unknown message type %d", msgType)
	}
}

// handleForward decodes an incoming ForwardRequest and executes the op locally.
func (m *Cluster) handleForward(payload []byte) ([]byte, error) {
	var req transport.ForwardRequest
	if err := transport.Decode(payload, &req); err != nil {
		return nil, fmt.Errorf("handler: decode forward: %w", err)
	}
	def, ok := opRegistry[req.Op]
	if !ok {
		return nil, fmt.Errorf("handler: unknown op %d", req.Op)
	}
	results, err := def.Exec(m, req.Key, req.Args)
	if err != nil {
		return nil, err
	}
	if results == nil {
		return nil, nil
	}
	return transport.Encode(transport.ForwardResponse{Results: results})
}

// handleForwardBatch decodes a batch of replicated ops and applies each one
// in order. Every entry is applied even if an earlier one fails, so one bad
// op doesn't stop the rest of the batch from reaching the replica; the first
// error encountered (if any) is returned so the sender still learns of it.
func (m *Cluster) handleForwardBatch(payload []byte) error {
	var batch transport.ForwardBatch
	if err := transport.Decode(payload, &batch); err != nil {
		return fmt.Errorf("handler: decode forward batch: %w", err)
	}
	var firstErr error
	for _, req := range batch.Requests {
		def, ok := opRegistry[req.Op]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("handler: unknown op %d", req.Op)
			}
			continue
		}
		if _, err := def.Exec(m, req.Key, req.Args); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dispatch routes op to the correct node(s) and executes it.
//
//   - ScopeRead:  execute locally if responsible, otherwise forward to primary.
//   - ScopeWrite: sync write to primary (exec locally if we are primary, forward otherwise),
//     then queue replication to replicas (nodes[1:]).
//   - ScopeLocal: always execute on this node regardless of ring ownership.
func (m *Cluster) dispatch(op transport.Op, key string, args ...[]byte) ([][]byte, error) {
	def, ok := opRegistry[op]
	if !ok {
		return nil, fmt.Errorf("cluster: unknown op %d", op)
	}

	nodes := m.responsibleNodes(key)

	switch def.Scope {
	case ScopeRead:
		return m.execOrForward(def, op, key, args, nodes)

	case ScopeWrite:
		result, err := m.execOrForward(def, op, key, args, nodes)
		if err != nil {
			return nil, err
		}
		req := transport.ForwardRequest{Op: op, Key: key, Args: args}
		m.fanOutReplicas(def, req, nodes[1:])
		return result, nil

	case ScopeLocal:
		return def.Exec(m, key, args)
	}

	return nil, fmt.Errorf("cluster: unhandled scope for op %d", op)
}

// execOrForward runs op locally if this node is the primary owner for key (or
// there is no other node), otherwise forwards it to the primary and returns
// its response.
func (m *Cluster) execOrForward(def opDef, op transport.Op, key string, args [][]byte, nodes []string) ([][]byte, error) {
	if len(nodes) == 0 || m.cfg.NodeID == nodes[0] {
		return def.Exec(m, key, args)
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.RoutingTimeout)
	defer cancel()
	resp, err := m.sendForward(ctx, nodes[0], transport.ForwardRequest{Op: op, Key: key, Args: args})
	if err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// fanOutReplicas delivers req to each node in nodes. A node that is this node
// executes the op synchronously (no network I/O involved); a remote node's
// write is queued on its replicator, which applies queued writes to that
// peer in order. nodes is typically nodes[1:] from the ring so the primary
// (nodes[0]) is never duplicated here.
func (m *Cluster) fanOutReplicas(def opDef, req transport.ForwardRequest, nodes []string) {
	for _, nodeID := range nodes {
		if nodeID == m.cfg.NodeID {
			def.Exec(m, req.Key, req.Args)
			continue
		}
		if rep, ok := m.getReplicator(nodeID); ok {
			rep.enqueue(req)
		}
	}
}

// sendForward encodes and sends a ForwardRequest to nodeID, returning the response.
func (m *Cluster) sendForward(ctx context.Context, nodeID string, req transport.ForwardRequest) (transport.ForwardResponse, error) {
	client, ok := m.getClient(nodeID)
	if !ok {
		return transport.ForwardResponse{}, fmt.Errorf("cluster: no client for node %s", nodeID)
	}
	framePayload, err := transport.Encode(req)
	if err != nil {
		return transport.ForwardResponse{}, err
	}
	respFrame, err := client.Send(ctx, transport.Frame{Type: transport.MsgForward, Payload: framePayload})
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

// sendForwardBatch encodes and sends a batch of queued replication writes to
// nodeID. Replication never reads the results of a write, so unlike
// sendForward there is no response payload to decode — only success/failure.
func (m *Cluster) sendForwardBatch(ctx context.Context, nodeID string, batch []transport.ForwardRequest) error {
	client, ok := m.getClient(nodeID)
	if !ok {
		return fmt.Errorf("cluster: no client for node %s", nodeID)
	}
	framePayload, err := transport.Encode(transport.ForwardBatch{Requests: batch})
	if err != nil {
		return err
	}
	_, err = client.Send(ctx, transport.Frame{Type: transport.MsgForwardBatch, Payload: framePayload})
	return err
}

// Exec is the exported entry point for store files to run a cluster op.
// It delegates to dispatch, keeping routing logic internal to this package.
func (m *Cluster) Exec(op transport.Op, key string, args ...[]byte) ([][]byte, error) {
	return m.dispatch(op, key, args...)
}
