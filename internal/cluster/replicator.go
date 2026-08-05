package cluster

import (
	"context"
	"sync"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// replicator serializes replication writes to one peer through a single
// worker goroutine, so writes to that peer are always applied in the order
// they were dispatched, with one goroutine per peer instead of one per write.
type replicator struct {
	nodeID string
	mgr    *Cluster
	jobs   chan transport.ForwardRequest
	stopCh chan struct{}
	once   sync.Once
}

func newReplicator(nodeID string, mgr *Cluster) *replicator {
	r := &replicator{
		nodeID: nodeID,
		mgr:    mgr,
		jobs:   make(chan transport.ForwardRequest, mgr.cfg.ReplicationQueueSize),
		stopCh: make(chan struct{}),
	}
	go r.run()
	return r
}

// enqueue blocks until req is queued or the peer is confirmed dead, giving
// bounded backpressure: a full queue blocks the caller only as long as it
// takes the worker to drain it, or to detect the peer is unreachable.
func (r *replicator) enqueue(req transport.ForwardRequest) {
	select {
	case r.jobs <- req:
	case <-r.stopCh:
	}
}

// run applies queued writes to the peer in order, opportunistically batching
// whatever else is already queued into each send, until the peer fails or
// this replicator is stopped.
func (r *replicator) run() {
	for {
		var first transport.ForwardRequest
		select {
		case first = <-r.jobs:
		case <-r.stopCh:
			return
		}
		batch := r.drain(first)

		ctx, cancel := context.WithTimeout(context.Background(), r.mgr.cfg.RoutingTimeout)
		err := r.mgr.sendForwardBatch(ctx, r.nodeID, batch)
		cancel()
		if err != nil {
			r.mgr.markDead(r.nodeID)
			return
		}
	}
}

// drain collects first plus whatever else is immediately available on jobs,
// up to ReplicationBatchSize, without blocking. It grows batch on demand
// rather than preallocating ReplicationBatchSize capacity up front, since
// most drains catch only a handful of jobs, not a full batch's worth.
func (r *replicator) drain(first transport.ForwardRequest) []transport.ForwardRequest {
	batch := []transport.ForwardRequest{first}
	for len(batch) < r.mgr.cfg.ReplicationBatchSize {
		select {
		case req := <-r.jobs:
			batch = append(batch, req)
		default:
			return batch
		}
	}
	return batch
}

func (r *replicator) stop() {
	r.once.Do(func() { close(r.stopCh) })
}
