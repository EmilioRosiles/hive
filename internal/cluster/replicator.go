package cluster

import (
	"context"
	"sync"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// replJob is one replication write bound for nodeID.
type replJob struct {
	nodeID string
	req    transport.ForwardRequest
}

// replDone reports the outcome of one batch sent to nodeID; the sender then
// waits on next for its following batch, or nil to exit.
type replDone struct {
	nodeID string
	err    error
	next   chan []transport.ForwardRequest
}

// replicator applies replication writes to every peer through one worker,
// keeping per-peer FIFO order with at most one batch in flight per peer.
type replicator struct {
	mgr     *Cluster
	once    sync.Once
	jobs    chan replJob
	done    chan replDone
	stopCh  chan struct{}
	enabled bool
}

// newReplicator builds the cluster's replicator, a no-op when ReplicationFactor is 1.
func newReplicator(mgr *Cluster) *replicator {
	r := &replicator{
		mgr:     mgr,
		stopCh:  make(chan struct{}),
		enabled: mgr.cfg.ReplicationFactor > 1,
	}
	if r.enabled {
		r.jobs = make(chan replJob, mgr.cfg.ReplicationQueueSize)
		r.done = make(chan replDone)
		go r.run()
	}
	return r
}

// enqueue blocks until req is queued for nodeID or the replicator is stopped.
func (r *replicator) enqueue(nodeID string, req transport.ForwardRequest) {
	if !r.enabled {
		return
	}
	select {
	case r.jobs <- replJob{nodeID: nodeID, req: req}:
	case <-r.stopCh:
	}
}

// run appends jobs to per-peer queues and keeps one sender per busy peer.
// It stops taking jobs while a peer's queue is full, and drops a peer's queue
// and marks it dead when a send fails.
func (r *replicator) run() {
	queues := make(map[string][]transport.ForwardRequest)
	inflight := make(map[string]bool)
	full := ""
	for {
		jobs := r.jobs
		if full != "" {
			jobs = nil
		}
		var nodeID string
		select {
		case job := <-jobs:
			nodeID = job.nodeID
			queues[nodeID] = append(queues[nodeID], job.req)
			if !inflight[nodeID] {
				inflight[nodeID] = true
				go r.send(nodeID, r.take(queues, nodeID))
			}
		case d := <-r.done:
			nodeID = d.nodeID
			if d.err != nil {
				r.mgr.markDead(nodeID)
				delete(queues, nodeID)
			}
			batch := r.take(queues, nodeID)
			if batch == nil {
				delete(inflight, nodeID)
			}
			d.next <- batch
		case <-r.stopCh:
			return
		}
		if len(queues[nodeID]) >= r.mgr.cfg.ReplicationQueueSize {
			full = nodeID
		} else if full == nodeID {
			full = ""
		}
	}
}

// take removes and returns the next batch from nodeID's queue, or nil if it is empty.
func (r *replicator) take(queues map[string][]transport.ForwardRequest, nodeID string) []transport.ForwardRequest {
	q := queues[nodeID]
	if len(q) == 0 {
		return nil
	}
	n := min(len(q), r.mgr.cfg.ReplicationBatchSize)
	if n == len(q) {
		delete(queues, nodeID)
	} else {
		queues[nodeID] = q[n:]
	}
	return q[:n]
}

// send delivers batches to nodeID until run has none left for it.
func (r *replicator) send(nodeID string, batch []transport.ForwardRequest) {
	next := make(chan []transport.ForwardRequest, 1)
	for batch != nil {
		ctx, cancel := context.WithTimeout(context.Background(), r.mgr.cfg.RoutingTimeout)
		err := r.mgr.sendForwardBatch(ctx, nodeID, batch)
		cancel()
		select {
		case r.done <- replDone{nodeID: nodeID, err: err, next: next}:
		case <-r.stopCh:
			return
		}
		batch = <-next
	}
}

func (r *replicator) stop() {
	r.once.Do(func() { close(r.stopCh) })
}
