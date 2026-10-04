package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// replJob is one replication write bound for nodeID.
type replJob struct {
	nodeID string
	req    transport.ForwardRequest
}

// replDone reports that a sender finished a batch for nodeID, dead if the peer
// is gone; the sender then waits on next for its following batch, or an empty one to exit.
type replDone struct {
	nodeID string
	dead   bool
	next   chan transport.ForwardBatch
}

// replicator applies replication writes to every peer through one worker,
// keeping per-peer FIFO order with at most one batch in flight per peer.
type replicator struct {
	mgr     *Cluster
	once    sync.Once
	jobs    chan replJob
	done    chan replDone
	stopCh  chan struct{}
	seq     uint64
	mu      sync.Mutex
	applied map[string]uint64
	enabled bool
}

// newReplicator builds the cluster's replicator, a no-op when ReplicationFactor is 1.
func newReplicator(mgr *Cluster) *replicator {
	r := &replicator{
		mgr:     mgr,
		stopCh:  make(chan struct{}),
		seq:     uint64(time.Now().UnixNano()),
		applied: make(map[string]uint64),
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
// once the peer is Dead.
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
			if d.dead {
				delete(queues, nodeID)
			}
			batch := r.take(queues, nodeID)
			if len(batch.Requests) == 0 {
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

// take removes and returns the next batch from nodeID's queue, or an empty
// batch if there is none. Each batch gets the next replicator-wide Seq.
func (r *replicator) take(queues map[string][]transport.ForwardRequest, nodeID string) transport.ForwardBatch {
	q := queues[nodeID]
	if len(q) == 0 {
		return transport.ForwardBatch{}
	}
	n := min(len(q), r.mgr.cfg.ReplicationBatchSize)
	if n == len(q) {
		delete(queues, nodeID)
	} else {
		queues[nodeID] = q[n:]
	}
	r.seq++
	return transport.ForwardBatch{From: r.mgr.cfg.NodeID, Seq: r.seq, Requests: q[:n]}
}

// send delivers batches to nodeID until run has none left for it.
func (r *replicator) send(nodeID string, batch transport.ForwardBatch) {
	next := make(chan transport.ForwardBatch, 1)
	for len(batch.Requests) > 0 {
		dead := !r.deliver(nodeID, batch)
		select {
		case r.done <- replDone{nodeID: nodeID, dead: dead, next: next}:
		case <-r.stopCh:
			return
		}
		batch = <-next
	}
}

// deliver sends batch to nodeID, retrying every ProbeInterval while the peer
// is Suspect or the send fails. It returns false once the peer is Dead.
func (r *replicator) deliver(nodeID string, batch transport.ForwardBatch) bool {
	for {
		status, ok := r.mgr.peerStatus(nodeID)
		if !ok || status == NodeDead {
			return false
		}
		if status == NodeAlive {
			ctx, cancel := context.WithTimeout(context.Background(), r.mgr.cfg.RoutingTimeout)
			err := r.mgr.sendForwardBatch(ctx, nodeID, batch)
			cancel()
			switch {
			case err == nil:
				return true
			case errors.Is(err, transport.ErrRejected):
				r.mgr.logger.Warn("replicator: replica rejected batch", "node", nodeID, "err", err)
				return true
			}
			r.mgr.markSuspect(nodeID)
		}
		select {
		case <-time.After(r.mgr.cfg.ProbeInterval):
		case <-r.stopCh:
			return false
		}
	}
}

// apply runs a batch received from a peer unless its Seq was already applied.
// Batches apply one at a time, so a duplicate waits for its original to finish.
// Every op runs even if an earlier one fails; the first error is returned.
func (r *replicator) apply(batch transport.ForwardBatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if batch.Seq <= r.applied[batch.From] {
		return nil
	}
	r.applied[batch.From] = batch.Seq
	var firstErr error
	for _, req := range batch.Requests {
		def, ok := opRegistry[req.Op]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("handler: unknown op %d", req.Op)
			}
			continue
		}
		if _, err := def.Exec(r.mgr, req.Key, req.Args, req.LockToken); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// forget drops the dedup state kept for a peer.
func (r *replicator) forget(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.applied, nodeID)
}

func (r *replicator) stop() {
	r.once.Do(func() { close(r.stopCh) })
}
