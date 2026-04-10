package cluster

import (
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/EmilioRosiles/hive/internal/ring"
	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// handleRebalance receives a batch of keys migrated from another node and
// applies them to the local store, adjusting TTLs for transit time.
func (m *Cluster) handleRebalance(payload []byte) ([]byte, error) {
	var batch transport.RebalanceBatch
	if err := transport.Decode(payload, &batch); err != nil {
		return nil, fmt.Errorf("handler: decode rebalance: %w", err)
	}
	received := time.Now()
	for _, re := range batch.Entries {
		entry, err := m.store.DecodeEntry(store.Kind(re.Kind), re.Data)
		if err != nil {
			slog.Warn("rebalance: decode entry failed", "key", re.Key, "err", err)
			continue
		}
		if re.TTL > 0 {
			remaining := time.Duration(re.TTL) - time.Since(received)
			if remaining <= 0 {
				continue // expired in transit
			}
			entry.SetKeyExpiry(received.Add(remaining).Unix())
		}
		m.store.Set(re.Key, entry)
	}
	return nil, nil
}

type rebalancer struct {
	mu       sync.Mutex
	timer    *time.Timer
	debounce time.Duration
	lastRing *ring.Ring
	mgr      *Cluster
}

func newRebalancer(debounce time.Duration, mgr *Cluster) *rebalancer {
	return &rebalancer{debounce: debounce, mgr: mgr}
}

// schedule debounces rebalance runs so rapid membership changes don't cause cascading migrations.
func (rm *rebalancer) schedule() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.timer != nil {
		rm.timer.Stop()
	}
	rm.timer = time.AfterFunc(rm.debounce, rm.run)
}

func (rm *rebalancer) run() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	m := rm.mgr
	oldRing := rm.lastRing
	newRing := m.ring

	if newRing.GetVersion() == oldRing.GetVersion() {
		return
	}

	m.mu.Lock()
	rm.lastRing = newRing.Copy()
	m.mu.Unlock()

	slog.Debug("rebalance: started")

	batchesByNode := make(map[string][]transport.RebalanceEntry)
	var deleteList []string

	m.store.Scan(-1, 0, func(key string, entry store.DataStructure) {
		oldOwners := oldRing.Get(key)
		newOwners := newRing.Get(key)

		targets := migrationTargets(oldOwners, newOwners)
		if len(targets) == 0 {
			return
		}

		leader := migrationLeader(oldOwners, newOwners, m)
		if leader == m.cfg.NodeID {
			ttl := int64(0)
			if exp := entry.KeyExpiry(); exp != 0 {
				ttl = time.Until(time.Unix(exp, 0)).Nanoseconds()
				if ttl <= 0 {
					return // already expired
				}
			}

			data, err := entry.Encode()
			if err != nil {
				slog.Warn("rebalance: encode failed", "key", key, "err", err)
				return
			}

			re := transport.RebalanceEntry{Key: key, Kind: uint8(entry.Kind()), Data: data, TTL: ttl}
			for _, nodeID := range targets {
				batchesByNode[nodeID] = append(batchesByNode[nodeID], re)
			}
		}

		if !slices.Contains(newOwners, m.cfg.NodeID) {
			deleteList = append(deleteList, key)
		}
	})

	for nodeID, entries := range batchesByNode {
		m.sendRebalanceBatch(nodeID, entries)
	}

	for _, key := range deleteList {
		m.store.Del(key)
	}

	slog.Debug("rebalance: finished")
}

func (m *Cluster) sendRebalanceBatch(nodeID string, entries []transport.RebalanceEntry) {
	client, ok := m.getClient(nodeID)
	if !ok {
		slog.Warn("rebalance: no client", "node", nodeID)
		return
	}

	const batchSize = 100
	for i := 0; i < len(entries); i += batchSize {
		end := min(i+batchSize, len(entries))
		batch := transport.RebalanceBatch{Entries: entries[i:end]}
		payload, err := transport.Encode(batch)
		if err != nil {
			slog.Warn("rebalance: encode batch failed", "node", nodeID, "err", err)
			continue
		}
		if _, err := client.Send(transport.Frame{Type: transport.MsgRebalance, Payload: payload}); err != nil {
			slog.Warn("rebalance: send failed", "node", nodeID, "err", err)
			m.markDead(nodeID)
			return
		}
	}
	slog.Info("rebalance: migration complete", "keys", len(entries), "node", nodeID)
}

// migrationTargets returns node IDs that are in newOwners but not in oldOwners.
func migrationTargets(oldOwners, newOwners []string) []string {
	old := make(map[string]struct{}, len(oldOwners))
	for _, id := range oldOwners {
		old[id] = struct{}{}
	}
	var diff []string
	for _, id := range newOwners {
		if _, exists := old[id]; !exists {
			diff = append(diff, id)
		}
	}
	return diff
}

// migrationLeader elects which node is responsible for pushing data to new owners.
// Prefers the original primary, falling back through old replicas.
func migrationLeader(oldOwners, newOwners []string, m *Cluster) string {
	for _, id := range oldOwners {
		if id == m.cfg.NodeID {
			return id
		}
		if p, ok := m.getPeer(id); ok && p.Alive {
			return id
		}
	}
	return newOwners[0]
}
