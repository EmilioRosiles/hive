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

type rebalanceManager struct {
	mu       sync.Mutex
	timer    *time.Timer
	debounce time.Duration
	lastRing *ring.Ring
	mgr      *Manager
}

func newRebalanceManager(debounce time.Duration, mgr *Manager) *rebalanceManager {
	return &rebalanceManager{debounce: debounce, mgr: mgr}
}

// schedule debounces rebalance runs so rapid membership changes don't cause cascading migrations.
func (rm *rebalanceManager) schedule() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.timer != nil {
		rm.timer.Stop()
	}
	rm.timer = time.AfterFunc(rm.debounce, rm.run)
}

func (rm *rebalanceManager) run() {
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
				slog.Warn(fmt.Sprintf("rebalance: encode %q: %v", key, err))
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

func (m *Manager) sendRebalanceBatch(nodeID string, entries []transport.RebalanceEntry) {
	client, ok := m.getClient(nodeID)
	if !ok {
		slog.Warn(fmt.Sprintf("rebalance: no client for %s", nodeID))
		return
	}

	const batchSize = 100
	for i := 0; i < len(entries); i += batchSize {
		end := min(i+batchSize, len(entries))
		batch := transport.RebalanceBatch{Entries: entries[i:end]}
		payload, err := transport.Encode(batch)
		if err != nil {
			slog.Warn(fmt.Sprintf("rebalance: encode batch for %s: %v", nodeID, err))
			continue
		}
		if _, err := client.Send(transport.Frame{Type: transport.MsgRebalance, Payload: payload}); err != nil {
			slog.Warn(fmt.Sprintf("rebalance: send to %s failed: %v", nodeID, err))
			m.markDead(nodeID)
			return
		}
	}
	slog.Info(fmt.Sprintf("rebalance: migrated %d keys to %s", len(entries), nodeID))
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
func migrationLeader(oldOwners, newOwners []string, m *Manager) string {
	for _, id := range oldOwners {
		if id == m.cfg.NodeID {
			return id
		}
		if p, ok := m.getPeer(id); ok && p.alive {
			return id
		}
	}
	return newOwners[0]
}
