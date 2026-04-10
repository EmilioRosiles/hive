// Package hive provides an embeddable, leaderless distributed cache for Go
// applications. Any Go application can include a Hive node to participate in
// a self-organizing cluster that shares in-memory state across instances
// without external infrastructure.
//
// Basic usage:
//
//	node, err := hive.NewNode(hive.Config{
//	    Mode:  hive.ModeCluster,
//	    Seeds: []string{"node1:7946", "node2:7946"},
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer node.Shutdown()
//
//	cache := node.Cache()
//	sessions := hive.NewValueStore[Session](cache, "sessions")
//	users    := hive.NewValueStore[User](cache, "users")
//
//	sessions.Set("abc", mySession)
//	val, err := sessions.Get("abc")
package hive

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/EmilioRosiles/hive/internal/cluster"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// Node is a single member of the Hive cluster. Create one per application
// instance via NewNode. Multiple stores can share a single Node.
type Node struct {
	cfg     Config
	cluster *cluster.Cluster
}

// NewNode creates and starts a Hive node with the given configuration.
// In ModeCluster the node will attempt to contact Seeds and join the cluster.
// In ModeStandalone the node operates as a local in-memory cache only.
func NewNode(cfg Config) (*Node, error) {
	cfg.applyDefaults()
	logLevel := slog.LevelError
	if cfg.LogLevel != nil {
		logLevel = *cfg.LogLevel
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	if cfg.NodeID == "" {
		id, err := randomID()
		if err != nil {
			return nil, fmt.Errorf("hive: generate node id: %w", err)
		}
		cfg.NodeID = id
	}

	mgr, err := cluster.NewCluster(cluster.Config{
		NodeID:            cfg.NodeID,
		BindAddr:          cfg.BindAddr,
		BindPort:          cfg.BindPort,
		Seeds:             cfg.Seeds,
		ReplicationFactor: cfg.ReplicationFactor,
		MemLimit:          cfg.MemLimit,
		GossipInterval:    cfg.GossipInterval,
		GossipFanout:      cfg.GossipFanout,
		RebalanceDebounce: cfg.RebalanceDebounce,
		DeadTimeout:       cfg.DeadTimeout,
		Clustered:         cfg.Mode == ModeCluster,
	})
	if err != nil {
		return nil, fmt.Errorf("hive: start cluster manager: %w", err)
	}

	return &Node{cfg: cfg, cluster: mgr}, nil
}

// Cache returns a handle to the cluster's data layer.
// Pass it to store constructors (NewValueStore, NewSetStore, NewHashStore).
// Multiple stores can share the same Cache.
func (n *Node) Cache() *Cache {
	return &Cache{cluster: n.cluster}
}

// Shutdown gracefully stops the node, announcing departure to peers before
// closing all connections and stopping background workers.
func (n *Node) Shutdown() error {
	return n.cluster.Shutdown()
}

// Status returns a snapshot of the current cluster state.
func (n *Node) Status() ClusterStatus {
	peers := n.cluster.Peers()
	return ClusterStatus{
		Config: n.cfg,
		Peers:  peers,
		Size:   len(peers) + 1,
	}
}

// ClusterStatus is a point-in-time snapshot of cluster membership.
type ClusterStatus struct {
	Config
	Peers []cluster.PeerInfo
	Size  int
}

// Cache is a handle to the cluster's data layer obtained from a Node.
// It is the entry point for store constructors and cluster-wide operations.
type Cache struct {
	cluster *cluster.Cluster
}

// exec routes an op through the cluster and returns raw result slots.
// Store files call this directly — no intermediate Manager methods.
func (c *Cache) exec(op transport.Op, key string, args ...[]byte) ([][]byte, error) {
	return c.cluster.Exec(op, key, args...)
}

// encodeTTL encodes a TTL as a big-endian uint64 nanosecond byte slice.
// Returns nil for zero (no expiry) — the exec layer treats an absent slot as 0.
func encodeTTL(ttl time.Duration) []byte {
	if ttl == 0 {
		return nil
	}
	return binary.BigEndian.AppendUint64(nil, uint64(ttl.Nanoseconds()))
}

func randomID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}
