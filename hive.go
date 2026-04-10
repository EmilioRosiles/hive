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
//	sessions := hive.NewValueStore[Session](node, "sessions")
//	users    := hive.NewValueStore[User](node, "users")
//
//	sessions.Set("abc", mySession)
//	val, err := sessions.Get("abc")
package hive

import (
	"crypto/rand"
	"fmt"

	"github.com/EmilioRosiles/hive/internal/cluster"
)

// Node is a single member of the Hive cluster. Create one per application
// instance via NewNode. Multiple stores can share a single Node.
type Node struct {
	cfg     Config
	cluster *cluster.Manager
}

// NewNode creates and starts a Hive node with the given configuration.
// In ModeCluster the node will attempt to contact Seeds and join the cluster.
// In ModeStandalone the node operates as a local in-memory cache only.
func NewNode(cfg Config) (*Node, error) {
	cfg.applyDefaults()

	if cfg.NodeID == "" {
		id, err := randomID()
		if err != nil {
			return nil, fmt.Errorf("hive: generate node id: %w", err)
		}
		cfg.NodeID = id
	}

	mgr, err := cluster.NewManager(cluster.Config{
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
	cluster *cluster.Manager
}

func randomID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}
