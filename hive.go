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
//	cluster := node.Cluster()
//	sessions := hive.NewValueStore[Session](cluster, "sessions")
//	users    := hive.NewValueStore[User](cluster, "users")
//
//	sessions.Set(ctx, "abc", mySession)
//	val, err := sessions.Get(ctx, "abc")
package hive

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/EmilioRosiles/hive/internal/cluster"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// Node is a single member of the Hive cluster. Create one per application
// instance via NewNode. It owns the lifecycle of this local process — use
// Cluster() to get a handle to the distributed data plane it participates in.
type Node struct {
	cfg       Config
	cluster   *Cluster
	startedAt time.Time
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
		NodeID:               cfg.NodeID,
		BindAddr:             cfg.BindAddr,
		BindPort:             cfg.BindPort,
		Seeds:                cfg.Seeds,
		ReplicationFactor:    cfg.ReplicationFactor,
		RoutingTimeout:       cfg.RoutingTimeout,
		ConnPoolSize:         cfg.ConnPoolSize,
		MemLimit:             *cfg.MemLimit,
		GossipInterval:       cfg.GossipInterval,
		GossipFanout:         cfg.GossipFanout,
		GossipTimeout:        cfg.GossipTimeout,
		RebalanceDebounce:    cfg.RebalanceDebounce,
		RebalanceBatchSize:   cfg.RebalanceBatchSize,
		ReplicationQueueSize: cfg.ReplicationQueueSize,
		ReplicationBatchSize: cfg.ReplicationBatchSize,
		CleanupInterval:      cfg.CleanupInterval,
		Clustered:            cfg.Mode == ModeCluster,
		TLSConfig:            cfg.TLSConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("hive: start cluster manager: %w", err)
	}

	return &Node{
		cfg:       cfg,
		cluster:   &Cluster{cfg: cfg, internal: mgr},
		startedAt: time.Now(),
	}, nil
}

// Shutdown gracefully stops the node, announcing departure to peers before
// closing all connections and stopping background workers.
func (n *Node) Shutdown() error {
	return n.cluster.internal.Shutdown()
}

// Cluster returns a handle to the distributed data plane this node
// participates in. Pass it to store constructors (NewValueStore, NewSetStore,
// NewHashStore, NewListStore, NewZSetStore) and use it to inspect cluster
// membership.
func (n *Node) Cluster() *Cluster {
	return n.cluster
}

// ID returns this node's unique identifier.
func (n *Node) ID() string {
	return n.cfg.NodeID
}

// Addr returns the address this node listens on for peer communication.
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.cfg.BindPort)
}

// MemUsed returns this node's current estimated local byte usage.
func (n *Node) MemUsed() uint64 {
	return uint64(n.cluster.internal.Used())
}

// MemLimit returns this node's configured memory limit in bytes.
func (n *Node) MemLimit() uint64 {
	return *n.cfg.MemLimit
}

// KeyCount returns the number of live keys held locally by this node.
func (n *Node) KeyCount() int {
	return n.cluster.internal.KeyCount()
}

// Uptime returns how long this node has been running.
func (n *Node) Uptime() time.Duration {
	return time.Since(n.startedAt)
}

// Cluster is a handle to the distributed data plane obtained from a Node.
// It is the entry point for store constructors and cluster-wide membership
// queries. Multiple stores can share the same Cluster.
type Cluster struct {
	cfg      Config
	internal *cluster.Cluster
}

// exec routes an op through the cluster and returns raw result slots.
// Store files call this directly — no intermediate Manager methods.
func (c *Cluster) exec(ctx context.Context, op transport.Op, key string, args ...[]byte) ([][]byte, error) {
	return c.internal.Exec(ctx, op, key, args...)
}

// Member is a point-in-time snapshot of one cluster member as seen by this
// node.
type Member struct {
	NodeID            string
	Addr              string
	Alive             bool
	ReplicationFactor int
	MemLimit          uint64
	MemUsed           uint64
}

// Members returns a snapshot of cluster membership, including this node
// itself. Recently-departed members are included until the cluster janitor
// evicts them — use AliveCount if you only want live members.
func (c *Cluster) Members() []Member {
	peers := c.internal.Peers()
	out := make([]Member, 0, len(peers)+1)

	out = append(out, Member{
		NodeID:            c.cfg.NodeID,
		Addr:              fmt.Sprintf("%s:%d", c.cfg.BindAddr, c.cfg.BindPort),
		Alive:             true,
		ReplicationFactor: c.cfg.ReplicationFactor,
		MemLimit:          *c.cfg.MemLimit,
		MemUsed:           uint64(c.internal.Used()),
	})

	for _, p := range peers {
		out = append(out, Member{
			NodeID:            p.NodeID,
			Addr:              p.Addr,
			Alive:             p.Status == cluster.NodeAlive,
			ReplicationFactor: p.ReplicationFactor,
			MemLimit:          p.MemLimit,
			MemUsed:           p.MemUsed,
		})
	}
	return out
}

// AliveCount returns the number of currently alive members, including this
// node.
func (c *Cluster) AliveCount() int {
	alive := 1 // this node
	for _, p := range c.internal.Peers() {
		if p.Status == cluster.NodeAlive {
			alive++
		}
	}
	return alive
}

func randomID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}
