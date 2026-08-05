package hive

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/EmilioRosiles/hive/internal/sys"
)

// Mode controls how the node participates in the cluster.
type Mode int

const (
	// ModeStandalone runs as a single local node with no clustering.
	// Useful for development or single-instance deployments.
	ModeStandalone Mode = iota

	// ModeCluster runs as a peer node that discovers and joins other nodes.
	ModeCluster
)

// Config holds all configuration for a Hive node.
type Config struct {
	// NodeID is a unique identifier for this node.
	// Defaults to a generated UUID if empty.
	NodeID string

	// Mode controls standalone vs cluster operation.
	// Defaults to ModeStandalone.
	Mode Mode

	// BindAddr is the address this node listens on for peer communication.
	// Defaults to "0.0.0.0".
	BindAddr string

	// BindPort is the port this node listens on for peer communication.
	// Defaults to 7946.
	BindPort int

	// Seeds is a list of peer addresses (host:port) used to bootstrap
	// cluster membership. At least one reachable seed is required when
	// Mode is ModeCluster.
	Seeds []string

	// ReplicationFactor is the number of nodes that should hold a copy
	// of each key. Must be <= cluster size. Defaults to 1.
	ReplicationFactor int

	// RoutingTimeout is how long this nodes waits before cancelling a
	// routed op (redirect/replicate).
	RoutingTimeout time.Duration

	// MemLimit is the maximum memory this node intends to use, in bytes.
	// It is used to compute the node's virtual node count on the hash ring:
	// nodes with more memory receive proportionally more keyspace.
	// A value of 0 uses the default of 100 virtual nodes.
	MemLimit uint64

	// GossipInterval is how often this node sends heartbeats to peers.
	// Defaults to 1s.
	GossipInterval time.Duration

	// GossipFanout is how many peers receive each heartbeat round.
	// Defaults to 3.
	GossipFanout int

	// GossipTimeout is how long this nodes waits before cancelling a heartbeat to a peer.
	// Defaults to 300ms.
	GossipTimeout time.Duration

	// RebalanceDebounce is the delay after a topology change before
	// rebalancing starts, to let the cluster stabilize.
	// Defaults to 500ms.
	RebalanceDebounce time.Duration

	// RebalanceBatchSize is the max number of migrated keys sent per rebalance
	// frame. Defaults to 128.
	RebalanceBatchSize int

	// ReplicationQueueSize is the max number of queued-but-unsent replication
	// writes held per peer before enqueuing blocks (backpressure).
	// Defaults to 4096.
	ReplicationQueueSize int

	// ReplicationBatchSize is the max number of queued replication writes sent
	// to a peer in one batch. Defaults to 256.
	ReplicationBatchSize int

	// CleanupInterval is how often the cluster janitor runs to evict dead peer
	// tombstones and expired store entries.
	// Default: 30s
	CleanupInterval time.Duration

	// LogLevel controls the verbosity of internal log output.
	// nil defaults to slog.LevelError (quiet). Set explicitly to enable
	// more verbose output, e.g. &slog.LevelInfo or &slog.LevelDebug.
	LogLevel *slog.Level

	// TLSConfig enables TLS for cluster-mode peer connections when non-nil.
	// nil disables TLS (default, plaintext). Build one with
	// NewClusterTLSConfig for mutual TLS with chain-only peer verification,
	// or construct your own *tls.Config for full control.
	TLSConfig *tls.Config
}

func defaultConfig() Config {
	return Config{
		Mode:                 ModeStandalone,
		BindAddr:             "0.0.0.0",
		BindPort:             7946,
		RoutingTimeout:       1 * time.Second,
		ReplicationFactor:    1,
		MemLimit:             sys.TotalMemory(),
		GossipInterval:       5 * time.Second,
		GossipFanout:         3,
		GossipTimeout:        300 * time.Millisecond,
		RebalanceDebounce:    500 * time.Millisecond,
		RebalanceBatchSize:   128,
		ReplicationQueueSize: 4096,
		ReplicationBatchSize: 256,
		CleanupInterval:      30 * time.Second,
	}
}

func (c *Config) applyDefaults() {
	d := defaultConfig()
	if c.Mode == 0 {
		c.Mode = d.Mode
	}
	if c.BindAddr == "" {
		c.BindAddr = d.BindAddr
	}
	if c.BindPort == 0 {
		c.BindPort = d.BindPort
	}
	if c.ReplicationFactor == 0 {
		c.ReplicationFactor = d.ReplicationFactor
	}
	if c.RoutingTimeout == 0 {
		c.RoutingTimeout = d.RoutingTimeout
	}
	if c.GossipInterval == 0 {
		c.GossipInterval = d.GossipInterval
	}
	if c.GossipFanout == 0 {
		c.GossipFanout = d.GossipFanout
	}
	if c.GossipTimeout == 0 {
		c.GossipTimeout = d.GossipTimeout
	}
	if c.RebalanceDebounce == 0 {
		c.RebalanceDebounce = d.RebalanceDebounce
	}
	if c.RebalanceBatchSize == 0 {
		c.RebalanceBatchSize = d.RebalanceBatchSize
	}
	if c.ReplicationQueueSize == 0 {
		c.ReplicationQueueSize = d.ReplicationQueueSize
	}
	if c.ReplicationBatchSize == 0 {
		c.ReplicationBatchSize = d.ReplicationBatchSize
	}
	if c.MemLimit == 0 {
		c.MemLimit = d.MemLimit
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = d.CleanupInterval
	}
}
