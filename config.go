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
	// At the default of 1, replication is a no-op and each peer's
	// replication queue is never allocated, saving memory.
	ReplicationFactor int

	// RoutingTimeout is the most time a request may spend reaching its
	// primary, including waiting out a suspected one, and how long a
	// replication batch may take. Defaults to 1s.
	RoutingTimeout time.Duration

	// RoutingRetryInterval is how long a request waits between attempts while
	// its primary is suspected or after a retryable failure. Defaults to 50ms.
	RoutingRetryInterval time.Duration

	// ConnPoolSize is the number of pooled connections maintained per peer,
	// round-robin shared across all traffic to that peer. Defaults to 4.
	ConnPoolSize int

	// MemLimit is the maximum memory this node intends to use.
	// It is used to compute the node's virtual node count on the hash ring:
	// nodes with more memory receive proportionally more keyspace.
	// nil (the zero value) means "use total system memory" (default).
	// Use Bytes(0) for a node that owns no keyspace at all — a pure
	// routing/relay worker that never stores or replicates data itself.
	// Such a node also skips rebalancer bookkeeping entirely, since it can
	// never be a migration source or target.
	MemLimit MemLimit

	// GossipInterval is how often this node sends heartbeats to peers.
	// Defaults to 1s.
	GossipInterval time.Duration

	// GossipFanout is how many peers receive each heartbeat round.
	// Defaults to 3.
	GossipFanout int

	// GossipTimeout is how long this nodes waits before cancelling a heartbeat to a peer.
	// Defaults to 300ms.
	GossipTimeout time.Duration

	// ProbeTimeout is how long a direct ping to a suspected peer may take;
	// pings relayed through a helper get twice as long. Defaults to 300ms.
	ProbeTimeout time.Duration

	// ProbeHelpers is how many alive peers are asked to ping a suspected peer
	// when a direct ping fails. Defaults to 3.
	ProbeHelpers int

	// ProbeInterval is how long a probe waits before retrying when no helper
	// answered. Defaults to 1s.
	ProbeInterval time.Duration

	// RebalanceDebounce is the delay after a topology change before
	// rebalancing starts, to let the cluster stabilize.
	// Defaults to 500ms.
	RebalanceDebounce time.Duration

	// RebalanceBatchSize is the max number of migrated keys sent per rebalance
	// frame. Defaults to 128.
	RebalanceBatchSize int

	// RebalanceTimeout is how long one rebalance frame may take before the
	// migration is retried on the next rebalance run. Defaults to 10s.
	RebalanceTimeout time.Duration

	// ReplicationQueueSize is the max number of queued-but-unsent replication
	// writes held per peer; further writes for that peer are dropped until it
	// catches up. Defaults to 4096.
	ReplicationQueueSize int

	// ReplicationBatchSize is the max number of queued replication writes sent
	// to a peer in one batch. Defaults to 256.
	ReplicationBatchSize int

	// CleanupInterval is how often the cluster janitor runs to evict dead peer
	// tombstones and expired store entries.
	// Default: 30s
	CleanupInterval time.Duration

	// DeadRetention is how long a dead peer is remembered before the janitor
	// forgets it, so stale gossip can't bring it back. Defaults to 10× GossipInterval.
	DeadRetention time.Duration

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

// Byte-size units for use with Bytes, e.g. hive.Bytes(4 * hive.GB).
const (
	KB uint64 = 1 << 10
	MB uint64 = 1 << 20
	GB uint64 = 1 << 30
)

// MemLimit is the maximum memory a node intends to use, in bytes.
// The zero value (nil) means "use total system memory" (default).
// Use Bytes(0) for a node that owns no keyspace at all — a pure
// routing/relay worker that never stores or replicates data itself.
type MemLimit *uint64

// Bytes returns a MemLimit of exactly n bytes. Bytes(0) means genuinely
// zero — no keyspace ownership — not "use the default."
// Combine with KB/MB/GB, e.g. Bytes(512 * MB).
func Bytes(n uint64) MemLimit { return &n }

func defaultConfig() Config {
	return Config{
		Mode:                 ModeStandalone,
		BindAddr:             "0.0.0.0",
		BindPort:             7946,
		RoutingTimeout:       1 * time.Second,
		RoutingRetryInterval: 50 * time.Millisecond,
		ConnPoolSize:         4,
		ReplicationFactor:    1,
		MemLimit:             Bytes(sys.TotalMemory()),
		GossipInterval:       5 * time.Second,
		GossipFanout:         3,
		GossipTimeout:        300 * time.Millisecond,
		ProbeTimeout:         300 * time.Millisecond,
		ProbeHelpers:         3,
		ProbeInterval:        1 * time.Second,
		RebalanceDebounce:    500 * time.Millisecond,
		RebalanceBatchSize:   128,
		RebalanceTimeout:     10 * time.Second,
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
	if c.RoutingRetryInterval == 0 {
		c.RoutingRetryInterval = d.RoutingRetryInterval
	}
	if c.ConnPoolSize == 0 {
		c.ConnPoolSize = d.ConnPoolSize
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
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = d.ProbeTimeout
	}
	if c.ProbeHelpers == 0 {
		c.ProbeHelpers = d.ProbeHelpers
	}
	if c.ProbeInterval == 0 {
		c.ProbeInterval = d.ProbeInterval
	}
	if c.RebalanceDebounce == 0 {
		c.RebalanceDebounce = d.RebalanceDebounce
	}
	if c.RebalanceBatchSize == 0 {
		c.RebalanceBatchSize = d.RebalanceBatchSize
	}
	if c.RebalanceTimeout == 0 {
		c.RebalanceTimeout = d.RebalanceTimeout
	}
	if c.ReplicationQueueSize == 0 {
		c.ReplicationQueueSize = d.ReplicationQueueSize
	}
	if c.ReplicationBatchSize == 0 {
		c.ReplicationBatchSize = d.ReplicationBatchSize
	}
	if c.MemLimit == nil {
		c.MemLimit = d.MemLimit
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = d.CleanupInterval
	}
	if c.DeadRetention == 0 {
		c.DeadRetention = 10 * c.GossipInterval
	}
}
