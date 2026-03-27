package hive

import "time"

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

	// VNodes is the number of virtual nodes per physical node on the
	// hash ring. Higher values improve key distribution.
	// Defaults to 100.
	VNodes int

	// GossipInterval is how often this node sends heartbeats to peers.
	// Defaults to 1s.
	GossipInterval time.Duration

	// GossipFanout is how many peers receive each heartbeat round.
	// Defaults to 3.
	GossipFanout int

	// RebalanceDebounce is the delay after a topology change before
	// rebalancing starts, to let the cluster stabilize.
	// Defaults to 500ms.
	RebalanceDebounce time.Duration

	// DeadTimeout is how long a peer can miss heartbeats before being
	// marked dead and removed from the ring.
	// Defaults to 10s.
	DeadTimeout time.Duration
}

func defaultConfig() Config {
	return Config{
		Mode:              ModeStandalone,
		BindAddr:          "0.0.0.0",
		BindPort:          7946,
		ReplicationFactor: 1,
		VNodes:            100,
		GossipInterval:    3 * time.Second,
		GossipFanout:      3,
		RebalanceDebounce: 500 * time.Millisecond,
		DeadTimeout:       10 * time.Second,
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
	if c.VNodes == 0 {
		c.VNodes = d.VNodes
	}
	if c.GossipInterval == 0 {
		c.GossipInterval = d.GossipInterval
	}
	if c.GossipFanout == 0 {
		c.GossipFanout = d.GossipFanout
	}
	if c.RebalanceDebounce == 0 {
		c.RebalanceDebounce = d.RebalanceDebounce
	}
	if c.DeadTimeout == 0 {
		c.DeadTimeout = d.DeadTimeout
	}
}
