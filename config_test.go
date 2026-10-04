package hive

import (
	"testing"
	"time"
)

func TestConfig_MemLimit_UnsetDefaultsToSystemMemory(t *testing.T) {
	var cfg Config
	cfg.applyDefaults()
	if cfg.MemLimit == nil {
		t.Fatal("MemLimit should default to non-nil (system memory)")
	}
	if *cfg.MemLimit == 0 {
		t.Error("default MemLimit should be system memory, not zero")
	}
}

func TestConfig_MemLimit_ExplicitZero_NotOverwritten(t *testing.T) {
	cfg := Config{MemLimit: Bytes(0)}
	cfg.applyDefaults()
	if cfg.MemLimit == nil {
		t.Fatal("explicit Bytes(0) should not become nil")
	}
	if *cfg.MemLimit != 0 {
		t.Errorf("explicit Bytes(0) should survive applyDefaults unchanged, got %d", *cfg.MemLimit)
	}
}

func TestConfig_MemLimit_ExplicitValue_RoundTrips(t *testing.T) {
	cfg := Config{MemLimit: Bytes(512 * MB)}
	cfg.applyDefaults()
	if cfg.MemLimit == nil || *cfg.MemLimit != 512*MB {
		t.Errorf("got %v, want %d", cfg.MemLimit, 512*MB)
	}
}

func TestByteUnits(t *testing.T) {
	if KB != 1024 {
		t.Errorf("KB: got %d, want 1024", KB)
	}
	if MB != 1024*1024 {
		t.Errorf("MB: got %d, want %d", MB, 1024*1024)
	}
	if GB != 1024*1024*1024 {
		t.Errorf("GB: got %d, want %d", GB, 1024*1024*1024)
	}
	if got := *Bytes(4 * GB); got != 4*1024*1024*1024 {
		t.Errorf("Bytes(4*GB): got %d, want %d", got, 4*1024*1024*1024)
	}
}

func TestConfig_ProbeDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.ProbeTimeout != 300*time.Millisecond || c.ProbeHelpers != 3 || c.ProbeInterval != time.Second {
		t.Errorf("got ProbeTimeout=%v ProbeHelpers=%d ProbeInterval=%v, want 300ms, 3, 1s", c.ProbeTimeout, c.ProbeHelpers, c.ProbeInterval)
	}

	c = Config{ProbeTimeout: time.Second, ProbeHelpers: 5, ProbeInterval: time.Minute}
	c.applyDefaults()
	if c.ProbeTimeout != time.Second || c.ProbeHelpers != 5 || c.ProbeInterval != time.Minute {
		t.Errorf("explicit values overwritten: got %v, %d, %v", c.ProbeTimeout, c.ProbeHelpers, c.ProbeInterval)
	}
}

func TestConfig_RebalanceTimeoutDefault(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.RebalanceTimeout != 10*time.Second {
		t.Errorf("got %v, want 10s", c.RebalanceTimeout)
	}
	c = Config{RebalanceTimeout: time.Minute}
	c.applyDefaults()
	if c.RebalanceTimeout != time.Minute {
		t.Errorf("explicit value overwritten: got %v", c.RebalanceTimeout)
	}
}

func TestConfig_DeadRetentionDefault(t *testing.T) {
	c := Config{GossipInterval: 200 * time.Millisecond}
	c.applyDefaults()
	if c.DeadRetention != 2*time.Second {
		t.Errorf("got %v, want 10x GossipInterval (2s)", c.DeadRetention)
	}
	c = Config{DeadRetention: time.Minute}
	c.applyDefaults()
	if c.DeadRetention != time.Minute {
		t.Errorf("explicit value overwritten: got %v", c.DeadRetention)
	}
}

func TestConfig_RoutingRetryIntervalDefault(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.RoutingRetryInterval != 50*time.Millisecond {
		t.Errorf("got %v, want 50ms", c.RoutingRetryInterval)
	}
	c = Config{RoutingRetryInterval: time.Second}
	c.applyDefaults()
	if c.RoutingRetryInterval != time.Second {
		t.Errorf("explicit value overwritten: got %v", c.RoutingRetryInterval)
	}
}

func TestConfig_AdvertiseAddrDefault(t *testing.T) {
	c := Config{BindAddr: "10.0.0.5", BindPort: 9000}
	c.applyDefaults()
	if c.AdvertiseAddr != "10.0.0.5:9000" {
		t.Errorf("got %q, want BindAddr:BindPort", c.AdvertiseAddr)
	}
	c = Config{AdvertiseAddr: "node.example:7946"}
	c.applyDefaults()
	if c.AdvertiseAddr != "node.example:7946" {
		t.Errorf("explicit value overwritten: got %q", c.AdvertiseAddr)
	}
}
