package hive

import "testing"

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
