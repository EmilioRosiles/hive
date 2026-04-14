package hive

import (
	"github.com/EmilioRosiles/hive/internal/cluster"
	"github.com/EmilioRosiles/hive/internal/store"
)

// ErrNotFound is returned by Get operations when the key or field does not
// exist or has expired.
var ErrNotFound = cluster.ErrNotFound

// ErrCapacityExceeded is returned by write operations when the node has reached
// its configured MemLimit. Scale the cluster horizontally to add capacity.
var ErrCapacityExceeded = store.ErrCapacityExceeded
