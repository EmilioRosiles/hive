package hive

import "github.com/EmilioRosiles/hive/internal/cluster"

// ErrNotFound is returned by Get operations when the key or field does not
// exist or has expired.
var ErrNotFound = cluster.ErrNotFound
