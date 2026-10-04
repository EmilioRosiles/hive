package hive

import (
	"github.com/EmilioRosiles/hive/internal/cluster"
	"github.com/EmilioRosiles/hive/internal/store"
)

// ErrNotFound is returned by Get operations when the key or field does not
// exist or has expired, and by Lock when the key does not exist.
var ErrNotFound = cluster.ErrNotFound

// ErrCapacityExceeded is returned by write operations when the node has reached
// its configured MemLimit. Scale the cluster horizontally to add capacity.
var ErrCapacityExceeded = store.ErrCapacityExceeded

// ErrKeyLocked is returned by any op against a key that is currently locked,
// unless the caller's context carries the current holder's token (see Lock.Context).
var ErrKeyLocked = cluster.ErrKeyLocked

// ErrLockNotHeld is returned by Lock.Unlock/Lock.Renew when the lock's token
// no longer matches — either it was never held, or it expired and was
// re-acquired by a different holder.
var ErrLockNotHeld = cluster.ErrLockNotHeld

// ErrUnavailable is returned when a key's primary can't be reached within
// RoutingTimeout, e.g. while it is suspected down. Safe to retry.
var ErrUnavailable = cluster.ErrUnavailable

// ErrInternal is returned for unexpected failures that indicate a Hive bug,
// such as an op applied to the wrong data structure kind.
var ErrInternal = cluster.ErrInternal
