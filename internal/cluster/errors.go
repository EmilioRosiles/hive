package cluster

import (
	"errors"
	"fmt"

	"github.com/EmilioRosiles/hive/internal/store"
)

// ErrNotFound is returned when a requested key or field does not exist or has expired.
var ErrNotFound = errors.New("hive: not found")

// ErrKeyLocked is returned by any op against a key that is currently locked,
// unless the caller's context carries the current holder's token.
var ErrKeyLocked = store.ErrKeyLocked

// ErrLockNotHeld is returned by Unlock/Renew when the provided token does not
// match the key's current lock — either the lock was never held, or it
// expired and was re-acquired by a different holder.
var ErrLockNotHeld = errors.New("hive: lock not held")

// errTypeMismatch is an internal sentinel for operations applied to the wrong
// data structure kind. This indicates a Hive bug, not a caller error.
var errTypeMismatch = errors.New("hive: type mismatch")

// errNotASet wraps errTypeMismatch with the expected kind.
var errNotASet = fmt.Errorf("%w: expected set", errTypeMismatch)

// errNotAHash wraps errTypeMismatch with the expected kind.
var errNotAHash = fmt.Errorf("%w: expected hash", errTypeMismatch)

// errNotAList wraps errTypeMismatch with the expected kind.
var errNotAList = fmt.Errorf("%w: expected list", errTypeMismatch)

// errNotAZSet wraps errTypeMismatch with the expected kind.
var errNotAZSet = fmt.Errorf("%w: expected zset", errTypeMismatch)
