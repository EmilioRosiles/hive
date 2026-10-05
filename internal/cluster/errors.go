package cluster

import (
	"errors"
	"fmt"
	"strings"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
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

// ErrUnavailable is returned when a key's primary can't be reached within
// RoutingTimeout, or the ring has no owner for it.
var ErrUnavailable = errors.New("hive: node unavailable")

// ErrInternal is returned for unexpected failures that indicate a Hive bug.
var ErrInternal = errors.New("hive: internal error")

// errTypeMismatch is an internal sentinel for operations applied to the wrong
// data structure kind. This indicates a Hive bug, not a caller error.
var errTypeMismatch = fmt.Errorf("%w: type mismatch", ErrInternal)

// errNotASet wraps errTypeMismatch with the expected kind.
var errNotASet = fmt.Errorf("%w: expected set", errTypeMismatch)

// errNotAHash wraps errTypeMismatch with the expected kind.
var errNotAHash = fmt.Errorf("%w: expected hash", errTypeMismatch)

// errNotAList wraps errTypeMismatch with the expected kind.
var errNotAList = fmt.Errorf("%w: expected list", errTypeMismatch)

// errNotAZSet wraps errTypeMismatch with the expected kind.
var errNotAZSet = fmt.Errorf("%w: expected zset", errTypeMismatch)

// errNotABitmap wraps errTypeMismatch with the expected kind.
var errNotABitmap = fmt.Errorf("%w: expected bitmap", errTypeMismatch)

// remoteErrors are the public sentinels a forwarded op can return.
var remoteErrors = []error{ErrNotFound, ErrKeyLocked, ErrLockNotHeld, store.ErrCapacityExceeded}

// remoteErr maps a rejected forward back to the sentinel the remote node
// returned, so errors.Is works across nodes. Unknown rejections become
// ErrInternal; errors that aren't rejections are returned as is.
func remoteErr(err error) error {
	if !errors.Is(err, transport.ErrRejected) {
		return err
	}
	remote := strings.TrimPrefix(err.Error(), transport.ErrRejected.Error()+": ")
	for _, sentinel := range remoteErrors {
		if strings.HasSuffix(remote, sentinel.Error()) {
			return sentinel
		}
	}
	return fmt.Errorf("%w: %s", ErrInternal, strings.TrimPrefix(remote, ErrInternal.Error()+": "))
}
