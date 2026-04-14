package cluster

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned when a requested key or field does not exist or has expired.
var ErrNotFound = errors.New("hive: not found")

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
