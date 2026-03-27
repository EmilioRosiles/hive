package hive

import "errors"

// ErrNotFound is returned when a key or field does not exist in the store.
var ErrNotFound = errors.New("hive: not found")

// ErrNodeUnavailable is returned when the responsible node cannot be reached.
var ErrNodeUnavailable = errors.New("hive: node unavailable")
