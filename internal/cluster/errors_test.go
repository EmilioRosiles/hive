package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/EmilioRosiles/hive/internal/store"
	"github.com/EmilioRosiles/hive/internal/transport"
)

func TestRemoteErr(t *testing.T) {
	rejected := func(remote error) error { return fmt.Errorf("%w: %s", transport.ErrRejected, remote) }

	tests := []struct {
		name string
		err  error
		want error
		msg  string
	}{
		{"not found", rejected(ErrNotFound), ErrNotFound, "hive: not found"},
		{"key locked", rejected(ErrKeyLocked), ErrKeyLocked, "hive: key is locked"},
		{"lock not held", rejected(ErrLockNotHeld), ErrLockNotHeld, "hive: lock not held"},
		{"capacity", rejected(store.ErrCapacityExceeded), store.ErrCapacityExceeded, "hive: node memory limit reached"},
		{"type mismatch", rejected(errNotASet), ErrInternal, "hive: internal error: type mismatch: expected set"},
		{"unknown rejection", rejected(errors.New("handler: unknown op 99")), ErrInternal, "hive: internal error: handler: unknown op 99"},
		{"not a rejection", context.DeadlineExceeded, context.DeadlineExceeded, "context deadline exceeded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := remoteErr(tt.err)
			if !errors.Is(got, tt.want) {
				t.Errorf("got %v, want an error matching %v", got, tt.want)
			}
			if got.Error() != tt.msg {
				t.Errorf("got message %q, want %q", got.Error(), tt.msg)
			}
		})
	}
}

func TestTypeMismatch_WrapsErrInternal(t *testing.T) {
	for _, err := range []error{errTypeMismatch, errNotASet, errNotAHash, errNotAList, errNotAZSet} {
		if !errors.Is(err, ErrInternal) {
			t.Errorf("%v does not wrap ErrInternal", err)
		}
	}
}
