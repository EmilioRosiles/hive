package cluster

import (
	"context"
)

// lockTokenCtxKey is the context key a lock's fencing token travels under.
type lockTokenCtxKey struct{}

// WithLockToken returns a context carrying token, authorizing store
// operations made with it against whichever key currently holds it.
func WithLockToken(ctx context.Context, token uint32) context.Context {
	return context.WithValue(ctx, lockTokenCtxKey{}, token)
}

// lockTokenFromContext extracts a token set by WithLockToken, or 0 if none.
func lockTokenFromContext(ctx context.Context) uint32 {
	token, _ := ctx.Value(lockTokenCtxKey{}).(uint32)
	return token
}
