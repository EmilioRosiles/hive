package hive

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/EmilioRosiles/hive/internal/cluster"
	"github.com/EmilioRosiles/hive/internal/transport"
)

// Lock is a handle to a distributed lock acquired on a store key. While held,
// every op against that key — on any store type — is rejected with
// ErrKeyLocked for every caller, including the holder itself, unless made
// with the context returned by Context.
type Lock struct {
	cluster *Cluster
	key     string
	token   uint32
}

// newLock attempts to acquire a lock on key, returning ErrKeyLocked if it is
// already held. Shared by every store type's Lock method — key is already
// namespaced with that store's prefix. The token is generated client-side
// so it replicates identically to every node instead of each one inventing
// its own random value.
func newLock(ctx context.Context, c *Cluster, key string, ttl time.Duration) (*Lock, error) {
	token := randomToken()
	if _, err := c.exec(ctx, transport.OpLock, key, encodeTTL(ttl), encodeUint32(token)); err != nil {
		return nil, err
	}
	return &Lock{cluster: c, key: key, token: token}, nil
}

// randomToken generates a non-zero random fencing token for a new lock. 0 is
// reserved to mean "no lock"/"no token provided", both at rest and on the wire.
func randomToken() uint32 {
	for {
		if t := rand.Uint32(); t != 0 {
			return t
		}
	}
}

// Unlock releases the lock. Returns ErrLockNotHeld if it already expired and
// was re-acquired by a different holder.
func (l *Lock) Unlock(ctx context.Context) error {
	_, err := l.cluster.exec(ctx, transport.OpUnlock, l.key, encodeUint32(l.token))
	return err
}

// Renew extends the lock's TTL. Returns ErrLockNotHeld if it already expired
// and was re-acquired by a different holder.
func (l *Lock) Renew(ctx context.Context, ttl time.Duration) error {
	_, err := l.cluster.exec(ctx, transport.OpRenew, l.key, encodeUint32(l.token), encodeTTL(ttl))
	return err
}

// Context returns ctx carrying this lock's fencing token — pass the result to
// other store operations on the same key to perform them while held, in
// place of ErrKeyLocked. Pass context.Background() for a critical section
// that should run independently of any request that's already in flight, or
// your own ctx to preserve its values/deadline/cancellation.
func (l *Lock) Context(ctx context.Context) context.Context {
	return cluster.WithLockToken(ctx, l.token)
}

// lockAndRun waits for a lock on key, runs fn with the lock's authorized
// context, then releases it. Shared by every store type's Atomic method —
// key is already namespaced with that store's prefix.
func lockAndRun(ctx context.Context, c *Cluster, key string, ttl time.Duration, fn func(ctx context.Context) error) (err error) {
	lock, err := acquireWithBackoff(ctx, c, key, ttl)
	if err != nil {
		return err
	}
	defer lock.Unlock(context.WithoutCancel(ctx))
	return fn(lock.Context(ctx))
}

// acquireWithBackoff retries newLock with jittered, capped exponential
// backoff until it succeeds, ctx is done, or the attempt fails with anything
// other than ErrKeyLocked.
func acquireWithBackoff(ctx context.Context, c *Cluster, key string, ttl time.Duration) (*Lock, error) {
	limit := 100 * time.Millisecond
	delay := time.Millisecond
	for {
		lock, err := newLock(ctx, c, key, ttl)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, ErrKeyLocked) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(cluster.Jitter(delay, 0.25)):
		}

		delay = min(delay*2, limit)
	}
}
