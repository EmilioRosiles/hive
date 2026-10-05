package hive

import (
	"context"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// BitmapStore is a distributed packed bit array backed by a Cluster, costing one
// bit per offset. Keys are namespaced as {name}:b:{key} to prevent collisions
// with other stores on the same node.
//
//	dau := hive.NewBitmapStore(cluster, "dau")
//	dau.SetBit(ctx, "2026-08-22", 123456, true)
//	dau.Expire(ctx, "2026-08-22", 48*time.Hour)
type BitmapStore struct {
	cluster *Cluster
	prefix  string
}

// NewBitmapStore creates a bitmap store backed by cluster.
// name is used as the namespace — use a distinct name per bitmap.
func NewBitmapStore(cluster *Cluster, name string) *BitmapStore {
	return &BitmapStore{cluster: cluster, prefix: name + ":b:"}
}

// SetBit sets or clears the bit at offset in the bitmap at key. The bitmap
// grows to fit offset, so memory is proportional to the highest bit set.
func (b *BitmapStore) SetBit(ctx context.Context, key string, offset uint32, on bool) error {
	v := byte(0)
	if on {
		v = 1
	}
	_, err := b.cluster.exec(ctx, transport.OpBitSet, b.prefix+key, encodeUint32(offset), []byte{v})
	return err
}

// GetBit reports whether the bit at offset is set in the bitmap at key.
func (b *BitmapStore) GetBit(ctx context.Context, key string, offset uint32) (bool, error) {
	results, err := b.cluster.exec(ctx, transport.OpBitGet, b.prefix+key, encodeUint32(offset))
	if err != nil {
		return false, err
	}
	return results[0][0] == 1, nil
}

// Count returns the number of set bits in the bitmap at key.
func (b *BitmapStore) Count(ctx context.Context, key string) (int, error) {
	results, err := b.cluster.exec(ctx, transport.OpBitCount, b.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// Del removes the entire bitmap at key.
func (b *BitmapStore) Del(ctx context.Context, key string) error {
	_, err := b.cluster.exec(ctx, transport.OpDel, b.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire bitmap is deleted after ttl elapses.
func (b *BitmapStore) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_, err := b.cluster.exec(ctx, transport.OpExpire, b.prefix+key, encodeTTL(ttl))
	return err
}

// Lock acquires a distributed lock on key, valid for ttl. Returns ErrKeyLocked
// if key is already locked.
func (b *BitmapStore) Lock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	return newLock(ctx, b.cluster, b.prefix+key, ttl)
}

// Atomic waits for a lock on key, then runs fn with the lock's authorized
// context and releases the lock when fn returns. ttl bounds how long the
// lock is held; ctx bounds how long Atomic waits to acquire it.
func (b *BitmapStore) Atomic(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	return lockAndRun(ctx, b.cluster, b.prefix+key, ttl, fn)
}
