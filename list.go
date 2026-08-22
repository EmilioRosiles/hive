package hive

import (
	"context"
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// ListStore[T] is a distributed ordered list backed by a Cluster. Each element is
// msgpack-encoded. Keys are namespaced as {name}:l:{key}.
//
//	queue := hive.NewListStore[Task](cluster, "work_queue")
//	queue.RPush(ctx, "jobs", task)
//	job, err := queue.LPop(ctx, "jobs")
type ListStore[T any] struct {
	cluster *Cluster
	prefix  string
}

// NewListStore creates a list store backed by cluster.
// name is used as the namespace — use a distinct name per list.
func NewListStore[T any](cluster *Cluster, name string) *ListStore[T] {
	return &ListStore[T]{cluster: cluster, prefix: name + ":l:"}
}

// LPush prepends value to the head of the list at key.
func (l *ListStore[T]) LPush(ctx context.Context, key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = l.cluster.exec(ctx, transport.OpLPush, l.prefix+key, data)
	return err
}

// RPush appends value to the tail of the list at key.
func (l *ListStore[T]) RPush(ctx context.Context, key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = l.cluster.exec(ctx, transport.OpRPush, l.prefix+key, data)
	return err
}

// LPop removes and returns the head element. Returns ErrNotFound if the list is empty.
func (l *ListStore[T]) LPop(ctx context.Context, key string) (T, error) {
	return l.pop(ctx, transport.OpLPop, key)
}

// RPop removes and returns the tail element. Returns ErrNotFound if the list is empty.
func (l *ListStore[T]) RPop(ctx context.Context, key string) (T, error) {
	return l.pop(ctx, transport.OpRPop, key)
}

func (l *ListStore[T]) pop(ctx context.Context, op transport.Op, key string) (T, error) {
	var zero T
	results, err := l.cluster.exec(ctx, op, l.prefix+key)
	if err != nil {
		return zero, err
	}
	return decode[T](results[0])
}

// LLen returns the number of elements in the list at key.
func (l *ListStore[T]) LLen(ctx context.Context, key string) (int, error) {
	results, err := l.cluster.exec(ctx, transport.OpLLen, l.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// LIndex returns the element at index. Negative indices count from the tail.
// Returns ErrNotFound if the index is out of bounds.
func (l *ListStore[T]) LIndex(ctx context.Context, key string, index int) (T, error) {
	var zero T
	results, err := l.cluster.exec(ctx, transport.OpLIndex, l.prefix+key, encodeInt64(int64(index)))
	if err != nil {
		return zero, err
	}
	return decode[T](results[0])
}

// LRange returns elements from start to stop inclusive. Negative indices
// are supported. Out-of-range bounds are clipped silently.
func (l *ListStore[T]) LRange(ctx context.Context, key string, start, stop int) ([]T, error) {
	results, err := l.cluster.exec(ctx, transport.OpLRange, l.prefix+key,
		encodeInt64(int64(start)), encodeInt64(int64(stop)))
	if err != nil {
		return nil, err
	}
	out := make([]T, len(results))
	for i, b := range results {
		v, err := decode[T](b)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// LSet overwrites the element at index. Returns ErrNotFound if out of bounds.
func (l *ListStore[T]) LSet(ctx context.Context, key string, index int, value T) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = l.cluster.exec(ctx, transport.OpLSet, l.prefix+key, encodeInt64(int64(index)), data)
	return err
}

// Del removes the entire list at key.
func (l *ListStore[T]) Del(ctx context.Context, key string) error {
	_, err := l.cluster.exec(ctx, transport.OpDel, l.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire list is deleted after ttl elapses.
func (l *ListStore[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_, err := l.cluster.exec(ctx, transport.OpExpire, l.prefix+key, encodeTTL(ttl))
	return err
}
