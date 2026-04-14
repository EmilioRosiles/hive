package hive

import (
	"time"

	"github.com/EmilioRosiles/hive/internal/transport"
)

// ListStore[T] is a distributed ordered list backed by a Cache. Each element is
// msgpack-encoded. Keys are namespaced as {name}:l:{key}.
//
//	queue := hive.NewListStore[Task](cache, "work_queue")
//	queue.RPush("jobs", task)
//	job, err := queue.LPop("jobs")
type ListStore[T any] struct {
	cache  *Cache
	prefix string
}

// NewListStore creates a list store backed by cache.
// name is used as the namespace — use a distinct name per list.
func NewListStore[T any](cache *Cache, name string) *ListStore[T] {
	return &ListStore[T]{cache: cache, prefix: name + ":l:"}
}

// LPush prepends value to the head of the list at key.
func (l *ListStore[T]) LPush(key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = l.cache.exec(transport.OpLPush, l.prefix+key, data)
	return err
}

// RPush appends value to the tail of the list at key.
func (l *ListStore[T]) RPush(key string, value T) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = l.cache.exec(transport.OpRPush, l.prefix+key, data)
	return err
}

// LPop removes and returns the head element. Returns ErrNotFound if the list is empty.
func (l *ListStore[T]) LPop(key string) (T, error) {
	return l.pop(transport.OpLPop, key)
}

// RPop removes and returns the tail element. Returns ErrNotFound if the list is empty.
func (l *ListStore[T]) RPop(key string) (T, error) {
	return l.pop(transport.OpRPop, key)
}

func (l *ListStore[T]) pop(op transport.Op, key string) (T, error) {
	var zero T
	results, err := l.cache.exec(op, l.prefix+key)
	if err != nil {
		return zero, err
	}
	return decode[T](results[0])
}

// LLen returns the number of elements in the list at key.
func (l *ListStore[T]) LLen(key string) (int, error) {
	results, err := l.cache.exec(transport.OpLLen, l.prefix+key)
	if err != nil {
		return 0, err
	}
	return decodeInt(results[0]), nil
}

// LIndex returns the element at index. Negative indices count from the tail.
// Returns ErrNotFound if the index is out of bounds.
func (l *ListStore[T]) LIndex(key string, index int) (T, error) {
	var zero T
	results, err := l.cache.exec(transport.OpLIndex, l.prefix+key, encodeInt64(int64(index)))
	if err != nil {
		return zero, err
	}
	return decode[T](results[0])
}

// LRange returns elements from start to stop inclusive. Negative indices
// are supported. Out-of-range bounds are clipped silently.
func (l *ListStore[T]) LRange(key string, start, stop int) ([]T, error) {
	results, err := l.cache.exec(transport.OpLRange, l.prefix+key,
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
func (l *ListStore[T]) LSet(key string, index int, value T) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = l.cache.exec(transport.OpLSet, l.prefix+key, encodeInt64(int64(index)), data)
	return err
}

// Del removes the entire list at key.
func (l *ListStore[T]) Del(key string) error {
	_, err := l.cache.exec(transport.OpDel, l.prefix+key)
	return err
}

// Expire sets a key-level TTL. The entire list is deleted after ttl elapses.
func (l *ListStore[T]) Expire(key string, ttl time.Duration) error {
	_, err := l.cache.exec(transport.OpExpire, l.prefix+key, encodeTTL(ttl))
	return err
}
