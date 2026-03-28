package hive

import (
	"bytes"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func encode[T any](v T) ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := msgpack.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	// Copy out before returning buf to the pool.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func decode[T any](data []byte) (T, error) {
	var v T
	err := msgpack.Unmarshal(data, &v)
	return v, err
}
