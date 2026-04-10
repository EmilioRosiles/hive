package transport

import "github.com/vmihailenco/msgpack/v5"

// Encode msgpack-encodes v into a byte slice.
func Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Decode msgpack-decodes data into v.
func Decode(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}
