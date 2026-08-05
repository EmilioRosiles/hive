package transport

import (
	"encoding/binary"
	"fmt"
)

// ForwardRequest asks the receiving node to execute an operation locally.
// Args are positional raw byte slots — no intermediate struct encoding.
type ForwardRequest struct {
	Op   Op
	Key  string
	Args [][]byte
}

// ForwardResponse carries the result of a forwarded operation.
// Results are positional raw byte slots matching the op's return layout.
// Write ops return nil. Read ops return one or more slots.
type ForwardResponse struct {
	Results [][]byte
}

// -- ForwardRequest wire codec --
//
// Wire layout (all integers big-endian):
//
//	Op       uint8
//	KeyLen   uint32
//	Key      [KeyLen]byte
//	ArgCount uint32
//	repeated ArgCount times:
//	  ArgLen uint32
//	  Arg    [ArgLen]byte

func (r ForwardRequest) MarshalBinary() ([]byte, error) {
	size := 1 + 4 + len(r.Key) + 4
	for _, a := range r.Args {
		size += 4 + len(a)
	}
	buf := make([]byte, size)
	i := 0
	buf[i] = byte(r.Op)
	i++
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Key)))
	i += 4
	i += copy(buf[i:], r.Key)
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Args)))
	i += 4
	for _, a := range r.Args {
		binary.BigEndian.PutUint32(buf[i:], uint32(len(a)))
		i += 4
		i += copy(buf[i:], a)
	}
	return buf, nil
}

func (r *ForwardRequest) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	op, err := br.uint8()
	if err != nil {
		return fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	key, err := br.string()
	if err != nil {
		return fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	argCount, err := br.count(4) // each arg needs at least its own 4-byte length prefix
	if err != nil {
		return fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	args := make([][]byte, 0, argCount)
	for i := uint32(0); i < argCount; i++ {
		a, err := br.bytes()
		if err != nil {
			return fmt.Errorf("transport: decode ForwardRequest: %w", err)
		}
		args = append(args, a)
	}
	if !br.done() {
		return fmt.Errorf("transport: decode ForwardRequest: trailing data")
	}
	r.Op = Op(op)
	r.Key = key
	r.Args = args
	return nil
}

// -- ForwardResponse wire codec --
//
// Wire layout:
//
//	ResultCount uint32
//	repeated ResultCount times:
//	  ResultLen uint32
//	  Result    [ResultLen]byte

func (r ForwardResponse) MarshalBinary() ([]byte, error) {
	size := 4
	for _, res := range r.Results {
		size += 4 + len(res)
	}
	buf := make([]byte, size)
	i := 0
	binary.BigEndian.PutUint32(buf[i:], uint32(len(r.Results)))
	i += 4
	for _, res := range r.Results {
		binary.BigEndian.PutUint32(buf[i:], uint32(len(res)))
		i += 4
		i += copy(buf[i:], res)
	}
	return buf, nil
}

func (r *ForwardResponse) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	count, err := br.count(4)
	if err != nil {
		return fmt.Errorf("transport: decode ForwardResponse: %w", err)
	}
	results := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		res, err := br.bytes()
		if err != nil {
			return fmt.Errorf("transport: decode ForwardResponse: %w", err)
		}
		results = append(results, res)
	}
	if !br.done() {
		return fmt.Errorf("transport: decode ForwardResponse: trailing data")
	}
	r.Results = results
	return nil
}
