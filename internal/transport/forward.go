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

// ForwardBatch carries multiple ForwardRequests in one frame, sent by a
// peer's replicator when it has more than one queued write to apply.
type ForwardBatch struct {
	Requests []ForwardRequest
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
	buf := make([]byte, r.encodedSize())
	r.encodeInto(buf, 0)
	return buf, nil
}

// encodedSize returns r's exact encoded length in bytes.
func (r ForwardRequest) encodedSize() int {
	size := 1 + 4 + len(r.Key) + 4
	for _, a := range r.Args {
		size += 4 + len(a)
	}
	return size
}

// encodeInto writes r's wire encoding into buf starting at offset i and
// returns the offset just past what it wrote. buf must have at least
// r.encodedSize() bytes available from i — used by ForwardBatch to encode
// many requests into one shared buffer without a per-request allocation.
func (r ForwardRequest) encodeInto(buf []byte, i int) int {
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
	return i
}

func (r *ForwardRequest) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	req, err := decodeForwardRequest(&br)
	if err != nil {
		return err
	}
	if !br.done() {
		return fmt.Errorf("transport: decode ForwardRequest: trailing data")
	}
	*r = req
	return nil
}

// decodeForwardRequest reads one ForwardRequest off br without checking for
// trailing data, so ForwardBatch can decode many back-to-back entries off a
// single shared cursor.
func decodeForwardRequest(br *binReader) (ForwardRequest, error) {
	op, err := br.uint8()
	if err != nil {
		return ForwardRequest{}, fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	key, err := br.string()
	if err != nil {
		return ForwardRequest{}, fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	argCount, err := br.count(4) // each arg needs at least its own 4-byte length prefix
	if err != nil {
		return ForwardRequest{}, fmt.Errorf("transport: decode ForwardRequest: %w", err)
	}
	args := make([][]byte, 0, argCount)
	for i := uint32(0); i < argCount; i++ {
		a, err := br.bytes()
		if err != nil {
			return ForwardRequest{}, fmt.Errorf("transport: decode ForwardRequest: %w", err)
		}
		args = append(args, a)
	}
	return ForwardRequest{Op: Op(op), Key: key, Args: args}, nil
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

// -- ForwardBatch wire codec --
//
// Wire layout:
//
//	RequestCount uint32
//	repeated RequestCount times: ForwardRequest (same layout as above)

// forwardRequestMinSize is a ForwardRequest's minimum possible encoded size
// (Op + KeyLen + ArgCount, with an empty key and no args) — used to bound
// RequestCount against the remaining buffer before allocating.
const forwardRequestMinSize = 1 + 4 + 4

func (b ForwardBatch) MarshalBinary() ([]byte, error) {
	size := 4
	for _, r := range b.Requests {
		size += r.encodedSize()
	}
	buf := make([]byte, size)
	i := 4
	binary.BigEndian.PutUint32(buf[0:], uint32(len(b.Requests)))
	for _, r := range b.Requests {
		i = r.encodeInto(buf, i)
	}
	return buf, nil
}

func (b *ForwardBatch) UnmarshalBinary(data []byte) error {
	br := binReader{b: data}
	count, err := br.count(forwardRequestMinSize)
	if err != nil {
		return fmt.Errorf("transport: decode ForwardBatch: %w", err)
	}
	requests := make([]ForwardRequest, 0, count)
	for i := uint32(0); i < count; i++ {
		req, err := decodeForwardRequest(&br)
		if err != nil {
			return fmt.Errorf("transport: decode ForwardBatch: %w", err)
		}
		requests = append(requests, req)
	}
	if !br.done() {
		return fmt.Errorf("transport: decode ForwardBatch: trailing data")
	}
	b.Requests = requests
	return nil
}
