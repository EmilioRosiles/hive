package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	frameHeaderSize = 10       // PayloadLen(4) + ID(4) + Type(1) + Flags(1)
	maxFrameSize    = 64 << 20 // guards against a corrupt/garbage length prefix
	flagErr         = 1 << 0   // payload bytes are a UTF-8 error string, not Frame.Payload
)

// ErrFrameTooLarge is returned when a frame's declared payload length exceeds maxFrameSize.
var ErrFrameTooLarge = errors.New("transport: frame exceeds max size")

// WriteFrame writes f to w as a fixed 10-byte big-endian header followed by
// the payload bytes:
//
//	offset 0: PayloadLen uint32  // length of the bytes that follow
//	offset 4: ID         uint32
//	offset 8: Type       uint8
//	offset 9: Flags      uint8   // bit 0 = error frame
//
// If f.Err is non-empty, the error flag is set and f.Err's bytes are written
// as the payload instead of f.Payload — a frame carries one or the other,
// never both (true of every handler in this codebase today: a non-nil error
// always comes with a nil payload).
func WriteFrame(w io.Writer, f Frame) error {
	payload := f.Payload
	var flags uint8
	if f.Err != "" {
		flags |= flagErr
		payload = []byte(f.Err)
	}
	if len(payload) > maxFrameSize {
		return fmt.Errorf("transport: encode frame: %w", ErrFrameTooLarge)
	}

	var hdr [frameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], f.ID)
	hdr[8] = byte(f.Type)
	hdr[9] = flags

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// ReadFrame reads one frame (header + payload) from r.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	payloadLen := binary.BigEndian.Uint32(hdr[0:4])
	if payloadLen > maxFrameSize {
		return Frame{}, fmt.Errorf("transport: decode frame: %w (%d bytes)", ErrFrameTooLarge, payloadLen)
	}
	f := Frame{
		ID:   binary.BigEndian.Uint32(hdr[4:8]),
		Type: MsgType(hdr[8]),
	}
	if payloadLen == 0 {
		return f, nil
	}
	buf := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, err
	}
	if hdr[9]&flagErr != 0 {
		f.Err = string(buf)
	} else {
		f.Payload = buf
	}
	return f, nil
}
