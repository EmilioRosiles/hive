package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

// ErrMuxClosed is returned when the connection closed after the frame was sent,
// so the peer may or may not have applied it.
var ErrMuxClosed = errors.New("mux: connection closed")

// ErrRejected is returned by Client.Send when the peer handled the request
// and answered with an error.
var ErrRejected = errors.New("transport: rejected")

// mux multiplexes concurrent request/response pairs over a single TCP
// connection. One readLoop goroutine reads all inbound frames and routes
// each response, by frame.ID, to the goroutine that sent the matching
// request. Server doesn't need this: it only ever answers the specific frame
// it just read, with nothing to correlate.
type mux struct {
	conn    net.Conn
	w       *frameWriter
	pending sync.Map // map[uint32]chan Frame
	nextID  atomic.Uint32
	done    chan struct{}
	once    sync.Once
	logger  *slog.Logger
}

func newMux(conn net.Conn, logger *slog.Logger) *mux {
	m := &mux{
		conn:   conn,
		w:      newFrameWriter(conn),
		done:   make(chan struct{}),
		logger: logger,
	}
	go m.readLoop()
	return m
}

// send delivers frame to the remote peer and returns the response, wrapping
// ErrUnsent when the frame never left. Multiple goroutines may call send concurrently.
func (m *mux) send(ctx context.Context, frame Frame) (Frame, error) {
	if m.closed() {
		return Frame{}, fmt.Errorf("mux: send: %w: %w", ErrUnsent, ErrMuxClosed)
	}

	id := m.nextID.Add(1)
	frame.ID = id

	ch := make(chan Frame, 1)
	m.pending.Store(id, ch)

	err := m.w.write(frame)
	if err != nil {
		m.pending.Delete(id)
		m.shutdown(err)
		return Frame{}, fmt.Errorf("mux: send: %w: %w", ErrUnsent, err)
	}

	var resp Frame
	select {
	case resp = <-ch:
	case <-m.done:
		m.pending.Delete(id)
		select {
		case resp = <-ch:
		default:
			return Frame{}, ErrMuxClosed
		}
	case <-ctx.Done():
		m.pending.Delete(id)
		return Frame{}, fmt.Errorf("mux: send: %w", ctx.Err())
	}
	if resp.Err != "" {
		return resp, fmt.Errorf("%w: %s", ErrRejected, resp.Err)
	}
	return resp, nil
}

// readLoop reads frames from the connection and routes each to its waiting sender.
// Returns when the connection is closed or errors.
func (m *mux) readLoop() {
	r := bufio.NewReader(m.conn)
	for {
		frame, err := ReadFrame(r)
		if err != nil {
			m.shutdown(err)
			return
		}
		if ch, ok := m.pending.LoadAndDelete(frame.ID); ok {
			ch.(chan Frame) <- frame
		} else {
			m.logger.Warn("mux: received response for unknown id", "id", frame.ID)
		}
	}
}

// shutdown closes the mux and its connection exactly once, waking pending
// senders through done.
func (m *mux) shutdown(cause error) {
	m.once.Do(func() {
		close(m.done)
		m.conn.Close()

		if cause != nil && !errors.Is(cause, net.ErrClosed) {
			m.logger.Warn("mux: connection lost", "remote_addr", m.conn.RemoteAddr(), "err", cause)
		}
	})
}

func (m *mux) closed() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}
