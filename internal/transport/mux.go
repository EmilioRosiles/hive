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

var errMuxClosed = errors.New("mux: connection closed")

// ErrRejected is returned by Client.Send when the remote handler rejected the
// request. It is distinct from a network/connection failure.
type ErrRejected struct{ msg string }

func (e *ErrRejected) Error() string { return e.msg }

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

// send delivers frame to the remote peer and returns the response.
// Multiple goroutines may call send concurrently.
func (m *mux) send(ctx context.Context, frame Frame) (Frame, error) {
	id := m.nextID.Add(1)
	frame.ID = id

	ch := make(chan Frame, 1)
	m.pending.Store(id, ch)

	err := m.w.write(frame)
	if err != nil {
		m.pending.Delete(id)
		m.shutdown(err)
		return Frame{}, fmt.Errorf("mux: send: %w", err)
	}

	select {
	case resp := <-ch:
		if resp.Err != "" {
			return resp, &ErrRejected{msg: resp.Err}
		}
		return resp, nil
	case <-m.done:
		m.pending.Delete(id)
		return Frame{}, errMuxClosed
	case <-ctx.Done():
		m.pending.Delete(id)
		return Frame{}, fmt.Errorf("mux: send: %w", ctx.Err())
	}
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

// shutdown closes the mux exactly once, signals all pending senders, and closes
// the underlying connection.
func (m *mux) shutdown(cause error) {
	m.once.Do(func() {
		close(m.done)
		m.conn.Close()

		errFrame := Frame{Err: errMuxClosed.Error()}
		m.pending.Range(func(k, v any) bool {
			v.(chan Frame) <- errFrame
			m.pending.Delete(k)
			return true
		})

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
