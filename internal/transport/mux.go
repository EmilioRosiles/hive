package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vmihailenco/msgpack/v5"
)

var errMuxClosed = errors.New("mux: connection closed")

// mux multiplexes concurrent request/response pairs over a single TCP connection.
// One readLoop goroutine reads all inbound frames and dispatches each response
// to the goroutine that sent the matching request.
type mux struct {
	conn    net.Conn
	enc     *msgpack.Encoder
	encMu   sync.Mutex    // serializes writes; encoder is not goroutine-safe
	pending sync.Map      // map[uint32]chan Frame
	nextID  atomic.Uint32
	done    chan struct{}
	once    sync.Once
}

func newMux(conn net.Conn) *mux {
	m := &mux{
		conn: conn,
		enc:  msgpack.NewEncoder(conn),
		done: make(chan struct{}),
	}
	go m.readLoop()
	return m
}

// send delivers frame to the remote peer and returns the response.
// Multiple goroutines may call send concurrently.
func (m *mux) send(frame Frame) (Frame, error) {
	id := m.nextID.Add(1)
	frame.ID = id

	ch := make(chan Frame, 1)
	m.pending.Store(id, ch)

	m.encMu.Lock()
	err := m.enc.Encode(frame)
	m.encMu.Unlock()

	if err != nil {
		m.pending.Delete(id)
		m.shutdown(err)
		return Frame{}, fmt.Errorf("mux: send: %w", err)
	}

	select {
	case resp := <-ch:
		if resp.Err != "" {
			return resp, errors.New(resp.Err)
		}
		return resp, nil
	case <-m.done:
		m.pending.Delete(id)
		return Frame{}, errMuxClosed
	}
}

// readLoop reads frames from the connection and routes each to its waiting sender.
// Returns when the connection is closed or errors.
func (m *mux) readLoop() {
	dec := msgpack.NewDecoder(m.conn)
	for {
		var frame Frame
		if err := dec.Decode(&frame); err != nil {
			m.shutdown(err)
			return
		}
		if ch, ok := m.pending.LoadAndDelete(frame.ID); ok {
			ch.(chan Frame) <- frame
		} else {
			slog.Warn(fmt.Sprintf("mux: received response for unknown id %d", frame.ID))
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
			slog.Warn(fmt.Sprintf("mux: connection to %s lost: %v", m.conn.RemoteAddr(), cause))
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
