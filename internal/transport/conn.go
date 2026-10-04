package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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

var errNoHandler = errors.New("transport: no handler for inbound request")

// conn is one TCP connection carrying requests in both directions. Its read
// loop routes each response, by frame ID, to the goroutine that sent the
// request, and serves each inbound request with handler.
type conn struct {
	nc       net.Conn
	w        *frameWriter
	pending  sync.Map // map[uint32]chan Frame
	nextID   atomic.Uint32
	inflight atomic.Int32  // requests being sent or served right now
	ops      atomic.Uint64 // requests sent or served so far
	seen     uint64        // ops at the previous Client.Reap; only Reap touches it
	handler  Handler
	onHello  func(nodeID string, c *conn)
	done     chan struct{}
	once     sync.Once
	logger   *slog.Logger
}

func newConn(nc net.Conn, handler Handler, logger *slog.Logger) *conn {
	return &conn{
		nc:      nc,
		w:       newFrameWriter(nc),
		handler: handler,
		done:    make(chan struct{}),
		logger:  logger,
	}
}

// send delivers frame to the remote peer and returns the response, wrapping
// ErrUnsent when the frame never left. Multiple goroutines may call send concurrently.
func (c *conn) send(ctx context.Context, frame Frame) (Frame, error) {
	if c.closed() {
		return Frame{}, fmt.Errorf("mux: send: %w: %w", ErrUnsent, ErrMuxClosed)
	}
	c.inflight.Add(1)
	defer c.inflight.Add(-1)
	c.ops.Add(1)

	id := c.nextID.Add(1)
	frame.ID = id

	ch := make(chan Frame, 1)
	c.pending.Store(id, ch)

	err := c.w.write(frame)
	if err != nil {
		c.pending.Delete(id)
		c.shutdown(err)
		return Frame{}, fmt.Errorf("mux: send: %w: %w", ErrUnsent, err)
	}

	var resp Frame
	select {
	case resp = <-ch:
	case <-c.done:
		c.pending.Delete(id)
		select {
		case resp = <-ch:
		default:
			return Frame{}, ErrMuxClosed
		}
	case <-ctx.Done():
		c.pending.Delete(id)
		return Frame{}, fmt.Errorf("mux: send: %w", ctx.Err())
	}
	if resp.Err != "" {
		return resp, fmt.Errorf("%w: %s", ErrRejected, resp.Err)
	}
	return resp, nil
}

// readLoop reads frames until the connection closes, routing responses to
// their senders, MsgHello to onHello, and requests to serve.
func (c *conn) readLoop() {
	r := bufio.NewReader(c.nc)
	for {
		frame, err := ReadFrame(r)
		if err != nil {
			c.shutdown(err)
			return
		}
		switch {
		case frame.Resp:
			if ch, ok := c.pending.LoadAndDelete(frame.ID); ok {
				ch.(chan Frame) <- frame
			} else {
				c.logger.Warn("mux: received response for unknown id", "id", frame.ID)
			}
		case frame.Type == MsgHello:
			if c.onHello != nil {
				go c.onHello(string(frame.Payload), c)
			}
		default:
			c.inflight.Add(1)
			c.ops.Add(1)
			go c.serve(frame)
		}
	}
}

// serve runs handler for an inbound request, counted in inflight by readLoop,
// and writes the response back under the same ID.
func (c *conn) serve(f Frame) {
	defer c.inflight.Add(-1)
	var payload []byte
	err := errNoHandler
	if c.handler != nil {
		payload, err = c.handler(f.Type, f.Payload)
	}
	resp := Frame{ID: f.ID, Type: f.Type, Payload: payload, Resp: true}
	if err != nil {
		c.logger.Warn("transport: handler error", "type", f.Type, "err", err)
		resp.Err = err.Error()
	}
	if err := c.w.write(resp); err != nil {
		c.logger.Warn("transport: encode response failed", "err", err)
	}
}

// shutdown closes the connection exactly once, waking pending senders through done.
func (c *conn) shutdown(cause error) {
	c.once.Do(func() {
		close(c.done)
		c.nc.Close()

		if cause != nil && !errors.Is(cause, net.ErrClosed) && !errors.Is(cause, io.EOF) {
			c.logger.Warn("mux: connection lost", "remote_addr", c.nc.RemoteAddr(), "err", cause)
		}
	})
}

func (c *conn) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
