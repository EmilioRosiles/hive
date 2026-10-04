package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTimeout = 3 * time.Second

// ErrUnsent marks a Send failure where the frame was never written, so the
// request is safe to retry.
var ErrUnsent = errors.New("transport: frame not sent")

var errClientClosed = errors.New("transport: client closed")

// Client maintains a small pool of persistent connections to a peer, round-
// robin selected so concurrent sends don't serialize on one connection. Its
// connections also serve the peer's requests with handler.
type Client struct {
	addr      string
	localID   string
	handler   Handler
	timeout   time.Duration
	tlsConfig *tls.Config
	mu        sync.Mutex
	closed    bool
	conns     []atomic.Pointer[conn]
	next      atomic.Uint32
	logger    *slog.Logger
}

// NewClient creates a client for addr with a pool of poolSize connections
// (minimum 1). A non-empty localID is sent as MsgHello on every dial so the
// peer can reuse the connection. If tlsConfig is non-nil, connections are dialed over TLS.
func NewClient(addr, localID string, handler Handler, tlsConfig *tls.Config, poolSize int, logger *slog.Logger) *Client {
	return &Client{addr: addr, localID: localID, handler: handler, timeout: defaultTimeout, tlsConfig: tlsConfig, conns: make([]atomic.Pointer[conn], max(1, poolSize)), logger: logger}
}

// Send delivers frame to the peer and returns the response, retrying only while
// the frame never left. Failures wrap ErrUnsent (not sent), ErrMuxClosed or a
// context error (outcome unknown), or ErrRejected (peer answered with an error).
func (c *Client) Send(ctx context.Context, frame Frame) (Frame, error) {
	slot := int(c.next.Add(1)-1) % len(c.conns)
	for attempt := range 3 {
		cn, err := c.getConn(slot)
		if err != nil {
			if attempt < 2 && !errors.Is(err, errClientClosed) {
				if serr := sleepCtx(ctx, 100*time.Millisecond); serr != nil {
					return Frame{}, serr
				}
				continue
			}
			return Frame{}, fmt.Errorf("transport: connect to %s: %w: %w", c.addr, ErrUnsent, err)
		}
		resp, err := cn.send(ctx, frame)
		if err == nil {
			return resp, nil
		}
		if errors.Is(err, ErrUnsent) && attempt < 2 {
			c.invalidate(slot, cn)
			if serr := sleepCtx(ctx, 100*time.Millisecond); serr != nil {
				return Frame{}, serr
			}
			continue
		}
		return Frame{}, err
	}
	return Frame{}, fmt.Errorf("transport: send to %s failed", c.addr)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getConn returns the live connection for slot, dialing a new one if needed.
// It fails with errClientClosed once Close has been called.
func (c *Client) getConn(slot int) (*conn, error) {
	if cn := c.conns[slot].Load(); cn != nil && !cn.closed() {
		return cn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cn := c.conns[slot].Load(); cn != nil && !cn.closed() {
		return cn, nil
	}
	if c.closed {
		return nil, errClientClosed
	}

	var nc net.Conn
	var err error
	if c.tlsConfig != nil {
		nc, err = tls.DialWithDialer(&net.Dialer{Timeout: c.timeout}, "tcp", c.addr, c.tlsConfig)
	} else {
		nc, err = net.DialTimeout("tcp", c.addr, c.timeout)
	}
	if err != nil {
		return nil, err
	}
	cn := newConn(nc, c.handler, c.logger)
	go cn.readLoop()
	if c.localID != "" {
		if err := cn.w.write(Frame{Type: MsgHello, Payload: []byte(c.localID)}); err != nil {
			cn.shutdown(err)
			return nil, err
		}
	}
	c.conns[slot].Store(cn)
	return cn, nil
}

// adopt puts an inbound connection from the peer into the first empty slot.
// With no empty slot it is left serving the peer's requests only.
func (c *Client) adopt(cn *conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for i := range c.conns {
		if cur := c.conns[i].Load(); cur == nil || cur.closed() {
			c.conns[i].Store(cn)
			return
		}
	}
}

// invalidate discards a dead connection so the next getConn dials fresh.
func (c *Client) invalidate(slot int, dead *conn) {
	c.conns[slot].CompareAndSwap(dead, nil)
}

// Close shuts down every connection in the pool and stops it from dialing new ones.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for i := range c.conns {
		if cn := c.conns[i].Swap(nil); cn != nil {
			cn.shutdown(nil)
		}
	}
}
