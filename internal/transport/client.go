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

var (
	errClientClosed = errors.New("transport: client closed")
	errPoolFull     = errors.New("transport: pool full")
)

// Client keeps up to poolSize persistent connections to a peer, sending on the
// least busy one and growing the pool only while every connection is busy. Its
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
	growing   atomic.Bool
	logger    *slog.Logger
}

// NewClient creates a client for addr with up to poolSize connections
// (minimum 1). A non-empty localID is sent as MsgHello on every dial so the
// peer can reuse the connection. If tlsConfig is non-nil, connections are dialed over TLS.
func NewClient(addr, localID string, handler Handler, tlsConfig *tls.Config, poolSize int, logger *slog.Logger) *Client {
	return &Client{addr: addr, localID: localID, handler: handler, timeout: defaultTimeout, tlsConfig: tlsConfig, conns: make([]atomic.Pointer[conn], max(1, poolSize)), logger: logger}
}

// Send delivers frame to the peer and returns the response. A frame that never
// left because its pooled connection had died is retried at once on another
// connection; a failed dial is not retried, callers decide. Failures wrap
// ErrUnsent (not sent), ErrMuxClosed or a context error (outcome unknown), or
// ErrRejected (peer answered with an error).
func (c *Client) Send(ctx context.Context, frame Frame) (Frame, error) {
	var err error
	for range 3 {
		var cn *conn
		cn, err = c.pick()
		if err != nil {
			return Frame{}, fmt.Errorf("transport: connect to %s: %w: %w", c.addr, ErrUnsent, err)
		}
		var resp Frame
		resp, err = cn.send(ctx, frame)
		if !errors.Is(err, ErrUnsent) {
			return resp, err
		}
	}
	return Frame{}, err
}

// pick returns the least busy live connection. When every connection is busy
// it grows the pool in the background; with none live it dials one.
func (c *Client) pick() (*conn, error) {
	var best *conn
	full := true
	for i := range c.conns {
		cn := c.conns[i].Load()
		if cn == nil || cn.closed() {
			full = false
			continue
		}
		if best == nil || cn.inflight.Load() < best.inflight.Load() {
			best = cn
		}
	}
	if best == nil {
		return c.grow(true)
	}
	if !full && best.inflight.Load() > 0 && c.growing.CompareAndSwap(false, true) {
		go func() {
			c.grow(false)
			c.growing.Store(false)
		}()
	}
	return best, nil
}

// grow dials a connection into the first empty slot. With reuse, a live
// connection another caller just added is returned instead. It fails with
// errClientClosed once Close has been called.
func (c *Client) grow(reuse bool) (*conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClientClosed
	}
	slot := -1
	for i := range c.conns {
		if cn := c.conns[i].Load(); cn != nil && !cn.closed() {
			if reuse {
				return cn, nil
			}
		} else if slot < 0 {
			slot = i
		}
	}
	if slot < 0 {
		return nil, errPoolFull
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

// Reap closes connections that sent or served nothing since the previous
// Reap, always keeping one live connection.
func (c *Client) Reap() {
	c.mu.Lock()
	defer c.mu.Unlock()
	live := 0
	for i := range c.conns {
		if cn := c.conns[i].Load(); cn != nil && !cn.closed() {
			live++
		}
	}
	for i := range c.conns {
		cn := c.conns[i].Load()
		if cn == nil || cn.closed() {
			continue
		}
		ops := cn.ops.Load()
		idle := ops == cn.seen && cn.inflight.Load() == 0
		cn.seen = ops
		if idle && live > 1 {
			c.conns[i].Store(nil)
			cn.shutdown(nil)
			live--
		}
	}
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
