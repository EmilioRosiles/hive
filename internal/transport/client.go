package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTimeout = 3 * time.Second

// Client maintains a small pool of persistent connections to a peer, round-
// robin selected so concurrent sends don't serialize on one connection.
type Client struct {
	addr      string
	timeout   time.Duration
	tlsConfig *tls.Config
	mu        sync.Mutex
	muxes     []*mux
	next      atomic.Uint32
}

// NewClient creates a client for addr with a pool of poolSize connections
// (minimum 1). If tlsConfig is non-nil, connections are dialed over TLS.
func NewClient(addr string, tlsConfig *tls.Config, poolSize int) *Client {
	return &Client{addr: addr, timeout: defaultTimeout, tlsConfig: tlsConfig, muxes: make([]*mux, max(1, poolSize))}
}

// Send delivers frame to the peer and returns the response.
func (c *Client) Send(ctx context.Context, frame Frame) (Frame, error) {
	slot := int(c.next.Add(1)-1) % len(c.muxes)
	for attempt := range 3 {
		m, err := c.getMux(slot)
		if err != nil {
			if attempt < 2 {
				if serr := sleepCtx(ctx, 100*time.Millisecond); serr != nil {
					return Frame{}, serr
				}
				continue
			}
			return Frame{}, fmt.Errorf("transport: connect to %s: %w", c.addr, err)
		}
		resp, err := m.send(ctx, frame)
		if err == nil {
			return resp, nil
		}
		if errors.Is(err, errMuxClosed) && attempt < 2 {
			c.invalidate(slot, m)
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

// getMux returns the live mux for slot, dialing a new connection if needed.
func (c *Client) getMux(slot int) (*mux, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if m := c.muxes[slot]; m != nil && !m.closed() {
		return m, nil
	}

	var conn net.Conn
	var err error
	if c.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: c.timeout}, "tcp", c.addr, c.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", c.addr, c.timeout)
	}
	if err != nil {
		return nil, err
	}
	c.muxes[slot] = newMux(conn)
	return c.muxes[slot], nil
}

// invalidate discards a dead mux so the next getMux dials fresh.
func (c *Client) invalidate(slot int, dead *mux) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.muxes[slot] == dead {
		c.muxes[slot] = nil
	}
}

// Close shuts down every connection in the pool.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, m := range c.muxes {
		if m != nil {
			m.shutdown(nil)
			c.muxes[i] = nil
		}
	}
}
