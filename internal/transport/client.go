package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const defaultTimeout = 3 * time.Second

// Client maintains a persistent multiplexed connection to a single peer.
// Concurrent calls to Send are all served over the same TCP connection.
// The connection is established lazily and re-established automatically
// after a failure.
type Client struct {
	addr      string
	timeout   time.Duration
	tlsConfig *tls.Config
	mu        sync.Mutex
	mux       *mux
}

// NewClient creates a client for addr. If tlsConfig is non-nil, connections
// are dialed over TLS using it; nil means plaintext.
func NewClient(addr string, tlsConfig *tls.Config) *Client {
	return &Client{addr: addr, timeout: defaultTimeout, tlsConfig: tlsConfig}
}

// Send delivers frame to the peer and returns the response.
func (c *Client) Send(ctx context.Context, frame Frame) (Frame, error) {
	for attempt := range 3 {
		m, err := c.getMux()
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
			c.invalidate(m)
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

// getMux returns the live mux, dialing a new connection if needed.
func (c *Client) getMux() (*mux, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mux != nil && !c.mux.closed() {
		return c.mux, nil
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
	c.mux = newMux(conn)
	return c.mux, nil
}

// invalidate discards a dead mux so the next getMux dials fresh.
func (c *Client) invalidate(dead *mux) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mux == dead {
		c.mux = nil
	}
}

// Close shuts down the client connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mux != nil {
		c.mux.shutdown(nil)
		c.mux = nil
	}
}
