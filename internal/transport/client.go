package transport

import (
	"bytes"
	"encoding/gob"
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
	addr    string
	timeout time.Duration
	mu      sync.Mutex
	mux     *mux
}

func NewClient(addr string) *Client {
	return &Client{addr: addr, timeout: defaultTimeout}
}

// Send delivers frame to the peer and returns the response.
// On a connection failure, the dead connection is discarded and the call
// is retried once on a fresh connection before returning an error.
func (c *Client) Send(frame Frame) (Frame, error) {
	m, err := c.getMux()
	if err != nil {
		return Frame{}, fmt.Errorf("transport: connect to %s: %w", c.addr, err)
	}

	resp, err := m.send(frame)
	if err != nil {
		if errors.Is(err, errMuxClosed) {
			// Connection died — discard and retry once on a fresh connection.
			c.mu.Lock()
			if c.mux == m {
				c.mux = nil
			}
			c.mu.Unlock()

			m2, err2 := c.getMux()
			if err2 != nil {
				return Frame{}, fmt.Errorf("transport: reconnect to %s: %w", c.addr, err2)
			}
			return m2.send(frame)
		}
		return Frame{}, err
	}
	return resp, nil
}

// getMux returns the current mux, creating a new connection if needed.
func (c *Client) getMux() (*mux, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mux != nil && !c.mux.closed() {
		return c.mux, nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return nil, err
	}
	c.mux = newMux(conn)
	return c.mux, nil
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

// -- gob helpers used by the cluster layer --

// Encode gob-encodes v into a byte slice.
func Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode gob-decodes data into v.
func Decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
