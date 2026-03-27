package transport

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	defaultTimeout  = 3 * time.Second
	defaultRetries  = 2
	defaultRetryGap = 200 * time.Millisecond
)

// Client sends frames to a remote peer.
type Client struct {
	addr    string
	timeout time.Duration
	retries int
}

func NewClient(addr string) *Client {
	return &Client{addr: addr, timeout: defaultTimeout, retries: defaultRetries}
}

// Send delivers a frame to the peer and returns the response frame.
// Retries up to c.retries times on transient network errors.
func (c *Client) Send(frame Frame) (Frame, error) {
	var lastErr error
	for attempt := range c.retries + 1 {
		if attempt > 0 {
			time.Sleep(defaultRetryGap)
		}
		resp, err := c.send(frame)
		if err == nil {
			if resp.Err != "" {
				return resp, errors.New(resp.Err)
			}
			return resp, nil
		}
		lastErr = err
	}
	return Frame{}, fmt.Errorf("transport: send to %s: %w", c.addr, lastErr)
}

func (c *Client) send(frame Frame) (Frame, error) {
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return Frame{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(c.timeout)) //nolint:errcheck

	if err := gob.NewEncoder(conn).Encode(frame); err != nil {
		return Frame{}, fmt.Errorf("encode: %w", err)
	}

	var resp Frame
	if err := gob.NewDecoder(conn).Decode(&resp); err != nil {
		return Frame{}, fmt.Errorf("decode: %w", err)
	}
	return resp, nil
}

// -- helpers for callers --

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
