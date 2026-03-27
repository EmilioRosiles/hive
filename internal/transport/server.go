package transport

import (
	"encoding/gob"
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// Handler processes an inbound message and returns a response payload (gob-encoded)
// and an error. Returning a nil payload sends an empty response.
type Handler func(msgType MsgType, payload []byte) ([]byte, error)

// Server listens for inbound peer connections and dispatches frames to a Handler.
// Each accepted connection is kept alive and can carry multiple concurrent requests.
type Server struct {
	ln      net.Listener
	handler Handler
	stop    chan struct{}
}

// NewServer creates a TCP server bound to addr.
func NewServer(addr string, handler Handler) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", addr, err)
	}
	return &Server{ln: ln, handler: handler, stop: make(chan struct{})}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

// Serve accepts connections until Close is called.
func (s *Server) Serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				slog.Warn(fmt.Sprintf("transport: accept error: %v", err))
				continue
			}
		}
		go s.handleConn(conn)
	}
}

// handleConn reads frames from a single persistent connection until it closes.
// Each frame is dispatched concurrently; the response is written back with the
// same ID so the remote mux can route it to the correct waiting goroutine.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	var encMu sync.Mutex

	for {
		var frame Frame
		if err := dec.Decode(&frame); err != nil {
			// Connection closed or peer went away — normal exit.
			return
		}

		go func(f Frame) {
			respPayload, handlerErr := s.handler(f.Type, f.Payload)
			resp := Frame{ID: f.ID, Type: f.Type, Payload: respPayload}
			if handlerErr != nil {
				slog.Warn(fmt.Sprintf("transport: handler error (type=%d): %v", f.Type, handlerErr))
				resp.Err = handlerErr.Error()
			}
			encMu.Lock()
			if err := enc.Encode(resp); err != nil {
				slog.Warn(fmt.Sprintf("transport: encode response: %v", err))
			}
			encMu.Unlock()
		}(frame)
	}
}

// Close stops the server.
func (s *Server) Close() error {
	close(s.stop)
	return s.ln.Close()
}
