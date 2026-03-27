package transport

import (
	"encoding/gob"
	"fmt"
	"log/slog"
	"net"
)

// Handler processes an inbound message and returns a response payload (gob-encoded)
// and an error. Returning a nil payload sends an empty response.
type Handler func(msgType MsgType, payload []byte) ([]byte, error)

// Server listens for inbound peer connections and dispatches frames to a Handler.
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
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	var frame Frame
	if err := gob.NewDecoder(conn).Decode(&frame); err != nil {
		slog.Warn(fmt.Sprintf("transport: decode frame: %v", err))
		return
	}

	respPayload, handlerErr := s.handler(frame.Type, frame.Payload)

	resp := Frame{Type: frame.Type, Payload: respPayload}
	if handlerErr != nil {
		slog.Warn(fmt.Sprintf("transport: handler error (type=%d): %v", frame.Type, handlerErr))
		resp.Err = handlerErr.Error()
	}

	if err := gob.NewEncoder(conn).Encode(resp); err != nil {
		slog.Warn(fmt.Sprintf("transport: encode response: %v", err))
	}
}

// Close stops the server.
func (s *Server) Close() error {
	close(s.stop)
	return s.ln.Close()
}
