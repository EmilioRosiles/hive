package transport

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// Handler processes an inbound message and returns a response payload and an
// error. Returning a nil payload sends an empty response.
type Handler func(msgType MsgType, payload []byte) ([]byte, error)

// Server listens for inbound peer connections and dispatches frames to a Handler.
// Each accepted connection is kept alive and can carry multiple concurrent requests.
type Server struct {
	ln      net.Listener
	handler Handler
	stop    chan struct{}
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
}

// NewServer creates a TCP server bound to addr. If tlsConfig is non-nil,
// connections are accepted over TLS using it; nil means plaintext.
func NewServer(addr string, handler Handler, tlsConfig *tls.Config) (*Server, error) {
	var ln net.Listener
	var err error
	if tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", addr, err)
	}
	return &Server{ln: ln, handler: handler, stop: make(chan struct{}), conns: make(map[net.Conn]struct{})}, nil
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
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := newFrameWriter(conn)

	for {
		frame, err := ReadFrame(r)
		if err != nil {
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
			if err := w.write(resp); err != nil {
				slog.Warn(fmt.Sprintf("transport: encode response: %v", err))
			}
		}(frame)
	}
}

// Close stops the server: it stops accepting new connections and closes
// every connection already accepted, so peers see this node disappear
// promptly instead of waiting on a connection nothing will answer.
func (s *Server) Close() error {
	close(s.stop)
	err := s.ln.Close()

	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}

	return err
}
