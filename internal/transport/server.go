package transport

import (
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
// A connection whose dialer identifies itself with MsgHello is adopted by that
// peer's Client, so this node can send over it too.
type Server struct {
	ln      net.Listener
	handler Handler
	clients func(nodeID string) (*Client, bool)
	stop    chan struct{}
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	logger  *slog.Logger
}

// NewServer creates a TCP server bound to addr. clients looks up a peer's Client
// for adoption and may be nil. If tlsConfig is non-nil, connections are
// accepted over TLS using it; nil means plaintext.
func NewServer(addr string, handler Handler, clients func(nodeID string) (*Client, bool), tlsConfig *tls.Config, logger *slog.Logger) (*Server, error) {
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
	return &Server{ln: ln, handler: handler, clients: clients, stop: make(chan struct{}), conns: make(map[net.Conn]struct{}), logger: logger}, nil
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
				s.logger.Warn("transport: accept error", "err", err)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

// handleConn serves a single persistent connection until it closes.
// A connection registered after Close has started is refused, so it can't outlive the server.
func (s *Server) handleConn(nc net.Conn) {
	s.mu.Lock()
	select {
	case <-s.stop:
		s.mu.Unlock()
		nc.Close()
		return
	default:
		s.conns[nc] = struct{}{}
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, nc)
		s.mu.Unlock()
	}()

	c := newConn(nc, s.handler, s.logger)
	c.onHello = s.adopt
	c.readLoop()
}

// adopt hands an inbound connection to the Client for nodeID, if there is one.
func (s *Server) adopt(nodeID string, c *conn) {
	if s.clients == nil {
		return
	}
	if client, ok := s.clients(nodeID); ok {
		client.adopt(c)
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
