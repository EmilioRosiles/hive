package transport

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/tlstest"
)

// startTestServerTLS mirrors startTestServer but binds over TLS.
func startTestServerTLS(t *testing.T, handler Handler, tlsConfig *tls.Config) *Server {
	t.Helper()
	s, err := NewServer("127.0.0.1:0", handler, tlsConfig)
	if err != nil {
		t.Fatalf("NewServer (TLS): %v", err)
	}
	go s.Serve()
	t.Cleanup(func() { s.Close() })
	return s
}

// TestServer_TLS_RoundTrip proves NewServer/NewClient correctly wire a
// supplied *tls.Config into tls.Listen/tls.DialWithDialer — the verification
// policy itself (chain-only, skip hostname) belongs to and is tested in the
// top-level hive package, since it's a property of the *tls.Config the
// caller builds, not of this transport layer.
func TestServer_TLS_RoundTrip(t *testing.T) {
	_, certPEM, keyPEM := tlstest.NewPair(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	handler, _ := echoHandler(t, map[MsgType][]byte{MsgForward: []byte("ok")})
	s := startTestServerTLS(t, handler, &tls.Config{Certificates: []tls.Certificate{cert}})

	client := NewClient(s.Addr().String(), &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true, // this test only exercises wiring, not verification policy
	}, 1)
	defer client.Close()

	resp, err := client.Send(context.Background(), Frame{Type: MsgForward, Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if string(resp.Payload) != "ok" {
		t.Errorf("got %q, want %q", resp.Payload, "ok")
	}
}

// TestClient_PlaintextDial_RejectedByTLSServer confirms a plaintext client
// talking to a TLS-only server fails cleanly rather than hanging or
// silently succeeding.
func TestClient_PlaintextDial_RejectedByTLSServer(t *testing.T) {
	_, certPEM, keyPEM := tlstest.NewPair(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	handler, _ := echoHandler(t, nil)
	s := startTestServerTLS(t, handler, &tls.Config{Certificates: []tls.Certificate{cert}})

	client := NewClient(s.Addr().String(), nil, 1)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Send(ctx, Frame{Type: MsgForward, Payload: []byte("hi")}); err == nil {
		t.Error("expected plaintext Send to a TLS server to fail")
	}
}
