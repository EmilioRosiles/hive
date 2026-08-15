package hive

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	"github.com/EmilioRosiles/hive/internal/tlstest"
)

func TestNewClusterTLSConfig_ValidInputs(t *testing.T) {
	caPEM, certPEM, keyPEM := tlstest.NewPair(t)
	cfg, err := NewClusterTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
}

func TestNewClusterTLSConfig_MalformedCert(t *testing.T) {
	caPEM, _, keyPEM := tlstest.NewPair(t)
	if _, err := NewClusterTLSConfig([]byte("not a cert"), keyPEM, caPEM); err == nil {
		t.Error("expected error for malformed cert PEM")
	}
}

func TestNewClusterTLSConfig_MalformedKey(t *testing.T) {
	caPEM, certPEM, _ := tlstest.NewPair(t)
	if _, err := NewClusterTLSConfig(certPEM, []byte("not a key"), caPEM); err == nil {
		t.Error("expected error for malformed key PEM")
	}
}

func TestNewClusterTLSConfig_MalformedCA(t *testing.T) {
	_, certPEM, keyPEM := tlstest.NewPair(t)
	if _, err := NewClusterTLSConfig(certPEM, keyPEM, []byte("not a ca cert")); err == nil {
		t.Error("expected error for malformed CA PEM")
	}
}

// tlsListenAndServe starts a raw tls.Listener with cfg, reading one byte
// from each accepted connection to force the (lazily-deferred) server-side
// handshake to complete. Testing NewClusterTLSConfig's verification
// behavior directly via crypto/tls, independent of internal/transport,
// isolates this from anything transport-layer-specific.
func tlsListenAndServe(t *testing.T, cfg *tls.Config) net.Listener {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 1)
				conn.Read(buf)
			}()
		}
	}()
	return ln
}

// TestClusterTLS_ChainValid_HostnameMismatch_Succeeds proves the "skip
// hostname/IP matching" half of the design: tlstest certs carry no IP/DNS
// SANs at all, and the dial target is a bare IP, so default Go verification
// would reject this on hostname grounds alone.
func TestClusterTLS_ChainValid_HostnameMismatch_Succeeds(t *testing.T) {
	caPEM, certPEM, keyPEM := tlstest.NewPair(t)
	serverCfg, err := NewClusterTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig (server): %v", err)
	}
	ln := tlsListenAndServe(t, serverCfg)

	clientCfg, err := NewClusterTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig (client): %v", err)
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("expected chain-valid connection to succeed despite hostname mismatch, got: %v", err)
	}
	conn.Close()
}

// TestClusterTLS_WrongCA_Rejected proves chain verification still actually
// happens despite InsecureSkipVerify being set — the one place a mistake in
// this pattern would be catastrophic and silent.
func TestClusterTLS_WrongCA_Rejected(t *testing.T) {
	caPEM, certPEM, keyPEM := tlstest.NewPair(t)
	serverCfg, err := NewClusterTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig (server): %v", err)
	}
	ln := tlsListenAndServe(t, serverCfg)

	// A second, unrelated CA/cert pair — not signed by the server's trusted CA.
	otherCAPEM, otherCertPEM, otherKeyPEM := tlstest.NewPair(t)
	clientCfg, err := NewClusterTLSConfig(otherCertPEM, otherKeyPEM, otherCAPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig (client): %v", err)
	}
	if _, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", ln.Addr().String(), clientCfg); err == nil {
		t.Fatal("expected connection with an untrusted CA to be rejected")
	}
}

// TestClusterTLS_NoClientCert_Rejected proves mTLS is actually enforced, not
// just available — a client presenting no certificate must be rejected by a
// server configured with RequireAndVerifyClientCert.
func TestClusterTLS_NoClientCert_Rejected(t *testing.T) {
	caPEM, certPEM, keyPEM := tlstest.NewPair(t)
	serverCfg, err := NewClusterTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("NewClusterTLSConfig (server): %v", err)
	}
	ln := tlsListenAndServe(t, serverCfg)

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	clientCfg := &tls.Config{RootCAs: pool, InsecureSkipVerify: true} // no Certificates set

	conn, dialErr := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", ln.Addr().String(), clientCfg)
	if dialErr != nil {
		// Rejected during the handshake itself — the strongest form of this
		// guarantee.
		return
	}
	defer conn.Close()
	// TLS 1.3 clients can complete their side of the handshake before the
	// server has processed and rejected the (missing) client certificate —
	// the rejection surfaces on a subsequent I/O, not necessarily from Dial
	// itself. Force one and confirm it fails.
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, writeErr := conn.Write([]byte("x"))
	var readErr error
	if writeErr == nil {
		buf := make([]byte, 1)
		_, readErr = conn.Read(buf)
	}
	if writeErr == nil && readErr == nil {
		t.Fatal("expected server to reject a client presenting no certificate")
	}
}
