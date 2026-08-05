package hive

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// NewClusterTLSConfig builds a *tls.Config implementing mutual TLS for
// cluster-mode peer connections: every node presents certPEM/keyPEM and
// verifies its peer's certificate chains to caPEM, but hostname/IP matching
// is skipped — peer addresses are dynamic (IP:port), not stable DNS names,
// so trust comes from "signed by our cluster CA," not "hostname matches."
// This is the same pattern used by etcd and Consul for peer TLS.
//
// For full control (custom verification, hot cert rotation via
// GetCertificate/GetClientCertificate), build your own *tls.Config instead.
func NewClusterTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("hive: parse cert/key: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("hive: parse CA certificate")
	}
	return &tls.Config{
		Certificates:          []tls.Certificate{cert},
		RootCAs:               pool,
		ClientCAs:             pool,
		ClientAuth:            tls.RequireAndVerifyClientCert,
		InsecureSkipVerify:    true, // client-role default hostname check disabled; see verifyChainOnly
		VerifyPeerCertificate: verifyChainOnly(pool),
		MinVersion:            tls.VersionTLS12,
	}, nil
}

// verifyChainOnly returns a VerifyPeerCertificate callback that checks the
// peer's certificate chains to a trusted root in pool, without matching
// hostname/IP (x509.VerifyOptions.DNSName is intentionally left empty).
func verifyChainOnly(pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("hive: no peer certificate presented")
		}
		certs := make([]*x509.Certificate, len(rawCerts))
		for i, raw := range rawCerts {
			c, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("hive: parse peer certificate: %w", err)
			}
			certs[i] = c
		}
		intermediates := x509.NewCertPool()
		for _, c := range certs[1:] {
			intermediates.AddCert(c)
		}
		_, err := certs[0].Verify(x509.VerifyOptions{
			Roots:         pool,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			// DNSName intentionally left empty: verify the chain, skip hostname/IP matching.
		})
		return err
	}
}
