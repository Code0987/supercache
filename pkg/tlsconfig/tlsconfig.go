// Package tlsconfig builds crypto/tls configs for SuperCache Cache and Peer gRPC.
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerFiles loads a server TLS config from PEM files.
//
//   - certFile/keyFile: server certificate and private key (required)
//   - clientCAFile: optional CA PEM to verify client certs (peer mTLS). Empty = no client auth.
//   - requireClientCert: when clientCAFile is set, require and verify client certificates.
func ServerFiles(certFile, keyFile, clientCAFile string, requireClientCert bool) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("tlsconfig: cert and key file paths are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: load key pair: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if clientCAFile != "" {
		pool, err := loadCertPool(clientCAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		if requireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return cfg, nil
}

// ClientFiles loads a client TLS config from PEM files.
//
//   - caFile: CA used to verify the server (required for secure dial)
//   - serverName: expected server name (SNI / cert verification); required unless InsecureSkipVerify
//   - certFile/keyFile: optional client certificate for mTLS
func ClientFiles(caFile, serverName, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" {
		return nil, fmt.Errorf("tlsconfig: ca file is required")
	}
	pool, err := loadCertPool(caFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
		ServerName: serverName,
	}
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("tlsconfig: client cert and key must both be set for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("tlsconfig: load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: read CA %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tlsconfig: no certificates parsed from CA file %s", caFile)
	}
	return pool, nil
}
