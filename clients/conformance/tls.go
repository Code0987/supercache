package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func writeTLS() (*TLSMaterial, error) {
	dir, err := os.MkdirTemp("", "supercache-conformance-")
	if err != nil {
		return nil, err
	}
	caCert, caKey, err := newCA()
	if err != nil {
		return nil, err
	}
	srvCert, srvKey, err := newLeaf(caCert, caKey, "localhost", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, err
	}
	cliCert, cliKey, err := newLeaf(caCert, caKey, "client", nil, nil)
	if err != nil {
		return nil, err
	}
	caPath, err := writePEM(filepath.Join(dir, "ca.pem"), "CERTIFICATE", caCert.Raw)
	if err != nil {
		return nil, err
	}
	certPath, err := writePEM(filepath.Join(dir, "server.pem"), "CERTIFICATE", srvCert.Raw)
	if err != nil {
		return nil, err
	}
	keyPath, err := writeKey(filepath.Join(dir, "server-key.pem"), srvKey)
	if err != nil {
		return nil, err
	}
	clientCert, err := writePEM(filepath.Join(dir, "client.pem"), "CERTIFICATE", cliCert.Raw)
	if err != nil {
		return nil, err
	}
	clientKey, err := writeKey(filepath.Join(dir, "client-key.pem"), cliKey)
	if err != nil {
		return nil, err
	}
	return &TLSMaterial{
		Dir: dir, CA: caPath, Cert: certPath, Key: keyPath,
		ClientCert: clientCert, ClientKey: clientKey,
	}, nil
}

func newCA() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "supercache-conformance-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func newLeaf(ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func writePEM(path, typ string, der []byte) (string, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		return "", err
	}
	return path, nil
}

func writeKey(path string, key *ecdsa.PrivateKey) (string, error) {
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", fmt.Errorf("marshal key: %w", err)
	}
	return writePEM(path, "EC PRIVATE KEY", b)
}
