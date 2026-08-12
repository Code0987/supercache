package tlsconfig_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/tlsconfig"
)

func TestServerAndClientFilesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := mustCA(t)
	srvCert, srvKey := mustLeaf(t, caCert, caKey, "server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	cliCert, cliKey := mustLeaf(t, caCert, caKey, "client", nil, nil)

	caFile := writePEM(t, dir, "ca.pem", "CERTIFICATE", caCert.Raw)
	srvCertFile := writePEM(t, dir, "server.pem", "CERTIFICATE", srvCert.Raw)
	srvKeyFile := writeKey(t, dir, "server-key.pem", srvKey)
	cliCertFile := writePEM(t, dir, "client.pem", "CERTIFICATE", cliCert.Raw)
	cliKeyFile := writeKey(t, dir, "client-key.pem", cliKey)

	serverTLS, err := tlsconfig.ServerFiles(srvCertFile, srvKeyFile, caFile, true)
	if err != nil {
		t.Fatal(err)
	}
	if serverTLS.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth=%v", serverTLS.ClientAuth)
	}

	clientTLS, err := tlsconfig.ClientFiles(caFile, "localhost", cliCertFile, cliKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(clientTLS.Certificates) != 1 {
		t.Fatal("expected client cert for mTLS")
	}

	// Handshake through real TCP.
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer c.Close()
		// Force handshake.
		if tc, ok := c.(*tls.Conn); ok {
			errCh <- tc.Handshake()
			return
		}
		errCh <- nil
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
}

func TestServerFilesRequiresCertKey(t *testing.T) {
	if _, err := tlsconfig.ServerFiles("", "k", "", false); err == nil {
		t.Fatal("expected error")
	}
}

func TestClientFilesRequiresCA(t *testing.T) {
	if _, err := tlsconfig.ClientFiles("", "localhost", "", ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestMTLSRejectsMissingClientCert(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := mustCA(t)
	srvCert, srvKey := mustLeaf(t, caCert, caKey, "server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	caFile := writePEM(t, dir, "ca.pem", "CERTIFICATE", caCert.Raw)
	srvCertFile := writePEM(t, dir, "server.pem", "CERTIFICATE", srvCert.Raw)
	srvKeyFile := writeKey(t, dir, "server-key.pem", srvKey)

	serverTLS, err := tlsconfig.ServerFiles(srvCertFile, srvKeyFile, caFile, true)
	if err != nil {
		t.Fatal(err)
	}
	// Client with CA but no client cert.
	clientTLS, err := tlsconfig.ClientFiles(caFile, "localhost", "", "")
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srvErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			srvErr <- err
			return
		}
		defer c.Close()
		if tc, ok := c.(*tls.Conn); ok {
			srvErr <- tc.Handshake()
			return
		}
		srvErr <- nil
	}()

	var clientHS error
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		clientHS = err
	} else {
		clientHS = conn.Handshake()
		_ = conn.Close()
	}
	serverHS := <-srvErr
	if clientHS == nil && serverHS == nil {
		t.Fatal("expected mTLS handshake failure when client cert is missing")
	}
}

func mustCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func mustLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
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
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeKey(t *testing.T, dir, name string, key *ecdsa.PrivateKey) string {
	t.Helper()
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return writePEM(t, dir, name, "EC PRIVATE KEY", b)
}

func TestServerClientFilesRejectBadPEMs(t *testing.T) {
	dir := t.TempDir()
	// bad key pair paths
	if _, err := tlsconfig.ServerFiles(filepath.Join(dir, "missing.pem"), filepath.Join(dir, "missing-key.pem"), "", false); err == nil {
		t.Fatal("missing cert")
	}
	// invalid CA file
	badCA := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	// need real cert/key for ServerFiles past LoadX509KeyPair
	caCert, caKey := mustCA(t)
	srvCert, srvKey := mustLeaf(t, caCert, caKey, "server", nil, nil)
	srvCertFile := writePEM(t, dir, "s.pem", "CERTIFICATE", srvCert.Raw)
	srvKeyFile := writeKey(t, dir, "s-key.pem", srvKey)

	if _, err := tlsconfig.ServerFiles(srvCertFile, srvKeyFile, badCA, true); err == nil {
		t.Fatal("bad CA")
	}
	// client CA with requireClientCert=false → VerifyClientCertIfGiven
	caFile := writePEM(t, dir, "ca.pem", "CERTIFICATE", caCert.Raw)
	cfg, err := tlsconfig.ServerFiles(srvCertFile, srvKeyFile, caFile, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("ClientAuth=%v", cfg.ClientAuth)
	}

	// ClientFiles bad CA
	if _, err := tlsconfig.ClientFiles(badCA, "localhost", "", ""); err == nil {
		t.Fatal("bad client CA")
	}
	// ClientFiles only cert without key
	if _, err := tlsconfig.ClientFiles(caFile, "localhost", srvCertFile, ""); err == nil {
		t.Fatal("cert without key")
	}
	// ClientFiles bad key pair
	if _, err := tlsconfig.ClientFiles(caFile, "localhost", srvCertFile, filepath.Join(dir, "nope.pem")); err == nil {
		t.Fatal("bad client key pair")
	}
	// missing CA path
	if _, err := tlsconfig.ClientFiles(filepath.Join(dir, "no-ca.pem"), "h", "", ""); err == nil {
		t.Fatal("missing ca file")
	}
}
