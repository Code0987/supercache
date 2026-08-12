package peer_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/tlsconfig"
)

func TestTransportWithTLSDial(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := mustCA(t)
	leaf, leafKey := mustLeaf(t, ca, caKey, "peer", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	caFile := writePEM(t, dir, "ca.pem", "CERTIFICATE", ca.Raw)
	certFile := writePEM(t, dir, "s.pem", "CERTIFICATE", leaf.Raw)
	keyFile := writeKey(t, dir, "s-key.pem", leafKey)

	srvTLS, err := tlsconfig.ServerFiles(certFile, keyFile, caFile, true)
	if err != nil {
		t.Fatal(err)
	}
	cliTLS, err := tlsconfig.ClientFiles(caFile, "localhost", certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	// Bind :0
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvTLS)))
	// peerserver.ListenAndServe doesn't take existing lis easily — use NewServer + Serve
	// Actually use ListenAndServe with fixed after get addr... ListenAndServe takes addr string.
	// Use lis.Addr after manual serve:
	// Simpler: ListenAndServe on 127.0.0.1:0 via peerserver
	gs.Stop()
	_ = lis.Close()

	gs2, lis2, err := peerserver.ListenAndServe("127.0.0.1:0", eng, grpc.Creds(credentials.NewTLS(srvTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer gs2.Stop()
	addr := lis2.Addr().String()

	tr := peer.NewTransport(500*time.Millisecond, peer.WithTLS(cliTLS))
	defer tr.Close()
	// ServerName empty → splitHostPort sets SNI from addr host.
	ok, err := tr.ApplyPut(context.Background(), addr, "demo", "tls-k",
		store.Entry{Value: []byte("v"), Version: 1}, 1)
	if err != nil || !ok {
		t.Fatalf("TLS ApplyPut: ok=%v err=%v", ok, err)
	}
	if !eng.HasLocal("demo", "tls-k") {
		t.Fatal("missing after TLS put")
	}
	// Reuse conn path (client pool hit).
	_, err = tr.ApplyPut(context.Background(), addr, "demo", "tls-k2",
		store.Entry{Value: []byte("v2"), Version: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
}

func mustCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

func mustLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames: dns, IPAddresses: ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = pem.Encode(f, &pem.Block{Type: typ, Bytes: der})
	_ = f.Close()
	return path
}

func writeKey(t *testing.T, dir, name string, key *ecdsa.PrivateKey) string {
	t.Helper()
	path := filepath.Join(dir, name)
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = pem.Encode(f, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	_ = f.Close()
	return path
}
