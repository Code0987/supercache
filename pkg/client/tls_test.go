package client_test

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

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/tlsconfig"
)

func TestClientDialTLSPutGet(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := mustCA(t)
	srvCert, srvKey := mustLeaf(t, caCert, caKey, "server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	caFile := writePEM(t, dir, "ca.pem", "CERTIFICATE", caCert.Raw)
	srvCertFile := writePEM(t, dir, "server.pem", "CERTIFICATE", srvCert.Raw)
	srvKeyFile := writeKey(t, dir, "server-key.pem", srvKey)

	serverTLS, err := tlsconfig.ServerFiles(srvCertFile, srvKeyFile, "", false)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := tlsconfig.ClientFiles(caFile, "localhost", "", "")
	if err != nil {
		t.Fatal(err)
	}

	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute})

	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng, grpc.Creds(credentials.NewTLS(serverTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Stop()

	cli, err := client.DialTLS(context.Background(), lis.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx := context.Background()
	if err := cli.Put(ctx, "demo", "k", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	v, err := cli.Get(ctx, "demo", "k")
	if err != nil || string(v) != "secret" {
		t.Fatalf("get: %v %s", err, v)
	}

	// Plaintext dial against TLS server must fail on RPC (Dial is lazy).
	plain, err := client.Dial(context.Background(), lis.Addr().String())
	if err != nil {
		return
	}
	defer plain.Close()
	if err := plain.Put(ctx, "demo", "x", []byte("y")); err == nil {
		t.Fatal("expected plaintext client to fail against TLS server")
	}
}

func mustCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true,
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
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames: dns, IPAddresses: ips,
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
