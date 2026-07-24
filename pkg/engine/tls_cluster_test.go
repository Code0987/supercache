package engine_test

import (
	"context"
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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/tlsconfig"
)

func TestClusterPutFanoutOverMTLS(t *testing.T) {
	dir := t.TempDir()
	caCert, caKey := mustCA2(t)
	// Shared server+client identity material per node (same CA).
	leafA, keyA := mustLeaf2(t, caCert, caKey, "a", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	leafB, keyB := mustLeaf2(t, caCert, caKey, "b", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	caFile := writePEM2(t, dir, "ca.pem", "CERTIFICATE", caCert.Raw)
	aCert, aKey := writePEM2(t, dir, "a.pem", "CERTIFICATE", leafA.Raw), writeKey2(t, dir, "a-key.pem", keyA)
	bCert, bKey := writePEM2(t, dir, "b.pem", "CERTIFICATE", leafB.Raw), writeKey2(t, dir, "b-key.pem", keyB)

	srvA, err := tlsconfig.ServerFiles(aCert, aKey, caFile, true)
	if err != nil {
		t.Fatal(err)
	}
	srvB, err := tlsconfig.ServerFiles(bCert, bKey, caFile, true)
	if err != nil {
		t.Fatal(err)
	}
	cliA, err := tlsconfig.ClientFiles(caFile, "localhost", aCert, aKey)
	if err != nil {
		t.Fatal(err)
	}
	cliB, err := tlsconfig.ClientFiles(caFile, "localhost", bCert, bKey)
	if err != nil {
		t.Fatal(err)
	}

	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute})
	}

	gsA, lisA, err := peerserver.ListenAndServe("127.0.0.1:0", engA, grpc.Creds(credentials.NewTLS(srvA)))
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, lisB, err := peerserver.ListenAndServe("127.0.0.1:0", engB, grpc.Creds(credentials.NewTLS(srvB)))
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()

	addrA, addrB := lisA.Addr().String(), lisB.Addr().String()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)

	rA, rB := ring.New(32), ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)

	trA := peer.NewTransport(time.Second, peer.WithTLS(cliA))
	trB := peer.NewTransport(time.Second, peer.WithTLS(cliB))
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("tk-%d", i)
		if err := engA.Put(ctx, "demo", k, []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	var hits int
	for time.Now().Before(deadline) {
		hits = 0
		for i := 0; i < 10; i++ {
			if _, err := engB.Get(ctx, "demo", fmt.Sprintf("tk-%d", i)); err == nil {
				hits++
			}
		}
		if hits >= 5 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	errN, dropN := engA.FanoutStats()
	t.Fatalf("mTLS fan-out hits on B=%d/10; fanout err/drop=%d/%d", hits, errN, dropN)
}

func mustCA2(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

func mustLeaf2(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string, ips []net.IP) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
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
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

func writePEM2(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, _ := os.Create(path)
	defer f.Close()
	_ = pem.Encode(f, &pem.Block{Type: typ, Bytes: der})
	return path
}

func writeKey2(t *testing.T, dir, name string, key *ecdsa.PrivateKey) string {
	t.Helper()
	b, _ := x509.MarshalECPrivateKey(key)
	return writePEM2(t, dir, name, "EC PRIVATE KEY", b)
}
