package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
)

func TestListenKeyspacesAndLoadThrough(t *testing.T) {
	srv, err := Listen("127.0.0.1:0", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if srv.TLS != nil {
		t.Fatal("plaintext server wrote TLS files")
	}

	got := map[string]string{}
	for _, ks := range srv.Engine.KeySpaceSnapshots() {
		got[ks.Name] = ks.Mode
	}
	want := map[string]string{
		"cacheonly": "CacheOnly", "loadthrough": "LoadThrough", "bloom": "Bloom",
		"set": "Set", "zset": "ZSet", "geo": "Geo", "list": "List", "hash": "Hash",
		"counter": "Counter", "json": "JSON", "bitmap": "Bitmap", "hll": "HLL",
		"topk": "TopK", "cms": "CMS", "vectorset": "VectorSet", "stream": "Stream",
	}
	if len(got) != len(want) {
		t.Fatalf("keyspaces: got %d want %d (%v)", len(got), len(want), got)
	}
	for name, mode := range want {
		if got[name] != mode {
			t.Fatalf("keyspace %s: mode %q want %q", name, got[name], mode)
		}
	}

	ctx := context.Background()
	v, err := srv.Engine.Get(ctx, "loadthrough", "seeded")
	if err != nil || string(v) != "from-source" {
		t.Fatalf("loadthrough seeded: %v %q", err, v)
	}
	if _, err := srv.Engine.Get(ctx, "loadthrough", "missing"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing loadthrough key: %v", err)
	}

	cli, err := client.Dial(ctx, srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Put(ctx, "cacheonly", "k", []byte("v"), client.WithTTL(time.Second)); err != nil {
		t.Fatal(err)
	}
	gotV, err := cli.Get(ctx, "cacheonly", "k")
	if err != nil || string(gotV) != "v" {
		t.Fatalf("grpc get: %v %q", err, gotV)
	}
}

func TestListenTLSFiles(t *testing.T) {
	srv, err := Listen("127.0.0.1:0", Options{MTLS: true})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if srv.TLS == nil {
		t.Fatal("missing TLS material")
	}
	for _, path := range []string{srv.TLS.CA, srv.TLS.Cert, srv.TLS.Key, srv.TLS.ClientCert, srv.TLS.ClientKey} {
		st, err := os.Stat(path)
		if err != nil || st.Size() == 0 {
			t.Fatalf("pem %s: %v", path, err)
		}
	}
	raw, err := os.ReadFile(srv.TLS.Cert)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("server pem: %#v", block)
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		t.Fatal(err)
	}
	lines := ReadyLines(srv.Addr, srv.TLS)
	if len(lines) < len("addr=") || lines[:5] != "addr=" {
		t.Fatalf("ready lines: %q", lines)
	}
}

func TestReadyLinesPlaintext(t *testing.T) {
	if got := ReadyLines("127.0.0.1:9", nil); got != "addr=127.0.0.1:9\n" {
		t.Fatalf("got %q", got)
	}
}
