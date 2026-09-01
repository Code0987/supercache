package client_test

import (
	"context"
	"testing"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestClientCMSOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "freq", Mode: keyspace.ModeCMS, MaxBytes: 1 << 20, TTL: time.Hour})
	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Stop()
	cli, err := client.Dial(context.Background(), lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()
	if err := cli.CMSIncr(ctx, "freq", "hot", []byte("t001"), 1); err != nil {
		t.Fatal(err)
	}
	if err := cli.CMSIncr(ctx, "freq", "hot", []byte("t001"), 0); err != nil {
		t.Fatal(err)
	}
	if err := cli.CMSIncr(ctx, "freq", "hot", []byte("t002"), 10); err != nil {
		t.Fatal(err)
	}
	n, ok, err := cli.CMSQuery(ctx, "freq", "hot", []byte("t001"))
	if err != nil || !ok || n < 2 {
		t.Fatalf("t001: %v %v %v", n, ok, err)
	}
	n2, ok, err := cli.CMSQuery(ctx, "freq", "hot", []byte("t002"))
	if err != nil || !ok || n2 < 10 {
		t.Fatalf("t002: %v %v %v", n2, ok, err)
	}
	if err := cli.Delete(ctx, "freq", "hot"); err != nil {
		t.Fatal(err)
	}
	n, ok, err = cli.CMSQuery(ctx, "freq", "hot", []byte("t001"))
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}
