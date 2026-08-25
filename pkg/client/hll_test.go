package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestClientHLLOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "uniq", Mode: keyspace.ModeHLL, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.HLLAdd(ctx, "uniq", "visitors", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	if err := cli.HLLAdd(ctx, "uniq", "visitors", []byte("bob")); err != nil {
		t.Fatal(err)
	}
	n, ok, err := cli.HLLCount(ctx, "uniq", "visitors")
	if err != nil || !ok || n < 1 || n > 4 {
		t.Fatalf("%v %v %v", n, ok, err)
	}
	if err := cli.Delete(ctx, "uniq", "visitors"); err != nil {
		t.Fatal(err)
	}
	n, ok, err = cli.HLLCount(ctx, "uniq", "visitors")
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}
