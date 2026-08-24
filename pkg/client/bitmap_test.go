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

func TestClientBitmapOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "flags", Mode: keyspace.ModeBitmap, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.BitSet(ctx, "flags", "seen", 0, true); err != nil {
		t.Fatal(err)
	}
	if err := cli.BitSet(ctx, "flags", "seen", 8, true); err != nil {
		t.Fatal(err)
	}
	bit, ok, err := cli.BitGet(ctx, "flags", "seen", 0)
	if err != nil || !ok || !bit {
		t.Fatalf("%v %v %v", bit, ok, err)
	}
	n, err := cli.BitCount(ctx, "flags", "seen", 0, -1)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	pos, found, err := cli.BitPos(ctx, "flags", "seen", true, 1, 1)
	if err != nil || !found || pos != 8 {
		t.Fatal(pos, found, err)
	}
	if err := cli.Delete(ctx, "flags", "seen"); err != nil {
		t.Fatal(err)
	}
	_, ok, err = cli.BitGet(ctx, "flags", "seen", 0)
	if err != nil || ok {
		t.Fatal(ok, err)
	}
}
