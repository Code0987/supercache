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

func TestClientSetOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "st", Mode: keyspace.ModeSet, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.SetAdd(ctx, "st", "s", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ok, err := cli.SetContains(ctx, "st", "s", []byte("a"))
	if err != nil || !ok {
		t.Fatalf("contains: %v %v", ok, err)
	}
	n, err := cli.SetCard(ctx, "st", "s")
	if err != nil || n != 1 {
		t.Fatalf("card: %d %v", n, err)
	}
	mem, err := cli.SetMembers(ctx, "st", "s")
	if err != nil || len(mem) != 1 {
		t.Fatalf("members: %v %v", mem, err)
	}
	if err := cli.SetRemove(ctx, "st", "s", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ok, err = cli.SetContains(ctx, "st", "s", []byte("a"))
	if err != nil || ok {
		t.Fatalf("after rem: %v %v", ok, err)
	}
}
