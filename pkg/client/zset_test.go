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

func TestClientZOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "zs", Mode: keyspace.ModeZSet, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.ZAdd(ctx, "zs", "lb", []byte("alice"), 10); err != nil {
		t.Fatal(err)
	}
	if err := cli.ZAdd(ctx, "zs", "lb", []byte("bob"), 20); err != nil {
		t.Fatal(err)
	}
	sc, ok, err := cli.ZScore(ctx, "zs", "lb", []byte("alice"))
	if err != nil || !ok || sc != 10 {
		t.Fatalf("score: %v %v %v", sc, ok, err)
	}
	n, err := cli.ZCard(ctx, "zs", "lb")
	if err != nil || n != 2 {
		t.Fatalf("card: %d %v", n, err)
	}
	r, err := cli.ZRange(ctx, "zs", "lb", 0, -1)
	if err != nil || len(r) != 2 || string(r[0].Member) != "alice" {
		t.Fatalf("range: %+v %v", r, err)
	}
	rs, err := cli.ZRangeByScore(ctx, "zs", "lb", 15, 30)
	if err != nil || len(rs) != 1 || string(rs[0].Member) != "bob" {
		t.Fatalf("rangebyscore: %+v %v", rs, err)
	}
	if err := cli.ZRem(ctx, "zs", "lb", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	_, ok, err = cli.ZScore(ctx, "zs", "lb", []byte("alice"))
	if err != nil || ok {
		t.Fatalf("after rem: %v %v", ok, err)
	}
}
