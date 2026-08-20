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

func TestClientCounterOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "ctr", Mode: keyspace.ModeCounter, MaxBytes: 1 << 20, TTL: time.Hour})
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
	n, err := cli.Incr(ctx, "ctr", "hits", 2)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	v, ok, err := cli.CounterGet(ctx, "ctr", "hits")
	if err != nil || !ok || v != 2 {
		t.Fatal(v, ok, err)
	}
	if err := cli.Delete(ctx, "ctr", "hits"); err != nil {
		t.Fatal(err)
	}
	_, ok, err = cli.CounterGet(ctx, "ctr", "hits")
	if err != nil || ok {
		t.Fatal(ok, err)
	}
}
