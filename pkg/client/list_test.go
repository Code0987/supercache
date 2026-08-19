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

func TestClientListOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "ls", Mode: keyspace.ModeList, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.RPush(ctx, "ls", "q", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := cli.LPush(ctx, "ls", "q", []byte("z")); err != nil {
		t.Fatal(err)
	}
	n, err := cli.LLen(ctx, "ls", "q")
	if err != nil || n != 2 {
		t.Fatalf("len %d %v", n, err)
	}
	r, err := cli.LRange(ctx, "ls", "q", 0, -1)
	if err != nil || len(r) != 2 || string(r[0]) != "z" {
		t.Fatalf("%q %v", r, err)
	}
	it, ok, err := cli.LPop(ctx, "ls", "q")
	if err != nil || !ok || string(it) != "z" {
		t.Fatalf("%s %v %v", it, ok, err)
	}
}
