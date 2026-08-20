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

func TestClientHashOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.HSet(ctx, "hash", "user", []byte("email"), []byte("a@b")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := cli.HGet(ctx, "hash", "user", []byte("email"))
	if err != nil || !ok || string(v) != "a@b" {
		t.Fatalf("%q %v %v", v, ok, err)
	}
	n, err := cli.HLen(ctx, "hash", "user")
	if err != nil || n != 1 {
		t.Fatalf("len %d %v", n, err)
	}
	ex, err := cli.HExists(ctx, "hash", "user", []byte("email"))
	if err != nil || !ex {
		t.Fatalf("exists %v %v", ex, err)
	}
	all, err := cli.HGetAll(ctx, "hash", "user")
	if err != nil || len(all) != 1 || string(all[0].Field) != "email" {
		t.Fatalf("getall %+v %v", all, err)
	}
	if err := cli.HSet(ctx, "hash", "user", []byte("empty"), nil); err != nil {
		t.Fatal(err)
	}
	v, ok, err = cli.HGet(ctx, "hash", "user", []byte("empty"))
	if err != nil || !ok || v == nil || len(v) != 0 {
		t.Fatalf("empty hget %#v ok=%v err=%v", v, ok, err)
	}
	all, err = cli.HGetAll(ctx, "hash", "user")
	if err != nil || len(all) != 2 {
		t.Fatalf("getall after empty %+v %v", all, err)
	}
	var emptyVal []byte
	for _, f := range all {
		if string(f.Field) == "empty" {
			emptyVal = f.Value
		}
	}
	if emptyVal == nil || len(emptyVal) != 0 {
		t.Fatalf("hgetall empty %#v", emptyVal)
	}

	if err := cli.HDel(ctx, "hash", "user", []byte("email")); err != nil {
		t.Fatal(err)
	}
	ex, err = cli.HExists(ctx, "hash", "user", []byte("email"))
	if err != nil || ex {
		t.Fatalf("after del %v %v", ex, err)
	}
}
