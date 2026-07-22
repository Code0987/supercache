package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestClientGetPutDelete(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute,
	})
	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Stop()

	ctx := context.Background()
	cli, err := client.Dial(ctx, lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if _, err := cli.Get(ctx, "demo", "k"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	if err := cli.Put(ctx, "demo", "k", []byte("v"), client.WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	v, err := cli.Get(ctx, "demo", "k")
	if err != nil || string(v) != "v" {
		t.Fatalf("get: %v %s", err, v)
	}
	if err := cli.Delete(ctx, "demo", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Get(ctx, "demo", "k"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestClientPutMany(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Stop()
	ctx := context.Background()
	cli, err := client.Dial(ctx, lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.PutMany(ctx, "demo", []client.KV{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}}); err != nil {
		t.Fatal(err)
	}
	v, _ := cli.Get(ctx, "demo", "a")
	if string(v) != "1" {
		t.Fatalf("got %s", v)
	}
}
