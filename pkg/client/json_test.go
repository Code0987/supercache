package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestClientJSONOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "doc", Mode: keyspace.ModeJSON, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.JsonSet(ctx, "doc", "user", "$", []byte(`{"name":"Ada"}`)); err != nil {
		t.Fatal(err)
	}
	v, ok, err := cli.JsonGet(ctx, "doc", "user", "$.name")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`"Ada"`)) {
		t.Fatalf("%q %v %v", v, ok, err)
	}
	if err := cli.JsonDel(ctx, "doc", "user", "$.name"); err != nil {
		t.Fatal(err)
	}
	v, ok, err = cli.JsonGet(ctx, "doc", "user", "$")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`{}`)) {
		t.Fatalf("after del: %s %v %v", v, ok, err)
	}
	if err := cli.Delete(ctx, "doc", "user"); err != nil {
		t.Fatal(err)
	}
	_, ok, err = cli.JsonGet(ctx, "doc", "user", "$")
	if err != nil || ok {
		t.Fatalf("after Delete: %v %v", ok, err)
	}
}
