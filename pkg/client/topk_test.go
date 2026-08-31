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

func TestClientTopKOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "plays", Mode: keyspace.ModeTopK, MaxBytes: 1 << 20, TTL: time.Hour, TopKSize: 10})
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
	if err := cli.TopKAdd(ctx, "plays", "hot", []byte("t001")); err != nil {
		t.Fatal(err)
	}
	if err := cli.TopKAdd(ctx, "plays", "hot", []byte("t002")); err != nil {
		t.Fatal(err)
	}
	if err := cli.TopKAdd(ctx, "plays", "hot", []byte("t001")); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := cli.TopKList(ctx, "plays", "hot")
	if err != nil || !ok || len(rows) != 2 {
		t.Fatalf("%v ok=%v err=%v", rows, ok, err)
	}
	if string(rows[0].Item) != "t001" || rows[0].Count != 2 {
		t.Fatalf("head %+v", rows[0])
	}
	if err := cli.Delete(ctx, "plays", "hot"); err != nil {
		t.Fatal(err)
	}
	rows, ok, err = cli.TopKList(ctx, "plays", "hot")
	if err != nil || ok || rows != nil {
		t.Fatal(rows, ok, err)
	}
}
