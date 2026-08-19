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

func TestClientGeoOps(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "geo", Mode: keyspace.ModeGeo, MaxBytes: 1 << 20, TTL: time.Hour})
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
	if err := cli.GeoAdd(ctx, "geo", "city", []byte("a"), -74, 40.7); err != nil {
		t.Fatal(err)
	}
	lon, lat, ok, err := cli.GeoPos(ctx, "geo", "city", []byte("a"))
	if err != nil || !ok || lon != -74 || lat != 40.7 {
		t.Fatalf("%v %v %v %v", lon, lat, ok, err)
	}
	n, err := cli.GeoCard(ctx, "geo", "city")
	if err != nil || n != 1 {
		t.Fatalf("card %d %v", n, err)
	}
	r, err := cli.GeoRadius(ctx, "geo", "city", -74, 40.7, 1000, 10)
	if err != nil || len(r) != 1 {
		t.Fatalf("radius %+v %v", r, err)
	}
	if err := cli.GeoRem(ctx, "geo", "city", []byte("a")); err != nil {
		t.Fatal(err)
	}
}
