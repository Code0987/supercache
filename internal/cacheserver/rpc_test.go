package cacheserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cachev1 "github.com/Code0987/supercache/api/gen/cache/v1"
	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/protect"
)

func startCache(t *testing.T, eng *engine.Engine) (cachev1.CacheClient, func()) {
	t.Helper()
	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gs.Stop()
		t.Fatal(err)
	}
	return cachev1.NewCacheClient(conn), func() {
		_ = conn.Close()
		gs.Stop()
	}
}

func TestCacheRPCPutGetDeleteBloomAndMapErr(t *testing.T) {
	eng := engine.New(engine.WithLimits(8, 16, 2))
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour})
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20, BloomBits: 1024, BloomHashes: 3,
	})

	cli, stop := startCache(t, eng)
	defer stop()
	ctx := context.Background()

	// Get miss → Found=false (not gRPC NotFound)
	resp, err := cli.Get(ctx, &cachev1.GetRequest{Keyspace: "c", Key: "missing"})
	if err != nil || resp.Found {
		t.Fatalf("miss: found=%v err=%v", resp.GetFound(), err)
	}

	// Put + Get hit
	if _, err := cli.Put(ctx, &cachev1.PutRequest{Keyspace: "c", Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	got, err := cli.Get(ctx, &cachev1.GetRequest{Keyspace: "c", Key: "k"})
	if err != nil || !got.Found || string(got.Value) != "v" {
		t.Fatalf("hit: %+v err=%v", got, err)
	}

	// Put with TTL
	if _, err := cli.Put(ctx, &cachev1.PutRequest{
		Keyspace: "c", Key: "ttl", Value: []byte("t"), TtlSet: true, TtlNanos: int64(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	// PutMany success with TTL
	pm, err := cli.PutMany(ctx, &cachev1.PutManyRequest{
		Keyspace: "c",
		Items:    []*cachev1.KV{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}},
		TtlSet:   true,
		TtlNanos: int64(time.Hour),
	})
	if err != nil || len(pm.Errors) != 0 {
		t.Fatalf("PutMany: %+v err=%v", pm, err)
	}

	// PutMany single key error (key too long)
	pm, err = cli.PutMany(ctx, &cachev1.PutManyRequest{
		Keyspace: "c",
		Items:    []*cachev1.KV{{Key: "toolong!!", Value: []byte("x")}}, // 9 > maxKeyLen 8
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pm.Errors) != 1 || pm.Errors[0].Key != "toolong!!" {
		t.Fatalf("PutMany keyerr: %+v", pm.Errors)
	}

	// PutMany multi key errors (join path); maxBatch=2 so only 2 items.
	pm, err = cli.PutMany(ctx, &cachev1.PutManyRequest{
		Keyspace: "c",
		Items: []*cachev1.KV{
			{Key: "badkey123", Value: []byte("x")}, // 9 > maxKeyLen 8
			{Key: "also-long", Value: []byte("y")}, // 9
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pm.Errors) < 2 {
		t.Fatalf("want multi key errors, got %+v", pm.Errors)
	}

	// PutMany batch too large → gRPC InvalidArgument (mapErr); maxBatch=2
	_, err = cli.PutMany(ctx, &cachev1.PutManyRequest{
		Keyspace: "c",
		Items:    []*cachev1.KV{{Key: "1", Value: []byte("a")}, {Key: "2", Value: []byte("b")}, {Key: "3", Value: []byte("c")}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("batch too large: %v", err)
	}

	// Delete ok
	if _, err := cli.Delete(ctx, &cachev1.DeleteRequest{Keyspace: "c", Key: "k"}); err != nil {
		t.Fatal(err)
	}

	// Delete missing keyspace → NotFound
	_, err = cli.Delete(ctx, &cachev1.DeleteRequest{Keyspace: "nope", Key: "k"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("delete missing ks: %v", err)
	}

	// Get invalid (empty key) → InvalidArgument
	_, err = cli.Get(ctx, &cachev1.GetRequest{Keyspace: "c", Key: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty key: %v", err)
	}

	// Put value too large
	_, err = cli.Put(ctx, &cachev1.PutRequest{Keyspace: "c", Key: "v", Value: make([]byte, 100)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("value too large: %v", err)
	}

	// DeleteMany success
	_, _ = cli.Put(ctx, &cachev1.PutRequest{Keyspace: "c", Key: "d1", Value: []byte("1")})
	dm, err := cli.DeleteMany(ctx, &cachev1.DeleteManyRequest{Keyspace: "c", Keys: []string{"d1"}})
	if err != nil || len(dm.Errors) != 0 {
		t.Fatalf("DeleteMany: %+v err=%v", dm, err)
	}

	// DeleteMany batch too large
	_, err = cli.DeleteMany(ctx, &cachev1.DeleteManyRequest{Keyspace: "c", Keys: []string{"a", "b", "c"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("DeleteMany batch: %v", err)
	}

	// BloomAdd / BloomTest
	if _, err := cli.BloomAdd(ctx, &cachev1.BloomAddRequest{Keyspace: "bf", Name: "f", Item: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	bt, err := cli.BloomTest(ctx, &cachev1.BloomTestRequest{Keyspace: "bf", Name: "f", Item: []byte("x")})
	if err != nil || !bt.Maybe {
		t.Fatalf("BloomTest: %+v err=%v", bt, err)
	}
	// BloomAdd error (empty item)
	_, err = cli.BloomAdd(ctx, &cachev1.BloomAddRequest{Keyspace: "bf", Name: "f", Item: nil})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bloom empty: %v", err)
	}
	// BloomTest wrong mode
	_, err = cli.BloomTest(ctx, &cachev1.BloomTestRequest{Keyspace: "c", Name: "f", Item: []byte("x")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bloom wrong mode: %v", err)
	}

	// ListenAndServe bad address
	if _, _, err := cacheserver.ListenAndServe("256.256.256.256:9999", eng); err == nil {
		t.Fatal("expected listen error")
	}
}

func TestDeleteMultiErrorResponse(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})

	r := ring.New(16)
	r.SetPeers([]ring.Peer{
		{ID: "self", Addr: "127.0.0.1:1"},
		{ID: "down", Addr: "127.0.0.1:9"},
	})
	tr := peer.NewTransport(30 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	eng.SetNodeInfo("self", "127.0.0.1:1")
	eng.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r, Transport: tr, Fanout: fo})

	cli, stop := startCache(t, eng)
	defer stop()
	ctx := context.Background()

	if _, err := cli.Put(ctx, &cachev1.PutRequest{Keyspace: "c", Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	del, err := cli.Delete(ctx, &cachev1.DeleteRequest{Keyspace: "c", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	// Owner delete fans out to down peer → structured PeerFailures (gRPC OK).
	if len(del.PeerFailures) == 0 {
		t.Fatal("expected peer failures when replica is down")
	}
}

func TestGetUnavailableWhenGlobalProtectBlocks(t *testing.T) {
	g := protect.New(protect.Config{
		RateLimitRPS: 0.0001, Burst: 1,
		FailureThreshold: 1, OpenTimeout: time.Hour,
	})
	eng := engine.New(engine.WithGlobalProtect(g))
	defer eng.Close()
	if err := g.AllowContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return []byte("v"), nil
		}),
	})
	cli, stop := startCache(t, eng)
	defer stop()
	_, err := cli.Get(context.Background(), &cachev1.GetRequest{Keyspace: "lt", Key: "k"})
	if err == nil {
		t.Fatal("expected unavailable")
	}
	if c := status.Code(err); c != codes.Unavailable && c != codes.Internal {
		t.Fatalf("code=%v err=%v", c, err)
	}
}

func TestGetKeyspaceNotFoundAfterEngineClose(t *testing.T) {
	eng := engine.New()
	eng.Close()
	cli, stop := startCache(t, eng)
	defer stop()
	_, err := cli.Get(context.Background(), &cachev1.GetRequest{Keyspace: "c", Key: "k"})
	if err == nil {
		t.Fatal("expected error on closed engine")
	}
}

func TestDeleteManyReportsPeerFailures(t *testing.T) {
	eng := engine.New(engine.WithLimits(64, 1024, 10))
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})

	r := ring.New(16)
	r.SetPeers([]ring.Peer{
		{ID: "a", Addr: "127.0.0.1:19191"},
		{ID: "b", Addr: "127.0.0.1:19192"},
	})
	tr := peer.NewTransport(40 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	eng.AttachCluster(&engine.Cluster{SelfID: "a", Ring: r, Transport: tr, Fanout: fo})

	cli, stop := startCache(t, eng)
	defer stop()
	ctx := context.Background()
	// Only put keys owned by "a" so Delete is DeleteAsOwner (not forward to down b).
	var keys []string
	for i := 0; len(keys) < 2 && i < 3000; i++ {
		k := fmt.Sprintf("own-%d", i)
		if o, ok := eng.OwnerOf(k); ok && o.ID == "a" {
			keys = append(keys, k)
			if _, err := cli.Put(ctx, &cachev1.PutRequest{Keyspace: "c", Key: k, Value: []byte("v")}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(keys) < 2 {
		t.Fatal("need two keys owned by a")
	}
	resp, err := cli.DeleteMany(ctx, &cachev1.DeleteManyRequest{Keyspace: "c", Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Errors) == 0 {
		t.Fatal("expected per-key errors when peers are down")
	}
	for _, ke := range resp.Errors {
		if len(ke.PeerFailures) == 0 {
			t.Fatalf("key %q missing peer_failures: %+v", ke.Key, ke)
		}
	}
}
