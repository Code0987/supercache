package peerserver_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	peerv1 "github.com/Code0987/supercache/api/gen/peer/v1"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/protect"
)

func startPeer(t *testing.T, eng *engine.Engine) (peerv1.PeerClient, func()) {
	t.Helper()
	gs, lis, err := peerserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gs.Stop()
		t.Fatal(err)
	}
	return peerv1.NewPeerClient(conn), func() {
		_ = conn.Close()
		gs.Stop()
	}
}

func TestPeerApplyForwardGetOrLoad(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour})
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20, NegativeTTL: time.Minute,
		DataSource: datasource.Func(func(_ context.Context, key string) ([]byte, error) {
			if key == "miss" {
				return nil, datasource.ErrNotFound
			}
			return []byte("loaded"), nil
		}),
	})

	cli, stop := startPeer(t, eng)
	defer stop()
	ctx := context.Background()

	// ApplyPut
	ap, err := cli.ApplyPut(ctx, &peerv1.ApplyPutRequest{
		Keyspace: "c", Key: "k",
		Entry:          &peerv1.Entry{Value: []byte("v"), Version: 1},
		RingGeneration: 1,
	})
	if err != nil || !ap.Applied {
		t.Fatalf("ApplyPut: %+v err=%v", ap, err)
	}
	// ApplyPut nil entry still ok (empty)
	_, err = cli.ApplyPut(ctx, &peerv1.ApplyPutRequest{Keyspace: "c", Key: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	// ApplyPut missing keyspace
	_, err = cli.ApplyPut(ctx, &peerv1.ApplyPutRequest{Keyspace: "nope", Key: "k", Entry: &peerv1.Entry{Version: 1}})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing ks: %v", err)
	}

	// ApplyDelete
	ad, err := cli.ApplyDelete(ctx, &peerv1.ApplyDeleteRequest{Keyspace: "c", Key: "k", DeleteVersion: 2})
	if err != nil || !ad.Applied {
		t.Fatalf("ApplyDelete: %+v err=%v", ad, err)
	}

	// ForwardPut with TTL
	_, err = cli.ForwardPut(ctx, &peerv1.ForwardPutRequest{
		Keyspace: "c", Key: "fp", Value: []byte("x"), TtlSet: true, TtlNanos: int64(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	// ForwardPut error (empty key)
	_, err = cli.ForwardPut(ctx, &peerv1.ForwardPutRequest{Keyspace: "c", Key: "", Value: []byte("x")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty key: %v", err)
	}

	// ForwardDelete success
	_, err = cli.ForwardDelete(ctx, &peerv1.ForwardDeleteRequest{Keyspace: "c", Key: "fp"})
	if err != nil {
		t.Fatal(err)
	}

	// GetOrLoad hit
	gol, err := cli.GetOrLoad(ctx, &peerv1.GetOrLoadRequest{Keyspace: "lt", Key: "hit"})
	if err != nil || !gol.Found || string(gol.Entry.Value) != "loaded" {
		t.Fatalf("GetOrLoad hit: %+v err=%v", gol, err)
	}
	// GetOrLoad negative
	gol, err = cli.GetOrLoad(ctx, &peerv1.GetOrLoadRequest{Keyspace: "lt", Key: "miss"})
	if err != nil || gol.Found {
		t.Fatalf("GetOrLoad miss: %+v err=%v", gol, err)
	}
	// GetOrLoad bad keyspace
	_, err = cli.GetOrLoad(ctx, &peerv1.GetOrLoadRequest{Keyspace: "nope", Key: "k"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("gol missing ks: %v", err)
	}

	// Listen bad addr
	if _, _, err := peerserver.ListenAndServe("256.256.256.256:1", eng); err == nil {
		t.Fatal("expected listen error")
	}

	// ApplyDelete invalid key → mapPeerErr InvalidArgument
	_, err = cli.ApplyDelete(ctx, &peerv1.ApplyDeleteRequest{Keyspace: "c", Key: "", DeleteVersion: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ApplyDelete empty: %v", err)
	}
	// ForwardDelete missing keyspace → gRPC NotFound (via grpcmap.Status, not bare error)
	_, err = cli.ForwardDelete(ctx, &peerv1.ForwardDeleteRequest{Keyspace: "nope", Key: "k"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ForwardDelete missing ks: code=%v err=%v", status.Code(err), err)
	}
}

func TestPeerMapErrValueTooLargeUnavailableInternal(t *testing.T) {
	ctx := context.Background()

	// Value too large on ForwardPut → InvalidArgument
	eng2 := engine.New(engine.WithLimits(64, 4, 10))
	defer eng2.Close()
	_ = eng2.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	cli2, stop2 := startPeer(t, eng2)
	defer stop2()
	_, err := cli2.ForwardPut(ctx, &peerv1.ForwardPutRequest{Keyspace: "c", Key: "k", Value: []byte("too-big")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("value too large: %v", err)
	}

	// Unavailable on GetOrLoad via exhausted global protect
	guard := protect.New(protect.Config{RateLimitRPS: 0.0001, Burst: 1})
	_ = guard.AllowContext(ctx)
	eng3 := engine.New(engine.WithGlobalProtect(guard))
	defer eng3.Close()
	_ = eng3.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return []byte("v"), nil
		}),
	})
	cli3, stop3 := startPeer(t, eng3)
	defer stop3()
	_, err = cli3.GetOrLoad(ctx, &peerv1.GetOrLoadRequest{Keyspace: "lt", Key: "k"})
	if err == nil {
		t.Fatal("expected unavailable")
	}
	if c := status.Code(err); c != codes.Unavailable && c != codes.Internal {
		t.Fatalf("code=%v err=%v", c, err)
	}

	// Internal: DataSource error
	eng4 := engine.New()
	defer eng4.Close()
	_ = eng4.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return nil, context.DeadlineExceeded
		}),
	})
	cli4, stop4 := startPeer(t, eng4)
	defer stop4()
	_, err = cli4.GetOrLoad(ctx, &peerv1.GetOrLoadRequest{Keyspace: "lt", Key: "k"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("internal: %v", err)
	}
}

func TestForwardDeleteMultiError(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	r := ring.New(16)
	r.SetPeers([]ring.Peer{
		{ID: "self", Addr: "127.0.0.1:1"},
		{ID: "down", Addr: "127.0.0.1:9"},
	})
	tr := peer.NewTransport(40 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	eng.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r, Transport: tr, Fanout: fo})

	cli, stop := startPeer(t, eng)
	defer stop()
	ctx := context.Background()
	if _, err := cli.ForwardPut(ctx, &peerv1.ForwardPutRequest{Keyspace: "c", Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	resp, err := cli.ForwardDelete(ctx, &peerv1.ForwardDeleteRequest{Keyspace: "c", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	// Local tombstone applied; down peer surfaces in Failures (gRPC OK).
	if len(resp.Failures) == 0 {
		t.Fatal("expected ApplyDelete failure for down peer")
	}
	found := false
	for _, f := range resp.Failures {
		if f.PeerId == "down" || f.Message != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("failures: %+v", resp.Failures)
	}
}
