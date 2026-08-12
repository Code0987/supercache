package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestClientCoverageEdges(t *testing.T) {
	eng := engine.New(engine.WithLimits(16, 1024, 10))
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute})
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20, BloomBits: 1024, BloomHashes: 3,
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

	// Close nil client
	var nilCli *client.Client
	if err := nilCli.Close(); err != nil {
		t.Fatal(err)
	}

	// DialTLS nil config
	if _, err := client.DialTLS(ctx, lis.Addr().String(), nil); err == nil {
		t.Fatal("DialTLS nil tls")
	}

	// PutMany with TTL
	if err := cli.PutMany(ctx, "demo", []client.KV{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}, client.WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}

	// DeleteMany success
	if err := cli.DeleteMany(ctx, "demo", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}

	// BloomAdd / BloomTest
	if err := cli.BloomAdd(ctx, "bf", "f1", []byte("item")); err != nil {
		t.Fatal(err)
	}
	ok, err := cli.BloomTest(ctx, "bf", "f1", []byte("item"))
	if err != nil || !ok {
		t.Fatalf("BloomTest: ok=%v err=%v", ok, err)
	}
	ok, err = cli.BloomTest(ctx, "bf", "f1", []byte("other"))
	if err != nil {
		t.Fatal(err)
	}
	_ = ok // may be false (no false positive guaranteed for one item)

	// Error paths: missing keyspace
	if _, err := cli.Get(ctx, "nope", "k"); err == nil {
		t.Fatal("expected get error")
	}
	if err := cli.Put(ctx, "nope", "k", []byte("v")); err == nil {
		t.Fatal("expected put error")
	}
	if err := cli.BloomAdd(ctx, "nope", "f", []byte("x")); err == nil {
		t.Fatal("expected bloom add error")
	}
	if _, err := cli.BloomTest(ctx, "nope", "f", []byte("x")); err == nil {
		t.Fatal("expected bloom test error")
	}

	// PutMany key errors (batch limits via oversized key after WithLimits on a new engine)
	// Use invalid empty key items — engine returns invalid argument per key.
	err = cli.PutMany(ctx, "demo", []client.KV{
		{Key: "", Value: []byte("x")},
		{Key: "", Value: []byte("y")},
	})
	if err == nil {
		t.Fatal("expected PutMany key errors")
	}
	var kes client.KeyErrors
	if !errors.As(err, &kes) {
		// KeyErrors is a slice type implementing error — As may need pointer
		if ke, ok := err.(client.KeyErrors); ok {
			kes = ke
		} else {
			t.Fatalf("want KeyErrors, got %T %v", err, err)
		}
	}
	_ = kes.Error()
	if len(kes) > 0 {
		_ = kes[0].Error()
	}

	// DeleteMany key errors on missing keyspace
	err = cli.DeleteMany(ctx, "nope", []string{"k1", "k2"})
	if err == nil {
		// engine Delete on missing ks returns ErrKeyspaceNotFound per key → KeyErrors
		t.Fatal("expected DeleteMany errors")
	}
	if ke, ok := err.(client.KeyErrors); ok {
		_ = ke.Error()
		if len(ke) > 0 {
			_ = ke[0].Error()
		}
	}
}

func TestClientPeerFailures(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})

	r := ring.New(16)
	r.SetPeers([]ring.Peer{
		{ID: "a", Addr: "127.0.0.1:19201"},
		{ID: "b", Addr: "127.0.0.1:19202"}, // down
	})
	tr := peer.NewTransport(50 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	eng.AttachCluster(&engine.Cluster{SelfID: "a", Ring: r, Transport: tr, Fanout: fo})

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

	_ = cli.Put(ctx, "demo", "k", []byte("v"))
	err = cli.Delete(ctx, "demo", "k")
	if err != nil {
		// PeerFailures from Delete
		if pf, ok := err.(client.PeerFailures); ok {
			_ = pf.Error()
			if len(pf) > 0 {
				_ = pf[0].Error()
			}
		} else {
			t.Logf("Delete err type %T: %v", err, err)
		}
	}

	_ = cli.Put(ctx, "demo", "k2", []byte("v"))
	err = cli.DeleteMany(ctx, "demo", []string{"k2"})
	if err != nil {
		if kes, ok := err.(client.KeyErrors); ok {
			_ = kes.Error()
			for _, ke := range kes {
				_ = ke.Error() // may include PeerFailures branch
			}
		}
	}

	// Force KeyError.Error with PeerFailures via type construction is internal;
	// DeleteMany with peer failures should populate PeerFailures on KeyError.
}

func TestClientErrorStringHelpers(t *testing.T) {
	// Exercise Error() methods without RPC.
	ke := client.KeyError{Key: "k", Message: "msg"}
	if ke.Error() == "" {
		t.Fatal("KeyError")
	}
	ke.PeerFailures = client.PeerFailures{{PeerID: "p1", Message: "down"}}
	if ke.Error() == "" {
		t.Fatal("KeyError with peers")
	}
	kes := client.KeyErrors{ke}
	if kes.Error() == "" {
		t.Fatal("KeyErrors")
	}
	pf := client.PeerFailure{PeerID: "p", Message: "e"}
	if pf.Error() == "" {
		t.Fatal("PeerFailure")
	}
	pfs := client.PeerFailures{pf}
	if pfs.Error() == "" {
		t.Fatal("PeerFailures")
	}
}
