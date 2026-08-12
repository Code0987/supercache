package client_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestClientDialBatchBloomAndErrors(t *testing.T) {
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
	// Unrelated item is allowed to false-positive rarely; assert API works.
	if _, err := cli.BloomTest(ctx, "bf", "f1", []byte("other")); err != nil {
		t.Fatal(err)
	}

	// Missing keyspace surfaces as gRPC/client error (not ErrNotFound).
	if _, err := cli.Get(ctx, "nope", "k"); err == nil {
		t.Fatal("expected get error for missing keyspace")
	}
	if err := cli.Put(ctx, "nope", "k", []byte("v")); err == nil {
		t.Fatal("expected put error for missing keyspace")
	}
	if err := cli.BloomAdd(ctx, "nope", "f", []byte("x")); err == nil {
		t.Fatal("expected bloom add error for missing keyspace")
	}
	if _, err := cli.BloomTest(ctx, "nope", "f", []byte("x")); err == nil {
		t.Fatal("expected bloom test error for missing keyspace")
	}

	// PutMany empty keys → per-key KeyErrors in the response (not a transport error).
	err = cli.PutMany(ctx, "demo", []client.KV{
		{Key: "", Value: []byte("x")},
		{Key: "", Value: []byte("y")},
	})
	if err == nil {
		t.Fatal("expected PutMany key errors")
	}
	kes, ok := err.(client.KeyErrors)
	if !ok {
		t.Fatalf("want KeyErrors, got %T %v", err, err)
	}
	if len(kes) != 2 {
		t.Fatalf("want 2 key errors, got %d (%v)", len(kes), kes)
	}
	if kes.Error() == "" || !errors.As(err, &kes) {
		t.Fatalf("KeyErrors.Error empty or As failed: %v", err)
	}

	// DeleteMany on missing keyspace → KeyErrors for each key.
	err = cli.DeleteMany(ctx, "nope", []string{"k1", "k2"})
	if err == nil {
		t.Fatal("expected DeleteMany errors")
	}
	kes, ok = err.(client.KeyErrors)
	if !ok || len(kes) != 2 {
		t.Fatalf("want KeyErrors len=2, got %T %v", err, err)
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

	// Put only keys we own so Delete is local owner path (not ForwardDelete to down owner).
	var owned string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("own-%d", i)
		if o, ok := eng.OwnerOf(k); ok && o.ID == "a" {
			owned = k
			break
		}
	}
	if owned == "" {
		t.Fatal("no key owned by a")
	}
	if err := cli.Put(ctx, "demo", owned, []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Owner delete fans out to down peer → PeerFailures (not a transport error).
	err = cli.Delete(ctx, "demo", owned)
	if err == nil {
		t.Fatal("expected peer failures when replica is down")
	}
	pf, ok := err.(client.PeerFailures)
	if !ok {
		t.Fatalf("want PeerFailures, got %T: %v", err, err)
	}
	if len(pf) == 0 || pf.Error() == "" {
		t.Fatalf("empty PeerFailures: %+v", pf)
	}
	if pf[0].PeerID == "" || pf[0].Error() == "" {
		t.Fatalf("peer failure detail: %+v", pf[0])
	}

	var owned2 string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("own2-%d", i)
		if o, ok := eng.OwnerOf(k); ok && o.ID == "a" && k != owned {
			owned2 = k
			break
		}
	}
	if err := cli.Put(ctx, "demo", owned2, []byte("v")); err != nil {
		t.Fatal(err)
	}
	err = cli.DeleteMany(ctx, "demo", []string{owned2})
	if err == nil {
		t.Fatal("expected DeleteMany key errors with peer failures")
	}
	kes, ok := err.(client.KeyErrors)
	if !ok || len(kes) == 0 {
		t.Fatalf("want KeyErrors, got %T %v", err, err)
	}
	if len(kes[0].PeerFailures) == 0 {
		t.Fatalf("want peer failures on key error, got %+v", kes[0])
	}
}

func TestClientErrorTypesFormat(t *testing.T) {
	ke := client.KeyError{Key: "k", Message: "msg"}
	if got := ke.Error(); got != `key "k": msg` {
		t.Fatalf("KeyError: %q", got)
	}
	ke.PeerFailures = client.PeerFailures{{PeerID: "p1", Message: "down"}}
	got := ke.Error()
	// Formats as: key "k": msg (1 peer failure(s))
	if !strings.Contains(got, `key "k": msg`) || !strings.Contains(got, "peer failure") {
		t.Fatalf("KeyError with peers: %q", got)
	}
	kes := client.KeyErrors{ke}
	if kes.Error() != "1 key error(s)" {
		t.Fatalf("KeyErrors: %q", kes.Error())
	}
	pf := client.PeerFailure{PeerID: "p", Message: "e"}
	if pf.Error() != "peer p: e" {
		t.Fatalf("PeerFailure: %q", pf.Error())
	}
	pfs := client.PeerFailures{pf}
	if pfs.Error() != "1 peer failure(s)" {
		t.Fatalf("PeerFailures: %q", pfs.Error())
	}
}
