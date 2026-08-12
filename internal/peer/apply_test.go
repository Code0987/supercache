package peer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// realPeer starts an engine + peer gRPC listener for unit apply/hint tests.
func realPeer(t *testing.T, id, addr string) (*engine.Engine, *peer.Transport, func()) {
	t.Helper()
	eng := engine.New()
	eng.SetNodeInfo(id, addr)
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	gs, _, err := peerserver.ListenAndServe(addr, eng)
	if err != nil {
		t.Fatal(err)
	}
	tr := peer.NewTransport(200 * time.Millisecond)
	cleanup := func() {
		gs.Stop()
		tr.Close()
		eng.Close()
	}
	return eng, tr, cleanup
}

func TestApplyPutAndDeleteAgainstPeer(t *testing.T) {
	const addrB = "127.0.0.1:19801"
	engB, _, stopB := realPeer(t, "b", addrB)
	defer stopB()

	trA := peer.NewTransport(200 * time.Millisecond)
	defer trA.Close()
	fo := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 16, DisableHints: true})
	defer fo.Close()

	p := ring.Peer{ID: "b", Addr: addrB}
	ent := store.Entry{Value: []byte("v"), Version: 3}
	fails := fo.Apply(context.Background(), []ring.Peer{p}, "demo", "k", ent, 1)
	if len(fails) != 0 {
		t.Fatalf("Apply put: %+v", fails)
	}
	if !engB.HasLocal("demo", "k") {
		t.Fatal("B missing after ApplyPut")
	}

	tomb := store.Entry{Version: 4, Flags: store.FlagTombstone}
	fails = fo.Apply(context.Background(), []ring.Peer{p}, "demo", "k", tomb, 1)
	if len(fails) != 0 {
		t.Fatalf("Apply delete: %+v", fails)
	}
	if engB.HasLocal("demo", "k") {
		t.Fatal("B still has key after ApplyDelete")
	}
}

func TestHintFlushReplaysPutThenDelete(t *testing.T) {
	const addrB = "127.0.0.1:19802"

	trA := peer.NewTransport(50 * time.Millisecond)
	defer trA.Close()
	fo := peer.NewFanoutPool(trA, peer.FanoutConfig{
		Workers: 2, QueueSize: 16, HintRetryInterval: 40 * time.Millisecond,
	})
	defer fo.Close()

	// Nothing listening: Apply fails and enqueues a put hint.
	fails := fo.Apply(context.Background(), []ring.Peer{{ID: "b", Addr: addrB}},
		"demo", "k", store.Entry{Value: []byte("v"), Version: 1}, 1)
	if len(fails) == 0 {
		t.Fatal("expected failure to down peer")
	}
	if fo.HintPending() < 1 {
		t.Fatal("put should be hinted")
	}

	// Replace with tombstone hint (LWW).
	fo.Hint(ring.Peer{ID: "b", Addr: addrB}, "demo", "k",
		store.Entry{Version: 2, Flags: store.FlagTombstone}, 1)

	// Bring B up; flusher should ApplyDelete and clear the queue.
	engB, _, stopB := realPeer(t, "b", addrB)
	defer stopB()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fo.HintPending() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if fo.HintPending() != 0 {
		t.Fatalf("hints not flushed, pending=%d", fo.HintPending())
	}
	if engB.HasLocal("demo", "k") {
		t.Fatal("delete hint should leave no live copy")
	}
}

func TestForwardPutDeleteAndGetOrLoad(t *testing.T) {
	const (
		addrA = "127.0.0.1:19811"
		addrB = "127.0.0.1:19812"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour,
		})
	}
	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()

	rA, rB := ring.New(16), ring.New(16)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(300 * time.Millisecond)
	trB := peer.NewTransport(300 * time.Millisecond)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 32, DisableHints: true})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 2, QueueSize: 32, DisableHints: true})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	// ForwardPut to A (owner of some key we pick).
	var key string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("fwd-key-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by A")
	}
	if err := trB.ForwardPut(ctx, addrA, "demo", key, []byte("v"), 0, false, 0); err != nil {
		t.Fatalf("ForwardPut: %v", err)
	}
	if !engA.HasLocal("demo", key) {
		t.Fatal("A missing after ForwardPut")
	}

	// GetOrLoad from B → A.
	res, err := trB.GetOrLoad(ctx, addrA, "demo", key)
	if err != nil || !res.Found || string(res.Entry.Value) != "v" {
		t.Fatalf("GetOrLoad: found=%v err=%v val=%q", res.Found, err, res.Entry.Value)
	}
	// Missing key.
	res, err = trB.GetOrLoad(ctx, addrA, "demo", "missing-key-xyz")
	if err != nil || res.Found {
		t.Fatalf("GetOrLoad missing: found=%v err=%v", res.Found, err)
	}

	// ForwardDelete A as owner.
	fails, err := trB.ForwardDelete(ctx, addrA, "demo", key)
	if err != nil {
		t.Fatalf("ForwardDelete: %v", err)
	}
	_ = fails
	if engA.HasLocal("demo", key) {
		t.Fatal("A still has key after ForwardDelete")
	}

	if peer.FormatAddr(ring.Peer{ID: "x", Addr: "1:2"}) != "x(1:2)" {
		t.Fatal("FormatAddr")
	}
	// Timeout / NewTransport defaults.
	tr0 := peer.NewTransport(0)
	defer tr0.Close()
	if tr0.Timeout() != 500*time.Millisecond {
		t.Fatalf("default timeout %v", tr0.Timeout())
	}
	// rpcContext with deadline set: no override panic.
	dctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, _ = trA.ApplyPut(dctx, addrA, "demo", "k2", store.Entry{Value: []byte("z"), Version: 1}, 1)

	// HintPending on nil.
	var nilFO *peer.FanoutPool
	if nilFO.HintPending() != 0 {
		t.Fatal("nil HintPending")
	}
	// Apply/Submit on closed pool are no-ops.
	foC := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 1, QueueSize: 4, DisableHints: true})
	foC.Close()
	if foC.Apply(ctx, []ring.Peer{{ID: "b", Addr: addrB}}, "demo", "x", store.Entry{Version: 1}, 1) != nil {
		t.Fatal("Apply on closed pool")
	}
	foC.Submit([]ring.Peer{{ID: "b", Addr: addrB}}, "demo", "x", store.Entry{Version: 1}, 1)
}

func TestForwardDeleteReportsPeerFailures(t *testing.T) {
	// Owner A has replica B up and C down → MultiError failures on ForwardDelete.
	const (
		addrA = "127.0.0.1:19821"
		addrB = "127.0.0.1:19822"
		addrC = "127.0.0.1:19823" // never listen
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour,
		})
	}
	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()

	peers := []ring.Peer{
		{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}, {ID: "c", Addr: addrC},
	}
	rA, rB := ring.New(16), ring.New(16)
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(80 * time.Millisecond)
	trB := peer.NewTransport(80 * time.Millisecond)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 16, DisableHints: true})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 2, QueueSize: 16, DisableHints: true})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	// DeleteAsOwner does not require ring ownership; A is the ForwardDelete target.
	const key = "del-fail-key"
	if err := engA.PutLocal(ctx, "demo", key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	fails, err := trB.ForwardDelete(ctx, addrA, "demo", key)
	if err != nil {
		t.Fatalf("ForwardDelete err=%v", err)
	}
	// At least C should appear as a peer failure (ApplyDelete to down addr).
	if len(fails) == 0 {
		t.Fatal("expected peer failures including down peer C")
	}
}

func TestApplyToPeerEmptyAndNilTransport(t *testing.T) {
	tr := peer.NewTransport(50 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 4, DisableHints: true})
	defer fo.Close()
	fails := fo.Apply(context.Background(), []ring.Peer{
		{ID: "empty"},
		{ID: "dead", Addr: "127.0.0.1:1"},
	}, "demo", "k", store.Entry{Value: []byte("v"), Version: 1}, 1)
	if len(fails) < 2 {
		t.Fatalf("want failures for empty+dead, got %d", len(fails))
	}
}

func TestHintBloomAddSeparateFromTombstone(t *testing.T) {
	tr := peer.NewTransport(20 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	// enable enqueue without flusher
	// peer package tests in peer_test cannot set unexported field — use Hint with DisableHints false
	// and no successful peer so flush fails and leaves queue.
	// Use package peer tests for enqueue internals if needed.
	// Here: public Hint + DisableHints false; flush will fail to dead addr, hints stay.
	fo2 := peer.NewFanoutPool(tr, peer.FanoutConfig{
		Workers: 1, QueueSize: 8, HintRetryInterval: time.Hour, // flusher almost never runs
	})
	defer fo2.Close()

	p := ring.Peer{ID: "b", Addr: "127.0.0.1:1"}
	fo2.Hint(p, "bf", "f", store.Entry{Value: []byte("item-a"), Version: 1, Flags: store.FlagBloomAdd}, 1)
	fo2.Hint(p, "bf", "f", store.Entry{Value: []byte("item-b"), Version: 1, Flags: store.FlagBloomAdd}, 1)
	// Two distinct item-adds must not coalesce into one.
	if fo2.HintPending() != 2 {
		t.Fatalf("bloom item-adds should be distinct, pending=%d", fo2.HintPending())
	}
	// Tombstone drops both item-add hints for that name.
	fo2.Hint(p, "bf", "f", store.Entry{Version: 3, Flags: store.FlagTombstone}, 1)
	if fo2.HintPending() != 1 {
		t.Fatalf("after tombstone want 1 pending, got %d", fo2.HintPending())
	}
}
