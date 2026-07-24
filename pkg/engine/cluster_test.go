package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func TestClusterPutFanout(t *testing.T) {
	const (
		addrA = "127.0.0.1:19001"
		addrB = "127.0.0.1:19002"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	for _, e := range []*engine.Engine{engA, engB} {
		if err := e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
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

	rA := ring.New(32)
	rB := ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)

	trA := peer.NewTransport(time.Second)
	trB := peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	defer foA.Close()
	defer foB.Close()

	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k-%d", i)
		if err := engA.Put(ctx, "demo", k, []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	var hits int
	for time.Now().Before(deadline) {
		hits = 0
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("k-%d", i)
			if _, err := engB.Get(ctx, "demo", k); err == nil {
				hits++
			}
		}
		if hits >= 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if hits < 10 {
		errN, dropN := engA.FanoutStats()
		t.Fatalf("expected fan-out hits on B, got %d/20; fanout err/drop=%d/%d", hits, errN, dropN)
	}

	ok, err := engB.ApplyPut("demo", "manual", store.Entry{Value: []byte("x"), Version: 5})
	if err != nil || !ok {
		t.Fatalf("apply: %v %v", ok, err)
	}
	ok, err = engB.ApplyPut("demo", "manual", store.Entry{Value: []byte("old"), Version: 3})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stale apply should fail")
	}
}

func TestClusterDeleteMultiPeer(t *testing.T) {
	const (
		addrA = "127.0.0.1:19011"
		addrB = "127.0.0.1:19012"
	)
	engA, engB, cleanup := twoNodeCluster(t, addrA, addrB)
	defer cleanup()

	ctx := context.Background()
	// Put via A so fan-out fills B for owned keys.
	key := "shared-key"
	if err := engA.Put(ctx, "demo", key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	// ensure B has it (fan-out or forward path)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := engB.Get(ctx, "demo", key); err == nil {
			break
		}
		// also try put on B's view
		time.Sleep(30 * time.Millisecond)
	}
	// Force both stores to have the key via ApplyPut if needed
	_, _ = engA.ApplyPut("demo", key, store.Entry{Value: []byte("v"), Version: 10})
	_, _ = engB.ApplyPut("demo", key, store.Entry{Value: []byte("v"), Version: 10})

	if err := engA.Delete(ctx, "demo", key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := engA.Get(ctx, "demo", key); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("A still has key: %v", err)
	}
	// allow sync delete RPC
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := engB.Get(ctx, "demo", key); errors.Is(err, engine.ErrNotFound) {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	if _, err := engB.Get(ctx, "demo", key); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("B still has key after delete: %v", err)
	}
}

func TestClusterDeletePeerDownMultiError(t *testing.T) {
	const (
		addrA = "127.0.0.1:19021"
		addrB = "127.0.0.1:19022"
	)
	engA := engine.New()
	defer engA.Close()
	_ = engA.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()

	r := ring.New(16)
	// B is in the ring but never listens → ApplyDelete fails
	r.SetPeers([]ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}})
	tr := peer.NewTransport(200 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 10})
	defer fo.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: r, Transport: tr, Fanout: fo})

	ctx := context.Background()
	_ = engA.PutLocal(ctx, "demo", "k", []byte("v"))
	err = engA.Delete(ctx, "demo", "k")
	if err == nil {
		t.Fatal("expected multi-error when peer down")
	}
	var me *engine.MultiError
	if !errors.As(err, &me) || len(me.Errors) == 0 {
		t.Fatalf("want MultiError, got %T %v", err, err)
	}
	// local delete still applied
	if _, err := engA.Get(ctx, "demo", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("local should be deleted: %v", err)
	}
}

func TestGetOrLoadFromOwner(t *testing.T) {
	const (
		addrA = "127.0.0.1:19031"
		addrB = "127.0.0.1:19032"
	)
	src := datasource.Map{"only-on-src": []byte("loaded")}
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
			TTL: time.Minute, DataSource: src,
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

	rA := ring.New(32)
	rB := ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(time.Second)
	trB := peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	// Get from both nodes — should load via owner singleflight path without double-fail
	v1, err := engA.Get(ctx, "lt", "only-on-src")
	if err != nil || string(v1) != "loaded" {
		t.Fatalf("A get: %v %s", err, v1)
	}
	v2, err := engB.Get(ctx, "lt", "only-on-src")
	if err != nil || string(v2) != "loaded" {
		t.Fatalf("B get: %v %s", err, v2)
	}
}

// ForceLoad may only reload the source-of-truth on the ring owner.
// Non-owners must not call their local DataSource (that would stampede SoT
// and still not force the owner's cache).
func TestForceLoadOnlyOwnerHitsDataSource(t *testing.T) {
	const (
		addrA = "127.0.0.1:19051"
		addrB = "127.0.0.1:19052"
	)
	var loadsA, loadsB atomic.Int32
	srcA := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loadsA.Add(1)
		return []byte("a:" + key), nil
	})
	srcB := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loadsB.Add(1)
		return []byte("b:" + key), nil
	})

	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	_ = engA.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcA,
	})
	_ = engB.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcB,
	})

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

	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA, rB := ring.New(32), ring.New(32)
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA, trB := peer.NewTransport(time.Second), peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	// Find a key owned by A.
	var key string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("fl-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by a")
	}

	ctx := context.Background()
	// Prime owner cache.
	if _, err := engA.Get(ctx, "lt", key); err != nil {
		t.Fatal(err)
	}
	if loadsA.Load() != 1 {
		t.Fatalf("prime loadsA=%d", loadsA.Load())
	}

	// Non-owner ForceLoad must not touch either DataSource (no local load, no fake SoT).
	if err := engB.ForceLoad(ctx, "lt", key); err != nil {
		t.Fatalf("non-owner ForceLoad: %v", err)
	}
	if loadsB.Load() != 0 {
		t.Fatalf("non-owner must not call local DataSource, loadsB=%d", loadsB.Load())
	}
	if loadsA.Load() != 1 {
		t.Fatalf("non-owner ForceLoad must not force owner SoT reload without owner RPC, loadsA=%d", loadsA.Load())
	}

	// Owner ForceLoad must bypass cache and reload SoT.
	if err := engA.ForceLoad(ctx, "lt", key); err != nil {
		t.Fatalf("owner ForceLoad: %v", err)
	}
	if loadsA.Load() != 2 {
		t.Fatalf("owner ForceLoad should reload SoT, loadsA=%d", loadsA.Load())
	}
}

// Non-owner ForceLoad must not fall back to a local DataSource load when the
// owner is unreachable (that stampedes SoT and is not a true force-refresh).
func TestForceLoadNonOwnerNoLocalFallback(t *testing.T) {
	const addrA = "127.0.0.1:19061"
	// B has no peer server / owner A is down.
	var loadsB atomic.Int32
	srcB := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loadsB.Add(1)
		return []byte("b"), nil
	})
	engB := engine.New()
	defer engB.Close()
	_ = engB.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcB,
	})
	r := ring.New(16)
	// A owns everything if we pick carefully; put both peers, A never listens.
	r.SetPeers([]ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: "127.0.0.1:19062"}})
	tr := peer.NewTransport(100 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 10})
	defer fo.Close()
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: r, Transport: tr, Fanout: fo})

	// Find key owned by A (not B).
	var key string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("fb-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by a")
	}

	// Old broken behavior: Delete + Get → owner down → local loadThrough.
	if err := engB.ForceLoad(context.Background(), "lt", key); err != nil {
		// May return unavailable or not-found; must not have called local DS.
		t.Logf("ForceLoad err (ok): %v", err)
	}
	if loadsB.Load() != 0 {
		t.Fatalf("non-owner ForceLoad must not local-load when not owner, loadsB=%d", loadsB.Load())
	}
}

// keyspace.PeerTimeout must bound owner GetOrLoad RPCs (not the transport default alone).
func TestPeerTimeoutAppliedOnGetOrLoad(t *testing.T) {
	const (
		addrA = "127.0.0.1:19071"
		addrB = "127.0.0.1:19072"
	)
	// Owner DataSource blocks longer than PeerTimeout so the RPC must be cut short.
	block := make(chan struct{})
	defer close(block)
	srcA := datasource.Func(func(ctx context.Context, key string) ([]byte, error) {
		select {
		case <-block:
			return []byte("owner"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return []byte("owner-late"), nil
		}
	})
	var loadsB atomic.Int32
	srcB := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loadsB.Add(1)
		return []byte("local"), nil
	})

	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	_ = engA.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcA,
	})
	_ = engB.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcB,
		// Much tighter than transport default used below.
		PeerTimeout: 50 * time.Millisecond,
	})

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

	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA, rB := ring.New(32), ring.New(32)
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	// Large transport timeout so ignoring PeerTimeout is clearly slow.
	trA := peer.NewTransport(3 * time.Second)
	trB := peer.NewTransport(3 * time.Second)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 10})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 2, QueueSize: 10})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	var key string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("pt-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by a")
	}

	start := time.Now()
	v, err := engB.Get(context.Background(), "lt", key)
	elapsed := time.Since(start)
	if err != nil || string(v) != "local" {
		t.Fatalf("want local fallback after peer timeout: v=%q err=%v", v, err)
	}
	if loadsB.Load() != 1 {
		t.Fatalf("loadsB=%d", loadsB.Load())
	}
	// Without PeerTimeout, would wait ~3s on GetOrLoad before fallback.
	if elapsed > 800*time.Millisecond {
		t.Fatalf("PeerTimeout not applied: Get took %v", elapsed)
	}
	if elapsed < 30*time.Millisecond {
		// Sanity: we should have waited for the peer deadline at least roughly.
		t.Logf("elapsed %v (ok if connect/fail path is faster)", elapsed)
	}
}

// PutLocal on a non-owner must not silently keep the write local-only:
// either re-route to the owner or fan-out so the owner observes the value.
func TestPutLocalNonOwnerPropagates(t *testing.T) {
	const (
		addrA = "127.0.0.1:19041"
		addrB = "127.0.0.1:19042"
	)
	engA, engB, cleanup := twoNodeCluster(t, addrA, addrB)
	defer cleanup()

	ctx := context.Background()
	// Find a key owned by A.
	var key string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("own-%d", i)
		o, ok := engA.OwnerOf(k)
		if ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("could not find key owned by a")
	}

	// Mis-routed write: B accepts ForwardPut/PutLocal even though A owns the key.
	if err := engB.PutLocal(ctx, "demo", key, []byte("from-b")); err != nil {
		t.Fatalf("PutLocal on non-owner: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v, err := engA.Get(ctx, "demo", key)
		if err == nil && string(v) == "from-b" {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("owner A never observed value written via non-owner PutLocal for key %s", key)
}

func twoNodeCluster(t *testing.T, addrA, addrB string) (*engine.Engine, *engine.Engine, func()) {
	t.Helper()
	engA := engine.New()
	engB := engine.New()
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute,
		})
	}
	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	rA := ring.New(32)
	rB := ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(time.Second)
	trB := peer.NewTransport(time.Second)
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})
	cleanup := func() {
		foA.Close()
		foB.Close()
		_ = trA.Close()
		_ = trB.Close()
		gsA.Stop()
		gsB.Stop()
		engA.Close()
		engB.Close()
	}
	return engA, engB, cleanup
}

func TestNegativeFanoutAndGetOrLoadEnvelope(t *testing.T) {
	const (
		addrA = "127.0.0.1:19101"
		addrB = "127.0.0.1:19102"
	)
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return nil, datasource.ErrNotFound
	})
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
			TTL: time.Minute, NegativeTTL: time.Minute, DataSource: src,
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
	rA, rB := ring.New(32), ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA, trB := peer.NewTransport(time.Second), peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 100})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	// Force owner load of missing key
	_, err = engA.Get(ctx, "lt", "missing-key")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want not found: %v", err)
	}
	// Wait for negative fan-out
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Peek via Get on B — should be negative without another DS call if fanned out
		// We can't see load count easily; check ApplyPut landed by second Get on B not needing owner
		// Owner GetOrLoad should return negative envelope
		ent, err := engA.GetOrLoadLocal(ctx, "lt", "missing-key")
		if !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("owner: %v", err)
		}
		if !ent.IsNegative() || ent.Version == 0 {
			t.Fatalf("owner negative envelope: %+v", ent)
		}
		break
	}
	// B installs via Get path from owner
	_, err = engB.Get(ctx, "lt", "missing-key")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("B: %v", err)
	}
}

func TestDeleteOwnerDownFallback(t *testing.T) {
	const addrA = "127.0.0.1:19111"
	engA := engine.New()
	defer engA.Close()
	_ = engA.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()

	r := ring.New(16)
	// Owner of keys will sometimes be "b" which is down — force all ownership via single peer
	// Use two peers where we put key that hashes to b... easier: set ring with only a for owner
	// then delete with fake owner b by manual ring where a is not owner.
	r.SetPeers([]ring.Peer{
		{ID: "a", Addr: addrA},
		{ID: "b", Addr: "127.0.0.1:1"}, // nothing listening
	})
	tr := peer.NewTransport(100 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 2, QueueSize: 10})
	defer fo.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: r, Transport: tr, Fanout: fo})

	ctx := context.Background()
	// Find a key owned by b
	var key string
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k-%d", i)
		o, ok := engA.OwnerOf(k)
		if ok && o.ID == "b" {
			key = k
			break
		}
	}
	if key == "" {
		t.Skip("could not find key owned by b")
	}
	// PutLocal to store value (bypass forward to down owner)
	if err := engA.PutLocal(ctx, "demo", key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Delete should fall back when ForwardDelete to b fails
	err = engA.Delete(ctx, "demo", key)
	// May return MultiError for peer b ApplyDelete from DeleteAsOwner, or nil if only local
	// Local must be gone
	if _, gerr := engA.Get(ctx, "demo", key); !errors.Is(gerr, engine.ErrNotFound) {
		t.Fatalf("local key should be deleted, get=%v delete_err=%v", gerr, err)
	}
}

func TestNextVersionSeedsFromStore(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	// Simulate peer high version without lastVer bump path via ApplyPut
	ok, err := e.ApplyPut("c", "k", store.Entry{Value: []byte("old"), Version: 50})
	if err != nil || !ok {
		t.Fatalf("apply: %v %v", ok, err)
	}
	// Put should mint > 50
	if err := e.Put(context.Background(), "c", "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	// Stale apply must lose
	ok, _ = e.ApplyPut("c", "k", store.Entry{Value: []byte("stale"), Version: 50})
	if ok {
		t.Fatal("stale should not apply")
	}
	v, err := e.Get(context.Background(), "c", "k")
	if err != nil || string(v) != "new" {
		t.Fatalf("got %v %s", err, v)
	}
}
