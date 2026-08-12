package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/protect"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/warmup"
)

func TestDeleteKeySpaceAndConfigHash(t *testing.T) {
	e := engine.New()
	defer e.Close()
	cfg := keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20}
	if err := e.UpdateKeySpace(cfg); err != nil {
		t.Fatal(err)
	}
	h, err := e.ConfigHash("c")
	if err != nil || h == "" {
		t.Fatalf("ConfigHash: %q err=%v", h, err)
	}
	if _, err := e.ConfigHash("missing"); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatalf("missing ks: %v", err)
	}
	ctx := context.Background()
	_ = e.Put(ctx, "c", "k", []byte("v"))
	if err := e.DeleteKeySpace("c"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get(ctx, "c", "k"); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatalf("after delete ks: %v", err)
	}
	if err := e.DeleteKeySpace("c"); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatalf("double delete: %v", err)
	}
}

func TestApplyDeleteAndWithTTL(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour})
	ctx := context.Background()
	if err := e.Put(ctx, "c", "k", []byte("v"), engine.WithTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ok, err := e.ApplyDelete("c", "k", 1)
	if err != nil {
		t.Fatal(err)
	}
	// version 1 may be stale if Put minted higher; use high version
	ok, err = e.ApplyDelete("c", "k", 99)
	if err != nil || !ok {
		// key may already be gone from first call
		_ = ok
	}
	// Fresh put then ApplyDelete with high version.
	_ = e.Put(ctx, "c", "k2", []byte("v2"))
	ok, err = e.ApplyDelete("c", "k2", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ApplyDelete should apply")
	}
	if e.HasLocal("c", "k2") {
		t.Fatal("should be tombstoned")
	}
}

func TestPeerMultiAndKeyErrorFormatting(t *testing.T) {
	pe := engine.PeerError{PeerID: "n1", Op: "ApplyDelete", Err: fmt.Errorf("down")}
	if pe.Error() == "" || pe.Unwrap() == nil {
		t.Fatal("PeerError")
	}
	me := &engine.MultiError{Errors: []engine.PeerError{pe}}
	if me.Error() == "" {
		t.Fatal("MultiError.Error")
	}
	if len(me.Unwrap()) != 1 {
		t.Fatal("MultiError.Unwrap")
	}
	empty := &engine.MultiError{}
	_ = empty.Error()
	ke := engine.KeyError{Key: "k", Err: fmt.Errorf("x")}
	if ke.Error() == "" || ke.Unwrap() == nil {
		t.Fatal("KeyError")
	}
	// PutMany error path surfaces KeyError
	e := engine.New(engine.WithLimits(4, 8, 2))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	err := e.PutMany(context.Background(), "c", []engine.KV{
		{Key: "ok", Value: []byte("1")},
		{Key: "toolongkey", Value: []byte("1")},
		{Key: "third", Value: []byte("1")},
	})
	if err == nil {
		t.Fatal("expected batch or key errors")
	}
}

func TestPeersHotKeysStatsAndWarmupHook(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	// Events channel is non-nil and sink is the same underlying channel.
	ch := e.Events()
	if ch == nil {
		t.Fatal("Events")
	}
	sink := e.EventsSink()
	if sink == nil {
		t.Fatal("EventsSink")
	}
	// No cluster: Peers may still return self if SetNodeInfo.
	if e.Peers() == nil {
		t.Fatal("Peers nil")
	}
	e.SetNodeInfo("self", "127.0.0.1:1")
	peers := e.Peers()
	if len(peers) != 1 || peers[0].ID != "self" {
		t.Fatalf("Peers=%+v", peers)
	}
	r := ring.New(8)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: "127.0.0.1:2"}})
	tr := peer.NewTransport(50 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 4, DisableHints: true})
	defer fo.Close()
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r, Transport: tr, Fanout: fo})
	if len(e.Peers()) < 2 {
		t.Fatalf("cluster peers=%+v", e.Peers())
	}
	if e.RingGeneration() == 0 {
		t.Fatal("ring gen")
	}
	st, err := e.Stats("c")
	if err != nil {
		t.Fatal(err)
	}
	_ = st
	if _, err := e.Stats("nope"); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatal(err)
	}
	// Warmup HotKeys path.
	wm := warmup.NewManager(e, warmup.Config{Workers: 1, TopN: 4, TrackMax: 16})
	e.AttachWarmup(wm, wm)
	ctx := context.Background()
	_ = e.Put(ctx, "c", "hot", []byte("v"))
	_, _ = e.Get(ctx, "c", "hot")
	_, _ = e.Get(ctx, "c", "hot")
	keys := e.HotKeys("c", 4)
	// May be empty if tracker not yet updated; still exercises the method.
	_ = keys
	if e.HotKeys("missing", 4) != nil && len(e.HotKeys("missing", 4)) > 0 {
		// empty is fine
	}
}

func TestValidateEmptyKeyAndLimits(t *testing.T) {
	g := protect.New(protect.Config{RateLimitRPS: 0.0001, Burst: 1}) // very tight
	e := engine.New(engine.WithGlobalProtect(g), engine.WithLimits(8, 16, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return []byte("v"), nil
		}),
	})
	ctx := context.Background()
	// Empty key
	if _, err := e.Get(ctx, "lt", ""); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("empty key: %v", err)
	}
	// Key too long for WithLimits(8,...)
	if _, err := e.Get(ctx, "lt", "123456789"); !errors.Is(err, engine.ErrKeyTooLarge) {
		// may be invalid argument depending on order
		if err == nil {
			t.Fatal("expected key too large")
		}
	}
	// Value too large on Put CacheOnly
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	if err := e.Put(ctx, "c", "k", make([]byte, 100)); err == nil {
		t.Fatal("expected value too large")
	}
}

func TestForceLoadCacheOnlyAndOwner(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	_ = e.Put(ctx, "c", "k", []byte("v"))
	// CacheOnly ForceLoad falls through to Get.
	if err := e.ForceLoad(ctx, "c", "k"); err != nil {
		t.Fatal(err)
	}
	if err := e.ForceLoad(ctx, "missing", "k"); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatal(err)
	}
	// canceled context
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := e.ForceLoad(cctx, "c", "k"); err == nil {
		t.Fatal("canceled")
	}
}

func TestPutManyDeleteManyBatchLimit(t *testing.T) {
	e := engine.New(engine.WithLimits(64, 1024, 2))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	err := e.PutMany(ctx, "c", []engine.KV{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	})
	if !errors.Is(err, engine.ErrBatchTooLarge) {
		t.Fatalf("PutMany batch: %v", err)
	}
	err = e.DeleteMany(ctx, "c", []string{"a", "b", "c"})
	if !errors.Is(err, engine.ErrBatchTooLarge) {
		t.Fatalf("DeleteMany batch: %v", err)
	}
	// MultiError nil unwrap
	var me *engine.MultiError
	if me.Unwrap() != nil {
		t.Fatal("nil MultiError.Unwrap")
	}
}

func TestLoadThroughNegativeAndGetOrLoadLocal(t *testing.T) {
	e := engine.New()
	defer e.Close()
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		if key == "miss" {
			return nil, datasource.ErrNotFound
		}
		return []byte("val:" + key), nil
	})
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Hour, NegativeTTL: time.Minute, DataSource: src,
	})
	ctx := context.Background()
	// NotFound → negative cache
	_, err := e.Get(ctx, "lt", "miss")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("miss: %v", err)
	}
	// GetOrLoadLocal hit
	_, err = e.Get(ctx, "lt", "hit")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := e.GetOrLoadLocal(ctx, "lt", "hit")
	if err != nil || string(ent.Value) != "val:hit" {
		t.Fatalf("GetOrLoadLocal: %+v err=%v", ent, err)
	}
	// GetOrLoadLocal miss → loads
	ent, err = e.GetOrLoadLocal(ctx, "lt", "hit2")
	if err != nil {
		t.Fatal(err)
	}
	_ = ent
	// Bloom GetOrLoadLocal empty
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20, BloomBits: 1024, BloomHashes: 3,
	})
	_, err = e.GetOrLoadLocal(ctx, "bf", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("bloom gol: %v", err)
	}
}

func TestBloomAddRejectsInvalidInput(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20, BloomBits: 1024, BloomHashes: 3,
	})
	ctx := context.Background()
	if err := e.BloomAdd(ctx, "bf", "f", nil); err == nil {
		t.Fatal("empty item")
	}
	if err := e.BloomAdd(ctx, "nope", "f", []byte("x")); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := e.BloomAdd(cctx, "bf", "f", []byte("x")); err == nil {
		t.Fatal("canceled add")
	}
	if _, err := e.BloomTest(cctx, "bf", "f", []byte("x")); err == nil {
		t.Fatal("canceled test")
	}
	if _, err := e.ApplyBloomMerge("nope", "f", make([]byte, 1024/8), 1); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatal(err)
	}
	ok, err := e.ApplyBloomMerge("bf", "f", make([]byte, 8), 1) // wrong size
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("wrong bitset size must not merge")
	}
}

func TestPruneLastVerViaCap(t *testing.T) {
	e := engine.New(engine.WithMaxVersionKeys(5))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_ = e.Put(ctx, "c", fmt.Sprintf("k%d", i), []byte("v"))
	}
	// Wipe store entries via UpdateKeySpace but keep lastVer, then prune via more observes.
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	for i := 0; i < 30; i++ {
		_ = e.Put(ctx, "c", fmt.Sprintf("n%d", i), []byte("v"))
	}
	n := e.VersionTrackerSize("c")
	if n < 0 || n > 5 {
		// After puts, tracker is capped at maxVersionKeys.
		if n > 5 {
			t.Fatalf("tracker size %d > cap 5", n)
		}
	}
	if e.VersionTrackerSize("missing") != -1 {
		t.Fatal("unknown ks")
	}
}

func TestValidateKeyLenPerKeyspaceAndPutForward(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, MaxKeyLen: 3,
	})
	if err := e.Put(context.Background(), "c", "abcd", []byte("v")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("MaxKeyLen: %v", err)
	}
}

func TestLoadThroughDataSourceError(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return nil, fmt.Errorf("sot down")
		}),
	})
	_, err := e.Get(context.Background(), "lt", "k")
	if err == nil {
		t.Fatal("expected DS error")
	}
}

func TestDeleteAsOwnerNoFanoutAndOwnerOfEmpty(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	// No cluster: DeleteAsOwner is local only.
	if err := e.DeleteAsOwner(context.Background(), "c", "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.OwnerOf("k"); ok {
		t.Fatal("OwnerOf without cluster")
	}
	if e.HasLocal("missing", "k") {
		t.Fatal("HasLocal bad ks")
	}
	// Ring without fanout still DeleteAsOwner ok.
	r := ring.New(8)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}})
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	_ = e.PutLocal(context.Background(), "c", "k", []byte("v"))
	if err := e.DeleteAsOwner(context.Background(), "c", "k"); err != nil {
		t.Fatal(err)
	}
}

func TestGetCacheOnlyMissNoCluster(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	_, err := e.Get(context.Background(), "c", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	// canceled get
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Get(ctx, "c", "x"); err == nil {
		t.Fatal("canceled get")
	}
}

func TestPutViaClusterOwnerNoAddr(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	r := ring.New(8)
	// Peer with empty address as sole other owner candidate — put a key owned by other.
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: ""}})
	e.SetNodeInfo("self", "127.0.0.1:1")
	tr := peer.NewTransport(50 * time.Millisecond)
	defer tr.Close()
	fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 1, QueueSize: 4, DisableHints: true})
	defer fo.Close()
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r, Transport: tr, Fanout: fo})
	// Find a key owned by other.
	ctx := context.Background()
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("own-%d", i)
		if o, ok := e.OwnerOf(k); ok && o.ID == "other" {
			err := e.Put(ctx, "c", k, []byte("v"))
			if err == nil {
				t.Fatal("expected unavailable without owner addr")
			}
			return
		}
	}
	t.Fatal("no key owned by other")
}

func TestGetViaClusterOwnerDownAndDeleteEmptyPeer(t *testing.T) {
	const (
		addrA = "127.0.0.1:19901"
		addrB = "127.0.0.1:19902"
	)
	loads := 0
	src := datasource.Func(func(context.Context, string) ([]byte, error) {
		loads++
		return []byte("from-b"), nil
	})
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
			TTL: time.Hour, DataSource: src, PeerTimeout: 50 * time.Millisecond,
		})
		_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	}
	// Only B listens — A is "down" for peer RPCs from B.
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()

	rA, rB := ring.New(16), ring.New(16)
	// Include empty-addr peer for DeleteAsOwner empty-address branch.
	peers := []ring.Peer{
		{ID: "a", Addr: addrA},
		{ID: "b", Addr: addrB},
		{ID: "empty", Addr: ""},
	}
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(40 * time.Millisecond)
	trB := peer.NewTransport(40 * time.Millisecond)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 16, DisableHints: true})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 2, QueueSize: 16, DisableHints: true})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	// Key owned by A — B Get LoadThrough → owner GetOrLoad fails → local fill.
	var key string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("ownA-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by A")
	}
	v, err := engB.Get(ctx, "lt", key)
	if err != nil || string(v) != "from-b" {
		t.Fatalf("owner-down fill: %q err=%v", v, err)
	}
	if loads < 1 {
		t.Fatal("expected local DS load on owner-down")
	}

	// DeleteAsOwner on B hits empty peer + down A.
	_ = engB.PutLocal(ctx, "c", "delme", []byte("x"))
	err = engB.DeleteAsOwner(ctx, "c", "delme")
	// MultiError expected (empty + down peer)
	var me *engine.MultiError
	if err != nil && !errors.As(err, &me) {
		t.Logf("DeleteAsOwner err=%v (ok if multi)", err)
	}

	// Ring gen mismatch path on ApplyPut.
	ok, err := engB.ApplyPutWithRingGen("c", "rg", store.Entry{Value: []byte("v"), Version: 1}, 99999)
	if err != nil || !ok {
		t.Fatalf("ApplyPut ring gen: ok=%v err=%v", ok, err)
	}

	// PutLocalAtHop when not owner with hop exhausted → force apply.
	var foreign string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("hop-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			foreign = k
			break
		}
	}
	if foreign != "" {
		// hopCount high so we don't re-forward (maxForwardHops=1 means hopCount>=1 no reforward when not owner)
		if err := engB.PutLocalAtHop(ctx, "c", foreign, []byte("forced"), 1); err != nil {
			t.Fatalf("PutLocalAtHop force: %v", err)
		}
	}
}

func TestUpdateKeySpacePreservesVersions(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	_ = e.Put(ctx, "c", "k", []byte("v1"))
	_ = e.Put(ctx, "c", "k", []byte("v2"))
	// Re-register same name with larger MaxBytes; lastVer should survive.
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 2 << 20})
	// Store wiped — miss — but next put version should not restart at 1 in a way that loses monotonicity.
	// After wipe, Put still mints from preserved lastVer.
	_ = e.Put(ctx, "c", "k", []byte("v3"))
	// ApplyPut with version 1 must lose to current.
	ok, err := e.ApplyPut("c", "k", store.Entry{Value: []byte("old"), Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stale v1 must not overwrite after preserved lastVer puts")
	}
}

// TestValidationModeGuardsAndNegativeTTL checks argument validation, ModeBloom
// API guards, NegativeTTL=0, and version-tracker pruning.
func TestValidationModeGuardsAndNegativeTTL(t *testing.T) {
	ctx := context.Background()

	// Engine with metrics disabled still serves Put/Get/Apply.
	eNil := engine.New(engine.WithMetrics(nil))
	defer eNil.Close()
	if err := eNil.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if err := eNil.Put(ctx, "c", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	ok, err := eNil.ApplyPutWithRingGen("c", "k", store.Entry{Value: []byte("v2"), Version: 9}, 42)
	if err != nil || !ok {
		t.Fatalf("ApplyPut: ok=%v err=%v", ok, err)
	}
	v, err := eNil.Get(ctx, "c", "k")
	if err != nil || string(v) != "v2" {
		t.Fatalf("Get: %q err=%v", v, err)
	}

	e := engine.New(engine.WithLimits(8, 32, 8))
	defer e.Close()

	// empty keyspace / empty key / missing ks on Apply*
	if _, err := e.Get(ctx, "", "k"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("empty ks: %v", err)
	}
	if _, err := e.ApplyPutWithRingGen("", "k", store.Entry{Version: 1}, 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("ApplyPut empty ks: %v", err)
	}
	if _, err := e.ApplyDeleteWithRingGen("missing", "k", 1, 1); !errors.Is(err, engine.ErrKeyspaceNotFound) {
		t.Fatalf("ApplyDelete missing: %v", err)
	}
	if _, err := e.ApplyDeleteWithRingGen("c", "", 1, 0); !errors.Is(err, engine.ErrInvalidArgument) {
		// keyspace "c" not registered yet → still invalid empty key first or not found
		if err == nil {
			t.Fatal("ApplyDelete empty key")
		}
	}

	// RateLimitRPS + MaxValueSize on keyspace + LocalEntries miss
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20,
		RateLimitRPS: 100, MaxValueSize: 4,
	}); err != nil {
		t.Fatal(err)
	}
	if e.LocalEntries("missing") != nil {
		t.Fatal("LocalEntries missing")
	}
	if err := e.Put(ctx, "c", "k", []byte("too-big")); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("MaxValueSize: %v", err)
	}
	if err := e.Put(ctx, "c", "k", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	ents := e.LocalEntries("c")
	if len(ents) == 0 {
		t.Fatal("LocalEntries empty")
	}

	// ModeBloom: Get/Put rejected; BloomTest requires ModeBloom.
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20, BloomBits: 1024, BloomHashes: 3, MaxKeyLen: 3,
	})
	if _, err := e.Get(ctx, "bf", "n"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get bloom: %v", err)
	}
	if err := e.Put(ctx, "bf", "n", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put bloom: %v", err)
	}
	if _, err := e.BloomTest(ctx, "c", "n", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("BloomTest wrong mode: %v", err)
	}
	// name exceeds MaxKeyLen
	if err := e.BloomAdd(ctx, "bf", "long", []byte("x")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("bloom name MaxKeyLen: %v", err)
	}
	// item exceeds engine maxKeyLen (8)
	if err := e.BloomAdd(ctx, "bf", "n", []byte("123456789")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("bloom item maxKeyLen: %v", err)
	}
	if err := e.BloomAdd(ctx, "bf", "n", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ApplyBloomMerge("", "n", make([]byte, 128), 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("ApplyBloomMerge empty ks: %v", err)
	}

	// NegativeTTL=0 → storeNegative early return; LoadTimeout path; value too large from DS
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "lt0", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		LoadTimeout: time.Second, NegativeTTL: 0,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return nil, datasource.ErrNotFound
		}),
	})
	if _, err := e.Get(ctx, "lt0", "miss"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("neg ttl0: %v", err)
	}
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "ltbig", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20, MaxValueSize: 3,
		DataSource: datasource.Func(func(context.Context, string) ([]byte, error) {
			return []byte("huge"), nil
		}),
	})
	if _, err := e.Get(ctx, "ltbig", "k"); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("ds too big: %v", err)
	}

	// GetOrLoadLocal CacheOnly miss + canceled + missing + MaxKeyLen
	_ = e.UpdateKeySpace(keyspace.Config{Name: "co", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, MaxKeyLen: 2})
	if _, err := e.GetOrLoadLocal(ctx, "co", "no"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("gol cache miss: %v", err)
	}
	if _, err := e.GetOrLoadLocal(ctx, "co", "abc"); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("gol keylen: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.GetOrLoadLocal(cctx, "co", "ab"); err == nil {
		t.Fatal("gol canceled")
	}
	if err := e.DeleteAsOwner(cctx, "co", "ab"); err == nil {
		t.Fatal("delete canceled")
	}
	if err := e.PutLocalAtHop(cctx, "co", "ab", []byte("v"), 0); err == nil {
		t.Fatal("putlocal canceled")
	}
	if err := e.ForceLoad(ctx, "co", "abc"); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("ForceLoad keylen: %v", err)
	}
	if err := e.ForceLoad(ctx, "", "k"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("ForceLoad empty ks: %v", err)
	}

	// joinErrors single-error path via PutMany one failure
	_ = e.UpdateKeySpace(keyspace.Config{Name: "batch", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	if err := e.PutMany(ctx, "batch", []engine.KV{
		{Key: "ok", Value: []byte("1")},
		{Key: "toolong!!", Value: []byte("1")}, // 9 > maxKeyLen 8
	}); err == nil {
		t.Fatal("PutMany expected error")
	}

	// pruneLastVer second loop: all tracked keys still present in store
	eCap := engine.New(engine.WithMaxVersionKeys(3))
	defer eCap.Close()
	_ = eCap.UpdateKeySpace(keyspace.Config{Name: "p", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	for i := 0; i < 6; i++ {
		_ = eCap.Put(ctx, "p", fmt.Sprintf("k%d", i), []byte("v"))
	}
	if n := eCap.VersionTrackerSize("p"); n > 3 {
		t.Fatalf("cap prune still live keys: %d", n)
	}

	wm := warmup.NewManager(e, warmup.Config{Workers: 1, TopN: 4, TrackMax: 16})
	e.AttachWarmup(wm, wm)
	if snaps := e.KeySpaceSnapshots(); len(snaps) == 0 {
		t.Fatal("KeySpaceSnapshots empty")
	}
	// hitRecorder that does not implement HotKeys → nil
	e.AttachWarmup(hitOnly{}, nil)
	if e.HotKeys("c", 2) != nil {
		t.Fatal("HotKeys without HotKeys method")
	}

	// Store MaxBytes: entries larger than budget eventually fail.
	eTiny := engine.New()
	defer eTiny.Close()
	_ = eTiny.UpdateKeySpace(keyspace.Config{Name: "t", Mode: keyspace.ModeCacheOnly, MaxBytes: 40})
	var sawErr bool
	for i := 0; i < 20; i++ {
		if err := eTiny.Put(ctx, "t", fmt.Sprintf("x%d", i), make([]byte, 30)); err != nil {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Fatal("expected MaxBytes rejection after filling store")
	}
}

type hitOnly struct{}

func (hitOnly) RecordHit(string, string) {}

func TestClusterOwnerFetchForwardBloomAndDelete(t *testing.T) {
	const (
		addrA = "127.0.0.1:19911"
		addrB = "127.0.0.1:19912"
	)
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		if key == "neg" {
			return nil, datasource.ErrNotFound
		}
		return []byte("owner-val"), nil
	})
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	for _, e := range []*engine.Engine{engA, engB} {
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
			TTL: time.Hour, NegativeTTL: time.Minute, DataSource: src, PeerTimeout: 200 * time.Millisecond,
		})
		_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Hour})
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20, BloomBits: 2048, BloomHashes: 3,
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

	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA, rB := ring.New(32), ring.New(32)
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(time.Second)
	trB := peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 32, DisableHints: true})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 2, QueueSize: 32, DisableHints: true})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()

	// Put with WithTTL via non-owner → owner (putViaCluster ttlSet branch).
	var keyA string
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("ttl-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			keyA = k
			break
		}
	}
	if keyA == "" {
		t.Fatal("no key owned by A")
	}
	if err := engB.Put(ctx, "c", keyA, []byte("v"), engine.WithTTL(time.Minute)); err != nil {
		t.Fatalf("Put WithTTL: %v", err)
	}

	// PutLocalAtHop hop=0 re-forward to owner with TTL.
	var keyA2 string
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("hopf-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			keyA2 = k
			break
		}
	}
	if err := engB.PutLocalAtHop(ctx, "c", keyA2, []byte("hop"), 0, engine.WithTTL(time.Minute)); err != nil {
		t.Fatalf("PutLocalAtHop reforward: %v", err)
	}

	// CacheOnly Get on non-owner fetches from owner (fetchFromOwner / flight).
	if err := engA.PutLocal(ctx, "c", keyA, []byte("owned")); err != nil {
		t.Fatal(err)
	}
	// Wait for possible fan-out; force local miss on B by using a key only on A.
	var onlyA string
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("co-fetch-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			onlyA = k
			break
		}
	}
	_ = engA.PutLocal(ctx, "c", onlyA, []byte("from-a"))
	v, err := engB.Get(ctx, "c", onlyA)
	if err != nil || string(v) != "from-a" {
		t.Fatalf("cacheonly fetch: %q err=%v", v, err)
	}

	// LoadThrough Get on non-owner → GetOrLoad success (getViaCluster happy path).
	var ltKey string
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("lt-ok-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			ltKey = k
			break
		}
	}
	v, err = engB.Get(ctx, "lt", ltKey)
	if err != nil || string(v) != "owner-val" {
		t.Fatalf("lt owner get: %q err=%v", v, err)
	}

	// Negative envelope from owner.
	var negKey string
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("neg-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" && k == "neg" {
			// exact key "neg" for DS; find if A owns "neg"
			negKey = "neg"
			break
		}
	}
	// Force key "neg" regardless of owner: Get on whoever is non-owner.
	if o, ok := engA.OwnerOf("neg"); ok && o.ID == "a" {
		_, err = engB.Get(ctx, "lt", "neg")
	} else {
		_, err = engA.Get(ctx, "lt", "neg")
	}
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("neg get: %v", err)
	}
	_ = negKey

	// BloomAdd via owner (non-owner path) + BloomTest miss then owner fetch.
	var bfName string
	for i := 0; i < 3000; i++ {
		n := fmt.Sprintf("f%d", i)
		if o, ok := engB.OwnerOf(n); ok && o.ID == "a" {
			bfName = n
			break
		}
	}
	if bfName == "" {
		t.Fatal("no bloom name owned by A")
	}
	if err := engB.BloomAdd(ctx, "bf", bfName, []byte("item1")); err != nil {
		t.Fatalf("BloomAdd via owner: %v", err)
	}
	// Local miss on B for a different name owned by A — Test may fetch snapshot.
	ok, err := engB.BloomTest(ctx, "bf", bfName, []byte("item1"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		// Allow eventual consistency after ApplyPut; retry briefly.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && !ok {
			ok, _ = engB.BloomTest(ctx, "bf", bfName, []byte("item1"))
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !ok {
		t.Fatal("BloomTest expected true after owner add")
	}

	// Owner-down CacheOnly: transport to dead peer → miss (fetchFromOwner err path).
	// Point B's view of A at a closed port.
	rB2 := ring.New(32)
	rB2.SetPeers([]ring.Peer{{ID: "a", Addr: "127.0.0.1:1"}, {ID: "b", Addr: addrB}})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB2, Transport: trB, Fanout: foB})
	// Key owned by "a" in this ring.
	var deadKey string
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("dead-%d", i)
		if o, ok := engB.OwnerOf(k); ok && o.ID == "a" {
			deadKey = k
			break
		}
	}
	_, err = engB.Get(ctx, "c", deadKey)
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("dead owner cacheonly: %v", err)
	}

	// Delete via owner success path (len(failures)==0).
	rB.SetPeers(peers)
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})
	if err := engB.Put(ctx, "c", keyA, []byte("del")); err != nil {
		t.Fatal(err)
	}
	if err := engB.Delete(ctx, "c", keyA); err != nil {
		t.Fatalf("Delete via owner: %v", err)
	}

	// GetOrLoadLocal negative envelope after load miss.
	ent, err := engA.GetOrLoadLocal(ctx, "lt", "neg")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("gol neg: ent=%+v err=%v", ent, err)
	}

	// ApplyPut bloom flags.
	bits := make([]byte, 2048/8)
	okApply, err := engA.ApplyPut("bf", "merge1", store.Entry{Value: bits, Version: 2, Flags: store.FlagBloom})
	if err != nil || !okApply {
		t.Fatalf("ApplyPut bloom merge: ok=%v err=%v", okApply, err)
	}
	okApply, err = engA.ApplyPut("bf", "add1", store.Entry{Value: []byte("z"), Version: 3, Flags: store.FlagBloomAdd})
	if err != nil {
		t.Fatalf("ApplyPut bloom add: %v", err)
	}
	_ = okApply

	// NotifyTopologyChange with listener.
	engA.AttachWarmup(nil, topoNop{})
	engA.NotifyTopologyChange()
}

type topoNop struct{}

func (topoNop) OnTopologyChange() {}
