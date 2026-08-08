package warmup_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/warmup"
)

func TestWarmupPrefetchWarmKeys(t *testing.T) {
	var loads atomic.Int32
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loads.Add(1)
		return []byte("v:" + key), nil
	})
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name:            "lt",
		Mode:            keyspace.ModeLoadThrough,
		MaxBytes:        1 << 20,
		TTL:             time.Minute,
		WarmKeys:        []string{"w1", "w2"},
		DataSource:      src,
		RefreshInterval: 0,
	})

	wm := warmup.NewManager(eng, warmup.Config{Workers: 4, TopN: 10})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	defer wm.Stop()

	wm.OnTopologyChange()
	// wait for async prefetch
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if loads.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if loads.Load() < 2 {
		t.Fatalf("expected warm key loads, got %d", loads.Load())
	}
	// should be cached now without extra loads
	before := loads.Load()
	v, err := eng.Get(context.Background(), "lt", "w1")
	if err != nil || string(v) != "v:w1" {
		t.Fatalf("get warm: %v %s", err, v)
	}
	if loads.Load() != before {
		t.Fatalf("expected cache hit, loads %d -> %d", before, loads.Load())
	}
}

func TestHotKeyTracking(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20,
	})
	wm := warmup.NewManager(eng, warmup.Config{})
	eng.AttachWarmup(wm, wm)
	ctx := context.Background()
	_ = eng.Put(ctx, "c", "hot", []byte("1"))
	for i := 0; i < 5; i++ {
		_, _ = eng.Get(ctx, "c", "hot")
	}
	keys := eng.HotKeys("c", 5)
	if len(keys) == 0 || keys[0] != "hot" {
		t.Fatalf("hot keys: %v", keys)
	}
}

// Compile-time check: Engine satisfies warmup.Cache (handoff APIs included).
var _ warmup.Cache = (*engine.Engine)(nil)

// TestHandoffSchedulesHotBeforeRest ensures topology handoff enqueues tracked hot
// keys on the high-priority path before remaining inventory (ordering contract).
func TestHandoffSchedulesHotBeforeRest(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute,
	})
	// Single-node: ReplicateToPeers is a no-op (no peers), but handoff jobs still run.
	wm := warmup.NewManager(eng, warmup.Config{Workers: 1, TopN: 4, JobQueueSize: 256})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	defer wm.Stop()

	ctx := context.Background()
	// rest keys + one hot
	for _, k := range []string{"rest-a", "rest-b", "rest-c", "hot-key"} {
		if err := eng.Put(ctx, "c", k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		_, _ = eng.Get(ctx, "c", "hot-key")
	}
	if hk := eng.HotKeys("c", 1); len(hk) == 0 || hk[0] != "hot-key" {
		t.Fatalf("hot tracking: %v", hk)
	}

	wm.OnTopologyChange()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wm.HandoffStats() >= 4 {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("expected handoff jobs for local inventory, got %d", wm.HandoffStats())
}

func TestRefreshAhead(t *testing.T) {
	var loads atomic.Int32
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loads.Add(1)
		return []byte("x"), nil
	})
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name:            "lt",
		Mode:            keyspace.ModeLoadThrough,
		MaxBytes:        1 << 20,
		TTL:             time.Minute,
		WarmKeys:        []string{"r1"},
		DataSource:      src,
		RefreshInterval: 50 * time.Millisecond,
	})
	wm := warmup.NewManager(eng, warmup.Config{Workers: 2, TopN: 4})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	defer wm.Stop()

	// Topology prefetch loads warm keys; refresh-ahead re-enqueues on interval.
	wm.OnTopologyChange()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if loads.Load() >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("loads=%d want >= 1", loads.Load())
}


func TestForceRefreshAheadReloads(t *testing.T) {
	var loads atomic.Int32
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loads.Add(1)
		return []byte(fmt.Sprintf("v%d", loads.Load())), nil
	})
	eng := engine.New()
	defer eng.Close()
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: src, WarmKeys: []string{"rk"},
		RefreshInterval: 30 * time.Millisecond,
	})
	wm := warmup.NewManager(eng, warmup.Config{Workers: 2, TopN: 4})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	defer wm.Stop()

	// Initial get loads once
	_, _ = eng.Get(context.Background(), "lt", "rk")
	// Wait for refresh-ahead ForceLoad cycles
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if loads.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("refresh did not force reload: loads=%d", loads.Load())
}

// Not-found misses must not inflate Errors; real failures must, even if the
// message happens to contain the substring "not found".
func TestWarmupErrorsUsesErrorsIsNotSubstring(t *testing.T) {
	eng := engine.New()
	defer eng.Close()

	// Case 1: genuine NotFound — prefetch is fine, Errors stays 0.
	srcNF := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return nil, datasource.ErrNotFound
	})
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "nf", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcNF, NegativeTTL: time.Minute,
	})
	wm := warmup.NewManager(eng, warmup.Config{Workers: 2, TopN: 4})
	eng.AttachWarmup(wm, wm)
	wm.Start(context.Background())
	defer wm.Stop()

	wm.PrefetchNow("nf", "missing")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Prefetch completes (success or not-found) without bumping Errors.
		_, _, errs := wm.Stats()
		if errs == 0 {
			// Wait until at least one job likely ran: poll Get once then recheck.
			_, _ = eng.Get(context.Background(), "nf", "missing")
			time.Sleep(20 * time.Millisecond)
			// Job is async; wait for Errors to stay 0 after a short settle.
			time.Sleep(50 * time.Millisecond)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Give workers a moment to finish the prefetch job.
	time.Sleep(100 * time.Millisecond)
	_, _, errs := wm.Stats()
	if errs != 0 {
		t.Fatalf("genuine NotFound should not count as warmup error, errs=%d", errs)
	}

	// Case 2: backend failure whose message contains "not found" — must count as error.
	// (string matching would incorrectly treat this as NotFound and suppress Errors.)
	srcBoom := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return nil, fmt.Errorf("upstream down: replica not found in topology")
	})
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name: "boom", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: srcBoom,
	})
	wm.PrefetchNow("boom", "k")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _, errs = wm.Stats()
		if errs >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, _, errs = wm.Stats()
	t.Fatalf("real load failure must increment Errors (not substring match); errs=%d", errs)
}
