package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/protect"
	"github.com/Code0987/supercache/pkg/store"
)

func TestCacheOnlyPutGetDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name:     "c",
		Mode:     keyspace.ModeCacheOnly,
		MaxBytes: 1 << 20,
		TTL:      time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := e.Get(ctx, "c", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	if err := e.Put(ctx, "c", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	v, err := e.Get(ctx, "c", "k")
	if err != nil || string(v) != "v" {
		t.Fatalf("get: %v %s", err, v)
	}
	if err := e.Delete(ctx, "c", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get(ctx, "c", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want not found after delete, got %v", err)
	}
}

func TestLoadThroughMissAndHit(t *testing.T) {
	var loads atomic.Int32
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loads.Add(1)
		if key == "missing" {
			return nil, datasource.ErrNotFound
		}
		return []byte("from-src:" + key), nil
	})
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name:       "lt",
		Mode:       keyspace.ModeLoadThrough,
		MaxBytes:   1 << 20,
		TTL:        time.Minute,
		DataSource: src,
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	v, err := e.Get(ctx, "lt", "a")
	if err != nil || string(v) != "from-src:a" {
		t.Fatalf("get: %v %s", err, v)
	}
	v, err = e.Get(ctx, "lt", "a")
	if err != nil || string(v) != "from-src:a" {
		t.Fatalf("second get: %v %s", err, v)
	}
	if loads.Load() != 1 {
		t.Fatalf("expected 1 load, got %d", loads.Load())
	}
}

func TestLoadThroughNegativeCache(t *testing.T) {
	var loads atomic.Int32
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loads.Add(1)
		return nil, datasource.ErrNotFound
	})
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name:        "lt",
		Mode:        keyspace.ModeLoadThrough,
		MaxBytes:    1 << 20,
		NegativeTTL: time.Minute,
		DataSource:  src,
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := e.Get(ctx, "lt", "x"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := e.Get(ctx, "lt", "x"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatalf("negative cache should suppress load; loads=%d", loads.Load())
	}
}

func TestPutOverridesNegative(t *testing.T) {
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return nil, datasource.ErrNotFound
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:        "lt",
		Mode:        keyspace.ModeLoadThrough,
		MaxBytes:    1 << 20,
		NegativeTTL: time.Minute,
		DataSource:  src,
	})
	ctx := context.Background()
	_, _ = e.Get(ctx, "lt", "k")
	if err := e.Put(ctx, "lt", "k", []byte("now")); err != nil {
		t.Fatal(err)
	}
	v, err := e.Get(ctx, "lt", "k")
	if err != nil || string(v) != "now" {
		t.Fatalf("got %v %s", err, v)
	}
}

func TestSingleflightStampede(t *testing.T) {
	var loads atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	src := datasource.Func(func(ctx context.Context, key string) ([]byte, error) {
		loads.Add(1)
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte("ok"), nil
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:       "lt",
		Mode:       keyspace.ModeLoadThrough,
		MaxBytes:   1 << 20,
		DataSource: src,
	})

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			v, err := e.Get(context.Background(), "lt", "hot")
			if err != nil {
				errCh <- err
				return
			}
			if string(v) != "ok" {
				errCh <- errors.New("bad value")
			}
		}()
	}
	<-started
	// give others time to join singleflight
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatalf("stampede: loads=%d want 1", loads.Load())
	}
}

func TestLWWApplyPut(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	_ = e.Put(ctx, "c", "k", []byte("v1"))
	ok, err := e.ApplyPut("c", "k", store.Entry{Value: []byte("old"), Version: 1})
	if err != nil || ok {
		t.Fatalf("stale apply should be rejected: ok=%v err=%v", ok, err)
	}
	// next local put should still work with higher version
	_ = e.Put(ctx, "c", "k", []byte("v2"))
	v, _ := e.Get(ctx, "c", "k")
	if string(v) != "v2" {
		t.Fatalf("got %s", v)
	}
}

func TestTTLExpiry(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	now := base
	e := engine.New(engine.WithNow(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:     "c",
		Mode:     keyspace.ModeCacheOnly,
		MaxBytes: 1 << 20,
		TTL:      time.Second,
	})
	ctx := context.Background()
	_ = e.Put(ctx, "c", "k", []byte("v"))
	mu.Lock()
	now = base.Add(2 * time.Second)
	mu.Unlock()
	if _, err := e.Get(ctx, "c", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want expired, got %v", err)
	}
}

func TestLimits(t *testing.T) {
	e := engine.New(engine.WithLimits(4, 8, 2))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	if err := e.Put(ctx, "c", "toolong", []byte("a")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("key: %v", err)
	}
	if err := e.Put(ctx, "c", "k", []byte("123456789")); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("val: %v", err)
	}
	if err := e.PutMany(ctx, "c", []engine.KV{{Key: "a"}, {Key: "b"}, {Key: "c"}}); !errors.Is(err, engine.ErrBatchTooLarge) {
		t.Fatalf("batch: %v", err)
	}
}

func TestLoadThroughRequiresDataSource(t *testing.T) {
	e := engine.New()
	defer e.Close()
	err := e.UpdateKeySpace(keyspace.Config{Name: "x", Mode: keyspace.ModeLoadThrough})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestCircuitBreaker(t *testing.T) {
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return nil, errors.New("boom")
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:       "lt",
		Mode:       keyspace.ModeLoadThrough,
		MaxBytes:   1 << 20,
		DataSource: src,
		CircuitBreaker: protect.Config{
			FailureThreshold: 2,
			OpenTimeout:      time.Minute,
		},
	})
	ctx := context.Background()
	_, _ = e.Get(ctx, "lt", "a")
	_, _ = e.Get(ctx, "lt", "a")
	_, err := e.Get(ctx, "lt", "a")
	if !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
}

func TestPutManyDeleteMany(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	if err := e.PutMany(ctx, "c", []engine.KV{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}}); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteMany(ctx, "c", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get(ctx, "c", "a"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestRaceGetPut(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:       "lt",
		Mode:       keyspace.ModeLoadThrough,
		MaxBytes:   1 << 20,
		DataSource: datasource.Map{"k": []byte("src")},
		TTL:        time.Minute,
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_ = e.Put(ctx, "lt", "k", []byte{byte(i)})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = e.Get(ctx, "lt", "k")
		}()
	}
	wg.Wait()
}

func TestConcurrentPutLWW(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = e.Put(ctx, "c", "k", []byte{byte(i)})
		}(i)
	}
	wg.Wait()
	// Store must hold some value; concurrent ApplyPut with low version must not win.
	ok, err := e.ApplyPut("c", "k", store.Entry{Value: []byte("stale"), Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		// Only OK if store somehow has version < 1, which should not happen after n puts.
		st, _ := e.Stats("c")
		t.Fatalf("stale ApplyPut accepted; stats=%+v", st)
	}
	v, err := e.Get(ctx, "c", "k")
	if err != nil || len(v) != 1 {
		t.Fatalf("get after concurrent put: %v %v", err, v)
	}
}

func TestLoadThroughDoesNotClobberPut(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	src := datasource.Func(func(ctx context.Context, key string) ([]byte, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte("from-src"), nil
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:       "lt",
		Mode:       keyspace.ModeLoadThrough,
		MaxBytes:   1 << 20,
		DataSource: src,
		TTL:        time.Minute,
	})
	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		_, err := e.Get(ctx, "lt", "k")
		errCh <- err
	}()
	<-started
	if err := e.Put(ctx, "lt", "k", []byte("from-put")); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	v, err := e.Get(ctx, "lt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "from-put" {
		t.Fatalf("load clobbered put: got %q", v)
	}
}

func TestNegativeDoesNotClobberPut(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	src := datasource.Func(func(ctx context.Context, key string) ([]byte, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, datasource.ErrNotFound
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:        "lt",
		Mode:        keyspace.ModeLoadThrough,
		MaxBytes:    1 << 20,
		NegativeTTL: time.Minute,
		DataSource:  src,
	})
	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		_, err := e.Get(ctx, "lt", "k")
		errCh <- err
	}()
	<-started
	if err := e.Put(ctx, "lt", "k", []byte("put-wins")); err != nil {
		t.Fatal(err)
	}
	close(release)
	// Get may return put value (preferred) or NotFound if negative won a race on
	// install — but final store after both complete must be put.
	<-errCh
	v, err := e.Get(ctx, "lt", "k")
	if err != nil || string(v) != "put-wins" {
		t.Fatalf("want put-wins, got %v %q", err, v)
	}
}

func TestWrappedNotFound(t *testing.T) {
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return nil, fmt.Errorf("wrap: %w", datasource.ErrNotFound)
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:           "lt",
		Mode:           keyspace.ModeLoadThrough,
		MaxBytes:       1 << 20,
		NegativeTTL:    time.Minute,
		DataSource:     src,
		CircuitBreaker: protect.Config{FailureThreshold: 1, OpenTimeout: time.Minute},
	})
	ctx := context.Background()
	if _, err := e.Get(ctx, "lt", "x"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	// Should not have tripped breaker (not found is success for protect)
	if _, err := e.Get(ctx, "lt", "x"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("negative/second: %v", err)
	}
}

func TestGetCancelledContext(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 64})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Get(ctx, "c", "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
}

// Canceling one LoadThrough Get must not fail a concurrent Get that still has a live context.
func TestSingleflightCanceledCallerDoesNotFailCoWaiters(t *testing.T) {
	var started sync.WaitGroup
	started.Add(1)
	release := make(chan struct{})
	src := datasource.Func(func(ctx context.Context, key string) ([]byte, error) {
		started.Done()
		select {
		case <-release:
			return []byte("ok"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20,
		TTL: time.Minute, DataSource: src,
	})

	ctx1, cancel1 := context.WithCancel(context.Background())
	errCh1 := make(chan error, 1)
	go func() {
		_, err := e.Get(ctx1, "lt", "k")
		errCh1 <- err
	}()
	started.Wait()

	// Second caller with an independent, non-canceled context joins the same singleflight.
	errCh2 := make(chan error, 1)
	valCh2 := make(chan []byte, 1)
	go func() {
		v, err := e.Get(context.Background(), "lt", "k")
		errCh2 <- err
		valCh2 <- v
	}()
	// Let second Get enter flight.Do waiters.
	time.Sleep(30 * time.Millisecond)
	cancel1()

	// Unblock the loader (if it still uses a non-canceled load context).
	close(release)

	err1 := <-errCh1
	err2 := <-errCh2
	// First may be canceled or still succeed depending on timing; second must succeed.
	if err2 != nil {
		t.Fatalf("co-waiter must not fail due to peer cancel: err2=%v err1=%v", err2, err1)
	}
	if string(<-valCh2) != "ok" {
		t.Fatal("co-waiter value")
	}
}

func TestEngineTombstoneTTLExpires(t *testing.T) {
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	e := engine.New(engine.WithNow(func() time.Time { return now }))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20,
		TombstoneTTL: time.Second,
	})
	ctx := context.Background()
	if err := e.Put(ctx, "c", "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(ctx, "c", "k"); err != nil {
		t.Fatal(err)
	}
	stale := store.Entry{Value: []byte("v1"), Version: 1}
	ok, err := e.ApplyPut("c", "k", stale)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("within TombstoneTTL a stale ApplyPut must not apply")
	}
	now = now.Add(2 * time.Second)
	ok, err = e.ApplyPut("c", "k", stale)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("after TombstoneTTL a stale ApplyPut may land")
	}
	got, err := e.Get(ctx, "c", "k")
	if err != nil || string(got) != "v1" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

// After Delete, a delayed ApplyPut with an older version must not resurrect the key.
func TestEngineNoResurrectionAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	if err := e.Put(ctx, "c", "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	// First Put mints version 1; Delete mints a higher tombstone version.
	stale := store.Entry{Value: []byte("v1"), Version: 1}
	if err := e.Delete(ctx, "c", "k"); err != nil {
		t.Fatal(err)
	}
	ok, err := e.ApplyPut("c", "k", stale)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stale ApplyPut after Delete must not apply")
	}
	if _, err := e.Get(ctx, "c", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("resurrected: %v", err)
	}
}

// ring_generation mismatches must not block LWW apply; they are metrics-only.
func TestApplyPutRingGenMismatchStillApplies(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})

	// Attach a ring so RingGeneration() is non-zero.
	r := ring.New(8)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}})
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	localGen := e.RingGeneration()
	if localGen == 0 {
		t.Fatal("expected non-zero ring gen")
	}

	ok, err := e.ApplyPutWithRingGen("c", "k", store.Entry{Value: []byte("v"), Version: 3}, localGen+99)
	if err != nil || !ok {
		t.Fatalf("LWW apply must succeed despite gen mismatch: ok=%v err=%v", ok, err)
	}
	v, err := e.Get(context.Background(), "c", "k")
	if err != nil || string(v) != "v" {
		t.Fatalf("get: %v %s", err, v)
	}
	snap := e.Metrics()
	if snap.RingGenMismatch < 1 {
		t.Fatalf("expected ring_gen_mismatch metric, snap=%+v", snap)
	}
	// Matching gen does not increment further when equal.
	before := snap.RingGenMismatch
	_, _ = e.ApplyPutWithRingGen("c", "k2", store.Entry{Value: []byte("x"), Version: 1}, localGen)
	if e.Metrics().RingGenMismatch != before {
		t.Fatalf("matching gen should not bump mismatch counter")
	}
}

// Version tracker must not grow without bound as keys are written and evicted.
func TestVersionTrackerBounded(t *testing.T) {
	const capN = 32
	e := engine.New(engine.WithMaxVersionKeys(capN))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly,
		// Tiny budget so entries are evicted quickly.
		MaxBytes: 256,
		TTL:      time.Minute,
	})
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%04d", i)
		if err := e.Put(ctx, "c", k, []byte("xxxxxxxx")); err != nil {
			// Some puts may fail MaxBytes for a single entry; use small values.
			_ = e.Put(ctx, "c", k, []byte("x"))
		}
	}
	n := e.VersionTrackerSize("c")
	if n < 0 {
		t.Fatal("keyspace missing")
	}
	// Allow a little headroom above cap while prune runs, but must not track all 200 forever.
	if n > capN*2 {
		t.Fatalf("version tracker size %d exceeds bound (cap=%d)", n, capN)
	}
}
