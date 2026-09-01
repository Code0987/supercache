package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/cmsx"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func cmsKS() keyspace.Config {
	return keyspace.Config{Name: "freq", Mode: keyspace.ModeCMS, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestCMSIncrQuery(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	if err := e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1); err != nil {
		t.Fatal(err)
	}
	if err := e.CMSIncr(ctx, "freq", "plays", []byte("bob"), 1); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("alice"))
	if err != nil || !ok || n < 1 {
		t.Fatalf("alice: %d %v %v", n, ok, err)
	}
	if err := e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1); err != nil {
		t.Fatal(err)
	}
	n2, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("alice"))
	if err != nil || !ok || n2 != n+1 {
		t.Fatalf("dup: %d → %d %v %v", n, n2, ok, err)
	}
}

func TestCMSIncrN(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	if err := e.CMSIncr(ctx, "freq", "plays", []byte("t"), 10); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("t"))
	if err != nil || !ok || n < 10 {
		t.Fatal(n, ok, err)
	}
	if err := e.CMSIncr(ctx, "freq", "plays", []byte("t"), 0); err != nil {
		t.Fatal(err)
	}
	n2, _, _ := e.CMSQuery(ctx, "freq", "plays", []byte("t"))
	if n2 != n+1 {
		t.Fatalf("n=0: %d → %d", n, n2)
	}
}

func TestCMSEmptyItem(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	if err := e.CMSIncr(ctx, "freq", "plays", nil, 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("nil: %v", err)
	}
	if err := e.CMSIncr(ctx, "freq", "plays", []byte{}, 2); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("empty: %v", err)
	}
	if _, _, err := e.CMSQuery(ctx, "freq", "plays", nil); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("query empty: %v", err)
	}
	if e.HasLocal("freq", "plays") {
		t.Fatal("created")
	}
}

func TestCMSItemTooLarge(t *testing.T) {
	e := engine.New(engine.WithLimits(8, 1<<20, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	err := e.CMSIncr(context.Background(), "freq", "plays", []byte("0123456789"), 1)
	if !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatal(err)
	}
}

func TestCMSOversizeSketch(t *testing.T) {
	e := engine.New(engine.WithLimits(64, 100, 10))
	defer e.Close()
	if err := e.UpdateKeySpace(cmsKS()); err != nil {
		t.Fatal(err)
	}
	if err := e.CMSIncr(context.Background(), "freq", "plays", []byte("a"), 1); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	if e.HasLocal("freq", "plays") {
		t.Fatal("inserted")
	}
}

func TestCMSWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "plays", Mode: keyspace.ModeTopK, MaxBytes: 1 << 20, TopKSize: 10})
	_ = e.UpdateKeySpace(cmsKS())
	_ = e.UpdateKeySpace(keyspace.Config{Name: "uniq", Mode: keyspace.ModeHLL, MaxBytes: 1 << 20})
	ctx := context.Background()
	if err := e.CMSIncr(ctx, "plays", "hot", []byte("x"), 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("CMSIncr on TopK: %v", err)
	}
	if _, err := e.Get(ctx, "freq", "b"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on CMS: %v", err)
	}
	if err := e.Put(ctx, "freq", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on CMS: %v", err)
	}
	if err := e.TopKAdd(ctx, "freq", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("TopKAdd on CMS: %v", err)
	}
	if _, _, err := e.CMSQuery(ctx, "uniq", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("CMSQuery on HLL: %v", err)
	}
	if err := e.HLLAdd(ctx, "freq", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HLLAdd on CMS: %v", err)
	}
}

func TestCMSMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	n, ok, err := e.CMSQuery(context.Background(), "freq", "nope", []byte("x"))
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}

func TestCMSEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1)
	if !e.HasLocal("freq", "plays") {
		t.Fatal("want live")
	}
	if err := e.Delete(ctx, "freq", "plays"); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("alice"))
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}

func TestCMSIncrAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1)
	_ = e.Delete(ctx, "freq", "plays")
	if err := e.CMSIncr(ctx, "freq", "plays", []byte("bob"), 1); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("bob"))
	if err != nil || !ok || n < 1 {
		t.Fatal(n, ok, err)
	}
}

func TestCMSTTLSlide(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1)
	ent, err := e.GetOrLoadLocal(ctx, "freq", "plays")
	if err != nil || !ent.IsCMS() || ent.ExpireAt == 0 {
		t.Fatalf("%+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("bob"), 1)
	ent, err = e.GetOrLoadLocal(ctx, "freq", "plays")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not slid %d then %d", first, ent.ExpireAt)
	}
}

func TestCMSGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	_, err := e.GetOrLoadLocal(context.Background(), "freq", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestCMSTombstoneBlocksSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1)
	_ = e.Delete(ctx, "freq", "plays")
	blob := make([]byte, cmsx.DenseSize)
	applied, err := e.ApplyPut("freq", "plays", store.Entry{
		Value: blob, Version: 1, Flags: store.FlagCMS,
	})
	if err != nil || applied {
		t.Fatalf("stale %v %v", applied, err)
	}
}

func TestCMSConcurrentIncrVersions(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("seed"), 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = e.CMSIncr(ctx, "freq", "plays", []byte("a"), 1)
	}()
	go func() {
		defer wg.Done()
		_ = e.CMSIncr(ctx, "freq", "plays", []byte("b"), 1)
	}()
	wg.Wait()
	ent, err := e.GetOrLoadLocal(ctx, "freq", "plays")
	if err != nil || ent.Version < 3 {
		t.Fatalf("ver=%d err=%v", ent.Version, err)
	}
	na, oka, _ := e.CMSQuery(ctx, "freq", "plays", []byte("a"))
	nb, okb, _ := e.CMSQuery(ctx, "freq", "plays", []byte("b"))
	if !oka || !okb || na < 1 || nb < 1 {
		t.Fatal(na, oka, nb, okb)
	}
}
