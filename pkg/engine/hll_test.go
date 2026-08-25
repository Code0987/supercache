package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func hllKS() keyspace.Config {
	return keyspace.Config{Name: "uniq", Mode: keyspace.ModeHLL, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestHLLAddCount(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	if err := e.HLLAdd(ctx, "uniq", "visitors", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	if err := e.HLLAdd(ctx, "uniq", "visitors", []byte("bob")); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.HLLCount(ctx, "uniq", "visitors")
	if err != nil || !ok || n < 1 || n > 4 {
		t.Fatalf("two items: %d %v %v", n, ok, err)
	}
	if err := e.HLLAdd(ctx, "uniq", "visitors", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	n2, ok, err := e.HLLCount(ctx, "uniq", "visitors")
	if err != nil || !ok || n2 != n {
		t.Fatalf("dup: %d → %d %v %v", n, n2, ok, err)
	}
}

func TestHLLEmptyItem(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	if err := e.HLLAdd(ctx, "uniq", "visitors", nil); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("nil: %v", err)
	}
	if err := e.HLLAdd(ctx, "uniq", "visitors", []byte{}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("empty: %v", err)
	}
	if e.HasLocal("uniq", "visitors") {
		t.Fatal("created")
	}
}

func TestHLLItemTooLarge(t *testing.T) {
	e := engine.New(engine.WithLimits(8, 1<<20, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	err := e.HLLAdd(context.Background(), "uniq", "visitors", []byte("0123456789"))
	if !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatal(err)
	}
}

func TestHLLOversizeSketch(t *testing.T) {
	e := engine.New(engine.WithLimits(64, 100, 10))
	defer e.Close()
	if err := e.UpdateKeySpace(hllKS()); err != nil {
		t.Fatal(err)
	}
	if err := e.HLLAdd(context.Background(), "uniq", "visitors", []byte("a")); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	if e.HasLocal("uniq", "visitors") {
		t.Fatal("inserted")
	}
}

func TestHLLWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "flags", Mode: keyspace.ModeBitmap, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(hllKS())
	_ = e.UpdateKeySpace(keyspace.Config{Name: "tags", Mode: keyspace.ModeSet, MaxBytes: 1 << 20})
	ctx := context.Background()
	if err := e.HLLAdd(ctx, "flags", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HLLAdd on Bitmap: %v", err)
	}
	if _, err := e.Get(ctx, "uniq", "b"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on HLL: %v", err)
	}
	if err := e.Put(ctx, "uniq", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on HLL: %v", err)
	}
	if err := e.BitSet(ctx, "uniq", "b", 0, true); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("BitSet on HLL: %v", err)
	}
	if _, _, err := e.HLLCount(ctx, "tags", "b"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HLLCount on Set: %v", err)
	}
}

func TestHLLMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	n, ok, err := e.HLLCount(context.Background(), "uniq", "nope")
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}

func TestHLLEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("alice"))
	if !e.HasLocal("uniq", "visitors") {
		t.Fatal("want live")
	}
	if err := e.Delete(ctx, "uniq", "visitors"); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.HLLCount(ctx, "uniq", "visitors")
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}

func TestHLLAddAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("alice"))
	_ = e.Delete(ctx, "uniq", "visitors")
	if err := e.HLLAdd(ctx, "uniq", "visitors", []byte("bob")); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.HLLCount(ctx, "uniq", "visitors")
	if err != nil || !ok || n < 1 {
		t.Fatal(n, ok, err)
	}
}

func TestHLLTTLSlide(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("alice"))
	ent, err := e.GetOrLoadLocal(ctx, "uniq", "visitors")
	if err != nil || !ent.IsHLL() || ent.ExpireAt == 0 {
		t.Fatalf("%+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("bob"))
	ent, err = e.GetOrLoadLocal(ctx, "uniq", "visitors")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not slid %d then %d", first, ent.ExpireAt)
	}
}

func TestHLLGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	_, err := e.GetOrLoadLocal(context.Background(), "uniq", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestHLLTombstoneBlocksSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("alice"))
	_ = e.Delete(ctx, "uniq", "visitors")
	blob := make([]byte, 12288)
	applied, err := e.ApplyPut("uniq", "visitors", store.Entry{
		Value: blob, Version: 1, Flags: store.FlagHLL,
	})
	if err != nil || applied {
		t.Fatalf("stale %v %v", applied, err)
	}
}

func TestHLLConcurrentAddVersions(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("seed"))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("a"))
	}()
	go func() {
		defer wg.Done()
		_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("b"))
	}()
	wg.Wait()
	ent, err := e.GetOrLoadLocal(ctx, "uniq", "visitors")
	if err != nil || ent.Version < 3 {
		t.Fatalf("ver=%d err=%v", ent.Version, err)
	}
	n, ok, err := e.HLLCount(ctx, "uniq", "visitors")
	if err != nil || !ok || n < 1 {
		t.Fatal(n, ok, err)
	}
}
