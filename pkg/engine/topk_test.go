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

func topkKS() keyspace.Config {
	return keyspace.Config{Name: "plays", Mode: keyspace.ModeTopK, MaxBytes: 1 << 20, TTL: time.Hour, TopKSize: 10}
}

func TestTopKAddList(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	if err := e.TopKAdd(ctx, "plays", "hot", []byte("t001")); err != nil {
		t.Fatal(err)
	}
	if err := e.TopKAdd(ctx, "plays", "hot", []byte("t002")); err != nil {
		t.Fatal(err)
	}
	if err := e.TopKAdd(ctx, "plays", "hot", []byte("t001")); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := e.TopKList(ctx, "plays", "hot")
	if err != nil || !ok || len(rows) != 2 {
		t.Fatalf("%v ok=%v err=%v", rows, ok, err)
	}
	if string(rows[0].Item) != "t001" || rows[0].Count != 2 {
		t.Fatalf("head %+v", rows[0])
	}
}

func TestTopKEmptyItem(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	if err := e.TopKAdd(ctx, "plays", "hot", nil); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("nil: %v", err)
	}
	if err := e.TopKAdd(ctx, "plays", "hot", []byte{}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("empty: %v", err)
	}
	if e.HasLocal("plays", "hot") {
		t.Fatal("created")
	}
}

func TestTopKItemTooLarge(t *testing.T) {
	e := engine.New(engine.WithLimits(8, 1<<20, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	err := e.TopKAdd(context.Background(), "plays", "hot", []byte("0123456789"))
	if !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatal(err)
	}
}

func TestTopKItemVsKeyspaceMaxKeyLen(t *testing.T) {
	e := engine.New()
	defer e.Close()
	cfg := topkKS()
	cfg.MaxKeyLen = 16
	if err := e.UpdateKeySpace(cfg); err != nil {
		t.Fatal(err)
	}
	err := e.TopKAdd(context.Background(), "plays", "hot", []byte("12345678901234567"))
	if !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatal(err)
	}
	if e.HasLocal("plays", "hot") {
		t.Fatal("created")
	}
}

func TestTopKOversizeTable(t *testing.T) {
	e := engine.New(engine.WithLimits(64, 100, 10))
	defer e.Close()
	if err := e.UpdateKeySpace(topkKS()); err != nil {
		t.Fatal(err)
	}
	if err := e.TopKAdd(context.Background(), "plays", "hot", []byte("a")); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	if e.HasLocal("plays", "hot") {
		t.Fatal("inserted")
	}
}

func TestTopKWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "uniq", Mode: keyspace.ModeHLL, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(topkKS())
	_ = e.UpdateKeySpace(keyspace.Config{Name: "board", Mode: keyspace.ModeZSet, MaxBytes: 1 << 20})
	ctx := context.Background()
	if err := e.TopKAdd(ctx, "uniq", "h", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("TopKAdd on HLL: %v", err)
	}
	if _, err := e.Get(ctx, "plays", "hot"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on TopK: %v", err)
	}
	if err := e.Put(ctx, "plays", "hot", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on TopK: %v", err)
	}
	if err := e.HLLAdd(ctx, "plays", "hot", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HLLAdd on TopK: %v", err)
	}
	if _, _, err := e.TopKList(ctx, "board", "hot"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("TopKList on ZSet: %v", err)
	}
}

func TestTopKMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	rows, ok, err := e.TopKList(context.Background(), "plays", "nope")
	if err != nil || ok || rows != nil {
		t.Fatal(rows, ok, err)
	}
}

func TestTopKEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t001"))
	if !e.HasLocal("plays", "hot") {
		t.Fatal("want live")
	}
	if err := e.Delete(ctx, "plays", "hot"); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := e.TopKList(ctx, "plays", "hot")
	if err != nil || ok || rows != nil {
		t.Fatal(rows, ok, err)
	}
}

func TestTopKAddAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t001"))
	_ = e.Delete(ctx, "plays", "hot")
	if err := e.TopKAdd(ctx, "plays", "hot", []byte("t002")); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := e.TopKList(ctx, "plays", "hot")
	if err != nil || !ok || len(rows) != 1 || string(rows[0].Item) != "t002" {
		t.Fatal(rows, ok, err)
	}
}

func TestTopKTTLSlide(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t001"))
	ent, err := e.GetOrLoadLocal(ctx, "plays", "hot")
	if err != nil || !ent.IsTopK() || ent.ExpireAt == 0 {
		t.Fatalf("%+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t002"))
	ent, err = e.GetOrLoadLocal(ctx, "plays", "hot")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not slid %d then %d", first, ent.ExpireAt)
	}
}

func TestTopKGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	_, err := e.GetOrLoadLocal(context.Background(), "plays", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestTopKTombstoneBlocksSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t001"))
	_ = e.Delete(ctx, "plays", "hot")
	applied, err := e.ApplyPut("plays", "hot", store.Entry{
		Value: []byte{1, 1, 'x'}, Version: 1, Flags: store.FlagTopK,
	})
	if err != nil || applied {
		t.Fatalf("stale %v %v", applied, err)
	}
}

func TestTopKConcurrentAddVersions(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("seed"))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = e.TopKAdd(ctx, "plays", "hot", []byte("a"))
	}()
	go func() {
		defer wg.Done()
		_ = e.TopKAdd(ctx, "plays", "hot", []byte("b"))
	}()
	wg.Wait()
	ent, err := e.GetOrLoadLocal(ctx, "plays", "hot")
	if err != nil || ent.Version < 3 {
		t.Fatalf("ver=%d err=%v", ent.Version, err)
	}
	rows, ok, err := e.TopKList(ctx, "plays", "hot")
	if err != nil || !ok || len(rows) < 3 {
		t.Fatal(rows, ok, err)
	}
}
