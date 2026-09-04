package bitmapx_test

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

func bitmapKS() keyspace.Config {
	return keyspace.Config{Name: "flags", Mode: keyspace.ModeBitmap, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestBitmapSetGetCountPos(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	if err := e.BitSet(ctx, "flags", "seen", 0, true); err != nil {
		t.Fatal(err)
	}
	if err := e.BitSet(ctx, "flags", "seen", 8, true); err != nil {
		t.Fatal(err)
	}
	bit, ok, err := e.BitGet(ctx, "flags", "seen", 0)
	if err != nil || !ok || !bit {
		t.Fatalf("0: %v %v %v", bit, ok, err)
	}
	bit, ok, err = e.BitGet(ctx, "flags", "seen", 8)
	if err != nil || !ok || !bit {
		t.Fatalf("8: %v %v %v", bit, ok, err)
	}
	bit, ok, err = e.BitGet(ctx, "flags", "seen", 3)
	if err != nil || !ok || bit {
		t.Fatalf("in-range 0: %v %v %v", bit, ok, err)
	}
	bit, ok, err = e.BitGet(ctx, "flags", "seen", 100)
	if err != nil || !ok || bit {
		t.Fatalf("past end: %v %v %v", bit, ok, err)
	}
	n, err := e.BitCount(ctx, "flags", "seen", 0, -1)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	n, err = e.BitCount(ctx, "flags", "seen", 0, 0)
	if err != nil || n != 1 {
		t.Fatal("byte0", n, err)
	}
	pos, found, err := e.BitPos(ctx, "flags", "seen", true, 0, -1)
	if err != nil || !found || pos != 0 {
		t.Fatal(pos, found, err)
	}
	pos, found, err = e.BitPos(ctx, "flags", "seen", true, 1, 1)
	if err != nil || !found || pos != 8 {
		t.Fatal(pos, found, err)
	}
}

func TestBitmapMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_, ok, err := e.BitGet(ctx, "flags", "nope", 0)
	if err != nil || ok {
		t.Fatal(ok, err)
	}
	n, err := e.BitCount(ctx, "flags", "nope", 0, -1)
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
	_, found, err := e.BitPos(ctx, "flags", "nope", true, 0, -1)
	if err != nil || found {
		t.Fatal(found, err)
	}
	if err := e.BitSet(ctx, "flags", "z", 0, false); err != nil {
		t.Fatal(err)
	}
	if !e.HasLocal("flags", "z") {
		t.Fatal("create on 0")
	}
}

func TestBitmapWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "doc", Mode: keyspace.ModeJSON, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	if err := e.BitSet(ctx, "doc", "b", 0, true); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("BitSet on JSON: %v", err)
	}
	if _, err := e.Get(ctx, "flags", "b"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on Bitmap: %v", err)
	}
	if err := e.Put(ctx, "flags", "b", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on Bitmap: %v", err)
	}
	if err := e.JsonSet(ctx, "flags", "b", "$", []byte(`{}`)); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("JsonSet on Bitmap: %v", err)
	}
}

func TestBitmapOversize(t *testing.T) {
	e := engine.New(engine.WithLimits(64, 2, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "flags", Mode: keyspace.ModeBitmap, MaxBytes: 1 << 20, MaxValueSize: 2,
	})
	ctx := context.Background()
	if err := e.BitSet(ctx, "flags", "b", 16, true); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	if e.HasLocal("flags", "b") {
		t.Fatal("inserted")
	}
	if err := e.BitSet(ctx, "flags", "b", 0, true); err != nil {
		t.Fatal(err)
	}
	if err := e.BitSet(ctx, "flags", "b", 16, true); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("grow: %v", err)
	}
	bit, ok, err := e.BitGet(ctx, "flags", "b", 0)
	if err != nil || !ok || !bit {
		t.Fatal("mutated")
	}
}

func TestBitmapEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	_ = e.BitSet(ctx, "flags", "seen", 0, false)
	n, err := e.BitCount(ctx, "flags", "seen", 0, -1)
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if !e.HasLocal("flags", "seen") {
		t.Fatal("want live empty")
	}
	if err := e.Delete(ctx, "flags", "seen"); err != nil {
		t.Fatal(err)
	}
	_, ok, err := e.BitGet(ctx, "flags", "seen", 0)
	if err != nil || ok {
		t.Fatal(ok, err)
	}
}

func TestBitmapSetAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	_ = e.Delete(ctx, "flags", "seen")
	if err := e.BitSet(ctx, "flags", "seen", 1, true); err != nil {
		t.Fatal(err)
	}
	bit, ok, err := e.BitGet(ctx, "flags", "seen", 1)
	if err != nil || !ok || !bit {
		t.Fatal(bit, ok, err)
	}
}

func TestBitmapLengthNeverShrinks(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 100, true)
	_ = e.BitSet(ctx, "flags", "seen", 100, false)
	_, ok, err := e.BitGet(ctx, "flags", "seen", 50)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
}

func TestBitmapTTLSlide(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	ent, err := e.GetOrLoadLocal(ctx, "flags", "seen")
	if err != nil || !ent.IsBitmap() || ent.ExpireAt == 0 {
		t.Fatalf("%+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_ = e.BitSet(ctx, "flags", "seen", 1, true)
	ent, err = e.GetOrLoadLocal(ctx, "flags", "seen")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not slid %d then %d", first, ent.ExpireAt)
	}
}

func TestBitmapGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	_, err := e.GetOrLoadLocal(context.Background(), "flags", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestBitmapTombstoneBlocksSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	_ = e.Delete(ctx, "flags", "seen")
	applied, err := e.ApplyPut("flags", "seen", store.Entry{
		Value: []byte{0x80}, Version: 1, Flags: store.FlagBitmap,
	})
	if err != nil || applied {
		t.Fatalf("stale %v %v", applied, err)
	}
}

func TestBitmapConcurrentSetVersions(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = e.BitSet(ctx, "flags", "seen", 1, true)
	}()
	go func() {
		defer wg.Done()
		_ = e.BitSet(ctx, "flags", "seen", 8, true)
	}()
	wg.Wait()
	ent, err := e.GetOrLoadLocal(ctx, "flags", "seen")
	if err != nil || ent.Version < 3 {
		t.Fatalf("ver=%d err=%v", ent.Version, err)
	}
	if bit, ok, _ := e.BitGet(ctx, "flags", "seen", 1); !ok || !bit {
		t.Fatal("lost 1")
	}
	if bit, ok, _ := e.BitGet(ctx, "flags", "seen", 8); !ok || !bit {
		t.Fatal("lost 8")
	}
}
