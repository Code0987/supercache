package engine_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func counterKS() keyspace.Config {
	return keyspace.Config{Name: "ctr", Mode: keyspace.ModeCounter, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestCounterIncrGet(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	n, err := e.Incr(ctx, "ctr", "hits", 5)
	if err != nil || n != 5 {
		t.Fatal(n, err)
	}
	n, err = e.Incr(ctx, "ctr", "hits", 1)
	if err != nil || n != 6 {
		t.Fatal(n, err)
	}
	n, err = e.Incr(ctx, "ctr", "hits", -6)
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
	v, ok, err := e.CounterGet(ctx, "ctr", "hits")
	if err != nil || !ok || v != 0 {
		t.Fatal(v, ok, err)
	}
	n, err = e.Incr(ctx, "ctr", "touch", 0)
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

func TestCounterOverflow(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	if _, err := e.Incr(ctx, "ctr", "c", math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Incr(ctx, "ctr", "c", 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("overflow: %v", err)
	}
	v, ok, err := e.CounterGet(ctx, "ctr", "c")
	if err != nil || !ok || v != math.MaxInt64 {
		t.Fatal(v, ok, err)
	}
}

func TestCounterWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	if _, err := e.Incr(ctx, "hash", "c", 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Incr on Hash: %v", err)
	}
	if _, err := e.Get(ctx, "ctr", "c"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on Counter: %v", err)
	}
	if err := e.HSet(ctx, "ctr", "c", []byte("f"), []byte("v")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HSet on Counter: %v", err)
	}
}

func TestCounterMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	v, ok, err := e.CounterGet(context.Background(), "ctr", "nope")
	if err != nil || ok || v != 0 {
		t.Fatal(v, ok, err)
	}
}

func TestCounterEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	_, _ = e.Incr(ctx, "ctr", "c", 1)
	_, _ = e.Incr(ctx, "ctr", "c", -1)
	v, ok, err := e.CounterGet(ctx, "ctr", "c")
	if err != nil || !ok || v != 0 {
		t.Fatal(v, ok, err)
	}
	if !e.HasLocal("ctr", "c") {
		t.Fatal("want empty-until-delete")
	}
	if err := e.Delete(ctx, "ctr", "c"); err != nil {
		t.Fatal(err)
	}
	if e.HasLocal("ctr", "c") {
		t.Fatal("after delete")
	}
}

func TestCounterIncrAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	_, _ = e.Incr(ctx, "ctr", "c", 3)
	_ = e.Delete(ctx, "ctr", "c")
	n, err := e.Incr(ctx, "ctr", "c", 2)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
}

func TestCounterTTLSlide(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	_, _ = e.Incr(ctx, "ctr", "c", 1)
	ent, err := e.GetOrLoadLocal(ctx, "ctr", "c")
	if err != nil || !ent.IsCounter() || ent.ExpireAt == 0 {
		t.Fatalf("%+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_, _ = e.Incr(ctx, "ctr", "c", 1)
	ent, err = e.GetOrLoadLocal(ctx, "ctr", "c")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not slid %d then %d", first, ent.ExpireAt)
	}
}

func TestCounterGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	_, err := e.GetOrLoadLocal(context.Background(), "ctr", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestCounterTombstoneBlocksSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(counterKS())
	ctx := context.Background()
	_, _ = e.Incr(ctx, "ctr", "c", 1)
	_ = e.Delete(ctx, "ctr", "c")
	applied, err := e.ApplyPut("ctr", "c", store.Entry{
		Value: []byte{1, 0, 0, 0, 0, 0, 0, 0}, Version: 1, Flags: store.FlagCounter,
	})
	if err != nil || applied {
		t.Fatalf("stale %v %v", applied, err)
	}
}
