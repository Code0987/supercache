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
	"github.com/Code0987/supercache/pkg/zset"
)

func zsetKS() keyspace.Config {
	return keyspace.Config{Name: "zs", Mode: keyspace.ModeZSet, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestZAddScoreRemCard(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(zsetKS()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.ZAdd(ctx, "zs", "lb", []byte("alice"), 10); err != nil {
		t.Fatal(err)
	}
	if err := e.ZAdd(ctx, "zs", "lb", []byte("bob"), 20); err != nil {
		t.Fatal(err)
	}
	// Update score
	if err := e.ZAdd(ctx, "zs", "lb", []byte("alice"), 15); err != nil {
		t.Fatal(err)
	}
	sc, ok, err := e.ZScore(ctx, "zs", "lb", []byte("alice"))
	if err != nil || !ok || sc != 15 {
		t.Fatalf("score=%v ok=%v err=%v", sc, ok, err)
	}
	n, err := e.ZCard(ctx, "zs", "lb")
	if err != nil || n != 2 {
		t.Fatalf("card=%d err=%v", n, err)
	}
	if err := e.ZRem(ctx, "zs", "lb", []byte("bob")); err != nil {
		t.Fatal(err)
	}
	_, ok, err = e.ZScore(ctx, "zs", "lb", []byte("bob"))
	if err != nil || ok {
		t.Fatalf("after rem: ok=%v err=%v", ok, err)
	}
	n, err = e.ZCard(ctx, "zs", "lb")
	if err != nil || n != 1 {
		t.Fatalf("card after rem=%d err=%v", n, err)
	}
}

func TestZRangeRanksAndNegatives(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	// scores: a=1, b=2, c=3
	_ = e.ZAdd(ctx, "zs", "lb", []byte("a"), 1)
	_ = e.ZAdd(ctx, "zs", "lb", []byte("b"), 2)
	_ = e.ZAdd(ctx, "zs", "lb", []byte("c"), 3)
	all, err := e.ZRange(ctx, "zs", "lb", 0, -1)
	if err != nil || len(all) != 3 {
		t.Fatalf("all: %v %v", all, err)
	}
	if string(all[0].Member) != "a" || string(all[2].Member) != "c" {
		t.Fatalf("%+v", all)
	}
	last, err := e.ZRange(ctx, "zs", "lb", -1, -1)
	if err != nil || len(last) != 1 || string(last[0].Member) != "c" {
		t.Fatalf("last: %+v %v", last, err)
	}
	mid, err := e.ZRange(ctx, "zs", "lb", 1, 1)
	if err != nil || len(mid) != 1 || string(mid[0].Member) != "b" {
		t.Fatalf("mid: %+v %v", mid, err)
	}
}

func TestZRangeByScoreInclusive(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	_ = e.ZAdd(ctx, "zs", "lb", []byte("a"), 1)
	_ = e.ZAdd(ctx, "zs", "lb", []byte("b"), 2)
	_ = e.ZAdd(ctx, "zs", "lb", []byte("c"), 3)
	win, err := e.ZRangeByScore(ctx, "zs", "lb", 2, 3)
	if err != nil || len(win) != 2 {
		t.Fatalf("%+v %v", win, err)
	}
	if string(win[0].Member) != "b" || string(win[1].Member) != "c" {
		t.Fatalf("%+v", win)
	}
	// equal scores ordered by member bytes
	_ = e.ZAdd(ctx, "zs", "eq", []byte("m2"), 5)
	_ = e.ZAdd(ctx, "zs", "eq", []byte("m1"), 5)
	eq, err := e.ZRangeByScore(ctx, "zs", "eq", 5, 5)
	if err != nil || len(eq) != 2 {
		t.Fatalf("%+v %v", eq, err)
	}
	if string(eq[0].Member) != "m1" || string(eq[1].Member) != "m2" {
		t.Fatalf("equal-score order: %+v", eq)
	}
}

func TestZWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "kv", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(keyspace.Config{Name: "st", Mode: keyspace.ModeSet, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	if err := e.ZAdd(ctx, "kv", "z", []byte("x"), 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("ZAdd on CacheOnly: %v", err)
	}
	if err := e.ZAdd(ctx, "st", "z", []byte("x"), 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("ZAdd on ModeSet: %v", err)
	}
	if err := e.SetAdd(ctx, "zs", "z", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("SetAdd on ModeZSet: %v", err)
	}
	if err := e.Put(ctx, "zs", "z", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on ModeZSet: %v", err)
	}
	if _, err := e.Get(ctx, "zs", "z"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on ModeZSet: %v", err)
	}
}

func TestZMissingEmpty(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	sc, ok, err := e.ZScore(ctx, "zs", "nope", []byte("x"))
	if err != nil || ok || sc != 0 {
		t.Fatalf("score: %v %v %v", sc, ok, err)
	}
	n, err := e.ZCard(ctx, "zs", "nope")
	if err != nil || n != 0 {
		t.Fatalf("card: %d %v", n, err)
	}
	r, err := e.ZRange(ctx, "zs", "nope", 0, -1)
	if err != nil || len(r) != 0 {
		t.Fatalf("range: %v %v", r, err)
	}
	rs, err := e.ZRangeByScore(ctx, "zs", "nope", 0, 100)
	if err != nil || len(rs) != 0 {
		t.Fatalf("rangebyscore: %v %v", rs, err)
	}
}

func TestZNaNScore(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	if err := e.ZAdd(ctx, "zs", "lb", []byte("x"), math.NaN()); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("NaN: %v", err)
	}
}

func TestZInfScoreAllowed(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	if err := e.ZAdd(ctx, "zs", "lb", []byte("hi"), math.Inf(1)); err != nil {
		t.Fatal(err)
	}
	if err := e.ZAdd(ctx, "zs", "lb", []byte("lo"), math.Inf(-1)); err != nil {
		t.Fatal(err)
	}
	sc, ok, err := e.ZScore(ctx, "zs", "lb", []byte("hi"))
	if err != nil || !ok || !math.IsInf(sc, 1) {
		t.Fatalf("%v %v %v", sc, ok, err)
	}
}

func TestZDeleteNameTombstone(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	_ = e.ZAdd(ctx, "zs", "lb", []byte("a"), 1)
	if err := e.Delete(ctx, "zs", "lb"); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := e.ZScore(ctx, "zs", "lb", []byte("a"))
	if ok {
		t.Fatal("after delete")
	}
	// Stale snapshot must not resurrect.
	blob := zset.EncodeAdd([]byte("a"), 1)
	// Use full encode
	z := zset.New()
	_ = z.Add([]byte("a"), 1)
	blob = z.Encode()
	applied, err := e.ApplyPut("zs", "lb", store.Entry{Value: blob, Version: 1, Flags: store.FlagZSet})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale FlagZSet install")
	}
}

func TestZAddAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	_ = e.ZAdd(ctx, "zs", "lb", []byte("a"), 1)
	_ = e.Delete(ctx, "zs", "lb")
	if err := e.ZAdd(ctx, "zs", "lb", []byte("b"), 2); err != nil {
		t.Fatal(err)
	}
	sc, ok, err := e.ZScore(ctx, "zs", "lb", []byte("b"))
	if err != nil || !ok || sc != 2 {
		t.Fatalf("recreate: %v %v %v", sc, ok, err)
	}
}

func TestZEmptyAfterLastRem(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(zsetKS())
	ctx := context.Background()
	_ = e.ZAdd(ctx, "zs", "lb", []byte("a"), 1)
	_ = e.ZRem(ctx, "zs", "lb", []byte("a"))
	n, err := e.ZCard(ctx, "zs", "lb")
	if err != nil || n != 0 {
		t.Fatalf("empty card: %d %v", n, err)
	}
	// Rem missing is no-op
	if err := e.ZRem(ctx, "zs", "lb", []byte("a")); err != nil {
		t.Fatal(err)
	}
}
