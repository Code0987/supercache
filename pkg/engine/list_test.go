package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/listx"
	"github.com/Code0987/supercache/pkg/store"
)

func listKS() keyspace.Config {
	return keyspace.Config{Name: "ls", Mode: keyspace.ModeList, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestListPushPopRange(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(listKS()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = e.RPush(ctx, "ls", "q", []byte("a"))
	_ = e.RPush(ctx, "ls", "q", []byte("b"))
	_ = e.LPush(ctx, "ls", "q", []byte("z"))
	r, err := e.LRange(ctx, "ls", "q", 0, -1)
	if err != nil || len(r) != 3 || string(r[0]) != "z" || string(r[2]) != "b" {
		t.Fatalf("%q %v", r, err)
	}
	it, ok, err := e.LPop(ctx, "ls", "q")
	if err != nil || !ok || string(it) != "z" {
		t.Fatalf("%s %v %v", it, ok, err)
	}
	it, ok, err = e.RPop(ctx, "ls", "q")
	if err != nil || !ok || string(it) != "b" {
		t.Fatalf("%s %v %v", it, ok, err)
	}
	n, err := e.LLen(ctx, "ls", "q")
	if err != nil || n != 1 {
		t.Fatalf("len=%d %v", n, err)
	}
}

func TestListRangeNegatives(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(listKS())
	ctx := context.Background()
	_ = e.RPush(ctx, "ls", "q", []byte("a"))
	_ = e.RPush(ctx, "ls", "q", []byte("b"))
	_ = e.RPush(ctx, "ls", "q", []byte("c"))
	it, ok, err := e.LIndex(ctx, "ls", "q", -1)
	if err != nil || !ok || string(it) != "c" {
		t.Fatalf("%s %v %v", it, ok, err)
	}
	if _, ok, err := e.LIndex(ctx, "ls", "q", 9); err != nil || ok {
		t.Fatalf("oob %v %v", ok, err)
	}
	r, err := e.LRange(ctx, "ls", "q", -2, -1)
	if err != nil || len(r) != 2 || string(r[0]) != "b" {
		t.Fatalf("%q %v", r, err)
	}
}

func TestListWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "st", Mode: keyspace.ModeSet, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(listKS())
	ctx := context.Background()
	if err := e.LPush(ctx, "st", "q", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("LPush on Set: %v", err)
	}
	if err := e.Put(ctx, "ls", "q", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on List: %v", err)
	}
	if _, err := e.Get(ctx, "ls", "q"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on List: %v", err)
	}
}

func TestListMissingEmpty(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(listKS())
	ctx := context.Background()
	n, err := e.LLen(ctx, "ls", "nope")
	if err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	r, err := e.LRange(ctx, "ls", "nope", 0, -1)
	if err != nil || len(r) != 0 {
		t.Fatalf("%v %v", r, err)
	}
	_, ok, err := e.LPop(ctx, "ls", "nope")
	if err != nil || ok {
		t.Fatalf("pop %v %v", ok, err)
	}
}

func TestListEmptyAfterLastPop(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(listKS())
	ctx := context.Background()
	_ = e.RPush(ctx, "ls", "q", []byte("a"))
	_, _, _ = e.LPop(ctx, "ls", "q")
	n, err := e.LLen(ctx, "ls", "q")
	if err != nil || n != 0 {
		t.Fatalf("empty len=%d %v", n, err)
	}
}

func TestListDeleteTombstone(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(listKS())
	ctx := context.Background()
	_ = e.RPush(ctx, "ls", "q", []byte("a"))
	_ = e.Delete(ctx, "ls", "q")
	n, _ := e.LLen(ctx, "ls", "q")
	if n != 0 {
		t.Fatal(n)
	}
	l := listx.New()
	l.RPush([]byte("a"))
	applied, err := e.ApplyPut("ls", "q", store.Entry{Value: l.Encode(), Version: 1, Flags: store.FlagList})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale snapshot")
	}
}
