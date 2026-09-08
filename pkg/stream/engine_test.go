package stream_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/stream"
)

func sxKS() keyspace.Config {
	return keyspace.Config{Name: "events", Mode: keyspace.ModeStream, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestStreamXAddRangeLen(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	ctx := context.Background()
	id1, err := e.XAdd(ctx, "events", "logs", []byte("a"))
	if err != nil || !strings.Contains(id1, "-") {
		t.Fatalf("id1 %q %v", id1, err)
	}
	id2, err := e.XAdd(ctx, "events", "logs", []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.XLen(ctx, "events", "logs")
	if err != nil || !ok || n != 2 {
		t.Fatal(n, ok, err)
	}
	rows, err := e.XRange(ctx, "events", "logs", "-", "+", 0)
	if err != nil || len(rows) != 2 || rows[0].ID != id1 || string(rows[1].Payload) != "b" {
		t.Fatalf("%v %+v", err, rows)
	}
	_ = id2
}

func TestStreamXAddSameMilliSeq(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	ctx := context.Background()
	stream.NowMilli = func() uint64 { return 1_700_000_000_000 }
	t.Cleanup(func() { stream.NowMilli = func() uint64 { return uint64(time.Now().UnixMilli()) } })
	id1, err := e.XAdd(ctx, "events", "logs", []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := e.XAdd(ctx, "events", "logs", []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if id1 != "1700000000000-0" || id2 != "1700000000000-1" {
		t.Fatalf("%s %s", id1, id2)
	}
}

func TestStreamXRevRangeAndCount(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	ctx := context.Background()
	id1, _ := e.XAdd(ctx, "events", "logs", []byte("old"))
	id2, _ := e.XAdd(ctx, "events", "logs", []byte("new"))
	rows, err := e.XRevRange(ctx, "events", "logs", "-", "+", 1)
	if err != nil || len(rows) != 1 || rows[0].ID != id2 || string(rows[0].Payload) != "new" {
		t.Fatalf("%v %+v", err, rows)
	}
	rows, err = e.XRevRange(ctx, "events", "logs", "("+id1, "+", 0)
	if err != nil || len(rows) != 1 || rows[0].ID != id2 {
		t.Fatalf("exclusive %v %+v", err, rows)
	}
}

func TestStreamXDelAndEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	ctx := context.Background()
	id, _ := e.XAdd(ctx, "events", "logs", []byte("only"))
	if err := e.XDel(ctx, "events", "logs", id); err != nil {
		t.Fatal(err)
	}
	n, ok, err := e.XLen(ctx, "events", "logs")
	if err != nil || !ok || n != 0 {
		t.Fatal(n, ok, err)
	}
	if err := e.Delete(ctx, "events", "logs"); err != nil {
		t.Fatal(err)
	}
	n, ok, err = e.XLen(ctx, "events", "logs")
	if err != nil || ok || n != 0 {
		t.Fatal(n, ok, err)
	}
}

func TestStreamXTrimMaxLen(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	ctx := context.Background()
	_, _ = e.XAdd(ctx, "events", "logs", []byte("old"))
	id2, _ := e.XAdd(ctx, "events", "logs", []byte("new"))
	if err := e.XTrim(ctx, "events", "logs", 1); err != nil {
		t.Fatal(err)
	}
	rows, _ := e.XRange(ctx, "events", "logs", "-", "+", 0)
	if len(rows) != 1 || rows[0].ID != id2 {
		t.Fatalf("%+v", rows)
	}
}

func TestStreamAutoTrim(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "events", Mode: keyspace.ModeStream, MaxBytes: 1 << 20, StreamMaxLen: 2,
	})
	ctx := context.Background()
	_, _ = e.XAdd(ctx, "events", "logs", []byte("1"))
	_, _ = e.XAdd(ctx, "events", "logs", []byte("2"))
	id3, _ := e.XAdd(ctx, "events", "logs", []byte("3"))
	rows, _ := e.XRange(ctx, "events", "logs", "-", "+", 0)
	if len(rows) != 2 || rows[1].ID != id3 || string(rows[0].Payload) != "2" {
		t.Fatalf("%+v", rows)
	}
}

func TestStreamWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	_ = e.UpdateKeySpace(keyspace.Config{Name: "kv", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	ctx := context.Background()
	if _, err := e.Get(ctx, "events", "logs"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("get: %v", err)
	}
	if err := e.Put(ctx, "events", "logs", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("put: %v", err)
	}
	if _, err := e.XAdd(ctx, "kv", "k", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("xadd kv: %v", err)
	}
}

func TestStreamExclusiveStart(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	ctx := context.Background()
	id1, _ := e.XAdd(ctx, "events", "logs", []byte("a"))
	id2, _ := e.XAdd(ctx, "events", "logs", []byte("b"))
	rows, err := e.XRange(ctx, "events", "logs", "("+id1, "+", 0)
	if err != nil || len(rows) != 1 || rows[0].ID != id2 {
		t.Fatalf("%v %+v", err, rows)
	}
}

func TestStreamClusterRF(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "events", Mode: keyspace.ModeStream, MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	nodes := c.Nodes()
	id, err := nodes[0].Engine.XAdd(ctx, "events", "logs", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var live int
	for time.Now().Before(deadline) {
		live = 0
		for _, n := range nodes {
			if n.Engine.HasLocal("events", "logs") {
				live++
			}
		}
		if live == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if live != 2 {
		t.Fatalf("live copies %d", live)
	}
	for _, n := range nodes {
		rows, err := n.Engine.XRange(ctx, "events", "logs", "-", "+", 0)
		if err != nil || len(rows) != 1 || rows[0].ID != id {
			t.Fatalf("%s %v %+v", n.ID, err, rows)
		}
	}
}

func TestStreamAddReturnsIdFromNonOwner(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "events", Mode: keyspace.ModeStream, MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	nodes := c.Nodes()
	var via *engine.Engine
	for _, n := range nodes {
		if o, ok := n.Engine.OwnerOf("logs"); ok && o.ID != n.ID {
			via = n.Engine
			break
		}
	}
	if via == nil {
		t.Fatal("no non-owner")
	}
	id, err := via.XAdd(ctx, "events", "logs", []byte("via"))
	if err != nil || id == "" {
		t.Fatal(id, err)
	}
	rows, err := nodes[0].Engine.XRange(ctx, "events", "logs", "-", "+", 0)
	if err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("%v %+v", err, rows)
	}
}

func TestFlagStreamUint64Wire(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	lg := stream.New()
	_, _ = lg.Append([]byte("x"), 1, 0)
	blob := lg.Encode()
	ok, err := e.ApplyPut("events", "logs", store.Entry{Value: blob, Flags: store.FlagStream, Version: 1})
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if !e.HasLocal("events", "logs") {
		t.Fatal("missing after apply")
	}
	v := e.LocalView("events", "logs")
	if v.Flags&store.FlagStream == 0 {
		t.Fatalf("flags %#x", v.Flags)
	}
}

func TestStreamHardCap(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(sxKS())
	lg := stream.New()
	for i := 0; i < stream.HardCap; i++ {
		if _, err := lg.Append([]byte("x"), uint64(i+1), 0); err != nil {
			t.Fatal(i, err)
		}
	}
	ok, err := e.ApplyPut("events", "logs", store.Entry{Value: lg.Encode(), Flags: store.FlagStream, Version: 1})
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	_, err = e.XAdd(context.Background(), "events", "logs", []byte("y"))
	if !errors.Is(err, engine.ErrInvalidArgument) || !strings.Contains(err.Error(), "stream full") {
		t.Fatalf("got %v", err)
	}
}

func TestStreamNotEvicted(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "events", Mode: keyspace.ModeStream, MaxBytes: 1, TTL: time.Hour})
	ctx := context.Background()
	if _, err := e.XAdd(ctx, "events", "logs", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	if !e.HasLocal("events", "logs") {
		t.Fatal("evicted")
	}
}
