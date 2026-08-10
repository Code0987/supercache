package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
)

func TestStartClose1(t *testing.T) {
	c, err := Start(Config{Nodes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.CacheAddrs(); len(got) != 1 || got[0] == "" {
		t.Fatalf("addrs=%v", got)
	}
	fs := c.FanoutStats()
	if fs.HintsDropped != 0 {
		t.Fatalf("hints: %+v", fs)
	}
}

func TestStartClose3(t *testing.T) {
	c, err := Start(Config{Nodes: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if len(c.Nodes()) != 3 {
		t.Fatalf("nodes=%d", len(c.Nodes()))
	}
	ctx := context.Background()
	cli, err := client.Dial(ctx, c.CacheAddrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Put(ctx, "bench", "x", []byte("y")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hits := 0
		for _, n := range c.Nodes() {
			if _, err := n.Engine.Get(ctx, "bench", "x"); err == nil {
				hits++
			}
		}
		if hits >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fan-out did not reach peers; stats=%+v", c.FanoutStats())
}

func TestPrefillAllVerifyLocalHits(t *testing.T) {
	c, err := Start(Config{Nodes: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	val := []byte("v")
	const n = 50
	if err := c.PrefillAll(ctx, "bench", "p:", n, val); err != nil {
		t.Fatal(err)
	}
	if err := c.VerifyLocalHits(ctx, "bench", "p:", n, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.VerifyLocalHits(ctx, "bench", "p:", n, 10); err != nil {
		t.Fatal(err)
	}
}

func TestHitThenMissIsolated(t *testing.T) {
	ctx := context.Background()
	c1, err := Start(Config{Nodes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.PrefillAll(ctx, "bench", "k", 20, []byte("x")); err != nil {
		c1.Close()
		t.Fatal(err)
	}
	if _, err := c1.Nodes()[0].Engine.Get(ctx, "bench", "k0"); err != nil {
		c1.Close()
		t.Fatal(err)
	}
	c1.Close()

	c2, err := Start(Config{Nodes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_, err = c2.Nodes()[0].Engine.Get(ctx, "bench", "k0")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("second cluster must be empty, got %v", err)
	}
}

func TestRejectBadNodeCount(t *testing.T) {
	if _, err := Start(Config{Nodes: 2}); err == nil {
		t.Fatal("want error for N=2")
	}
}

func TestStartClose10(t *testing.T) {
	c, err := Start(Config{Nodes: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if len(c.CacheAddrs()) != 10 {
		t.Fatalf("addrs=%d", len(c.CacheAddrs()))
	}
	ctx := context.Background()
	if err := c.PrefillAll(ctx, "bench", "t:", 20, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := c.VerifyLocalHits(ctx, "bench", "t:", 20, 5); err != nil {
		t.Fatal(err)
	}
}
