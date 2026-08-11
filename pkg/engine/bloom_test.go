package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func bloomKS() keyspace.Config {
	return keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom,
		MaxBytes: 1 << 20, BloomBits: 2048, BloomHashes: 4,
	}
}

func TestBloomAddTestDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(bloomKS()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.BloomAdd(ctx, "bf", "seen", []byte("id-1")); err != nil {
		t.Fatal(err)
	}
	maybe, err := e.BloomTest(ctx, "bf", "seen", []byte("id-1"))
	if err != nil || !maybe {
		t.Fatalf("test after add: maybe=%v err=%v", maybe, err)
	}
	if err := e.Delete(ctx, "bf", "seen"); err != nil {
		t.Fatal(err)
	}
	maybe, err = e.BloomTest(ctx, "bf", "seen", []byte("id-1"))
	if err != nil || maybe {
		t.Fatalf("after delete: maybe=%v err=%v", maybe, err)
	}
}

func TestBloomWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "kv", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(bloomKS())
	ctx := context.Background()
	if err := e.BloomAdd(ctx, "kv", "f", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("BloomAdd on CacheOnly: %v", err)
	}
	if err := e.Put(ctx, "bf", "f", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on ModeBloom: %v", err)
	}
	if _, err := e.Get(ctx, "bf", "f"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on ModeBloom: %v", err)
	}
}

func TestBloomMissingIsFalse(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bloomKS())
	maybe, err := e.BloomTest(context.Background(), "bf", "nope", []byte("x"))
	if err != nil || maybe {
		t.Fatalf("missing filter: maybe=%v err=%v", maybe, err)
	}
}

func TestBloomTombstoneBlocksAdd(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bloomKS())
	ctx := context.Background()
	_ = e.BloomAdd(ctx, "bf", "f", []byte("a"))
	_ = e.Delete(ctx, "bf", "f")
	ok, err := e.ApplyBloomMerge("bf", "f", make([]byte, 2048/8), 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stale merge must not beat tombstone")
	}
	if err := e.BloomAdd(ctx, "bf", "f", []byte("b")); err != nil {
		t.Fatal(err)
	}
	maybe, err := e.BloomTest(ctx, "bf", "f", []byte("b"))
	if err != nil || !maybe {
		t.Fatalf("add after delete: maybe=%v err=%v", maybe, err)
	}
}

func TestBloomReplicaAddAndTest(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "bf", Mode: keyspace.ModeBloom,
			MaxBytes: 1 << 20, BloomBits: 4096, BloomHashes: 4,
			ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "seen"
	if err := c.Nodes()[0].Engine.BloomAdd(ctx, "bf", name, []byte("item")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("bf", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local bloom copies=%d want 2", locals)
	}
	for _, n := range c.Nodes() {
		maybe, err := n.Engine.BloomTest(ctx, "bf", name, []byte("item"))
		if err != nil || !maybe {
			t.Fatalf("Test on %s: maybe=%v err=%v", n.ID, maybe, err)
		}
	}
	if got := countLocalBloom(c, name); got != 2 {
		t.Fatalf("Test must not widen RF: copies=%d", got)
	}
}

func countLocalBloom(c *testcluster.Cluster, name string) int {
	n := 0
	for _, node := range c.Nodes() {
		if node.Engine.HasLocal("bf", name) {
			n++
		}
	}
	return n
}

func TestBloomHintAfterPeerDown(t *testing.T) {
	const (
		addrA = "127.0.0.1:19701"
		addrB = "127.0.0.1:19702"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := bloomKS()
	cfg.ReplicationFactor = 2
	for _, e := range []*engine.Engine{engA, engB} {
		if err := e.UpdateKeySpace(cfg); err != nil {
			t.Fatal(err)
		}
	}
	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	rA, rB := ring.New(32), ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(80 * time.Millisecond)
	trB := peer.NewTransport(80 * time.Millisecond)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 4, QueueSize: 64, HintRetryInterval: 50 * time.Millisecond})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 64})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	var name string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("bloom-down-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no filter name owned by A")
	}
	gsB.Stop()
	if err := engA.BloomAdd(ctx, "bf", name, []byte("x")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fe, fd := engA.FanoutStats(); fe+fd > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	gsB2, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB2.Stop()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		maybe, err := engB.BloomTest(ctx, "bf", name, []byte("x"))
		if err == nil && maybe {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("B did not observe BloomAdd after return")
}
