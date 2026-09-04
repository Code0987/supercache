package bitmapx_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/bitmapx"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func TestBitmapReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "flags", Mode: keyspace.ModeBitmap,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "seen"
	if err := c.Nodes()[0].Engine.BitSet(ctx, "flags", name, 0, true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("flags", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local bitmap copies=%d want 2", locals)
	}
	var nonReplica *engine.Engine
	for _, n := range c.Nodes() {
		bit, ok, err := n.Engine.BitGet(ctx, "flags", name, 0)
		if err != nil || !ok || !bit {
			t.Fatalf("BitGet on %s: %v %v %v", n.ID, bit, ok, err)
		}
		if !n.Engine.HasLocal("flags", name) {
			nonReplica = n.Engine
		}
	}
	if nonReplica == nil {
		t.Fatal("expected a non-replica")
	}
}

func TestBitmapNonOwnerSet(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "flags", Mode: keyspace.ModeBitmap,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "seen"
	var owner, other *engine.Engine
	for _, n := range c.Nodes() {
		if o, ok := n.Engine.OwnerOf(name); ok && o.ID == n.ID {
			owner = n.Engine
		} else {
			other = n.Engine
		}
	}
	if owner == nil || other == nil {
		t.Fatal("need owner and other")
	}
	if err := other.BitSet(ctx, "flags", name, 0, true); err != nil {
		t.Fatal(err)
	}
	bit, ok, err := owner.BitGet(ctx, "flags", name, 0)
	if err != nil || !ok || !bit {
		t.Fatalf("owner: %v %v %v", bit, ok, err)
	}
}

func TestBitmapReplicaIgnoresInboxFlags(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(bitmapKS())
	r := ring.New(32)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: "127.0.0.1:2"}})
	e.SetNodeInfo("self", "127.0.0.1:1")
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	var name string
	for i := 0; i < 4000; i++ {
		k := fmt.Sprintf("bk-%d", i)
		if o, ok := e.OwnerOf(k); ok && o.ID != "self" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no non-owned name")
	}
	inbox := bitmapx.EncodeSet(0, true)
	applied, err := e.ApplyPut("flags", name, store.Entry{Value: inbox, Version: 1, Flags: store.FlagBitmapSet})
	if err != nil || applied {
		t.Fatalf("inbox on non-owner: applied=%v err=%v", applied, err)
	}
	if e.HasLocal("flags", name) {
		t.Fatal("inbox created local")
	}
}

func TestBitmapHintAfterPeerDownKeepsBothBits(t *testing.T) {
	const (
		addrA = "127.0.0.1:19881"
		addrB = "127.0.0.1:19882"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := bitmapKS()
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
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA, rB := ring.New(32), ring.New(32)
	rA.SetPeers(peers)
	rB.SetPeers(peers)
	trA := peer.NewTransport(200 * time.Millisecond)
	trB := peer.NewTransport(200 * time.Millisecond)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 2, QueueSize: 32})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 2, QueueSize: 32})
	defer foA.Close()
	defer foB.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	var name string
	for i := 0; i < 4000; i++ {
		k := fmt.Sprintf("bm-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no key owned by a")
	}
	if err := engA.BitSet(ctx, "flags", name, 0, true); err != nil {
		t.Fatal(err)
	}
	if err := engA.BitSet(ctx, "flags", name, 8, true); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	_ = engA.BitSet(ctx, "flags", name, 16, true)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b0, ok0, _ := engB.BitGet(ctx, "flags", name, 0)
		b8, ok8, _ := engB.BitGet(ctx, "flags", name, 8)
		if ok0 && b0 && ok8 && b8 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("B missing both offset 0 and 8")
}
