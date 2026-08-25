package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func TestHLLReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "uniq", Mode: keyspace.ModeHLL,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "visitors"
	if err := c.Nodes()[0].Engine.HLLAdd(ctx, "uniq", name, []byte("alice")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("uniq", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local HLL copies=%d want 2", locals)
	}
	var ownerN uint64
	for _, n := range c.Nodes() {
		cnt, ok, err := n.Engine.HLLCount(ctx, "uniq", name)
		if err != nil || !ok {
			t.Fatalf("HLLCount on %s: %v %v %v", n.ID, cnt, ok, err)
		}
		if o, okn := n.Engine.OwnerOf(name); okn && o.ID == n.ID {
			ownerN = cnt
		}
	}
	if ownerN < 1 || ownerN > 2 {
		t.Fatalf("owner count %d", ownerN)
	}
}

func TestHLLNonOwnerAdd(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "uniq", Mode: keyspace.ModeHLL,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "visitors"
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
	if err := other.HLLAdd(ctx, "uniq", name, []byte("alice")); err != nil {
		t.Fatal(err)
	}
	n, ok, err := owner.HLLCount(ctx, "uniq", name)
	if err != nil || !ok || n < 1 {
		t.Fatalf("owner: %v %v %v", n, ok, err)
	}
}

func TestHLLReplicaIgnoresInboxFlags(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hllKS())
	r := ring.New(32)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: "127.0.0.1:2"}})
	e.SetNodeInfo("self", "127.0.0.1:1")
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	var name string
	for i := 0; i < 4000; i++ {
		k := fmt.Sprintf("hk-%d", i)
		if o, ok := e.OwnerOf(k); ok && o.ID != "self" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no non-owned name")
	}
	applied, err := e.ApplyPut("uniq", name, store.Entry{Value: []byte("alice"), Version: 1, Flags: store.FlagHLLAdd})
	if err != nil || applied {
		t.Fatalf("inbox on non-owner: applied=%v err=%v", applied, err)
	}
	if e.HasLocal("uniq", name) {
		t.Fatal("inbox created local")
	}
}

func TestHLLHintAfterPeerDownKeepsBothItems(t *testing.T) {
	const (
		addrA = "127.0.0.1:19891"
		addrB = "127.0.0.1:19892"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := hllKS()
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
		k := fmt.Sprintf("hl-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no key owned by a")
	}
	if err := engA.HLLAdd(ctx, "uniq", name, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := engA.HLLAdd(ctx, "uniq", name, []byte("b")); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		na, oka, _ := engA.HLLCount(ctx, "uniq", name)
		nb, okb, _ := engB.HLLCount(ctx, "uniq", name)
		if oka && okb && na == nb && na >= 1 && na <= 4 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	na, _, _ := engA.HLLCount(ctx, "uniq", name)
	nb, _, _ := engB.HLLCount(ctx, "uniq", name)
	t.Fatalf("B missing both items A=%d B=%d", na, nb)
}
