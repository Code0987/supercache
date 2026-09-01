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
	"github.com/Code0987/supercache/pkg/cmsx"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func cmsClusterKS() keyspace.Config {
	return keyspace.Config{
		Name: "freq", Mode: keyspace.ModeCMS,
		MaxBytes: 1 << 20, ReplicationFactor: 2,
	}
}

func TestCMSReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: []keyspace.Config{cmsClusterKS()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "hot"
	if err := c.Nodes()[0].Engine.CMSIncr(ctx, "freq", name, []byte("t001"), 1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("freq", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local CMS copies=%d want 2", locals)
	}
	var nonReplica *engine.Engine
	for _, n := range c.Nodes() {
		cnt, ok, err := n.Engine.CMSQuery(ctx, "freq", name, []byte("t001"))
		if err != nil || !ok || cnt < 1 {
			t.Fatalf("CMSQuery on %s: %v %v %v", n.ID, cnt, ok, err)
		}
		if !n.Engine.HasLocal("freq", name) {
			nonReplica = n.Engine
		}
	}
	if nonReplica == nil {
		t.Fatal("expected a non-replica (GetOrLoad path)")
	}
}

func TestCMSNonOwnerIncr(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: []keyspace.Config{cmsClusterKS()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "hot"
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
	if err := other.CMSIncr(ctx, "freq", name, []byte("alice"), 1); err != nil {
		t.Fatal(err)
	}
	n, ok, err := owner.CMSQuery(ctx, "freq", name, []byte("alice"))
	if err != nil || !ok || n < 1 {
		t.Fatalf("owner: %v %v %v", n, ok, err)
	}
}

func TestCMSReplicaIgnoresInboxFlags(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(cmsKS())
	r := ring.New(32)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: "127.0.0.1:2"}})
	e.SetNodeInfo("self", "127.0.0.1:1")
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	var name string
	for i := 0; i < 4000; i++ {
		k := fmt.Sprintf("ck-%d", i)
		if o, ok := e.OwnerOf(k); ok && o.ID != "self" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no non-owned name")
	}
	inbox, err := cmsx.EncodeInbox(1, []byte("alice"))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := e.ApplyPut("freq", name, store.Entry{Value: inbox, Version: 1, Flags: store.FlagCMSIncr})
	if err != nil || applied {
		t.Fatalf("inbox on non-owner: applied=%v err=%v", applied, err)
	}
	if e.HasLocal("freq", name) {
		t.Fatal("inbox created local")
	}
}

func TestCMSHintAfterPeerDownKeepsBothItems(t *testing.T) {
	const (
		addrA = "127.0.0.1:19941"
		addrB = "127.0.0.1:19942"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := cmsKS()
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
		k := fmt.Sprintf("cm-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no key owned by a")
	}
	if err := engA.CMSIncr(ctx, "freq", name, []byte("a"), 1); err != nil {
		t.Fatal(err)
	}
	if err := engA.CMSIncr(ctx, "freq", name, []byte("b"), 1); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		aa, oka, _ := engA.CMSQuery(ctx, "freq", name, []byte("a"))
		ab, okb, _ := engA.CMSQuery(ctx, "freq", name, []byte("b"))
		ba, okba, _ := engB.CMSQuery(ctx, "freq", name, []byte("a"))
		bb, okbb, _ := engB.CMSQuery(ctx, "freq", name, []byte("b"))
		if oka && okb && okba && okbb && aa == ba && ab == bb && aa >= 1 && ab >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	aa, _, _ := engA.CMSQuery(ctx, "freq", name, []byte("a"))
	ba, _, _ := engB.CMSQuery(ctx, "freq", name, []byte("a"))
	t.Fatalf("B missing both items Aa=%d Ba=%d", aa, ba)
}
