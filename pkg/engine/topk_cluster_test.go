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
	"github.com/Code0987/supercache/pkg/topkx"
)

func topkClusterKS() keyspace.Config {
	return keyspace.Config{
		Name: "plays", Mode: keyspace.ModeTopK,
		MaxBytes: 1 << 20, ReplicationFactor: 2, TopKSize: 10,
	}
}

func topkHas(rows []engine.TopKEntry, item string) bool {
	for _, r := range rows {
		if string(r.Item) == item {
			return true
		}
	}
	return false
}

func TestTopKReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: []keyspace.Config{topkClusterKS()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "hot"
	if err := c.Nodes()[0].Engine.TopKAdd(ctx, "plays", name, []byte("t001")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("plays", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local TopK copies=%d want 2", locals)
	}
	var nonReplica *engine.Engine
	for _, n := range c.Nodes() {
		rows, ok, err := n.Engine.TopKList(ctx, "plays", name)
		if err != nil || !ok || !topkHas(rows, "t001") {
			t.Fatalf("TopKList on %s: %v ok=%v err=%v", n.ID, rows, ok, err)
		}
		if !n.Engine.HasLocal("plays", name) {
			nonReplica = n.Engine
		}
	}
	if nonReplica == nil {
		t.Fatal("expected a non-replica (GetOrLoad path)")
	}
}

func TestTopKNonOwnerAdd(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: []keyspace.Config{topkClusterKS()},
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
	if err := other.TopKAdd(ctx, "plays", name, []byte("t001")); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := owner.TopKList(ctx, "plays", name)
	if err != nil || !ok || !topkHas(rows, "t001") {
		t.Fatalf("owner: %v ok=%v err=%v", rows, ok, err)
	}
}

func TestTopKReplicaIgnoresInboxFlags(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	r := ring.New(32)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: "127.0.0.1:2"}})
	e.SetNodeInfo("self", "127.0.0.1:1")
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	var name string
	for i := 0; i < 4000; i++ {
		k := fmt.Sprintf("tk-%d", i)
		if o, ok := e.OwnerOf(k); ok && o.ID != "self" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no non-owned name")
	}
	applied, err := e.ApplyPut("plays", name, store.Entry{Value: []byte("t001"), Version: 1, Flags: store.FlagTopKAdd})
	if err != nil || applied {
		t.Fatalf("inbox on non-owner: applied=%v err=%v", applied, err)
	}
	if e.HasLocal("plays", name) {
		t.Fatal("inbox created local")
	}
}

func TestTopKHintAfterPeerDownKeepsBothTracks(t *testing.T) {
	const (
		addrA = "127.0.0.1:19931"
		addrB = "127.0.0.1:19932"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := topkKS()
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
		k := fmt.Sprintf("tkh-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no key owned by a")
	}
	if err := engA.TopKAdd(ctx, "plays", name, []byte("t001")); err != nil {
		t.Fatal(err)
	}
	if err := engA.TopKAdd(ctx, "plays", name, []byte("t002")); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ra, oka, _ := engA.TopKList(ctx, "plays", name)
		rb, okb, _ := engB.TopKList(ctx, "plays", name)
		if oka && okb && topkHas(ra, "t001") && topkHas(ra, "t002") && topkHas(rb, "t001") && topkHas(rb, "t002") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	ra, _, _ := engA.TopKList(ctx, "plays", name)
	rb, _, _ := engB.TopKList(ctx, "plays", name)
	t.Fatalf("B missing both tracks A=%v B=%v", ra, rb)
}

func TestTopKInstallKeepsBothSnapshots(t *testing.T) {
	// Two owner snapshots in version order: replica LWW replace must not drop the earlier track.
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(topkKS())
	tab := topkx.New(10)
	_ = tab.Add([]byte("t001"))
	v1 := tab.Encode()
	_ = tab.Add([]byte("t002"))
	v2 := tab.Encode()
	ok, err := e.ApplyPut("plays", "hot", store.Entry{Value: v1, Version: 1, Flags: store.FlagTopK})
	if err != nil || !ok {
		t.Fatalf("v1: %v %v", ok, err)
	}
	ok, err = e.ApplyPut("plays", "hot", store.Entry{Value: v2, Version: 2, Flags: store.FlagTopK})
	if err != nil || !ok {
		t.Fatalf("v2: %v %v", ok, err)
	}
	rows, present, err := e.TopKList(context.Background(), "plays", "hot")
	if err != nil || !present || !topkHas(rows, "t001") || !topkHas(rows, "t002") {
		t.Fatalf("want both tracks: %v present=%v err=%v", rows, present, err)
	}
	ok, err = e.ApplyPut("plays", "hot", store.Entry{Value: v1, Version: 1, Flags: store.FlagTopK})
	if err != nil || ok {
		t.Fatalf("stale v1 must not apply: %v %v", ok, err)
	}
	rows, _, _ = e.TopKList(context.Background(), "plays", "hot")
	if !topkHas(rows, "t002") {
		t.Fatal("stale install dropped t002")
	}
}
