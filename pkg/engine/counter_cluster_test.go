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
)

func TestCounterReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "ctr", Mode: keyspace.ModeCounter,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "hits"
	n, err := c.Nodes()[0].Engine.Incr(ctx, "ctr", name, 3)
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, node := range c.Nodes() {
			if node.Engine.HasLocal("ctr", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("locals=%d", locals)
	}
	var nonReplica *engine.Engine
	for _, node := range c.Nodes() {
		v, ok, err := node.Engine.CounterGet(ctx, "ctr", name)
		if err != nil || !ok || v != 3 {
			t.Fatalf("%s: %d %v %v", node.ID, v, ok, err)
		}
		if !node.Engine.HasLocal("ctr", name) {
			nonReplica = node.Engine
		}
	}
	if nonReplica == nil {
		t.Fatal("expected a non-replica")
	}
}

func TestCounterNonOwnerIncr(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "ctr", Mode: keyspace.ModeCounter,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "n"
	var owner, other *engine.Engine
	for _, node := range c.Nodes() {
		if o, ok := node.Engine.OwnerOf(name); ok && o.ID == node.ID {
			owner = node.Engine
		} else {
			other = node.Engine
		}
	}
	if owner == nil || other == nil {
		t.Fatal("need owner and other")
	}
	n, err := other.Incr(ctx, "ctr", name, 4)
	if err != nil || n != 4 {
		t.Fatal(n, err)
	}
	v, ok, err := owner.CounterGet(ctx, "ctr", name)
	if err != nil || !ok || v != 4 {
		t.Fatal(v, ok, err)
	}
}

func TestCounterHintAfterPeerDown(t *testing.T) {
	const (
		addrA = "127.0.0.1:19861"
		addrB = "127.0.0.1:19862"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := counterKS()
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
		k := fmt.Sprintf("ck-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no key owned by a")
	}
	if _, err := engA.Incr(ctx, "ctr", name, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := engA.Incr(ctx, "ctr", name, 1); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v, ok, err := engB.CounterGet(ctx, "ctr", name)
		if err == nil && ok && v == 2 && engB.HasLocal("ctr", name) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	v, ok, _ := engA.CounterGet(ctx, "ctr", name)
	t.Fatalf("B missing count 2; owner=%d ok=%v", v, ok)
}
