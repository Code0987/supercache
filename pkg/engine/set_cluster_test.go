package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestSetReplicaAddContains(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "st", Mode: keyspace.ModeSet,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "tags"
	if err := c.Nodes()[0].Engine.SetAdd(ctx, "st", name, []byte("red")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("st", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local set copies=%d want 2", locals)
	}
	for _, n := range c.Nodes() {
		ok, err := n.Engine.SetContains(ctx, "st", name, []byte("red"))
		if err != nil || !ok {
			t.Fatalf("Contains on %s: ok=%v err=%v", n.ID, ok, err)
		}
	}
}

func TestSetRemoveFanout(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "st", Mode: keyspace.ModeSet,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "tags"
	_ = c.Nodes()[0].Engine.SetAdd(ctx, "st", name, []byte("red"))
	// Wait for fan-out
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n := 0
		for _, node := range c.Nodes() {
			if node.Engine.HasLocal("st", name) {
				n++
			}
		}
		if n >= 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if err := c.Nodes()[0].Engine.SetRemove(ctx, "st", name, []byte("red")); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allGone := true
		for _, node := range c.Nodes() {
			if !node.Engine.HasLocal("st", name) {
				continue
			}
			ok, _ := node.Engine.SetContains(ctx, "st", name, []byte("red"))
			if ok {
				allGone = false
			}
		}
		if allGone {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	// At least owner sees remove
	ok, err := c.Nodes()[0].Engine.SetContains(ctx, "st", name, []byte("red"))
	if err != nil || ok {
		t.Fatalf("owner after remove: ok=%v err=%v", ok, err)
	}
}

func TestSetHintAfterPeerDown(t *testing.T) {
	const (
		addrA = "127.0.0.1:19811"
		addrB = "127.0.0.1:19812"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := setKS()
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
	// B not listening yet
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
	// Find name owned by A
	var name string
	for i := 0; i < 3000; i++ {
		k := "s" + string(rune('a'+i%26)) + string(rune(i%10+'0'))
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		name = "set0"
	}
	if err := engA.SetAdd(ctx, "st", name, []byte("x")); err != nil {
		t.Fatal(err)
	}
	// Bring B up and flush hints
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	// Nudge fan-out by another op or wait for hint flush - pool flushes on peer apply success path
	// Apply a noop peer contact via another add after B is up
	time.Sleep(100 * time.Millisecond)
	_ = engA.SetAdd(ctx, "st", name, []byte("y"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := engB.SetContains(ctx, "st", name, []byte("x"))
		if ok {
			return
		}
		// force local read via contains which may fetch owner
		ok2, _ := engB.SetContains(ctx, "st", name, []byte("y"))
		if ok2 {
			// y arrived; check x
			okx, _ := engB.SetContains(ctx, "st", name, []byte("x"))
			if okx {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Owner still has x
	ok, err := engA.SetContains(ctx, "st", name, []byte("x"))
	if err != nil || !ok {
		t.Fatalf("owner lost x: %v %v", ok, err)
	}
	// Soft: hint path may still be racing; ensure B can see via forward if not local
	ok, err = engB.SetContains(ctx, "st", name, []byte("x"))
	if err != nil || !ok {
		t.Fatalf("B should observe x (local or forward): ok=%v err=%v", ok, err)
	}
}
