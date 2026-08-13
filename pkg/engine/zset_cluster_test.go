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

func TestZReplicaAddScore(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "zs", Mode: keyspace.ModeZSet,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "lb"
	if err := c.Nodes()[0].Engine.ZAdd(ctx, "zs", name, []byte("alice"), 10); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("zs", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local zset copies=%d want 2", locals)
	}
	for _, n := range c.Nodes() {
		sc, ok, err := n.Engine.ZScore(ctx, "zs", name, []byte("alice"))
		if err != nil || !ok || sc != 10 {
			t.Fatalf("ZScore on %s: sc=%v ok=%v err=%v", n.ID, sc, ok, err)
		}
	}
}

func TestZRemFanout(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "zs", Mode: keyspace.ModeZSet,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "lb"
	_ = c.Nodes()[0].Engine.ZAdd(ctx, "zs", name, []byte("alice"), 10)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n := 0
		for _, node := range c.Nodes() {
			if node.Engine.HasLocal("zs", name) {
				n++
			}
		}
		if n >= 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if err := c.Nodes()[0].Engine.ZRem(ctx, "zs", name, []byte("alice")); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allGone := true
		for _, node := range c.Nodes() {
			if !node.Engine.HasLocal("zs", name) {
				continue
			}
			_, ok, _ := node.Engine.ZScore(ctx, "zs", name, []byte("alice"))
			if ok {
				allGone = false
			}
		}
		if allGone {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	_, ok, err := c.Nodes()[0].Engine.ZScore(ctx, "zs", name, []byte("alice"))
	if err != nil || ok {
		t.Fatalf("owner after rem: ok=%v err=%v", ok, err)
	}
}

func TestZHintAfterPeerDown(t *testing.T) {
	const (
		addrA = "127.0.0.1:19821"
		addrB = "127.0.0.1:19822"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := zsetKS()
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
	for i := 0; i < 3000; i++ {
		k := "z" + string(rune('a'+i%26)) + string(rune(i%10+'0'))
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		name = "zset0"
	}
	if err := engA.ZAdd(ctx, "zs", name, []byte("x"), 1); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	time.Sleep(100 * time.Millisecond)
	_ = engA.ZAdd(ctx, "zs", name, []byte("y"), 2)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, ok, _ := engB.ZScore(ctx, "zs", name, []byte("x"))
		if ok {
			return
		}
		_, ok2, _ := engB.ZScore(ctx, "zs", name, []byte("y"))
		if ok2 {
			_, okx, _ := engB.ZScore(ctx, "zs", name, []byte("x"))
			if okx {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, ok, err := engA.ZScore(ctx, "zs", name, []byte("x"))
	if err != nil || !ok {
		t.Fatalf("owner lost x: %v %v", ok, err)
	}
}
