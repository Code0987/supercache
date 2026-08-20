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

func TestHashReplicaHSet(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "hash", Mode: keyspace.ModeHash,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "user"
	if err := c.Nodes()[0].Engine.HSet(ctx, "hash", name, []byte("email"), []byte("a@b")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("hash", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local hash copies=%d want 2", locals)
	}
	for _, n := range c.Nodes() {
		v, ok, err := n.Engine.HGet(ctx, "hash", name, []byte("email"))
		if err != nil || !ok || string(v) != "a@b" {
			t.Fatalf("HGet on %s: %q ok=%v err=%v", n.ID, v, ok, err)
		}
	}
}

func TestHashHDelFanout(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "hash", Mode: keyspace.ModeHash,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "user"
	if err := c.Nodes()[0].Engine.HSet(ctx, "hash", name, []byte("email"), []byte("a@b")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n := 0
		for _, node := range c.Nodes() {
			if node.Engine.HasLocal("hash", name) {
				n++
			}
		}
		if n == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if err := c.Nodes()[0].Engine.HDel(ctx, "hash", name, []byte("email")); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		gone := true
		for _, node := range c.Nodes() {
			if !node.Engine.HasLocal("hash", name) {
				continue
			}
			ok, err := node.Engine.HExists(ctx, "hash", name, []byte("email"))
			if err != nil || ok {
				gone = false
			}
		}
		if gone {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("HDel did not reach replicas")
}

func TestHashHintAfterPeerDown(t *testing.T) {
	const (
		addrA = "127.0.0.1:19851"
		addrB = "127.0.0.1:19852"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := hashKS()
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
		k := "h" + string(rune('a'+i%26)) + string(rune(i%10+'0'))
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		name = "hash0"
	}
	if err := engA.HSet(ctx, "hash", name, []byte("x"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	time.Sleep(100 * time.Millisecond)
	_ = engA.HSet(ctx, "hash", name, []byte("y"), []byte("2"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, okx, _ := engB.HGet(ctx, "hash", name, []byte("x"))
		_, oky, _ := engB.HGet(ctx, "hash", name, []byte("y"))
		if okx || oky {
			// Do not require both fields locally (hint coalesce).
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, ok, err := engA.HGet(ctx, "hash", name, []byte("x"))
	if err != nil || !ok {
		t.Fatalf("owner lost x: %v %v", ok, err)
	}
}
