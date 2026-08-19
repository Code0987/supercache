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

func TestListReplicaRange(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "ls", Mode: keyspace.ModeList,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "q"
	if err := c.Nodes()[0].Engine.RPush(ctx, "ls", name, []byte("a")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("ls", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local list copies=%d want 2", locals)
	}
	for _, n := range c.Nodes() {
		r, err := n.Engine.LRange(ctx, "ls", name, 0, -1)
		if err != nil || len(r) != 1 || string(r[0]) != "a" {
			t.Fatalf("LRange on %s: %q %v", n.ID, r, err)
		}
	}
}

func TestListHintAfterPeerDownKeepsBothPushes(t *testing.T) {
	const (
		addrA = "127.0.0.1:19841"
		addrB = "127.0.0.1:19842"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := listKS()
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
		k := "l" + string(rune('a'+i%26)) + string(rune(i%10+'0'))
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		name = "list0"
	}
	if err := engA.RPush(ctx, "ls", name, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := engA.RPush(ctx, "ls", name, []byte("y")); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	time.Sleep(100 * time.Millisecond)
	_ = engA.RPush(ctx, "ls", name, []byte("z"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, err := engB.LRange(ctx, "ls", name, 0, -1)
		if err == nil && len(r) >= 2 {
			sawX, sawY := false, false
			for _, it := range r {
				if string(it) == "x" {
					sawX = true
				}
				if string(it) == "y" {
					sawY = true
				}
			}
			if sawX && sawY {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	r, _ := engA.LRange(ctx, "ls", name, 0, -1)
	t.Fatalf("B missing both x and y; owner=%q", r)
}
