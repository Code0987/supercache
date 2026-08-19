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

func TestGeoReplicaAddPos(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "geo", Mode: keyspace.ModeGeo,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "city"
	if err := c.Nodes()[0].Engine.GeoAdd(ctx, "geo", name, []byte("shop"), -74, 40.7); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("geo", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local geo copies=%d want 2", locals)
	}
	for _, n := range c.Nodes() {
		_, _, ok, err := n.Engine.GeoPos(ctx, "geo", name, []byte("shop"))
		if err != nil || !ok {
			t.Fatalf("GeoPos on %s: ok=%v err=%v", n.ID, ok, err)
		}
	}
}

func TestGeoHintAfterPeerDown(t *testing.T) {
	const (
		addrA = "127.0.0.1:19831"
		addrB = "127.0.0.1:19832"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := geoKS()
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
		k := "g" + string(rune('a'+i%26)) + string(rune(i%10+'0'))
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		name = "geo0"
	}
	if err := engA.GeoAdd(ctx, "geo", name, []byte("x"), -74, 40.7); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	time.Sleep(100 * time.Millisecond)
	_ = engA.GeoAdd(ctx, "geo", name, []byte("y"), -73, 40.8)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, ok, _ := engB.GeoPos(ctx, "geo", name, []byte("x"))
		if ok {
			return
		}
		_, _, ok2, _ := engB.GeoPos(ctx, "geo", name, []byte("y"))
		if ok2 {
			_, _, okx, _ := engB.GeoPos(ctx, "geo", name, []byte("x"))
			if okx {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, _, ok, err := engA.GeoPos(ctx, "geo", name, []byte("x"))
	if err != nil || !ok {
		t.Fatalf("owner lost x: %v %v", ok, err)
	}
}
