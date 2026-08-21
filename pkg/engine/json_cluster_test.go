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
	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func TestJSONReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "doc", Mode: keyspace.ModeJSON,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "user"
	if err := c.Nodes()[0].Engine.JsonSet(ctx, "doc", name, "$", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("doc", name) {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local json copies=%d want 2", locals)
	}
	var nonReplica *engine.Engine
	for _, n := range c.Nodes() {
		v, ok, err := n.Engine.JsonGet(ctx, "doc", name, "$.a")
		if err != nil || !ok || !jsonx.Equal(v, []byte("1")) {
			t.Fatalf("JsonGet on %s: %q ok=%v err=%v", n.ID, v, ok, err)
		}
		if !n.Engine.HasLocal("doc", name) {
			nonReplica = n.Engine
		}
	}
	if nonReplica == nil {
		t.Fatal("expected a non-replica")
	}
}

func TestJSONNonOwnerSet(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes: 3,
		Keyspaces: []keyspace.Config{{
			Name: "doc", Mode: keyspace.ModeJSON,
			MaxBytes: 1 << 20, ReplicationFactor: 2,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	const name = "user"
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
	if err := other.JsonSet(ctx, "doc", name, "$.a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := owner.JsonGet(ctx, "doc", name, "$.a")
	if err != nil || !ok || !jsonx.Equal(v, []byte("1")) {
		t.Fatalf("owner: %s %v %v", v, ok, err)
	}
}

func TestJSONReplicaIgnoresInboxFlags(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1}`))
	// Single-node is owner, so ApplyPut of inbox flags would apply. Attach a
	// ring where this node is not owner of "user".
	r := ring.New(32)
	r.SetPeers([]ring.Peer{{ID: "self", Addr: "127.0.0.1:1"}, {ID: "other", Addr: "127.0.0.1:2"}})
	e.SetNodeInfo("self", "127.0.0.1:1")
	e.AttachCluster(&engine.Cluster{SelfID: "self", Ring: r})
	// Pick a name we do not own.
	var name string
	for i := 0; i < 4000; i++ {
		k := "k" + string(rune('a'+i%26)) + string(rune(i%10+'0'))
		if o, ok := e.OwnerOf(k); ok && o.ID != "self" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no non-owned name")
	}
	_ = e.UpdateKeySpace(jsonKS())
	inbox := jsonx.EncodeSet("$.a", []byte("1"))
	applied, err := e.ApplyPut("doc", name, store.Entry{Value: inbox, Version: 1, Flags: store.FlagJSONSet})
	if err != nil || applied {
		t.Fatalf("inbox on non-owner: applied=%v err=%v", applied, err)
	}
	if e.HasLocal("doc", name) {
		t.Fatal("inbox created local")
	}
	_ = ctx
}

func TestJSONHintAfterPeerDownKeepsBothPaths(t *testing.T) {
	const (
		addrA = "127.0.0.1:19871"
		addrB = "127.0.0.1:19872"
	)
	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	cfg := jsonKS()
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
		k := fmt.Sprintf("jk-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			name = k
			break
		}
	}
	if name == "" {
		t.Fatal("no key owned by a")
	}
	if err := engA.JsonSet(ctx, "doc", name, "$.x", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := engA.JsonSet(ctx, "doc", name, "$.y", []byte("2")); err != nil {
		t.Fatal(err)
	}
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	_ = engA.JsonSet(ctx, "doc", name, "$.z", []byte("3"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, okx, _ := engB.JsonGet(ctx, "doc", name, "$.x")
		_, oky, _ := engB.JsonGet(ctx, "doc", name, "$.y")
		if okx && oky {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	vx, _, _ := engA.JsonGet(ctx, "doc", name, "$.x")
	vy, _, _ := engA.JsonGet(ctx, "doc", name, "$.y")
	t.Fatalf("B missing both x and y; owner x=%s y=%s", vx, vy)
}
