package vecset_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/vecset"
)

func vsClusterKS() keyspace.Config {
	return keyspace.Config{
		Name: "items", Mode: keyspace.ModeVectorSet,
		MaxBytes: 1 << 20, ReplicationFactor: 2,
	}
}

func TestVectorSetReplicaSnapshot(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: []keyspace.Config{vsClusterKS()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if err := c.Nodes()[0].Engine.VAdd(ctx, "items", "set", []byte("a"), []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var locals int
	for time.Now().Before(deadline) {
		locals = 0
		for _, n := range c.Nodes() {
			if n.Engine.HasLocal("items", "set") {
				locals++
			}
		}
		if locals == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if locals != 2 {
		t.Fatalf("local copies=%d want 2", locals)
	}
	var nonReplica *engine.Engine
	for _, n := range c.Nodes() {
		hits, err := n.Engine.VSim(ctx, "items", "set", []float32{1, 0}, 1)
		if err != nil || len(hits) != 1 || string(hits[0].Member) != "a" {
			t.Fatalf("VSim on %s: %v %v", n.ID, hits, err)
		}
		if !n.Engine.HasLocal("items", "set") {
			nonReplica = n.Engine
		}
	}
	if nonReplica == nil {
		t.Fatal("expected a non-replica")
	}
	if nonReplica.HasLocal("items", "set") {
		t.Fatal("non-replica installed")
	}
}

func TestVectorSetInboxIgnoredOnReplica(t *testing.T) {
	c, err := testcluster.Start(testcluster.Config{
		Nodes:     3,
		Keyspaces: []keyspace.Config{vsClusterKS()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	name := "set"
	// pick a non-owner
	var nonOwner *engine.Engine
	for _, n := range c.Nodes() {
		if o, ok := n.Engine.OwnerOf(name); ok && o.ID != n.ID {
			nonOwner = n.Engine
			break
		}
	}
	if nonOwner == nil {
		t.Fatal("no non-owner")
	}
	inbox, err := vecset.EncodeInboxAdd([]byte("x"), []float32{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := nonOwner.ApplyPut("items", name, store.Entry{Value: inbox, Flags: store.FlagVectorSet, Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("non-owner applied inbox")
	}
	_ = ctx
}
