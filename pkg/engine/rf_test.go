package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestReplicationFactorLimitsCopies(t *testing.T) {
	for _, rf := range []int{1, 2} {
		rf := rf
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			c, err := testcluster.Start(testcluster.Config{
				Nodes: 3,
				Keyspaces: []keyspace.Config{{
					Name: "bench", Mode: keyspace.ModeCacheOnly,
					MaxBytes: 64 << 20, TTL: time.Hour,
					ReplicationFactor: rf,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			ctx := context.Background()
			const key = "rf-limited"
			want := []byte("v")
			if err := c.Nodes()[0].Engine.Put(ctx, "bench", key, want); err != nil {
				t.Fatal(err)
			}

			deadline := time.Now().Add(2 * time.Second)
			var locals int
			for time.Now().Before(deadline) {
				locals = countLocal(c, "bench", key)
				if locals == rf {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if locals != rf {
				t.Fatalf("after Put: local copies=%d want %d", locals, rf)
			}

			for _, n := range c.Nodes() {
				got, err := n.Engine.Get(ctx, "bench", key)
				if err != nil {
					t.Fatalf("Get on %s: %v", n.ID, err)
				}
				if string(got) != string(want) {
					t.Fatalf("Get on %s: %q", n.ID, got)
				}
			}
			if got := countLocal(c, "bench", key); got != rf {
				t.Fatalf("Get must not widen RF: local copies=%d want %d", got, rf)
			}
		})
	}
}

func countLocal(c *testcluster.Cluster, ks, key string) int {
	n := 0
	for _, node := range c.Nodes() {
		if node.Engine.HasLocal(ks, key) {
			n++
		}
	}
	return n
}
