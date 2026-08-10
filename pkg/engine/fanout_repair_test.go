package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

// TestFanoutMissedWhilePeerDownIsRepairedOnReturn encodes:
//
//	t0  PUT k=v100
//	t1  owner stores v100
//	t2  client receives success
//	t3  peer B is down (peer gRPC stopped; still in the ring — no leave/join)
//	t4  fan-out ApplyPut → B fails (metric only, no retry)
//	t5  owner remains healthy
//	t6  B's listener returns — B must observe v100
//
// Missed ApplyPuts are remembered as per-peer hints and replayed after B returns.
func TestFanoutMissedWhilePeerDownIsRepairedOnReturn(t *testing.T) {
	const (
		addrA = "127.0.0.1:19601"
		addrB = "127.0.0.1:19602"
	)

	engA := engine.New()
	engB := engine.New()
	defer engA.Close()
	defer engB.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)

	for _, e := range []*engine.Engine{engA, engB} {
		if err := e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}

	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}

	rA, rB := ring.New(32), ring.New(32)
	peers := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(peers)
	rB.SetPeers(peers)

	// Short timeout so a down peer fails ApplyPut quickly.
	trA := peer.NewTransport(80 * time.Millisecond)
	trB := peer.NewTransport(80 * time.Millisecond)
	defer trA.Close()
	defer trB.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{
		Workers: 4, QueueSize: 64, HintRetryInterval: 50 * time.Millisecond,
	})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 4, QueueSize: 64})
	defer foA.Close()
	defer foB.Close()

	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	var key string
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("fanout-down-%d", i)
		if o, ok := engA.OwnerOf(k); ok && o.ID == "a" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by A")
	}

	// t3 (ahead of Put so fan-out cannot race a successful ApplyPut): B peer down.
	gsB.Stop()

	// t0–t2: PUT succeeds once owner has accepted.
	const want = "v100"
	if err := engA.Put(ctx, "demo", key, []byte(want)); err != nil {
		t.Fatalf("t2 Put should succeed on owner: %v", err)
	}

	// t1 / t5: owner still has the value.
	got, err := engA.Get(ctx, "demo", key)
	if err != nil || string(got) != want {
		t.Fatalf("t1/t5 owner: got %q err=%v want %s", got, err, want)
	}

	// t4: wait until fan-out has attempted B and failed.
	deadline := time.Now().Add(2 * time.Second)
	var ferr, fdrop uint64
	for time.Now().Before(deadline) {
		ferr, fdrop = engA.FanoutStats()
		if ferr+fdrop > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ferr+fdrop == 0 {
		t.Fatal("t4 expected fan-out error or drop to down peer B")
	}

	// t6: B returns (same empty store, still in ring — no NotifyTopologyChange).
	gsB2, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB2.Stop()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		v, gerr := engB.Get(ctx, "demo", key)
		if gerr == nil && string(v) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	v, gerr := engB.Get(ctx, "demo", key)
	if errors.Is(gerr, engine.ErrNotFound) {
		t.Fatalf("t6 B returned but still missing %s=%s (fanout err/drop=%d/%d); missed ApplyPut was not repaired",
			key, want, ferr, fdrop)
	}
	t.Fatalf("t6 B Get: val=%q err=%v want %s", v, gerr, want)
}
