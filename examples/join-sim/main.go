// Simulate join cold-miss problem and async handoff fix.
//
// Automated coverage (keep in sync when changing handoff behavior):
//   go test ./pkg/engine/ -run 'TestJoinHandoffCoversOriginalProblem|TestJoinWithoutHandoff|TestJoinTopologyHandoff|TestJoinHandoffAvoids' -v
//   go test ./pkg/warmup/ -run 'TestHandoff' -v
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/warmup"
)

func main() {
	ctx := context.Background()
	const (
		addrA = "127.0.0.1:19401"
		addrB = "127.0.0.1:19402"
		addrC = "127.0.0.1:19403"
		nKeys = 120
	)

	var dsLoads int
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		dsLoads++
		return []byte("ds:" + key), nil
	})

	newEng := func(id, addr string) *engine.Engine {
		e := engine.New()
		e.SetNodeInfo(id, addr)
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 8 << 20, TTL: time.Minute,
		})
		_ = e.UpdateKeySpace(keyspace.Config{
			Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 8 << 20,
			TTL: time.Minute, DataSource: src, LoadTimeout: 2 * time.Second,
		})
		return e
	}

	engA, engB, engC := newEng("a", addrA), newEng("b", addrB), newEng("c", addrC)
	defer engA.Close()
	defer engB.Close()
	defer engC.Close()

	attachWM := func(e *engine.Engine) *warmup.Manager {
		wm := warmup.NewManager(e, warmup.Config{Workers: 8, TopN: 24, JobQueueSize: 8192})
		e.AttachWarmup(wm, wm)
		wm.Start(ctx)
		return wm
	}
	wmA, wmB, wmC := attachWM(engA), attachWM(engB), attachWM(engC)
	defer wmA.Stop()
	defer wmB.Stop()
	defer wmC.Stop()

	mustListen := func(addr string, e *engine.Engine) func() {
		gs, _, err := peerserver.ListenAndServe(addr, e)
		if err != nil {
			fail("listen %s: %v", addr, err)
		}
		return func() { gs.Stop() }
	}
	stopA, stopB, stopC := mustListen(addrA, engA), mustListen(addrB, engB), mustListen(addrC, engC)
	defer stopA()
	defer stopB()
	defer stopC()

	rA, rB, rC := ring.New(32), ring.New(32), ring.New(32)
	two := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(two)
	rB.SetPeers(two)

	mkCluster := func(e *engine.Engine, id string, r *ring.Ring, addr string) (*peer.Transport, *peer.FanoutPool) {
		tr := peer.NewTransport(time.Second)
		fo := peer.NewFanoutPool(tr, peer.FanoutConfig{Workers: 16, QueueSize: 4000})
		e.AttachCluster(&engine.Cluster{SelfID: id, Ring: r, Transport: tr, Fanout: fo})
		return tr, fo
	}
	trA, foA := mkCluster(engA, "a", rA, addrA)
	trB, foB := mkCluster(engB, "b", rB, addrB)
	defer foA.Close()
	defer foB.Close()
	defer trA.Close()
	defer trB.Close()

	keyAt := func(i int) string {
		return fmt.Sprintf("k-%04d-%x", i, uint32(i)*0x9e3779b1)
	}

	fmt.Println("=== SuperCache join handoff simulation ===")
	fmt.Println()
	fmt.Println("Phase 1: 2-node cluster (A,B) — seed CacheOnly + LoadThrough keys")

	for i := 0; i < nKeys; i++ {
		k := keyAt(i)
		if err := engA.Put(ctx, "demo", k, []byte("v-"+k)); err != nil {
			fail("put: %v", err)
		}
	}
	// Hot subset
	for i := 0; i < 15; i++ {
		for j := 0; j < 8; j++ {
			_, _ = engA.Get(ctx, "demo", keyAt(i))
		}
	}
	// LoadThrough seed
	ltKey := keyAt(3)
	v, err := engA.Get(ctx, "lt", ltKey)
	if err != nil {
		fail("lt seed: %v", err)
	}
	_ = v
	seedLoads := dsLoads

	// Wait fan-out majority on B
	deadline := time.Now().Add(3 * time.Second)
	var bHits int
	for time.Now().Before(deadline) {
		bHits = 0
		for i := 0; i < nKeys; i++ {
			if _, err := engB.Get(ctx, "demo", keyAt(i)); err == nil {
				bHits++
			}
		}
		if bHits >= nKeys/2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("  seeded %d CacheOnly keys; B local hits=%d/%d; DS loads so far=%d\n", nKeys, bHits, nKeys, seedLoads)
	fmt.Printf("  hot keys tracked on A (top 5): %v\n", engA.HotKeys("demo", 5))

	fmt.Println()
	fmt.Println("Phase 2: Join empty node C — expand ring (NO handoff yet)")
	three := []ring.Peer{
		{ID: "a", Addr: addrA},
		{ID: "b", Addr: addrB},
		{ID: "c", Addr: addrC},
	}
	rA.SetPeers(three)
	rB.SetPeers(three)
	rC.SetPeers(three)
	trC, foC := mkCluster(engC, "c", rC, addrC)
	defer foC.Close()
	defer trC.Close()

	// Ownership shift
	var ownedByC int
	for i := 0; i < nKeys; i++ {
		if o, ok := engC.OwnerOf(keyAt(i)); ok && o.ID == "c" {
			ownedByC++
		}
	}
	cold := countHits(engC, "demo", nKeys, keyAt)
	fmt.Printf("  ring remapped: keys owned by C = %d/%d\n", ownedByC, nKeys)
	fmt.Printf("  C local hits BEFORE handoff = %d/%d  ← ORIGINAL PROBLEM (cold joiner)\n", cold, nKeys)

	// Without waiting: if we Get LoadThrough on C as owner, would hit DS
	// Find C-owned key present on A
	var sample string
	for i := 0; i < nKeys; i++ {
		k := keyAt(i)
		o, ok := engC.OwnerOf(k)
		if !ok || o.ID != "c" {
			continue
		}
		if _, err := engA.Get(ctx, "demo", k); err == nil {
			sample = k
			break
		}
	}
	if sample == "" {
		fail("no C-owned key on A")
	}
	fmt.Printf("  sample C-owned key present on A: %q\n", sample)

	fmt.Println()
	fmt.Println("Phase 3: Trigger topology handoff (hot first, then rest)")
	t0 := time.Now()
	engA.NotifyTopologyChange()
	engB.NotifyTopologyChange()
	engC.NotifyTopologyChange()

	// Poll until C is warm
	var hotHits, allHits int
	for time.Now().Before(t0.Add(5 * time.Second)) {
		hotHits = 0
		for i := 0; i < 15; i++ {
			if _, err := engC.Get(ctx, "demo", keyAt(i)); err == nil {
				hotHits++
			}
		}
		allHits = countHits(engC, "demo", nKeys, keyAt)
		if hotHits == 15 && allHits >= int(float64(nKeys)*0.9) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	elapsed := time.Since(t0)

	// Check sample + LT key filled without extra DS if LT was handed off
	_, sampleErr := engC.Get(ctx, "demo", sample)
	// Wait for LT handoff entry
	ltFilled := false
	for time.Now().Before(t0.Add(5 * time.Second)) {
		for _, e := range engC.LocalEntries("lt") {
			if e.Key == ltKey {
				ltFilled = true
				break
			}
		}
		if ltFilled {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	loadsBeforeLTGet := dsLoads
	ltVal, ltErr := engC.Get(ctx, "lt", ltKey)
	extraLT := dsLoads - loadsBeforeLTGet

	fmt.Printf("  handoff elapsed: %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  C local hits AFTER handoff: hot=%d/15 total=%d/%d\n", hotHits, allHits, nKeys)
	fmt.Printf("  handoff jobs completed: A=%d B=%d C=%d\n", wmA.HandoffStats(), wmB.HandoffStats(), wmC.HandoffStats())
	fmt.Printf("  sample key on C: err=%v (want nil)\n", sampleErr)
	fmt.Printf("  LoadThrough key %q on C: filled_via_handoff=%v get_err=%v val_ok=%v extra_ds_loads=%d (want 0)\n",
		ltKey, ltFilled, ltErr, ltErr == nil && string(ltVal) == "ds:"+ltKey, extraLT)

	fmt.Println()
	fmt.Println("=== Verdict ===")
	ok := true
	if cold == 0 {
		fmt.Printf("PASS pre-condition: C was fully cold before handoff (%d hits)\n", cold)
	} else if cold <= nKeys/10 {
		fmt.Printf("PASS pre-condition: C mostly cold before handoff (%d hits)\n", cold)
	} else {
		fmt.Printf("WARN: expected C mostly cold before handoff, hits=%d\n", cold)
	}
	if allHits < int(float64(nKeys)*0.9) {
		fmt.Printf("FAIL: C not warm enough after handoff (%d/%d)\n", allHits, nKeys)
		ok = false
	} else {
		fmt.Printf("PASS: C warm after async handoff (%d/%d)\n", allHits, nKeys)
	}
	if hotHits < 15 {
		fmt.Printf("FAIL: hot keys not fully handed off (%d/15)\n", hotHits)
		ok = false
	} else {
		fmt.Printf("PASS: hot keys filled first/complete (%d/15)\n", hotHits)
	}
	if sampleErr != nil {
		fmt.Printf("FAIL: C-owned sample still missing: %v\n", sampleErr)
		ok = false
	} else {
		fmt.Printf("PASS: C-owned key present on C (ownership shift no longer means permanent miss)\n")
	}
	if !ltFilled || extraLT != 0 {
		fmt.Printf("FAIL: LoadThrough should be served from handoff without DS reload (filled=%v extra_ds=%d)\n", ltFilled, extraLT)
		ok = false
	} else {
		fmt.Printf("PASS: LoadThrough served from handoff with 0 extra DataSource loads\n")
	}
	if !ok {
		os.Exit(1)
	}
	fmt.Println()
	fmt.Println("Original problem verified fixed under simulation.")
}

func countHits(e *engine.Engine, ks string, n int, keyAt func(int) string) int {
	hits := 0
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, err := e.Get(ctx, ks, keyAt(i)); err == nil {
			hits++
		} else if !errors.Is(err, engine.ErrNotFound) {
			// ignore
		}
	}
	return hits
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
