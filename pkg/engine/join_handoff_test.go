package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/peerserver"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/warmup"
)

// TestJoinHandoffEndToEnd: join empty node C; cold before handoff, warm after
// (hot then rest); LoadThrough Get does not re-hit DataSource after fill.
func TestJoinHandoffEndToEnd(t *testing.T) {
	const (
		addrA = "127.0.0.1:19501"
		addrB = "127.0.0.1:19502"
		addrC = "127.0.0.1:19503"
		nKeys = 120
		nHot  = 15
	)

	var loads atomic.Int64
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		loads.Add(1)
		return []byte("from-ds:" + key), nil
	})

	engA := engine.New()
	engB := engine.New()
	engC := engine.New()
	defer engA.Close()
	defer engB.Close()
	defer engC.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	engC.SetNodeInfo("c", addrC)

	for _, e := range []*engine.Engine{engA, engB, engC} {
		if err := e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 8 << 20, TTL: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
		if err := e.UpdateKeySpace(keyspace.Config{
			Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 8 << 20,
			TTL: time.Minute, DataSource: src, LoadTimeout: 2 * time.Second,
		}); err != nil {
			t.Fatal(err)
		}
	}

	wmA := warmup.NewManager(engA, warmup.Config{Workers: 8, TopN: 32, JobQueueSize: 8192})
	wmB := warmup.NewManager(engB, warmup.Config{Workers: 8, TopN: 32, JobQueueSize: 8192})
	wmC := warmup.NewManager(engC, warmup.Config{Workers: 4, TopN: 32, JobQueueSize: 8192})
	engA.AttachWarmup(wmA, wmA)
	engB.AttachWarmup(wmB, wmB)
	engC.AttachWarmup(wmC, wmC)
	bg := context.Background()
	wmA.Start(bg)
	wmB.Start(bg)
	wmC.Start(bg)
	defer wmA.Stop()
	defer wmB.Stop()
	defer wmC.Stop()

	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	gsC, _, err := peerserver.ListenAndServe(addrC, engC)
	if err != nil {
		t.Fatal(err)
	}
	defer gsC.Stop()

	rA, rB, rC := ring.New(32), ring.New(32), ring.New(32)
	two := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(two)
	rB.SetPeers(two)

	trA := peer.NewTransport(time.Second)
	trB := peer.NewTransport(time.Second)
	trC := peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	defer trC.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 16, QueueSize: 4000})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 16, QueueSize: 4000})
	foC := peer.NewFanoutPool(trC, peer.FanoutConfig{Workers: 16, QueueSize: 4000})
	defer foA.Close()
	defer foB.Close()
	defer foC.Close()

	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	keyAt := func(i int) string {
		return fmt.Sprintf("k-%04d-%x", i, uint32(i)*0x9e3779b1)
	}

	for i := 0; i < nKeys; i++ {
		k := keyAt(i)
		if err := engA.Put(ctx, "demo", k, []byte("v-"+k)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	for i := 0; i < nHot; i++ {
		for j := 0; j < 8; j++ {
			_, _ = engA.Get(ctx, "demo", keyAt(i))
		}
	}
	ltKey := keyAt(3)
	if v, err := engA.Get(ctx, "lt", ltKey); err != nil || string(v) != "from-ds:"+ltKey {
		t.Fatalf("lt seed: %v %q", err, v)
	}
	seedLoads := loads.Load()
	waitFanoutHits(t, engB, "demo", nKeys, keyAt, nKeys/2, 3*time.Second)

	three := []ring.Peer{
		{ID: "a", Addr: addrA},
		{ID: "b", Addr: addrB},
		{ID: "c", Addr: addrC},
	}
	rA.SetPeers(three)
	rB.SetPeers(three)
	rC.SetPeers(three)
	engC.AttachCluster(&engine.Cluster{SelfID: "c", Ring: rC, Transport: trC, Fanout: foC})

	t.Run("before_handoff_joiner_is_cold", func(t *testing.T) {
		hits := countLocalHits(engC, "demo", nKeys, keyAt)
		if hits != 0 {
			t.Fatalf("C must be cold before handoff, hits=%d/%d", hits, nKeys)
		}
		var ownedByC int
		for i := 0; i < nKeys; i++ {
			if o, ok := engC.OwnerOf(keyAt(i)); ok && o.ID == "c" {
				ownedByC++
			}
		}
		if ownedByC == 0 {
			t.Fatal("expected some keys to remap ownership to C")
		}
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
			t.Fatal("need C-owned key present on A")
		}
		if _, err := engC.Get(ctx, "demo", sample); !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("C-owned sample %q should miss on cold C, err=%v", sample, err)
		}
	})

	engA.NotifyTopologyChange()
	engB.NotifyTopologyChange()
	engC.NotifyTopologyChange()

	t.Run("after_handoff_cacheonly_warm", func(t *testing.T) {
		deadline := time.Now().Add(5 * time.Second)
		var hits, hotHits, ownedByC, ownedHits int
		for time.Now().Before(deadline) {
			hits = countLocalHits(engC, "demo", nKeys, keyAt)
			hotHits = 0
			for i := 0; i < nHot; i++ {
				if _, err := engC.Get(ctx, "demo", keyAt(i)); err == nil {
					hotHits++
				}
			}
			ownedByC, ownedHits = 0, 0
			for i := 0; i < nKeys; i++ {
				k := keyAt(i)
				o, ok := engC.OwnerOf(k)
				if !ok || o.ID != "c" {
					continue
				}
				ownedByC++
				if _, err := engC.Get(ctx, "demo", k); err == nil {
					ownedHits++
				}
			}
			if hotHits == nHot && hits == nKeys && ownedHits == ownedByC && ownedByC > 0 {
				if wmA.HandoffStats() == 0 && wmB.HandoffStats() == 0 {
					t.Fatal("expected handoff jobs on A and/or B")
				}
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatalf("handoff incomplete: hits=%d/%d hot=%d/%d ownedHits=%d/%d handoffs A/B/C=%d/%d/%d",
			hits, nKeys, hotHits, nHot, ownedHits, ownedByC,
			wmA.HandoffStats(), wmB.HandoffStats(), wmC.HandoffStats())
	})

	t.Run("after_handoff_loadthrough_no_extra_ds", func(t *testing.T) {
		// Wait for LT entry via inventory (Get would load from DS if cold).
		deadline := time.Now().Add(5 * time.Second)
		filled := false
		for time.Now().Before(deadline) {
			for _, e := range engC.LocalEntries("lt") {
				if e.Key == ltKey && !e.Entry.IsNegative() {
					filled = true
					break
				}
			}
			if filled {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !filled {
			t.Fatalf("LT key %q not handed off to C (seedLoads=%d handoffs A/B=%d/%d)",
				ltKey, seedLoads, wmA.HandoffStats(), wmB.HandoffStats())
		}
		before := loads.Load()
		v, err := engC.Get(ctx, "lt", ltKey)
		if err != nil {
			t.Fatalf("Get LT on C: %v", err)
		}
		if string(v) != "from-ds:"+ltKey {
			t.Fatalf("value=%q", v)
		}
		if extra := loads.Load() - before; extra != 0 {
			t.Fatalf("Get on C re-hit DataSource after handoff; extra_loads=%d", extra)
		}
	})
}

// TestJoinWithoutHandoffStaysCold: with DisableHandoff, joiner stays empty for
// peer-held CacheOnly keys after topology notify.
func TestJoinWithoutHandoffStaysCold(t *testing.T) {
	const (
		addrA = "127.0.0.1:19511"
		addrB = "127.0.0.1:19512"
		addrC = "127.0.0.1:19513"
		nKeys = 80
	)

	engA := engine.New()
	engB := engine.New()
	engC := engine.New()
	defer engA.Close()
	defer engB.Close()
	defer engC.Close()
	engA.SetNodeInfo("a", addrA)
	engB.SetNodeInfo("b", addrB)
	engC.SetNodeInfo("c", addrC)

	for _, e := range []*engine.Engine{engA, engB, engC} {
		if err := e.UpdateKeySpace(keyspace.Config{
			Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 4 << 20, TTL: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfgOff := warmup.Config{Workers: 4, TopN: 16, DisableHandoff: true, JobQueueSize: 1024}
	wmA := warmup.NewManager(engA, cfgOff)
	wmB := warmup.NewManager(engB, cfgOff)
	wmC := warmup.NewManager(engC, cfgOff)
	engA.AttachWarmup(wmA, wmA)
	engB.AttachWarmup(wmB, wmB)
	engC.AttachWarmup(wmC, wmC)
	bg := context.Background()
	wmA.Start(bg)
	wmB.Start(bg)
	wmC.Start(bg)
	defer wmA.Stop()
	defer wmB.Stop()
	defer wmC.Stop()

	gsA, _, err := peerserver.ListenAndServe(addrA, engA)
	if err != nil {
		t.Fatal(err)
	}
	defer gsA.Stop()
	gsB, _, err := peerserver.ListenAndServe(addrB, engB)
	if err != nil {
		t.Fatal(err)
	}
	defer gsB.Stop()
	gsC, _, err := peerserver.ListenAndServe(addrC, engC)
	if err != nil {
		t.Fatal(err)
	}
	defer gsC.Stop()

	rA, rB, rC := ring.New(32), ring.New(32), ring.New(32)
	two := []ring.Peer{{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}}
	rA.SetPeers(two)
	rB.SetPeers(two)
	trA := peer.NewTransport(time.Second)
	trB := peer.NewTransport(time.Second)
	trC := peer.NewTransport(time.Second)
	defer trA.Close()
	defer trB.Close()
	defer trC.Close()
	foA := peer.NewFanoutPool(trA, peer.FanoutConfig{Workers: 8, QueueSize: 500})
	foB := peer.NewFanoutPool(trB, peer.FanoutConfig{Workers: 8, QueueSize: 500})
	foC := peer.NewFanoutPool(trC, peer.FanoutConfig{Workers: 8, QueueSize: 500})
	defer foA.Close()
	defer foB.Close()
	defer foC.Close()
	engA.AttachCluster(&engine.Cluster{SelfID: "a", Ring: rA, Transport: trA, Fanout: foA})
	engB.AttachCluster(&engine.Cluster{SelfID: "b", Ring: rB, Transport: trB, Fanout: foB})

	ctx := context.Background()
	keyAt := func(i int) string {
		return fmt.Sprintf("cold-%04d-%x", i, uint32(i)*0x9e3779b1)
	}
	for i := 0; i < nKeys; i++ {
		if err := engA.Put(ctx, "demo", keyAt(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	waitFanoutHits(t, engB, "demo", nKeys, keyAt, nKeys/2, 3*time.Second)

	three := []ring.Peer{
		{ID: "a", Addr: addrA}, {ID: "b", Addr: addrB}, {ID: "c", Addr: addrC},
	}
	rA.SetPeers(three)
	rB.SetPeers(three)
	rC.SetPeers(three)
	engC.AttachCluster(&engine.Cluster{SelfID: "c", Ring: rC, Transport: trC, Fanout: foC})

	engA.NotifyTopologyChange()
	engB.NotifyTopologyChange()
	engC.NotifyTopologyChange()

	time.Sleep(200 * time.Millisecond)
	hits := countLocalHits(engC, "demo", nKeys, keyAt)
	if hits != 0 {
		t.Fatalf("with DisableHandoff, C should stay cold; hits=%d handoffs A/B=%d/%d",
			hits, wmA.HandoffStats(), wmB.HandoffStats())
	}
	if wmA.HandoffStats() != 0 || wmB.HandoffStats() != 0 {
		t.Fatalf("DisableHandoff should not run handoff jobs; A=%d B=%d", wmA.HandoffStats(), wmB.HandoffStats())
	}
}

func countLocalHits(e *engine.Engine, ks string, n int, keyAt func(int) string) int {
	hits := 0
	for i := 0; i < n; i++ {
		if e.HasLocal(ks, keyAt(i)) {
			hits++
		}
	}
	return hits
}
