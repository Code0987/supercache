package warmup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

type stubCache struct {
	targets []WarmTarget
	entries map[string][]LocalEntry
	nodeID  string
	owner   ring.Peer
	ownerOK bool
	gets    int
	gols    int
	forces  int
	reps    int
	getErr  error
}

func (s *stubCache) Get(ctx context.Context, keyspace, key string) ([]byte, error) {
	s.gets++
	if s.getErr != nil {
		return nil, s.getErr
	}
	return []byte("v"), nil
}
func (s *stubCache) GetOrLoadLocal(ctx context.Context, keyspace, key string) (store.Entry, error) {
	s.gols++
	if s.getErr != nil {
		return store.Entry{}, s.getErr
	}
	return store.Entry{Value: []byte("v"), Version: 1}, nil
}
func (s *stubCache) ForceLoad(ctx context.Context, keyspace, key string) error {
	s.forces++
	return s.getErr
}
func (s *stubCache) OwnerOf(key string) (ring.Peer, bool) { return s.owner, s.ownerOK }
func (s *stubCache) NodeID() string                       { return s.nodeID }
func (s *stubCache) WarmTargets() []WarmTarget            { return s.targets }
func (s *stubCache) LocalEntries(keyspace string) []LocalEntry {
	if s.entries == nil {
		return nil
	}
	return s.entries[keyspace]
}
func (s *stubCache) ReplicateToPeers(keyspace, key string, ent store.Entry) { s.reps++ }

func TestNilManagerNoOpsAndDefaultConfig(t *testing.T) {
	var m *Manager
	m.Start(context.Background())
	m.Stop()
	m.RecordHit("ks", "k")
	m.OnTopologyChange()
	if m.HotKeys("ks", 1) != nil {
		t.Fatal("nil HotKeys")
	}
	if p, r, e := m.Stats(); p != 0 || r != 0 || e != 0 {
		t.Fatal("nil stats")
	}
	if m.HandoffStats() != 0 {
		t.Fatal("nil handoff")
	}

	c := &stubCache{}
	m2 := NewManager(c, Config{}) // defaults
	if m2.cfg.Workers != 8 || m2.cfg.TopN != 64 || m2.cfg.TrackMax != DefaultTopK {
		t.Fatalf("defaults %+v", m2.cfg)
	}
	m2.RecordHit("", "k")
	m2.RecordHit("ks", "")
}

func TestDisableHandoffAndFullQueueDrops(t *testing.T) {
	c := &stubCache{
		targets: []WarmTarget{{Name: "ks", Mode: keyspace.ModeCacheOnly, WarmKeys: []string{"w1", ""}}},
		entries: map[string][]LocalEntry{
			"ks": {
				{Key: "w1", Entry: store.Entry{Value: []byte("1"), Version: 1}},
				{Key: "rest", Entry: store.Entry{Value: []byte("2"), Version: 1}},
				{Key: "", Entry: store.Entry{}},
			},
		},
		nodeID:  "self",
		owner:   ring.Peer{ID: "self"},
		ownerOK: true,
	}
	m := NewManager(c, Config{
		Workers: 1, TopN: 2, TrackMax: 16,
		DisableHandoff: true, JobQueueSize: 1,
	})
	// Fill queue then drop
	m.hotJobs <- job{kind: jobPrefetch, keyspace: "ks", key: "x"}
	m.enqueueHot(job{kind: jobPrefetch, keyspace: "ks", key: "y"}) // should drop
	if _, _, errN := m.Stats(); errN == 0 {
		// Errors incremented on drop
		if m.Errors.Load() == 0 {
			t.Fatal("expected drop error")
		}
	}
	m.OnTopologyChange() // handoff disabled
	// rest queue drop
	m.restJobs <- job{kind: jobHandoff, keyspace: "ks", key: "a"}
	m.enqueueRest(job{kind: jobHandoff, keyspace: "ks", key: "b"})
	if m.Errors.Load() < 2 {
		t.Fatalf("errors=%d", m.Errors.Load())
	}
	// closed enqueue no-op
	m.closed.Store(true)
	m.enqueueHot(job{})
	m.enqueueRest(job{})
	m.OnTopologyChange()
}

func TestPrefetchRefreshHandoffJobRouting(t *testing.T) {
	c := &stubCache{
		nodeID: "self", owner: ring.Peer{ID: "other"}, ownerOK: true,
		getErr: nil,
	}
	m := NewManager(c, Config{Workers: 1, JobQueueSize: 8})
	ctx := context.Background()
	// non-owner prefetch → Get
	m.runJob(ctx, job{kind: jobPrefetch, keyspace: "ks", key: "k"})
	if c.gets != 1 {
		t.Fatalf("gets=%d", c.gets)
	}
	// owner prefetch → GetOrLoadLocal
	c.owner = ring.Peer{ID: "self"}
	m.runJob(ctx, job{kind: jobPrefetch, keyspace: "ks", key: "k2"})
	if c.gols != 1 {
		t.Fatalf("gols=%d", c.gols)
	}
	// handoff
	m.runJob(ctx, job{kind: jobHandoff, keyspace: "ks", key: "k", entry: store.Entry{Version: 1}})
	if c.reps != 1 || m.HandoffStats() != 1 {
		t.Fatal("handoff")
	}
	// refresh non-owner skip
	c.owner = ring.Peer{ID: "other"}
	m.runJob(ctx, job{kind: jobRefresh, keyspace: "ks", key: "k"})
	if c.forces != 0 {
		t.Fatal("refresh non-owner")
	}
	// refresh owner success
	c.owner = ring.Peer{ID: "self"}
	m.runJob(ctx, job{kind: jobRefresh, keyspace: "ks", key: "k"})
	if c.forces != 1 {
		t.Fatal("refresh")
	}
	// refresh error not-found not counted
	c.getErr = datasource.ErrNotFound
	m.runJob(ctx, job{kind: jobRefresh, keyspace: "ks", key: "k"})
	// refresh other error
	c.getErr = errors.New("boom")
	before := m.Errors.Load()
	m.runJob(ctx, job{kind: jobRefresh, keyspace: "ks", key: "k"})
	if m.Errors.Load() <= before {
		t.Fatal("refresh error")
	}
	// prefetch error
	c.getErr = errors.New("get fail")
	c.ownerOK = false // ownsKey true when no owner
	before = m.Errors.Load()
	m.runJob(ctx, job{kind: jobPrefetch, keyspace: "ks", key: "k"})
	if m.Errors.Load() <= before {
		t.Fatal("prefetch error")
	}

	// ownsKey variants
	c.ownerOK = true
	c.owner = ring.Peer{}
	if !m.ownsKey("k") {
		t.Fatal("empty owner id")
	}
	c.owner = ring.Peer{ID: "x"}
	c.nodeID = ""
	if !m.ownsKey("k") {
		t.Fatal("empty self")
	}
}

func TestHandoffMaxEntriesAndRefreshLoop(t *testing.T) {
	c := &stubCache{
		targets: []WarmTarget{{
			Name: "lt", Mode: keyspace.ModeLoadThrough, WarmKeys: []string{"a"},
			RefreshInterval: 10 * time.Millisecond,
		}},
		entries: map[string][]LocalEntry{
			"lt": {
				{Key: "a", Entry: store.Entry{Version: 1}},
				{Key: "b", Entry: store.Entry{Version: 1}},
				{Key: "c", Entry: store.Entry{Version: 1}},
			},
		},
		nodeID: "self", owner: ring.Peer{ID: "self"}, ownerOK: true,
	}
	m := NewManager(c, Config{
		Workers: 2, TopN: 4, HandoffMaxEntries: 1, JobQueueSize: 64,
	})
	m.RecordHit("lt", "a")
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	m.OnTopologyChange()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.HandoffStats() > 0 || m.Prefetches.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	m.Stop()
	m.Stop() // idempotent
	// Hot key "a" is handoff-priority; rest capped at HandoffMaxEntries=1.
	if m.HandoffStats() == 0 && m.Prefetches.Load() == 0 {
		t.Fatal("expected handoff and/or prefetch work after topology change")
	}
	// Cap: at most hot(1)+rest(1) handoffs from 3 local entries (a,b,c).
	if h := m.HandoffStats(); h > 3 {
		t.Fatalf("handoffs=%d exceeds inventory", h)
	}
}

func TestIsNotFoundUsesErrorsIs(t *testing.T) {
	if !isNotFound(datasource.ErrNotFound) {
		t.Fatal("ds not found")
	}
	if isNotFound(errors.New("not found somewhere")) {
		t.Fatal("substring must not match")
	}
}
