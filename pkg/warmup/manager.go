package warmup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// Cache is the engine surface used by warmup (avoids import cycles).
type Cache interface {
	Get(ctx context.Context, keyspace, key string) ([]byte, error)
	GetOrLoadLocal(ctx context.Context, keyspace, key string) (store.Entry, error)
	// ForceLoad reloads from DataSource bypassing cache hits (refresh-ahead).
	ForceLoad(ctx context.Context, keyspace, key string) error
	OwnerOf(key string) (ring.Peer, bool)
	NodeID() string
	WarmTargets() []WarmTarget
}

// WarmTarget describes per-keyspace warmup config.
type WarmTarget struct {
	Name            string
	Mode            keyspace.Mode
	WarmKeys        []string
	RefreshInterval time.Duration
}

// Config for the Manager.
type Config struct {
	// Workers bounds concurrent prefetches.
	Workers int
	// TopN is how many hot keys to prefetch per keyspace.
	TopN int
	// TrackMax is tracker cardinality per keyspace.
	TrackMax int
}

// Manager tracks hot keys, prefetches on topology change, and refresh-ahead.
type Manager struct {
	cfg   Config
	cache Cache

	mu       sync.Mutex
	trackers map[string]*Tracker

	jobs   chan job
	wg     sync.WaitGroup
	cancel context.CancelFunc
	closed atomic.Bool

	Prefetches atomic.Uint64
	Refreshs   atomic.Uint64
	Errors     atomic.Uint64
}

type job struct {
	keyspace string
	key      string
	refresh  bool
}

// NewManager creates a warmup manager. Call Start to run workers.
func NewManager(cache Cache, cfg Config) *Manager {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.TopN <= 0 {
		cfg.TopN = 64
	}
	if cfg.TrackMax <= 0 {
		cfg.TrackMax = DefaultTopK
	}
	return &Manager{
		cfg:      cfg,
		cache:    cache,
		trackers: make(map[string]*Tracker),
		jobs:     make(chan job, 4096),
	}
}

// Start launches workers and optional refresh-ahead loop.
func (m *Manager) Start(parent context.Context) {
	if m == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel
	for i := 0; i < m.cfg.Workers; i++ {
		m.wg.Add(1)
		go m.worker(ctx)
	}
	m.wg.Add(1)
	go m.refreshLoop(ctx)
}

// Stop shuts down workers.
func (m *Manager) Stop() {
	if m == nil || !m.closed.CompareAndSwap(false, true) {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	// drain is not required; workers exit on ctx
	m.wg.Wait()
}

// RecordHit notes a key access for hot-key tracking.
func (m *Manager) RecordHit(keyspace, key string) {
	if m == nil || keyspace == "" || key == "" {
		return
	}
	m.tracker(keyspace).Hit(key)
}

// OnTopologyChange schedules prefetch of warm + hot keys (owned when clustered).
func (m *Manager) OnTopologyChange() {
	if m == nil || m.closed.Load() {
		return
	}
	for _, t := range m.cache.WarmTargets() {
		keys := m.prefetchSet(t)
		for _, k := range keys {
			m.enqueue(job{keyspace: t.Name, key: k, refresh: false})
		}
	}
}

// PrefetchNow runs a one-shot prefetch for tests.
func (m *Manager) PrefetchNow(keyspace, key string) {
	m.enqueue(job{keyspace: keyspace, key: key, refresh: false})
}

// HotKeys returns top-N hot keys for a keyspace.
func (m *Manager) HotKeys(keyspace string, n int) []string {
	if m == nil {
		return nil
	}
	return m.tracker(keyspace).Top(n)
}

// Stats returns counters.
func (m *Manager) Stats() (prefetches, refreshs, errors uint64) {
	if m == nil {
		return 0, 0, 0
	}
	return m.Prefetches.Load(), m.Refreshs.Load(), m.Errors.Load()
}

func (m *Manager) tracker(ks string) *Tracker {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.trackers[ks]
	if !ok {
		tr = NewTracker(m.cfg.TrackMax)
		m.trackers[ks] = tr
	}
	return tr
}

func (m *Manager) prefetchSet(t WarmTarget) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(k string) {
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		// When clustered, prefer keys we own; always include explicit WarmKeys.
		if owner, ok := m.cache.OwnerOf(k); ok {
			self := m.cache.NodeID()
			if self != "" && owner.ID != "" && owner.ID != self {
				// still allow WarmKeys to be prefetched via Get (routes to owner)
			}
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	for _, k := range t.WarmKeys {
		add(k)
	}
	for _, k := range m.tracker(t.Name).Top(m.cfg.TopN) {
		add(k)
	}
	return out
}

func (m *Manager) enqueue(j job) {
	if m.closed.Load() {
		return
	}
	select {
	case m.jobs <- j:
	default:
		// drop under pressure
		m.Errors.Add(1)
	}
}

func (m *Manager) worker(ctx context.Context) {
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-m.jobs:
			if !ok {
				return
			}
			m.runJob(ctx, j)
		}
	}
}

func (m *Manager) runJob(ctx context.Context, j job) {
	var err error
	if j.refresh {
		// True refresh-ahead: force DataSource reload (not a cache hit).
		err = m.cache.ForceLoad(ctx, j.keyspace, j.key)
	} else {
		// Prefer local owner fill when we own the key; else Get (may remote).
		owner, ok := m.cache.OwnerOf(j.key)
		self := m.cache.NodeID()
		if !ok || owner.ID == "" || owner.ID == self {
			_, err = m.cache.GetOrLoadLocal(ctx, j.keyspace, j.key)
		} else {
			_, err = m.cache.Get(ctx, j.keyspace, j.key)
		}
	}
	if err != nil {
		// NotFound / negative is fine for warmup.
		if !isNotFound(err) {
			m.Errors.Add(1)
		}
		return
	}
	if j.refresh {
		m.Refreshs.Add(1)
	} else {
		m.Prefetches.Add(1)
	}
}

func (m *Manager) refreshLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	// last refresh per keyspace name
	last := make(map[string]time.Time)

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, t := range m.cache.WarmTargets() {
				if t.RefreshInterval <= 0 {
					continue
				}
				if t.Mode != keyspace.ModeLoadThrough {
					continue
				}
				if prev, ok := last[t.Name]; ok && now.Sub(prev) < t.RefreshInterval {
					continue
				}
				last[t.Name] = now
				for _, k := range m.prefetchSet(t) {
					m.enqueue(job{keyspace: t.Name, key: k, refresh: true})
				}
			}
		}
	}
}

func isNotFound(err error) bool {
	// engine.ErrNotFound wraps datasource.ErrNotFound; do not substring-match
	// (that hides real failures whose messages mention "not found").
	return errors.Is(err, datasource.ErrNotFound)
}
