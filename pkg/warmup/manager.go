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

// LocalEntry is one inventory item exported by the cache for peer handoff.
type LocalEntry struct {
	Key   string
	Entry store.Entry
}

// Cache is the engine surface used by warmup (avoids import cycles).
type Cache interface {
	Get(ctx context.Context, keyspace, key string) ([]byte, error)
	GetOrLoadLocal(ctx context.Context, keyspace, key string) (store.Entry, error)
	// ForceLoad reloads from DataSource bypassing cache hits (refresh-ahead).
	ForceLoad(ctx context.Context, keyspace, key string) error
	OwnerOf(key string) (ring.Peer, bool)
	NodeID() string
	WarmTargets() []WarmTarget
	// LocalEntries returns live non-tombstone entries for topology handoff.
	LocalEntries(keyspace string) []LocalEntry
	// ReplicateToPeers force-fans an entry to the key's replica set (async).
	ReplicateToPeers(keyspace, key string, ent store.Entry)
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
	// Workers bounds concurrent prefetches / handoff jobs.
	Workers int
	// TopN is how many hot keys to prefetch per keyspace.
	TopN int
	// TrackMax is tracker cardinality per keyspace.
	TrackMax int
	// DisableHandoff turns off push of local inventory on topology change
	// (prefetch of WarmKeys / hot keys still runs). Default false = handoff on.
	DisableHandoff bool
	// HandoffMaxEntries caps rest-of-keys push per topology event (0 = unlimited).
	// Hot keys are always attempted first and do not count against this cap.
	HandoffMaxEntries int
	// JobQueueSize is the per-priority queue depth (hot and rest). 0 → 4096.
	JobQueueSize int
}

// Manager tracks hot keys, prefetches on topology change, and refresh-ahead.
// On topology change it also pushes local inventory to peers: hot keys first,
// then the rest of live entries (async join / rebalance warm).
type Manager struct {
	cfg   Config
	cache Cache

	mu       sync.Mutex
	trackers map[string]*Tracker

	// Priority queues: workers always prefer hotJobs over restJobs.
	hotJobs  chan job
	restJobs chan job
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	closed   atomic.Bool

	Prefetches atomic.Uint64
	Refreshs   atomic.Uint64
	Handoffs   atomic.Uint64
	Errors     atomic.Uint64
}

type jobKind int

const (
	jobPrefetch jobKind = iota
	jobRefresh
	jobHandoff
)

type job struct {
	kind     jobKind
	keyspace string
	key      string
	entry    store.Entry // handoff only
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
	q := cfg.JobQueueSize
	if q <= 0 {
		q = 4096
	}
	return &Manager{
		cfg:      cfg,
		cache:    cache,
		trackers: make(map[string]*Tracker),
		hotJobs:  make(chan job, q),
		restJobs: make(chan job, q),
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

// OnTopologyChange schedules:
//  1. Pull-prefetch of WarmKeys + tracked hot keys (self fill / owner load)
//  2. Push-handoff of local inventory to peers: hot first, then rest
//
// Existing nodes seed joiners; the joiner itself has little to push until traffic.
func (m *Manager) OnTopologyChange() {
	if m == nil || m.closed.Load() {
		return
	}
	for _, t := range m.cache.WarmTargets() {
		for _, k := range m.prefetchSet(t) {
			m.enqueueHot(job{kind: jobPrefetch, keyspace: t.Name, key: k})
		}
		if m.cfg.DisableHandoff {
			continue
		}
		m.scheduleHandoff(t)
	}
}

// scheduleHandoff enqueues ReplicateToPeers for local entries: hot/warm first.
func (m *Manager) scheduleHandoff(t WarmTarget) {
	entries := m.cache.LocalEntries(t.Name)
	if len(entries) == 0 {
		return
	}
	hotSet := m.hotPrioritySet(t)
	var hot, rest []LocalEntry
	for _, e := range entries {
		if e.Key == "" {
			continue
		}
		if _, ok := hotSet[e.Key]; ok {
			hot = append(hot, e)
		} else {
			rest = append(rest, e)
		}
	}
	for _, e := range hot {
		m.enqueueHot(job{
			kind:     jobHandoff,
			keyspace: t.Name,
			key:      e.Key,
			entry:    e.Entry,
		})
	}
	limit := m.cfg.HandoffMaxEntries
	for i, e := range rest {
		if limit > 0 && i >= limit {
			break
		}
		m.enqueueRest(job{
			kind:     jobHandoff,
			keyspace: t.Name,
			key:      e.Key,
			entry:    e.Entry,
		})
	}
}

func (m *Manager) hotPrioritySet(t WarmTarget) map[string]struct{} {
	set := make(map[string]struct{})
	for _, k := range t.WarmKeys {
		if k != "" {
			set[k] = struct{}{}
		}
	}
	for _, k := range m.tracker(t.Name).Top(m.cfg.TopN) {
		if k != "" {
			set[k] = struct{}{}
		}
	}
	return set
}

// PrefetchNow runs a one-shot prefetch for tests (hot priority).
func (m *Manager) PrefetchNow(keyspace, key string) {
	m.enqueueHot(job{kind: jobPrefetch, keyspace: keyspace, key: key})
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

// HandoffStats returns how many handoff replicate jobs completed.
func (m *Manager) HandoffStats() uint64 {
	if m == nil {
		return 0
	}
	return m.Handoffs.Load()
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

func (m *Manager) enqueueHot(j job) {
	if m == nil || m.closed.Load() {
		return
	}
	select {
	case m.hotJobs <- j:
	default:
		m.Errors.Add(1)
	}
}

func (m *Manager) enqueueRest(j job) {
	if m == nil || m.closed.Load() {
		return
	}
	select {
	case m.restJobs <- j:
	default:
		m.Errors.Add(1)
	}
}

// enqueue is used by refresh-ahead (hot priority so refresh stays timely).
func (m *Manager) enqueue(j job) {
	m.enqueueHot(j)
}

func (m *Manager) worker(ctx context.Context) {
	defer m.wg.Done()
	for {
		// Prefer hot jobs without starving rest forever: non-blocking check
		// of hot first, then blocking select on both.
		select {
		case <-ctx.Done():
			return
		case j := <-m.hotJobs:
			m.runJob(ctx, j)
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case j := <-m.hotJobs:
			m.runJob(ctx, j)
		case j := <-m.restJobs:
			m.runJob(ctx, j)
		}
	}
}

func (m *Manager) runJob(ctx context.Context, j job) {
	switch j.kind {
	case jobHandoff:
		m.cache.ReplicateToPeers(j.keyspace, j.key, j.entry)
		m.Handoffs.Add(1)
		return
	case jobRefresh:
		if !m.ownsKey(j.key) {
			return
		}
		if err := m.cache.ForceLoad(ctx, j.keyspace, j.key); err != nil {
			if !isNotFound(err) {
				m.Errors.Add(1)
			}
			return
		}
		m.Refreshs.Add(1)
		return
	default: // jobPrefetch
		var err error
		if m.ownsKey(j.key) {
			_, err = m.cache.GetOrLoadLocal(ctx, j.keyspace, j.key)
		} else {
			_, err = m.cache.Get(ctx, j.keyspace, j.key)
		}
		if err != nil {
			if !isNotFound(err) {
				m.Errors.Add(1)
			}
			return
		}
		m.Prefetches.Add(1)
	}
}

// ownsKey reports whether this node should act as owner for key.
// Single-node / empty ring → true (local is authoritative).
func (m *Manager) ownsKey(key string) bool {
	owner, ok := m.cache.OwnerOf(key)
	if !ok || owner.ID == "" {
		return true
	}
	self := m.cache.NodeID()
	return self == "" || owner.ID == self
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
					m.enqueue(job{kind: jobRefresh, keyspace: t.Name, key: k})
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
