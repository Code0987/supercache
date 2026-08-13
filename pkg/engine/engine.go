package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/protect"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/telemetry"
)

// KV is a key/value pair for batch puts.
type KV struct {
	Key   string
	Value []byte
}

// ClusterEvent is emitted on membership changes (cluster mode; stub channel in M1).
type ClusterEvent struct {
	Type EventType
	Peer PeerInfo
}

// EventType classifies cluster events.
type EventType int

const (
	PeerJoined EventType = iota
	PeerLeft
	PeerUpdated
)

// PeerInfo describes a ring member.
type PeerInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// DefaultMaxVersionKeys caps per-keyspace version counters (high-cardinality safety).
const DefaultMaxVersionKeys = 1_000_000

// Engine is the SuperCache façade.
type Engine struct {
	mu        sync.RWMutex
	keyspaces map[string]*ksRuntime
	global    *protect.Guard
	events    chan ClusterEvent
	metrics   *telemetry.Metrics

	nodeID       string
	nodeAddr     string
	ringGen      uint64
	closed       bool
	cluster      *Cluster
	hitRecorder  HitRecorder
	topoListener TopologyListener

	maxKeyLen      int
	maxValueSize   int
	maxBatch       int
	maxVersionKeys int
	now            func() time.Time
}

type ksRuntime struct {
	cfg    keyspace.Config
	store  store.Store
	guard  *protect.Guard
	flight singleflight.Group
	// lastVer tracks highest issued version per key (owner path).
	verMu          sync.Mutex
	lastVer        map[string]uint64
	maxVersionKeys int
}

// Option configures Engine construction.
type Option func(*Engine)

// WithGlobalProtect sets a process-wide load guard.
func WithGlobalProtect(g *protect.Guard) Option {
	return func(e *Engine) { e.global = g }
}

// WithMetrics attaches telemetry counters/spans.
func WithMetrics(m *telemetry.Metrics) Option {
	return func(e *Engine) { e.metrics = m }
}

// WithNow injects a clock (tests).
func WithNow(now func() time.Time) Option {
	return func(e *Engine) { e.now = now }
}

// WithLimits overrides default key/value/batch limits.
func WithLimits(maxKeyLen, maxValueSize, maxBatch int) Option {
	return func(e *Engine) {
		if maxKeyLen > 0 {
			e.maxKeyLen = maxKeyLen
		}
		if maxValueSize > 0 {
			e.maxValueSize = maxValueSize
		}
		if maxBatch > 0 {
			e.maxBatch = maxBatch
		}
	}
}

// WithMaxVersionKeys caps the per-keyspace last-version map size (0 keeps default).
func WithMaxVersionKeys(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.maxVersionKeys = n
		}
	}
}

// New creates a single-node Engine (WithSingleNode semantics).
func New(opts ...Option) *Engine {
	e := &Engine{
		keyspaces:      make(map[string]*ksRuntime),
		events:         make(chan ClusterEvent, 64),
		metrics:        telemetry.New(),
		maxKeyLen:      keyspace.DefaultMaxKeyLen,
		maxValueSize:   keyspace.DefaultMaxValueSize,
		maxBatch:       keyspace.DefaultMaxBatch,
		maxVersionKeys: DefaultMaxVersionKeys,
		now:            time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Events returns the cluster event channel (may drop on slow consumers later).
func (e *Engine) Events() <-chan ClusterEvent { return e.events }

// EventsSink returns the send side for membership bridges (internal use).
func (e *Engine) EventsSink() chan<- ClusterEvent { return e.events }

// Close releases keyspace stores and marks the engine not ready.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for name, ks := range e.keyspaces {
		ks.store.Close()
		delete(e.keyspaces, name)
	}
}

// UpdateKeySpace registers or replaces a keyspace (local only).
//
// M1 behavior: the in-memory store is replaced (data wiped). Version counters
// (lastVer) are preserved so LWW remains monotonic across config updates.
// Callers must not race other ops against a keyspace being updated/deleted;
// concurrent use of a closed store is undefined.
func (e *Engine) UpdateKeySpace(cfg keyspace.Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	lastVer := make(map[string]uint64)
	if old, ok := e.keyspaces[cfg.Name]; ok {
		old.verMu.Lock()
		for k, v := range old.lastVer {
			lastVer[k] = v
		}
		old.verMu.Unlock()
		old.store.Close()
	}
	guardCfg := cfg.CircuitBreaker
	if cfg.RateLimitRPS > 0 {
		guardCfg.RateLimitRPS = cfg.RateLimitRPS
	}
	e.keyspaces[cfg.Name] = &ksRuntime{
		cfg: cfg,
		store: store.NewMemory(cfg.MaxBytes,
			store.WithClock(e.now),
			store.WithTombstoneTTL(cfg.EffectiveTombstoneTTL()),
		),
		guard:          protect.New(guardCfg),
		lastVer:        lastVer,
		maxVersionKeys: e.maxVersionKeys,
	}
	return nil
}

// DeleteKeySpace removes a keyspace locally.
func (e *Engine) DeleteKeySpace(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	ks, ok := e.keyspaces[name]
	if !ok {
		return ErrKeyspaceNotFound
	}
	ks.store.Close()
	delete(e.keyspaces, name)
	return nil
}

type putConfig struct {
	ttl time.Duration
	// ttlSet distinguishes explicit 0 from unset.
	ttlSet bool
}

// PutOption configures Put.
type PutOption func(*putConfig)

// WithTTL sets entry TTL for this Put (0 = no expiry for this write).
func WithTTL(d time.Duration) PutOption {
	return func(c *putConfig) {
		c.ttl = d
		c.ttlSet = true
	}
}

// Get returns the value for key or ErrNotFound.
// Store.Get already returns a defensive copy of Value.
func (e *Engine) Get(ctx context.Context, keyspaceName, key string) ([]byte, error) {
	ctx, end := e.startSpan(ctx, "engine.Get")
	defer end()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.validateKey(keyspaceName, key); err != nil {
		return nil, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if err := e.validateKeyLen(ks, key); err != nil {
		return nil, err
	}
	if ks.cfg.Mode == keyspace.ModeBloom {
		return nil, fmt.Errorf("%w: use BloomTest", ErrInvalidArgument)
	}
	if ks.cfg.Mode == keyspace.ModeSet {
		return nil, fmt.Errorf("%w: use SetContains", ErrInvalidArgument)
	}
	if ks.cfg.Mode == keyspace.ModeZSet {
		return nil, fmt.Errorf("%w: use ZScore/ZRange", ErrInvalidArgument)
	}

	if ent, ok := ks.store.Get(key); ok {
		if ent.IsNegative() {
			e.metrics.RecordGet(keyspaceName, "negative")
			return nil, ErrNotFound
		}
		e.metrics.RecordGet(keyspaceName, "hit")
		e.recordHit(keyspaceName, key)
		return ent.Value, nil
	}

	if ks.cfg.Mode == keyspace.ModeCacheOnly {
		c := e.clusterSnapshot()
		if c == nil || c.Ring == nil || c.Transport == nil {
			e.metrics.RecordGet(keyspaceName, "miss")
			return nil, ErrNotFound
		}
		// Replica repair / non-replica proxy: ask the owner. Do not treat a
		// successful forward as a local hit; store only if we are a replica.
		v, err, _ := ks.flight.Do("get:"+key, func() (any, error) {
			if ent, ok := ks.store.Get(key); ok {
				if ent.IsNegative() {
					return nil, ErrNotFound
				}
				return ent.Value, nil
			}
			return e.fetchFromOwner(context.WithoutCancel(ctx), ks, key)
		})
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				e.metrics.RecordGet(keyspaceName, "miss")
			}
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if v == nil {
			e.metrics.RecordGet(keyspaceName, "miss")
			return nil, ErrNotFound
		}
		e.metrics.RecordGet(keyspaceName, "miss")
		return v.([]byte), nil
	}

	// LoadThrough miss — owner GetOrLoad when clustered; else local fill.
	// singleflight coalesces concurrent misses on this node.
	// Use a non-cancelable context inside the flight so one caller's cancel does not
	// abort the shared load for co-waiters (classic singleflight+context hazard).
	// Flight key is prefixed so it never shares a result type with GetOrLoadLocal
	// (which stores store.Entry in the same singleflight.Group).
	v, err, _ := ks.flight.Do("get:"+key, func() (any, error) {
		if ent, ok := ks.store.Get(key); ok {
			if ent.IsNegative() {
				return nil, ErrNotFound
			}
			return ent.Value, nil
		}
		return e.getViaCluster(context.WithoutCancel(ctx), ks, key)
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			e.metrics.RecordGet(keyspaceName, "negative")
		} else if errors.Is(err, ErrUnavailable) {
			e.metrics.RecordUnavailable(keyspaceName)
		}
		return nil, err
	}
	// Respect this caller's cancellation after the shared work completes.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if v == nil {
		e.metrics.RecordGet(keyspaceName, "miss")
		return nil, ErrNotFound
	}
	e.metrics.RecordGet(keyspaceName, "miss")
	e.recordHit(keyspaceName, key)
	return v.([]byte), nil
}

func (e *Engine) loadThrough(ctx context.Context, ks *ksRuntime, key string, allowFanout bool) ([]byte, error) {
	if e.global != nil {
		if err := e.global.AllowContext(ctx); err != nil {
			e.metrics.RecordUnavailable(ks.cfg.Name)
			return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
	}
	if err := ks.guard.AllowContext(ctx); err != nil {
		e.metrics.RecordUnavailable(ks.cfg.Name)
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}

	loadCtx := ctx
	var cancel context.CancelFunc
	if ks.cfg.LoadTimeout > 0 {
		loadCtx, cancel = context.WithTimeout(ctx, ks.cfg.LoadTimeout)
		defer cancel()
	}

	val, err := ks.cfg.DataSource.Load(loadCtx, key)
	if err != nil {
		if errorsIsNotFound(err) {
			e.metrics.RecordLoad(ks.cfg.Name, nil) // not-found is a successful backend call
			e.storeNegative(ks, key, allowFanout)
			if e.global != nil {
				e.global.OnSuccess()
			}
			ks.guard.OnSuccess()
			// Concurrent Put may have won; surface it if present.
			if ent, ok := ks.store.Get(key); ok && !ent.IsNegative() {
				return ent.Value, nil
			}
			return nil, ErrNotFound
		}
		e.metrics.RecordLoad(ks.cfg.Name, err)
		if e.global != nil {
			e.global.OnFailure()
		}
		ks.guard.OnFailure()
		return nil, err
	}
	e.metrics.RecordLoad(ks.cfg.Name, nil)
	if e.global != nil {
		e.global.OnSuccess()
	}
	ks.guard.OnSuccess()

	if err := e.validateValueSize(ks, val); err != nil {
		return nil, err
	}

	// PLAN §7: load fill must not overwrite a present non-expired positive entry.
	if ent, ok := ks.store.Get(key); ok && !ent.IsNegative() {
		return ent.Value, nil
	}

	ent := store.Entry{
		Value:   append([]byte(nil), val...),
		Version: ks.nextVersion(key, 0),
		Flags:   0,
	}
	ent.ExpireAt = e.expireAt(ks.cfg.TTL)
	if !ks.store.AcceptIfNewer(key, ent) {
		// Concurrent higher version (e.g. Put) won LWW — return winner.
		if cur, ok := ks.store.Get(key); ok && !cur.IsNegative() {
			return cur.Value, nil
		}
		// Could not cache (too large or only negative); still return loaded value.
		return append([]byte(nil), val...), nil
	}
	// Owner load path: async fan-out like Put (recommended default).
	if allowFanout {
		e.replicate(ks.cfg.Name, key, ent, false)
	}
	return append([]byte(nil), val...), nil
}

func (e *Engine) storeNegative(ks *ksRuntime, key string, allowFanout bool) {
	if ks.cfg.NegativeTTL <= 0 {
		return
	}
	// Bail if a concurrent Put already filled a positive value.
	if cur, ok := ks.store.Peek(key); ok && !cur.IsNegative() {
		return
	}
	ent := store.Entry{
		Version:  ks.nextVersion(key, 0),
		ExpireAt: e.expireAt(ks.cfg.NegativeTTL),
		Flags:    store.FlagNegative,
	}
	// AcceptNegative never replaces a live positive (even with higher version).
	if !ks.store.AcceptNegative(key, ent) {
		return
	}
	// PLAN: owner fans out negative entries so peers avoid SoT stampedes.
	if allowFanout {
		e.replicate(ks.cfg.Name, key, ent, false)
	}
}

// Put stores value via owner routing when clustered, otherwise local owner path.
func (e *Engine) Put(ctx context.Context, keyspaceName, key string, value []byte, opts ...PutOption) error {
	ctx, end := e.startSpan(ctx, "engine.Put")
	defer end()
	if ks, err := e.getKS(keyspaceName); err == nil {
		if ks.cfg.Mode == keyspace.ModeBloom {
			return fmt.Errorf("%w: use BloomAdd", ErrInvalidArgument)
		}
		if ks.cfg.Mode == keyspace.ModeSet {
			return fmt.Errorf("%w: use SetAdd", ErrInvalidArgument)
		}
		if ks.cfg.Mode == keyspace.ModeZSet {
			return fmt.Errorf("%w: use ZAdd", ErrInvalidArgument)
		}
	}
	return e.putViaCluster(ctx, keyspaceName, key, value, opts...)
}

// PutMany puts many keys; not atomic. Returns nil, a single KeyError, or errors.Join of KeyErrors.
func (e *Engine) PutMany(ctx context.Context, keyspaceName string, kvs []KV, opts ...PutOption) error {
	if len(kvs) > e.maxBatch {
		return ErrBatchTooLarge
	}
	var errs []error
	for _, kv := range kvs {
		if err := e.Put(ctx, keyspaceName, kv.Key, kv.Value, opts...); err != nil {
			errs = append(errs, KeyError{Key: kv.Key, Err: err})
		}
	}
	return joinErrors(errs)
}

// Delete invalidates the key: owner mints a version, installs a tombstone,
// and replicateWaits the replica set (same apply+hint path as Put).
// Returns MultiError if any replica is unreachable on the first attempt.
func (e *Engine) Delete(ctx context.Context, keyspaceName, key string) error {
	ctx, end := e.startSpan(ctx, "engine.Delete")
	defer end()
	return e.deleteViaCluster(ctx, keyspaceName, key)
}

// DeleteMany deletes many keys; not atomic.
func (e *Engine) DeleteMany(ctx context.Context, keyspaceName string, keys []string) error {
	if len(keys) > e.maxBatch {
		return ErrBatchTooLarge
	}
	var errs []error
	for _, k := range keys {
		if err := e.Delete(ctx, keyspaceName, k); err != nil {
			errs = append(errs, KeyError{Key: k, Err: err})
		}
	}
	return joinErrors(errs)
}

// ApplyPut applies a remote/versioned put (LWW). Used by peer path and tests.
// ringGeneration is the sender's ring generation (0 = unknown). Mismatches are
// recorded for ops diagnostics; acceptance remains version LWW only.
func (e *Engine) ApplyPut(keyspaceName, key string, ent store.Entry) (bool, error) {
	return e.ApplyPutWithRingGen(keyspaceName, key, ent, 0)
}

// ApplyPutWithRingGen is ApplyPut with an optional wire ring generation.
func (e *Engine) ApplyPutWithRingGen(keyspaceName, key string, ent store.Entry, ringGeneration uint64) (bool, error) {
	e.noteRingGeneration(ringGeneration)
	if err := e.validateKey(keyspaceName, key); err != nil {
		return false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return false, err
	}
	// Track observed versions so future local puts stay monotonic.
	ks.observeVersion(key, ent.Version)
	if ent.IsBloomAdd() {
		ok := e.applyBloomAdd(ks, key, ent.Value, ent.Version, ent.ExpireAt)
		if ok {
			if c := e.clusterSnapshot(); c != nil && c.Ring != nil {
				if owner, yes := c.Ring.Owner(key); yes && owner.ID == c.SelfID {
					e.replicate(keyspaceName, key, ent, false)
				}
			}
		}
		return ok, nil
	}
	if ent.IsBloom() {
		return e.applyBloomMerge(ks, key, ent.Value, ent.Version, ent.ExpireAt), nil
	}
	if ent.IsSetAdd() {
		ok := e.applySetAdd(ks, key, ent.Value, ent.Version, ent.ExpireAt)
		if ok {
			if c := e.clusterSnapshot(); c != nil && c.Ring != nil {
				if owner, yes := c.Ring.Owner(key); yes && owner.ID == c.SelfID {
					e.replicate(keyspaceName, key, ent, false)
				}
			}
		}
		return ok, nil
	}
	if ent.IsSetRemove() {
		ok := e.applySetRemove(ks, key, ent.Value, ent.Version, ent.ExpireAt)
		if ok {
			if c := e.clusterSnapshot(); c != nil && c.Ring != nil {
				if owner, yes := c.Ring.Owner(key); yes && owner.ID == c.SelfID {
					e.replicate(keyspaceName, key, ent, false)
				}
			}
		}
		return ok, nil
	}
	if ent.IsSet() {
		return e.applySetInstall(ks, key, ent.Value, ent.Version, ent.ExpireAt), nil
	}
	if ent.IsZSetAdd() {
		ok := e.applyZSetAdd(ks, key, ent.Value, ent.Version, ent.ExpireAt)
		if ok {
			if c := e.clusterSnapshot(); c != nil && c.Ring != nil {
				if owner, yes := c.Ring.Owner(key); yes && owner.ID == c.SelfID {
					e.replicate(keyspaceName, key, ent, false)
				}
			}
		}
		return ok, nil
	}
	if ent.IsZSetRem() {
		ok := e.applyZSetRem(ks, key, ent.Value, ent.Version, ent.ExpireAt)
		if ok {
			if c := e.clusterSnapshot(); c != nil && c.Ring != nil {
				if owner, yes := c.Ring.Owner(key); yes && owner.ID == c.SelfID {
					e.replicate(keyspaceName, key, ent, false)
				}
			}
		}
		return ok, nil
	}
	if ent.IsZSet() {
		return e.applyZSetInstall(ks, key, ent.Value, ent.Version, ent.ExpireAt), nil
	}
	if ent.IsNegative() {
		// Negatives must not clobber live positives (AcceptNegative).
		ok := ks.store.AcceptNegative(key, ent)
		return ok, nil
	}
	ok := ks.store.AcceptIfNewer(key, ent)
	return ok, nil
}

// ApplyDelete applies a versioned delete.
func (e *Engine) ApplyDelete(keyspaceName, key string, deleteVersion uint64) (bool, error) {
	return e.ApplyDeleteWithRingGen(keyspaceName, key, deleteVersion, 0)
}

// ApplyDeleteWithRingGen is ApplyDelete with an optional wire ring generation.
func (e *Engine) ApplyDeleteWithRingGen(keyspaceName, key string, deleteVersion, ringGeneration uint64) (bool, error) {
	e.noteRingGeneration(ringGeneration)
	if err := e.validateKey(keyspaceName, key); err != nil {
		return false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return false, err
	}
	ks.observeVersion(key, deleteVersion)
	return ks.store.DeleteIfVersion(key, deleteVersion), nil
}

// noteRingGeneration records wire vs local ring generation mismatches (non-fatal).
func (e *Engine) noteRingGeneration(wire uint64) {
	if wire == 0 || e.metrics == nil {
		return
	}
	local := e.RingGeneration()
	if local == 0 {
		return
	}
	if wire != local {
		e.metrics.RecordRingGenMismatch()
	}
}

// Stats returns store stats for a keyspace.
func (e *Engine) Stats(keyspaceName string) (store.Stats, error) {
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return store.Stats{}, err
	}
	return ks.store.Stats(), nil
}

// ConfigHash returns the config hash for a keyspace (drift detection).
func (e *Engine) ConfigHash(keyspaceName string) (string, error) {
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return "", err
	}
	return ks.cfg.ConfigHash(), nil
}

func (e *Engine) getKS(name string) (*ksRuntime, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ks, ok := e.keyspaces[name]
	if !ok {
		return nil, ErrKeyspaceNotFound
	}
	return ks, nil
}

func (e *Engine) validateKey(ksName, key string) error {
	if ksName == "" {
		return fmt.Errorf("%w: empty keyspace", ErrInvalidArgument)
	}
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidArgument)
	}
	if len(key) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	return nil
}

// validateKeyLen applies per-keyspace MaxKeyLen when configured.
func (e *Engine) validateKeyLen(ks *ksRuntime, key string) error {
	max := e.maxKeyLen
	if ks.cfg.MaxKeyLen > 0 {
		max = ks.cfg.MaxKeyLen
	}
	if len(key) > max {
		return ErrKeyTooLarge
	}
	return nil
}

func (e *Engine) validateValueSize(ks *ksRuntime, value []byte) error {
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	if len(value) > max {
		return ErrValueTooLarge
	}
	return nil
}

func (e *Engine) expireAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return e.now().Add(ttl).UnixNano()
}

// nextVersion allocates a monotonic version for key.
// Seeds from lastVer and any live store entry so ownership transfer does not mint v=1
// below peers' observed versions (PLAN §6).
// Lock order: verMu then store.PeekVersion (store never takes verMu).
func (ks *ksRuntime) nextVersion(key string, atLeast uint64) uint64 {
	ks.verMu.Lock()
	defer ks.verMu.Unlock()
	cur := ks.lastVer[key]
	if atLeast > cur {
		cur = atLeast
	}
	if ver, ok := ks.store.PeekVersion(key); ok && ver > cur {
		cur = ver
	}
	n := cur + 1
	ks.lastVer[key] = n
	ks.pruneLastVerLocked()
	return n
}

func (ks *ksRuntime) observeVersion(key string, v uint64) {
	ks.verMu.Lock()
	defer ks.verMu.Unlock()
	if v > ks.lastVer[key] {
		ks.lastVer[key] = v
	}
	ks.pruneLastVerLocked()
}

// pruneLastVerLocked drops version counters when over maxVersionKeys.
// Prefer keys no longer present in the store; then delete arbitrary extras.
// Caller must hold verMu.
func (ks *ksRuntime) pruneLastVerLocked() {
	max := ks.maxVersionKeys
	if max <= 0 || len(ks.lastVer) <= max {
		return
	}
	for k := range ks.lastVer {
		if len(ks.lastVer) <= max {
			return
		}
		if _, ok := ks.store.Peek(k); !ok {
			delete(ks.lastVer, k)
		}
	}
	for k := range ks.lastVer {
		if len(ks.lastVer) <= max {
			return
		}
		delete(ks.lastVer, k)
	}
}

// VersionTrackerSize returns the number of keys in the version map for a keyspace.
// Returns -1 if the keyspace is unknown. Intended for tests and diagnostics.
func (e *Engine) VersionTrackerSize(keyspaceName string) int {
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return -1
	}
	ks.verMu.Lock()
	defer ks.verMu.Unlock()
	return len(ks.lastVer)
}

func errorsIsNotFound(err error) bool {
	return errors.Is(err, datasource.ErrNotFound) || errors.Is(err, ErrNotFound)
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return errors.Join(errs...)
}

func (e *Engine) startSpan(ctx context.Context, name string) (context.Context, func()) {
	if e.metrics == nil {
		return ctx, func() {}
	}
	ctx, sp := e.metrics.StartSpan(ctx, name)
	return ctx, func() { sp.End() }
}
