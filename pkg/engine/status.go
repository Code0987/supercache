package engine

import (
	"encoding/json"

	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/telemetry"
)

// LocalKind is the classified local store state for one name (Peek, not Get).
type LocalKind uint8

const (
	LocalMissing LocalKind = iota
	LocalLive
	LocalTombstone
	LocalNegative
)

func (k LocalKind) String() string {
	switch k {
	case LocalLive:
		return "live"
	case LocalTombstone:
		return "tombstone"
	case LocalNegative:
		return "negative"
	default:
		return "missing"
	}
}

func (k LocalKind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

// LocalView is a read-only Peek classification. It does not touch LRU or stats.
type LocalView struct {
	Kind    LocalKind `json:"kind"`
	Version uint64    `json:"version"`
	Flags   uint32    `json:"flags"`
	Bytes   int       `json:"bytes"`
}

// LocalView reports the local copy of key without owner-forward or LRU update.
// Unknown keyspace or absent key → LocalMissing.
func (e *Engine) LocalView(keyspaceName, key string) LocalView {
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return LocalView{Kind: LocalMissing}
	}
	ent, ok := ks.store.Peek(key)
	if !ok {
		return LocalView{Kind: LocalMissing}
	}
	v := LocalView{Version: ent.Version, Flags: ent.Flags, Bytes: len(ent.Value)}
	switch {
	case ent.IsTombstone():
		v.Kind = LocalTombstone
	case ent.IsNegative():
		v.Kind = LocalNegative
	default:
		v.Kind = LocalLive
	}
	return v
}

// BloomDump is a read-only copy of the local Bloom bitset (Peek, no LRU).
// ok is false when the name is missing, tombstoned, or not a Bloom entry.
// m and k still come from the keyspace when present.
func (e *Engine) BloomDump(keyspaceName, name string) (bits []byte, m, k int, ok bool) {
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, 0, 0, false
	}
	m, k = ks.cfg.EffectiveBloomBits(), ks.cfg.EffectiveBloomHashes()
	ent, found := ks.store.Peek(name)
	if !found || ent.IsTombstone() || !ent.IsBloom() {
		return nil, m, k, false
	}
	return append([]byte(nil), ent.Value...), m, k, true
}

// KeySpaceSnapshot is admin/JSON diagnostics for one keyspace.
type KeySpaceSnapshot struct {
	Name              string      `json:"name"`
	Mode              string      `json:"mode"`
	ConfigHash        string      `json:"config_hash"`
	MaxBytes          int64       `json:"max_bytes"`
	TTL               string      `json:"ttl"`
	NegTTL            string      `json:"negative_ttl"`
	Stats             store.Stats `json:"stats"`
	Breaker           string      `json:"breaker_state"`
	RateLimited       uint64      `json:"rate_limited"`
	BreakerOpens      uint64      `json:"breaker_opens"`
	HotKeys           []string    `json:"hot_keys,omitempty"`
	ReplicationFactor int         `json:"replication_factor"`
	TombstoneTTL      string      `json:"tombstone_ttl"`
}

// NodeID returns this node's identity (empty until SetNodeInfo / AttachCluster).
func (e *Engine) NodeID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.nodeID
}

// SetNodeInfo sets local node identity for admin / clustering.
func (e *Engine) SetNodeInfo(id, address string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nodeID = id
	e.nodeAddr = address
}

// Peers returns known ring members.
func (e *Engine) Peers() []PeerInfo {
	e.mu.RLock()
	c := e.cluster
	nodeID := e.nodeID
	nodeAddr := e.nodeAddr
	e.mu.RUnlock()

	if c != nil && c.Ring != nil {
		rp := c.Ring.Peers()
		out := make([]PeerInfo, 0, len(rp))
		for _, p := range rp {
			out = append(out, PeerInfo{ID: p.ID, Address: p.Addr})
		}
		return out
	}
	if nodeID == "" {
		return []PeerInfo{}
	}
	return []PeerInfo{{ID: nodeID, Address: nodeAddr}}
}

// RingGeneration returns membership generation.
func (e *Engine) RingGeneration() uint64 {
	e.mu.RLock()
	c := e.cluster
	gen := e.ringGen
	e.mu.RUnlock()
	if c != nil && c.Ring != nil {
		return c.Ring.Generation()
	}
	return gen
}

// Ready reports whether the engine can serve traffic.
func (e *Engine) Ready() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return !e.closed
}

// Metrics returns process counters (empty if telemetry not configured).
func (e *Engine) Metrics() telemetry.Snapshot {
	e.mu.RLock()
	m := e.metrics
	e.mu.RUnlock()
	if m == nil {
		return telemetry.Snapshot{}
	}
	// Merge live fan-out counters from transport when clustered.
	fe, fd := e.FanoutStats()
	m.SetFanoutStats(fe, fd)
	return m.Snapshot()
}

// KeySpaceSnapshots returns diagnostics for all keyspaces.
func (e *Engine) KeySpaceSnapshots() []KeySpaceSnapshot {
	e.mu.RLock()
	type tmp struct {
		snap KeySpaceSnapshot
	}
	items := make([]tmp, 0, len(e.keyspaces))
	peerN := 0
	if e.cluster != nil && e.cluster.Ring != nil {
		peerN = e.cluster.Ring.Len()
	}
	if peerN <= 0 {
		peerN = 1
	}
	for _, ks := range e.keyspaces {
		limited, opens := uint64(0), uint64(0)
		state := "closed"
		if ks.guard != nil {
			limited, opens = ks.guard.Stats()
			state = ks.guard.State()
		}
		items = append(items, tmp{snap: KeySpaceSnapshot{
			Name:              ks.cfg.Name,
			Mode:              ks.cfg.Mode.String(),
			ConfigHash:        ks.cfg.ConfigHash(),
			MaxBytes:          ks.cfg.MaxBytes,
			TTL:               ks.cfg.TTL.String(),
			NegTTL:            ks.cfg.NegativeTTL.String(),
			Stats:             ks.store.Stats(),
			Breaker:           state,
			RateLimited:       limited,
			BreakerOpens:      opens,
			ReplicationFactor: ks.cfg.EffectiveReplication(peerN),
			TombstoneTTL:      ks.cfg.EffectiveTombstoneTTL().String(),
		}})
	}
	rec := e.hitRecorder
	e.mu.RUnlock()

	out := make([]KeySpaceSnapshot, len(items))
	for i, it := range items {
		out[i] = it.snap
		if rec != nil {
			if m, ok := rec.(interface {
				HotKeys(string, int) []string
			}); ok {
				out[i].HotKeys = m.HotKeys(out[i].Name, 16)
			}
		}
	}
	return out
}
