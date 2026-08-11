package engine

import (
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/telemetry"
)

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
