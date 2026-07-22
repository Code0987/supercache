package engine

import (
	"github.com/Code0987/supercache/pkg/warmup"
)

// HitRecorder records key accesses for hot-key tracking (implemented by warmup.Manager).
type HitRecorder interface {
	RecordHit(keyspace, key string)
}

// TopologyListener is notified on membership changes (implemented by warmup.Manager).
type TopologyListener interface {
	OnTopologyChange()
}

// AttachWarmup wires hit recording and topology prefetch.
func (e *Engine) AttachWarmup(rec HitRecorder, topo TopologyListener) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hitRecorder = rec
	e.topoListener = topo
}

// NotifyTopologyChange triggers warmup prefetch (call from membership bridge).
func (e *Engine) NotifyTopologyChange() {
	e.mu.RLock()
	t := e.topoListener
	e.mu.RUnlock()
	if t != nil {
		t.OnTopologyChange()
	}
}

// WarmTargets implements warmup.Cache.
func (e *Engine) WarmTargets() []warmup.WarmTarget {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]warmup.WarmTarget, 0, len(e.keyspaces))
	for _, ks := range e.keyspaces {
		out = append(out, warmup.WarmTarget{
			Name:            ks.cfg.Name,
			Mode:            ks.cfg.Mode,
			WarmKeys:        append([]string(nil), ks.cfg.WarmKeys...),
			RefreshInterval: ks.cfg.RefreshInterval,
		})
	}
	return out
}

// HotKeys returns tracked hot keys when a warmup.Manager is attached.
func (e *Engine) HotKeys(keyspace string, n int) []string {
	e.mu.RLock()
	rec := e.hitRecorder
	e.mu.RUnlock()
	if m, ok := rec.(interface {
		HotKeys(string, int) []string
	}); ok {
		return m.HotKeys(keyspace, n)
	}
	return nil
}

func (e *Engine) recordHit(keyspace, key string) {
	e.mu.RLock()
	rec := e.hitRecorder
	e.mu.RUnlock()
	if rec != nil {
		rec.RecordHit(keyspace, key)
	}
}
