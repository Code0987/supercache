package peer

import (
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/store"
)

func TestHintCoalesceAndBound(t *testing.T) {
	tr := NewTransport(20 * time.Millisecond)
	defer tr.Close()
	fo := NewFanoutPool(tr, FanoutConfig{
		Workers:        1,
		QueueSize:      8,
		HintMaxPerPeer: 2,
		DisableHints:   true, // enqueueHint still works if we call it; Disable skips loop + enqueue from apply
	})
	defer fo.Close()
	// Direct enqueue for unit bounds (DisableHints skips enqueueHint).
	fo.hintsDisabled = false

	peerA := ring.Peer{ID: "b", Addr: "127.0.0.1:1"}
	fo.enqueueHint(peerA, "demo", "k1", store.Entry{Value: []byte("a"), Version: 1}, 1)
	fo.enqueueHint(peerA, "demo", "k1", store.Entry{Value: []byte("b"), Version: 2}, 1)
	if fo.HintPending() != 1 {
		t.Fatalf("coalesce: pending=%d want 1", fo.HintPending())
	}
	h, ok := fo.peekHint(peerA.Addr)
	if !ok || h.ent.Version != 2 || string(h.ent.Value) != "b" {
		t.Fatalf("coalesce kept latest: %+v ok=%v", h.ent, ok)
	}

	fo.enqueueHint(peerA, "demo", "k2", store.Entry{Value: []byte("c"), Version: 1}, 1)
	fo.enqueueHint(peerA, "demo", "k3", store.Entry{Value: []byte("d"), Version: 1}, 1)
	if fo.HintPending() != 2 {
		t.Fatalf("bound: pending=%d want 2", fo.HintPending())
	}
	if fo.HintsDropped.Load() < 1 {
		t.Fatalf("expected drop under HintMaxPerPeer=2")
	}
}

func TestHintDeleteSupersedesOlderPut(t *testing.T) {
	tr := NewTransport(20 * time.Millisecond)
	defer tr.Close()
	fo := NewFanoutPool(tr, FanoutConfig{Workers: 1, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	fo.hintsDisabled = false

	p := ring.Peer{ID: "b", Addr: "127.0.0.1:1"}
	fo.Hint(p, "demo", "k", store.Entry{Value: []byte("v"), Version: 5}, 1)
	fo.Hint(p, "demo", "k", store.Entry{Version: 6, Flags: store.FlagTombstone}, 1)
	if fo.HintPending() != 1 {
		t.Fatalf("pending=%d", fo.HintPending())
	}
	h, ok := fo.peekHint(p.Addr)
	if !ok || !h.ent.IsTombstone() || h.ent.Version != 6 {
		t.Fatalf("want tombstone v6, got %+v ok=%v", h.ent, ok)
	}
	// Stale put hint must not replace the delete.
	fo.Hint(p, "demo", "k", store.Entry{Value: []byte("old"), Version: 5}, 1)
	h, ok = fo.peekHint(p.Addr)
	if !ok || !h.ent.IsTombstone() || h.ent.Version != 6 {
		t.Fatalf("stale put overwrote delete: %+v ok=%v", h.ent, ok)
	}
}

func TestHintDisabledSkipsQueue(t *testing.T) {
	tr := NewTransport(20 * time.Millisecond)
	defer tr.Close()
	fo := NewFanoutPool(tr, FanoutConfig{DisableHints: true, Workers: 1})
	defer fo.Close()
	fo.enqueueHint(ring.Peer{ID: "x", Addr: "127.0.0.1:1"}, "d", "k", store.Entry{Version: 1}, 0)
	if fo.HintPending() != 0 {
		t.Fatalf("disabled hints should not enqueue")
	}
}
