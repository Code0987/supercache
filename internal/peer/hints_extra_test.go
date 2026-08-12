package peer

import (
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/store"
)

func TestPopHintMiddleOfQueue(t *testing.T) {
	tr := NewTransport(20 * time.Millisecond)
	defer tr.Close()
	fo := NewFanoutPool(tr, FanoutConfig{Workers: 1, QueueSize: 8, DisableHints: true})
	defer fo.Close()
	fo.hintsDisabled = false

	p := ring.Peer{ID: "b", Addr: "127.0.0.1:9"}
	fo.enqueueHint(p, "d", "k1", store.Entry{Value: []byte("a"), Version: 1}, 1)
	fo.enqueueHint(p, "d", "k2", store.Entry{Value: []byte("b"), Version: 1}, 1)
	fo.enqueueHint(p, "d", "k3", store.Entry{Value: []byte("c"), Version: 1}, 1)
	// Pop middle id (not front).
	mid := hintID("d", "k2", store.Entry{Value: []byte("b")})
	fo.popHint(p.Addr, mid)
	if fo.HintPending() != 2 {
		t.Fatalf("pending=%d want 2", fo.HintPending())
	}
	h, ok := fo.peekHint(p.Addr)
	if !ok || h.key != "k1" {
		t.Fatalf("front should still be k1: %+v ok=%v", h, ok)
	}
	// Pop front.
	fo.popHint(p.Addr, hintID("d", "k1", store.Entry{Value: []byte("a")}))
	// Pop missing / empty queue is no-op.
	fo.popHint(p.Addr, "nope")
	fo.popHint("missing-addr", "x")
	// Corrupt order: item missing from map.
	fo.enqueueHint(p, "d", "k4", store.Entry{Value: []byte("d"), Version: 1}, 1)
	fo.hintMu.Lock()
	q := fo.hints[p.Addr]
	if q != nil && len(q.order) > 0 {
		delete(q.items, q.order[0])
	}
	fo.hintMu.Unlock()
	_, ok = fo.peekHint(p.Addr)
	if ok {
		// peek returns ok only if map has the id; after delete ok=false
		t.Fatal("peek with missing map entry should be false")
	}
}

func TestTimeoutNilTransport(t *testing.T) {
	var tr *Transport
	if tr.Timeout() != 500*time.Millisecond {
		t.Fatalf("nil Timeout=%v", tr.Timeout())
	}
}

func TestHintNewerEqualVersionTombstoneWins(t *testing.T) {
	put := store.Entry{Value: []byte("v"), Version: 5}
	tomb := store.Entry{Version: 5, Flags: store.FlagTombstone}
	if !hintNewer(tomb, put) {
		t.Fatal("tombstone should beat equal-version put")
	}
	if hintNewer(put, tomb) {
		t.Fatal("put must not beat equal-version tombstone")
	}
}
