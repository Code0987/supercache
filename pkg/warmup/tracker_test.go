package warmup

import "testing"

func TestTrackerTopAndBound(t *testing.T) {
	tr := NewTracker(3)
	tr.Hit("a")
	tr.Hit("a")
	tr.Hit("b")
	tr.Hit("c")
	tr.Hit("d") // may evict
	top := tr.Top(2)
	if len(top) == 0 {
		t.Fatal("expected top keys")
	}
	if top[0] != "a" {
		t.Fatalf("want a first, got %v", top)
	}
	if len(tr.Snapshot()) > 3 {
		t.Fatalf("exceeded bound: %d", len(tr.Snapshot()))
	}
}
