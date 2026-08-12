package ring

import "testing"

func TestRingEdges(t *testing.T) {
	// default vnodes
	r := New(0)
	if r.vnodes != 64 {
		t.Fatalf("default vnodes %d", r.vnodes)
	}
	if r.Generation() != 0 {
		t.Fatal("gen before set")
	}
	if r.Len() != 0 {
		t.Fatal("empty len")
	}
	if _, ok := r.Owner("k"); ok {
		t.Fatal("empty owner")
	}
	if r.Replicas("k", 3) != nil {
		t.Fatal("empty replicas")
	}
	if r.IsReplica("k", "a", 3) {
		t.Fatal("empty IsReplica")
	}
	if r.IsReplica("k", "", 3) {
		t.Fatal("empty id")
	}
	if r.IsReplica("k", "a", 0) {
		t.Fatal("rf0")
	}

	// skip empty peer IDs
	r.SetPeers([]Peer{{ID: "", Addr: "x"}, {ID: "a", Addr: "a:1"}, {ID: "b", Addr: "b:1"}})
	if r.Len() != 2 {
		t.Fatalf("len=%d", r.Len())
	}
	if r.Generation() == 0 {
		t.Fatal("gen")
	}
	// ReplicasExcept empty id returns full set
	reps := r.ReplicasExcept("key", "", 2)
	if len(reps) != 2 {
		t.Fatalf("except empty id: %d", len(reps))
	}
	// non-replica id
	if r.IsReplica("key", "nope", 1) {
		t.Fatal("unknown id")
	}
	// wrap-around path: many keys so Owner/Replicas hit i==len(keys)
	for i := 0; i < 50; i++ {
		_, _ = r.Owner(string(rune(i)))
		_ = r.Replicas(string(rune(i)), 2)
	}
	// single peer ring
	r2 := New(8)
	r2.SetPeers([]Peer{{ID: "solo", Addr: "s:1"}})
	o, ok := r2.Owner("anything")
	if !ok || o.ID != "solo" {
		t.Fatalf("solo owner: %+v", o)
	}
	if !r2.IsReplica("anything", "solo", 99) {
		t.Fatal("solo is replica at rf>=N")
	}
}
