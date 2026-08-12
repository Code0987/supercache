package ring

import "testing"

func TestOwnerStableAndDistribution(t *testing.T) {
	r := New(32)
	r.SetPeers([]Peer{
		{ID: "a", Addr: "a:1"},
		{ID: "b", Addr: "b:1"},
		{ID: "c", Addr: "c:1"},
	})
	o1, ok := r.Owner("my-key")
	if !ok {
		t.Fatal("no owner")
	}
	o2, _ := r.Owner("my-key")
	if o1.ID != o2.ID {
		t.Fatal("unstable owner")
	}
	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		p, _ := r.Owner(string(rune('A'+i%26)) + string(rune(i)))
		counts[p.ID]++
	}
	if len(counts) < 2 {
		t.Fatalf("poor distribution: %v", counts)
	}
	others := r.PeersExcept("a")
	if len(others) != 2 {
		t.Fatalf("peers except: %d", len(others))
	}
}

func TestReplicasOwnerFirstAndRF(t *testing.T) {
	r := New(32)
	r.SetPeers([]Peer{
		{ID: "a", Addr: "a:1"},
		{ID: "b", Addr: "b:1"},
		{ID: "c", Addr: "c:1"},
		{ID: "d", Addr: "d:1"},
	})
	const key = "replica-key"
	owner, ok := r.Owner(key)
	if !ok {
		t.Fatal("no owner")
	}
	reps := r.Replicas(key, 2)
	if len(reps) != 2 {
		t.Fatalf("replicas: %d", len(reps))
	}
	if reps[0].ID != owner.ID {
		t.Fatalf("first replica want owner %s got %s", owner.ID, reps[0].ID)
	}
	if reps[1].ID == owner.ID {
		t.Fatal("second replica is owner")
	}
	if !r.IsReplica(key, owner.ID, 2) {
		t.Fatal("owner must be replica")
	}
	except := r.ReplicasExcept(key, owner.ID, 2)
	if len(except) != 1 || except[0].ID != reps[1].ID {
		t.Fatalf("except owner: %+v", except)
	}
	all := r.Replicas(key, 99)
	if len(all) != 4 {
		t.Fatalf("cap at N: %d", len(all))
	}
	if r.Replicas(key, 0) != nil {
		t.Fatal("rf=0 should be empty")
	}
	// Every peer is a replica when rf >= N.
	for _, p := range r.Peers() {
		if !r.IsReplica(key, p.ID, 4) {
			t.Fatalf("%s should be replica at rf=N", p.ID)
		}
	}
}

func TestEmptyRingDefaultsAndReplicaEdges(t *testing.T) {
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
