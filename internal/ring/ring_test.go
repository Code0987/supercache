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
