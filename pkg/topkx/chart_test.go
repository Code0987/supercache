package topkx

import "testing"

func TestHonestChart(t *testing.T) {
	tab := New(10)
	ingestHonestStream(t, tab)
	got := tab.List()
	if len(got) != 10 {
		t.Fatalf("len %d", len(got))
	}
	want := []struct {
		id    string
		count uint64
	}{
		{"t001", 200},
		{"t002", 150},
		{"t495", 109},
		{"t496", 109},
		{"t497", 109},
		{"t498", 109},
		{"t499", 109},
		{"t500", 109},
		{"t493", 108},
		{"t494", 108},
	}
	for i, w := range want {
		if string(got[i].Item) != w.id || got[i].Count != w.count {
			t.Fatalf("rank %d: got %s@%d want %s@%d\nfull=%s",
				i+1, got[i].Item, got[i].Count, w.id, w.count, formatList(got))
		}
	}
	seen := map[string]bool{}
	for _, e := range got {
		seen[string(e.Item)] = true
	}
	for _, evicted := range []string{"t003", "t004", "t005"} {
		if seen[evicted] {
			t.Fatalf("%s still on the chart: %s", evicted, formatList(got))
		}
	}
}
