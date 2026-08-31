package topkx

import "testing"

func TestEmptyTable(t *testing.T) {
	tab := New(10)
	if tab.Occupied() != 0 {
		t.Fatalf("occupied %d", tab.Occupied())
	}
	if got := tab.List(); got != nil {
		t.Fatalf("list %v", got)
	}
	if got := tab.Encode(); len(got) != 0 {
		t.Fatalf("encode %v", got)
	}
	dec, err := Decode(nil, 10)
	if err != nil || dec.Occupied() != 0 {
		t.Fatalf("decode nil: %+v %v", dec, err)
	}
}

func TestSortEqualCounts(t *testing.T) {
	tab := New(4)
	for _, id := range []string{"c", "a", "b"} {
		if err := tab.Add([]byte(id)); err != nil {
			t.Fatal(err)
		}
	}
	got := tab.List()
	if len(got) != 3 || string(got[0].Item) != "a" || string(got[1].Item) != "b" || string(got[2].Item) != "c" {
		t.Fatalf("%s", formatList(got))
	}
}
