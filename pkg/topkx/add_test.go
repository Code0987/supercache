package topkx

import (
	"fmt"
	"math"
	"testing"
)

func TestOneItem(t *testing.T) {
	tab := New(10)
	if err := tab.Add([]byte("alice")); err != nil {
		t.Fatal(err)
	}
	got := tab.List()
	if len(got) != 1 || got[0].Count != 1 || string(got[0].Item) != "alice" {
		t.Fatalf("%+v", got)
	}
}

func TestBound(t *testing.T) {
	const k = 5
	tab := New(k)
	for i := 0; i < k+3; i++ {
		if err := tab.Add([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if tab.Occupied() != k {
		t.Fatalf("occupied %d", tab.Occupied())
	}
}

func TestReplaceMin(t *testing.T) {
	tab := New(2)
	for _, id := range []string{"a", "b", "c"} {
		if err := tab.Add([]byte(id)); err != nil {
			t.Fatal(err)
		}
	}
	got := tab.List()
	if len(got) != 2 {
		t.Fatalf("len %d", len(got))
	}
	if string(got[0].Item) != "c" || got[0].Count != 2 {
		t.Fatalf("head %+v (want c@2; evicted min-count/smallest-bytes a)", got)
	}
	if string(got[1].Item) != "b" || got[1].Count != 1 {
		t.Fatalf("tail %+v", got)
	}
	for _, e := range got {
		if string(e.Item) == "a" {
			t.Fatal("a should be evicted")
		}
	}
}

func TestHotStays(t *testing.T) {
	tab := New(10)
	for i := 0; i < 50; i++ {
		if err := tab.Add([]byte("hot")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 200; i++ {
		if err := tab.Add([]byte(fmt.Sprintf("tail%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	list := tab.List()
	var hot *Entry
	for i := range list {
		if string(list[i].Item) == "hot" {
			hot = &list[i]
			break
		}
	}
	if hot == nil || hot.Count != 50 {
		t.Fatalf("hot missing or count=%v list=%s", hot, formatList(list))
	}
}

func TestMonotonic(t *testing.T) {
	tab := New(4)
	if err := tab.Add([]byte("hot")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := tab.Add([]byte("hot")); err != nil {
			t.Fatal(err)
		}
	}
	n := tab.List()[0].Count
	if err := tab.Add([]byte("hot")); err != nil {
		t.Fatal(err)
	}
	if tab.List()[0].Count <= n {
		t.Fatalf("count did not grow %d → %d", n, tab.List()[0].Count)
	}
}

func TestEmptyItem(t *testing.T) {
	tab := New(2)
	if err := tab.Add(nil); err != ErrEmpty {
		t.Fatalf("nil: %v", err)
	}
	if err := tab.Add([]byte{}); err != ErrEmpty {
		t.Fatalf("empty: %v", err)
	}
	if tab.Occupied() != 0 {
		t.Fatal("created")
	}
}

func TestOverflowResident(t *testing.T) {
	tab := New(2)
	if err := tab.Add([]byte("a")); err != nil {
		t.Fatal(err)
	}
	tab.slots[0].Count = math.MaxUint64
	if err := tab.Add([]byte("a")); err != ErrOverflow {
		t.Fatalf("err %v", err)
	}
	if tab.slots[0].Count != math.MaxUint64 || string(tab.slots[0].Item) != "a" {
		t.Fatalf("mutated %+v", tab.slots)
	}
}

func TestOverflowFullTableNewItem(t *testing.T) {
	tab := New(2)
	if err := tab.Add([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := tab.Add([]byte("b")); err != nil {
		t.Fatal(err)
	}
	tab.slots[0].Count = math.MaxUint64
	tab.slots[1].Count = math.MaxUint64
	if err := tab.Add([]byte("c")); err != ErrOverflow {
		t.Fatalf("err %v", err)
	}
	if tab.Occupied() != 2 {
		t.Fatalf("occupied %d", tab.Occupied())
	}
	seen := map[string]bool{}
	for _, e := range tab.List() {
		seen[string(e.Item)] = true
	}
	if !seen["a"] || !seen["b"] || seen["c"] {
		t.Fatalf("membership %s", formatList(tab.List()))
	}
}
