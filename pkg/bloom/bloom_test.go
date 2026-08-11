package bloom

import (
	"fmt"
	"testing"
)

func TestFilterAddTest(t *testing.T) {
	f := New(1024, 3)
	if f.Test([]byte("nope")) {
		t.Fatal("empty filter must not claim an item")
	}
	f.Add([]byte("yes"))
	if !f.Test([]byte("yes")) {
		t.Fatal("added item must test true")
	}
}

func TestFilterNoFalseNegative(t *testing.T) {
	f := New(1<<14, 5)
	for i := 0; i < 200; i++ {
		item := []byte(fmt.Sprintf("k-%d", i))
		f.Add(item)
		if !f.Test(item) {
			t.Fatalf("false negative on %q", item)
		}
	}
	for i := 0; i < 200; i++ {
		if !f.Test([]byte(fmt.Sprintf("k-%d", i))) {
			t.Fatalf("lost item k-%d", i)
		}
	}
}

func TestFilterMergeOR(t *testing.T) {
	a := New(2048, 4)
	b := New(2048, 4)
	a.Add([]byte("from-a"))
	b.Add([]byte("from-b"))
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	if !a.Test([]byte("from-a")) || !a.Test([]byte("from-b")) {
		t.Fatal("merge must keep items from both sides")
	}
}
