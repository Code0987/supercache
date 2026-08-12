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

func TestNewClampsParamsAndOpenBitset(t *testing.T) {
	f := New(1, 0) // clamps m>=8, k>=1
	if f.m < 8 || f.k < 1 {
		t.Fatalf("clamps: m=%d k=%d", f.m, f.k)
	}
	if f.Bytes() == nil {
		t.Fatal("Bytes")
	}

	// Open happy
	o, err := Open(f.m, f.k, f.Bytes())
	if err != nil || o == nil {
		t.Fatalf("Open: %v", err)
	}
	o.Add([]byte("x"))
	if !o.Test([]byte("x")) {
		t.Fatal("Open Add/Test")
	}

	// Open invalid params
	if _, err := Open(4, 1, make([]byte, 1)); err == nil {
		t.Fatal("Open m<8")
	}
	if _, err := Open(64, 0, make([]byte, 8)); err == nil {
		t.Fatal("Open k<1")
	}
	if _, err := Open(64, 3, make([]byte, 1)); err == nil {
		t.Fatal("Open wrong len")
	}
}

func TestNilFilterAndMergeShapeMismatch(t *testing.T) {
	var nilF *Filter
	nilF.Add([]byte("x"))
	if nilF.Test([]byte("x")) {
		t.Fatal("nil Test")
	}
	if nilF.Bytes() != nil {
		t.Fatal("nil Bytes")
	}
	if err := nilF.Merge(New(64, 2)); err == nil {
		t.Fatal("nil merge left")
	}
	a := New(64, 2)
	if err := a.Merge(nil); err == nil {
		t.Fatal("nil merge right")
	}
	b := New(128, 2)
	if err := a.Merge(b); err == nil {
		t.Fatal("shape mismatch m")
	}
	c := New(64, 3)
	if err := a.Merge(c); err == nil {
		t.Fatal("shape mismatch k")
	}
	// empty bits Test
	empty := &Filter{bits: nil, m: 64, k: 1}
	if empty.Test([]byte("z")) {
		t.Fatal("empty bits")
	}
}
