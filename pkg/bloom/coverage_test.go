package bloom

import "testing"

func TestNewClampsAndOpen(t *testing.T) {
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

func TestNilAndMergeErrors(t *testing.T) {
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
