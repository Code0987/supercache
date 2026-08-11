package store

import "testing"

func TestBloomAddDoesNotClobber(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()
	const bits, k = 2048, 4
	if !m.BloomAdd("f", []byte("a"), bits, k, 1, 0) {
		t.Fatal("add a")
	}
	if !m.BloomAdd("f", []byte("b"), bits, k, 1, 0) {
		t.Fatal("add b")
	}
	if !m.BloomTest("f", []byte("a"), bits, k) {
		t.Fatal("a lost after adding b")
	}
	if !m.BloomTest("f", []byte("b"), bits, k) {
		t.Fatal("b missing")
	}
	if m.BloomTest("f", []byte("c"), bits, k) {
		t.Fatal("c was never added")
	}
}
