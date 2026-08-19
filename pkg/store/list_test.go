package store

import "testing"

func TestStoreListPushPopRange(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.RPush("q", []byte("a"), 1, 0) {
		t.Fatal("rpush a")
	}
	if !m.RPush("q", []byte("b"), 2, 0) {
		t.Fatal("rpush b")
	}
	if !m.LPush("q", []byte("z"), 3, 0) {
		t.Fatal("lpush z")
	}
	if m.LLen("q") != 3 {
		t.Fatal(m.LLen("q"))
	}
	r := m.LRange("q", 0, -1)
	if len(r) != 3 || string(r[0]) != "z" || string(r[2]) != "b" {
		t.Fatalf("%q", r)
	}
	it, popped, applied := m.LPop("q", 4, 0)
	if !applied || !popped || string(it) != "z" {
		t.Fatalf("%s %v %v", it, popped, applied)
	}
	it, popped, applied = m.RPop("q", 5, 0)
	if !applied || !popped || string(it) != "b" {
		t.Fatalf("%s", it)
	}
	if m.LLen("q") != 1 {
		t.Fatal(m.LLen("q"))
	}
}
