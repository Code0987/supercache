package store

import "testing"

func TestStoreZAddScoreRemRange(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.ZAdd("lb", []byte("alice"), 10, 1, 0) {
		t.Fatal("add alice")
	}
	if !m.ZAdd("lb", []byte("bob"), 20, 2, 0) {
		t.Fatal("add bob")
	}
	if !m.ZAdd("lb", []byte("alice"), 15, 3, 0) {
		t.Fatal("update alice")
	}
	sc, ok := m.ZScore("lb", []byte("alice"))
	if !ok || sc != 15 {
		t.Fatalf("score=%v ok=%v", sc, ok)
	}
	if m.ZCard("lb") != 2 {
		t.Fatal(m.ZCard("lb"))
	}
	r := m.ZRange("lb", 0, -1)
	if len(r) != 2 || string(r[0].Member) != "alice" || string(r[1].Member) != "bob" {
		t.Fatalf("%+v", r)
	}
	if !m.ZRem("lb", []byte("bob"), 4, 0) {
		t.Fatal("rem")
	}
	if m.ZCard("lb") != 1 {
		t.Fatal(m.ZCard("lb"))
	}
	rs := m.ZRangeByScore("lb", 0, 100)
	if len(rs) != 1 || string(rs[0].Member) != "alice" {
		t.Fatalf("%+v", rs)
	}
}
