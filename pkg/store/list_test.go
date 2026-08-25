package store

import (
	"sync"
	"testing"
)

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

func TestStoreLPushStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.LPush("q", []byte("a"), 99, 0) {
		t.Fatal("create")
	}
	ent, ok := m.Peek("q")
	if !ok || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	if !m.LPush("q", []byte("b"), 99, 0) {
		t.Fatal("second")
	}
	ent, _ = m.Peek("q")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	if !m.LPush("q", []byte("c"), 99, 0) {
		t.Fatal("third")
	}
	ent, _ = m.Peek("q")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
}

func TestStoreLPopStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.LPush("q", []byte("a"), 1, 0)
	m.LPush("q", []byte("b"), 1, 0)
	it, popped, applied := m.LPop("q", 99, 0)
	if !applied || !popped || string(it) != "b" {
		t.Fatalf("%s %v %v", it, popped, applied)
	}
	ent, _ := m.Peek("q")
	if ent.Version != 3 {
		t.Fatalf("pop ver=%d", ent.Version)
	}
}

func TestStoreConcurrentLPushVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.LPush("q", []byte("seed"), 1, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.LPush("q", []byte("a"), 1, 0)
	}()
	go func() {
		defer wg.Done()
		m.LPush("q", []byte("b"), 1, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("q")
	if ent.Version < 3 {
		t.Fatalf("ver=%d", ent.Version)
	}
	got := map[string]bool{}
	for _, it := range m.LRange("q", 0, -1) {
		got[string(it)] = true
	}
	if !got["seed"] || !got["a"] || !got["b"] {
		t.Fatalf("lost item: %v", got)
	}
}
