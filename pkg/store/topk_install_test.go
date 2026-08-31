package store

import (
	"sync"
	"testing"

	"github.com/Code0987/supercache/pkg/topkx"
)

func TestStoreTopKInstallVersionGate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	tab := topkx.New(10)
	if err := tab.Add([]byte("a")); err != nil {
		t.Fatal(err)
	}
	blob := tab.Encode()
	if !m.TopKInstall("hot", blob, 2, 0, 10) {
		t.Fatal("install")
	}
	if m.TopKInstall("hot", blob, 2, 0, 10) {
		t.Fatal("equal")
	}
	if m.TopKInstall("hot", blob, 1, 0, 10) {
		t.Fatal("lower")
	}
	if !m.DeleteIfVersion("hot", 3) {
		t.Fatal("tomb")
	}
	if m.TopKInstall("hot", blob, 3, 0, 10) {
		t.Fatal("tombstone")
	}
	if m.HasTopK("hot") {
		t.Fatal("resurrected")
	}
	if m.TopKInstall("hot", []byte{1}, 4, 0, 10) {
		t.Fatal("bad blob")
	}
}

func TestStoreHasTopKEmptyBlob(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.TopKInstall("hot", nil, 1, 0, 10) {
		t.Fatal("install empty")
	}
	if !m.HasTopK("hot") {
		t.Fatal("HasTopK")
	}
	rows, ok := m.TopKList("hot", 10)
	if !ok || len(rows) != 0 {
		t.Fatalf("list %v ok=%v", rows, ok)
	}
}

func TestStoreTopKShrinkKNoMutate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	for _, id := range []string{"a", "b", "c"} {
		ok, too := m.TopKAdd("hot", []byte(id), 1, 0, 3, 0)
		if !ok || too {
			t.Fatal(id, ok, too)
		}
	}
	before, _ := m.Peek("hot")
	costBefore := m.Stats().Bytes
	ok, too := m.TopKAdd("hot", []byte("d"), 1, 0, 1, 0)
	if ok || too {
		t.Fatalf("shrink add: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
	after, _ := m.Peek("hot")
	if after.Version != before.Version || string(after.Value) != string(before.Value) {
		t.Fatal("mutated")
	}
	if !m.HasTopK("hot") {
		t.Fatal("HasTopK")
	}
	if _, present := m.TopKList("hot", 1); present {
		t.Fatal("list should miss on decode fail")
	}
}

func TestStoreConcurrentTopKAddVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.TopKAdd("hot", []byte("seed"), 1, 0, 10, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.TopKAdd("hot", []byte("a"), 1, 0, 10, 0)
	}()
	go func() {
		defer wg.Done()
		m.TopKAdd("hot", []byte("b"), 1, 0, 10, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("hot")
	if ent.Version < 3 {
		t.Fatalf("ver=%d", ent.Version)
	}
	rows, ok := m.TopKList("hot", 10)
	if !ok || len(rows) < 3 {
		t.Fatalf("rows=%d ok=%v", len(rows), ok)
	}
}
