package store

import (
	"sync"
	"testing"

	"github.com/Code0987/supercache/pkg/hllx"
)

func TestStoreHLLAddCount(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.HLLAdd("h", []byte("alice"), 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	n, present := m.HLLCount("h")
	if !present || n < 1 || n > 2 {
		t.Fatal(n, present)
	}
	ok, too = m.HLLAdd("h", []byte("bob"), 1, 0, 0)
	if !ok || too {
		t.Fatal("second")
	}
	n2, _ := m.HLLCount("h")
	if n2 < n {
		t.Fatalf("did not grow %d → %d", n, n2)
	}
	ok, too = m.HLLAdd("h", []byte("alice"), 1, 0, 0)
	if !ok || too {
		t.Fatal("dup")
	}
	n3, _ := m.HLLCount("h")
	if n3 != n2 {
		t.Fatalf("duplicate grew like a set %d → %d", n2, n3)
	}
}

func TestStoreHLLAddStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.HLLAdd("h", []byte("a"), 99, 0, 0)
	if !ok {
		t.Fatal("create")
	}
	ent, present := m.Peek("h")
	if !present || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	ok, _ = m.HLLAdd("h", []byte("b"), 99, 0, 0)
	if !ok {
		t.Fatal("second")
	}
	ent, _ = m.Peek("h")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	ok, _ = m.HLLAdd("h", []byte("c"), 99, 0, 0)
	if !ok {
		t.Fatal("third")
	}
	ent, _ = m.Peek("h")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
}

func TestStoreHLLAddTouch(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.HLLAdd("h", []byte("a"), 1, 0, 0)
	m.HLLAdd("h", []byte("a"), 1, 0, 0)
	ent, _ := m.Peek("h")
	if ent.Version != 2 {
		t.Fatalf("touch ver=%d", ent.Version)
	}
}

func TestStoreHLLAddTooLarge(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	costBefore := m.Stats().Bytes
	ok, too := m.HLLAdd("h", []byte("a"), 1, 0, 100)
	if ok || !too {
		t.Fatalf("want tooLarge: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
	if m.HasHLL("h") {
		t.Fatal("inserted")
	}
}

func TestStoreHLLInstallVersionGate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	blob := hllx.New()
	hllx.Add(blob, []byte("a"))
	if !m.HLLInstall("h", blob, 2, 0) {
		t.Fatal("install")
	}
	if m.HLLInstall("h", blob, 2, 0) {
		t.Fatal("equal")
	}
	if m.HLLInstall("h", blob, 1, 0) {
		t.Fatal("lower")
	}
	if !m.DeleteIfVersion("h", 3) {
		t.Fatal("tomb")
	}
	if m.HLLInstall("h", blob, 3, 0) {
		t.Fatal("tombstone")
	}
	if m.HasHLL("h") {
		t.Fatal("resurrected")
	}
	if m.HLLInstall("h", []byte{1, 2, 3}, 4, 0) {
		t.Fatal("bad size")
	}
}

func TestStoreHasHLLZeroBlob(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.HLLInstall("h", hllx.New(), 1, 0) {
		t.Fatal("install zeros")
	}
	if !m.HasHLL("h") {
		t.Fatal("HasHLL")
	}
	n, ok := m.HLLCount("h")
	if !ok || n != 0 {
		t.Fatal(n, ok)
	}
}

func TestStoreConcurrentHLLAddVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.HLLAdd("h", []byte("seed"), 1, 0, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.HLLAdd("h", []byte("a"), 1, 0, 0)
	}()
	go func() {
		defer wg.Done()
		m.HLLAdd("h", []byte("b"), 1, 0, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("h")
	if ent.Version < 3 {
		t.Fatalf("ver=%d", ent.Version)
	}
	n, ok := m.HLLCount("h")
	if !ok || n < 1 {
		t.Fatal(n, ok)
	}
}
