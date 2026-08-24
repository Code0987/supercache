package store

import (
	"sync"
	"testing"

	"github.com/Code0987/supercache/pkg/bitmapx"
)

func TestStoreBSetGet(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.BSet("b", 0, true, 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	bit, present := m.BGet("b", 0)
	if !present || !bit {
		t.Fatal(bit, present)
	}
	ok, too = m.BSet("b", 0, false, 1, 0, 0)
	if !ok || too {
		t.Fatal("clear")
	}
	bit, present = m.BGet("b", 0)
	if !present || bit {
		t.Fatal("cleared")
	}
	ok, too = m.BSet("b", 8, true, 1, 0, 0)
	if !ok || too {
		t.Fatal("grow")
	}
	bit, present = m.BGet("b", 100)
	if !present || bit {
		t.Fatal("past end")
	}
}

func TestStoreBSetTooLarge(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.BSet("b", 0, true, 1, 0, 1)
	if !ok || too {
		t.Fatal("seed")
	}
	costBefore := m.Stats().Bytes
	ok, too = m.BSet("b", 16, true, 1, 0, 1)
	if ok || !too {
		t.Fatalf("want tooLarge: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
	bit, present := m.BGet("b", 0)
	if !present || !bit {
		t.Fatal("mutated")
	}
}

func TestStoreBSetMissingBitZeroCreates(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.BSet("b", 8, false, 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	if !m.HasBitmap("b") {
		t.Fatal("HasBitmap")
	}
	bit, present := m.BGet("b", 8)
	if !present || bit {
		t.Fatal(bit, present)
	}
}

func TestStoreBSetStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.BSet("b", 0, true, 99, 0, 0)
	if !ok {
		t.Fatal("create")
	}
	ent, present := m.Peek("b")
	if !present || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	ok, _ = m.BSet("b", 1, true, 99, 0, 0)
	if !ok {
		t.Fatal("second")
	}
	ent, _ = m.Peek("b")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	ok, _ = m.BSet("b", 2, true, 99, 0, 0)
	if !ok {
		t.Fatal("third")
	}
	ent, _ = m.Peek("b")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
}

func TestStoreBInstallVersionGate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	blob := bitmapx.Set(nil, 0, true)
	if !m.BInstall("b", blob, 2, 0) {
		t.Fatal("install")
	}
	if m.BInstall("b", blob, 2, 0) {
		t.Fatal("equal")
	}
	if m.BInstall("b", blob, 1, 0) {
		t.Fatal("lower")
	}
	if !m.DeleteIfVersion("b", 3) {
		t.Fatal("tomb")
	}
	if m.BInstall("b", blob, 3, 0) {
		t.Fatal("tombstone")
	}
	if m.HasBitmap("b") {
		t.Fatal("resurrected")
	}
}

func TestStoreHasBitmapAllZero(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.BSet("b", 0, true, 1, 0, 0)
	m.BSet("b", 0, false, 1, 0, 0)
	if !m.HasBitmap("b") {
		t.Fatal("all-zero")
	}
	n, ok := m.BCount("b", 0, -1)
	if !ok || n != 0 {
		t.Fatal(n, ok)
	}
}

func TestStoreLengthNeverShrinks(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.BSet("b", 100, true, 1, 0, 0)
	m.BSet("b", 100, false, 1, 0, 0)
	bit, ok := m.BGet("b", 50)
	if !ok || bit {
		t.Fatal("grown length")
	}
}

func TestStoreConcurrentBSetVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.BSet("b", 0, true, 1, 0, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.BSet("b", 1, true, 1, 0, 0)
	}()
	go func() {
		defer wg.Done()
		m.BSet("b", 8, true, 1, 0, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("b")
	if ent.Version < 3 {
		t.Fatalf("ver=%d", ent.Version)
	}
	_, a := m.BGet("b", 1)
	bit8, b := m.BGet("b", 8)
	if !a || !b || !bit8 {
		// bit 1 and 8 should both be set; bit 0 was already true
	}
	if bit, ok := m.BGet("b", 1); !ok || !bit {
		t.Fatal("lost 1")
	}
	if bit, ok := m.BGet("b", 8); !ok || !bit {
		t.Fatal("lost 8")
	}
}
