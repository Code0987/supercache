package store

import (
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/Code0987/supercache/pkg/cmsx"
)

func TestStoreCMSIncrQuery(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.CMSIncr("c", []byte("alice"), 1, 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	n, present := m.CMSQuery("c", []byte("alice"))
	if !present || n < 1 {
		t.Fatal(n, present)
	}
	ok, too = m.CMSIncr("c", []byte("bob"), 1, 1, 0, 0)
	if !ok || too {
		t.Fatal("second")
	}
	n2, _ := m.CMSQuery("c", []byte("bob"))
	if n2 < 1 {
		t.Fatal(n2)
	}
	ok, too = m.CMSIncr("c", []byte("alice"), 1, 1, 0, 0)
	if !ok || too {
		t.Fatal("dup")
	}
	n3, _ := m.CMSQuery("c", []byte("alice"))
	if n3 != n+1 {
		t.Fatalf("duplicate %d → %d", n, n3)
	}
}

func TestStoreCMSIncrN(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.CMSIncr("c", []byte("t"), 10, 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	n, present := m.CMSQuery("c", []byte("t"))
	if !present || n < 10 {
		t.Fatal(n, present)
	}
	ok, too = m.CMSIncr("c", []byte("u"), 0, 1, 0, 0)
	if !ok || too {
		t.Fatal("n=0")
	}
	n0, _ := m.CMSQuery("c", []byte("u"))
	if n0 < 1 {
		t.Fatalf("n=0 created %d", n0)
	}
}

func TestStoreCMSIncrStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.CMSIncr("c", []byte("a"), 1, 99, 0, 0)
	if !ok {
		t.Fatal("create")
	}
	ent, present := m.Peek("c")
	if !present || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	ok, _ = m.CMSIncr("c", []byte("b"), 1, 99, 0, 0)
	if !ok {
		t.Fatal("second")
	}
	ent, _ = m.Peek("c")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	ok, _ = m.CMSIncr("c", []byte("c"), 1, 99, 0, 0)
	if !ok {
		t.Fatal("third")
	}
	ent, _ = m.Peek("c")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
}

func TestStoreCMSIncrDuplicateVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.CMSIncr("c", []byte("a"), 1, 1, 0, 0)
	m.CMSIncr("c", []byte("a"), 1, 1, 0, 0)
	ent, _ := m.Peek("c")
	if ent.Version != 2 {
		t.Fatalf("dup ver=%d", ent.Version)
	}
}

func TestStoreCMSIncrTooLarge(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	costBefore := m.Stats().Bytes
	ok, too := m.CMSIncr("c", []byte("a"), 1, 1, 0, 100)
	if ok || !too {
		t.Fatalf("want tooLarge: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
	if m.HasCMS("c") {
		t.Fatal("inserted")
	}
}

func TestStoreCMSIncrOverflow(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	blob := cmsx.New()
	for row := 0; row < cmsx.Depth; row++ {
		setCMSCell(blob, []byte("a"), row, math.MaxUint64)
	}
	if !m.CMSInstall("c", blob, 1, 0) {
		t.Fatal("install")
	}
	costBefore := m.Stats().Bytes
	ok, too := m.CMSIncr("c", []byte("a"), 1, 2, 0, 0)
	if ok || too {
		t.Fatalf("want reject: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost changed")
	}
	n, _ := m.CMSQuery("c", []byte("a"))
	if n != math.MaxUint64 {
		t.Fatalf("mutated query=%d", n)
	}
}

func TestStoreCMSInstallVersionGate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	blob := cmsx.New()
	_ = cmsx.Incr(blob, []byte("a"), 1)
	if !m.CMSInstall("c", blob, 2, 0) {
		t.Fatal("install")
	}
	if m.CMSInstall("c", blob, 2, 0) {
		t.Fatal("equal")
	}
	if m.CMSInstall("c", blob, 1, 0) {
		t.Fatal("lower")
	}
	if !m.DeleteIfVersion("c", 3) {
		t.Fatal("tomb")
	}
	if m.CMSInstall("c", blob, 3, 0) {
		t.Fatal("tombstone")
	}
	if m.HasCMS("c") {
		t.Fatal("resurrected")
	}
	if m.CMSInstall("c", []byte{1, 2, 3}, 4, 0) {
		t.Fatal("bad size")
	}
}

func TestStoreHasCMSZeroBlob(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.CMSInstall("c", cmsx.New(), 1, 0) {
		t.Fatal("install zeros")
	}
	if !m.HasCMS("c") {
		t.Fatal("HasCMS")
	}
	n, ok := m.CMSQuery("c", []byte("x"))
	if !ok || n != 0 {
		t.Fatal(n, ok)
	}
}

func TestStoreConcurrentCMSIncrVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.CMSIncr("c", []byte("seed"), 1, 1, 0, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.CMSIncr("c", []byte("a"), 1, 1, 0, 0)
	}()
	go func() {
		defer wg.Done()
		m.CMSIncr("c", []byte("b"), 1, 1, 0, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("c")
	if ent.Version < 3 {
		t.Fatalf("ver=%d", ent.Version)
	}
	na, oka := m.CMSQuery("c", []byte("a"))
	nb, okb := m.CMSQuery("c", []byte("b"))
	if !oka || !okb || na < 1 || nb < 1 {
		t.Fatal(na, oka, nb, okb)
	}
}

func setCMSCell(s, item []byte, row int, v uint64) {
	x := mix64cms(cmsHash64(item))
	h1 := x >> 32
	h2 := (x & 0xffffffff) | 1
	col := (h1 + uint64(row)*h2) & (cmsx.Width - 1)
	off := (uint64(row)*cmsx.Width + col) * 8
	binary.LittleEndian.PutUint64(s[off:], v)
}

func cmsHash64(item []byte) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for _, c := range item {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

func mix64cms(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
