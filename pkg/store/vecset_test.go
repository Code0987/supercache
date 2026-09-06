package store

import (
	"testing"

	"github.com/Code0987/supercache/pkg/vecset"
)

func TestStoreVAddLocksDimAndReplace(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.VAdd("v", []byte("a"), []float32{1, 0}, 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	ok, too = m.VAdd("v", []byte("b"), []float32{1, 0, 0}, 1, 0, 0)
	if ok || too {
		t.Fatal("dim mismatch applied")
	}
	ent, _ := m.Peek("v")
	if ent.Version != 1 {
		t.Fatalf("version bumped on reject: %d", ent.Version)
	}
	ok, _ = m.VAdd("v", []byte("a"), []float32{0, 1}, 1, 0, 0)
	if !ok {
		t.Fatal("replace")
	}
	n, present := m.VCard("v")
	if !present || n != 1 {
		t.Fatal(n, present)
	}
	vec, ok := m.VEmb("v", []byte("a"))
	if !ok || vec[1] != 1 {
		t.Fatalf("%v %v", vec, ok)
	}
	ent, _ = m.Peek("v")
	if ent.Version != 2 {
		t.Fatalf("replace ver=%d", ent.Version)
	}
}

func TestStoreVRemKeepsEmptyDim(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.VAdd("v", []byte("a"), []float32{1, 0}, 1, 0, 0)
	if !m.VRem("v", []byte("a"), 1, 0) {
		t.Fatal("rem")
	}
	if !m.HasVectorSet("v") {
		t.Fatal("dropped")
	}
	n, ok := m.VCard("v")
	if !ok || n != 0 {
		t.Fatal(n, ok)
	}
	dim, ok := m.VDim("v")
	if !ok || dim != 2 {
		t.Fatal(dim, ok)
	}
}

func TestStoreVAddFull(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	for i := 0; i < vecset.MaxMembers; i++ {
		id := []byte{byte(i >> 8), byte(i)}
		ok, _ := m.VAdd("v", id, []float32{1, 0}, 1, 0, 0)
		if !ok {
			t.Fatal(i)
		}
	}
	ok, too := m.VAdd("v", []byte("zz"), []float32{1, 0}, 1, 0, 0)
	if ok || too {
		t.Fatal("513th new id")
	}
	ok, _ = m.VAdd("v", []byte{0, 0}, []float32{0, 1}, 1, 0, 0)
	if !ok {
		t.Fatal("replace full")
	}
}

func TestStoreVSimCosine(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.VAdd("v", []byte("far"), []float32{0, 1}, 1, 0, 0)
	m.VAdd("v", []byte("near"), []float32{1, 0.1}, 1, 0, 0)
	hits, ok := m.VSim("v", []float32{1, 0}, 2, int(vecset.MetricCosine))
	if !ok || len(hits) != 2 || string(hits[0].Member) != "near" {
		t.Fatalf("%v %+v", ok, hits)
	}
}

func TestStoreMemberTooLong(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.VAdd("v", []byte("a"), []float32{1, 0}, 1, 0, 0)
	ok, _ := m.VAdd("v", make([]byte, 256), []float32{1, 0}, 1, 0, 0)
	if ok {
		t.Fatal("256-byte member")
	}
	ent, _ := m.Peek("v")
	if ent.Version != 1 {
		t.Fatal(ent.Version)
	}
}

func TestStoreVSInstallLWW(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	s := vecset.NewSet(2)
	_ = s.Add([]byte("a"), []float32{1, 0})
	blob := s.Encode()
	if !m.VSInstall("v", blob, 2, 0) {
		t.Fatal("install")
	}
	if m.VSInstall("v", blob, 2, 0) || m.VSInstall("v", blob, 1, 0) {
		t.Fatal("stale")
	}
}

func TestStoreVectorSetNotEvicted(t *testing.T) {
	m := NewMemory(1)
	defer m.Close()
	ok, _ := m.VAdd("v", []byte("a"), []float32{1, 0}, 1, 0, 0)
	if !ok {
		t.Fatal("add")
	}
	// pressure: a normal KV should not evict the live vector set
	_ = m.Set("other", Entry{Value: []byte("x"), Version: 1})
	if !m.HasVectorSet("v") {
		t.Fatal("evicted")
	}
}

func TestStoreVSimTiesAndK(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.VAdd("v", []byte("bb"), []float32{1, 0}, 1, 0, 0)
	m.VAdd("v", []byte("aa"), []float32{1, 0}, 1, 0, 0)
	hits, ok := m.VSim("v", []float32{1, 0}, 50, int(vecset.MetricCosine))
	if !ok || len(hits) != 2 || string(hits[0].Member) != "aa" {
		t.Fatalf("%v %+v", ok, hits)
	}
}
