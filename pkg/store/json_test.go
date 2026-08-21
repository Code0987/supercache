package store

import (
	"sync"
	"testing"

	"github.com/Code0987/supercache/pkg/jsonx"
)

func TestStoreJSetGetDel(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.JSet("d", "$", []byte(`{"a":1}`), 1, 0, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	ok, too = m.JSet("d", "$.b", []byte(`2`), 1, 0, 0)
	if !ok || too {
		t.Fatal("overwrite sibling")
	}
	v, present := m.JGet("d", "$.a")
	if !present || !jsonx.Equal(v, []byte("1")) {
		t.Fatalf("a: %s %v", v, present)
	}
	v, present = m.JGet("d", "$.nope")
	if present {
		t.Fatal("missing path")
	}
	okDel, mut := m.JDel("d", "$.nope", 1, 0)
	if !okDel || mut {
		t.Fatalf("del missing path: %v %v", okDel, mut)
	}
	okDel, mut = m.JDel("d", "$.a", 1, 0)
	if !okDel || !mut {
		t.Fatal("del a")
	}
	_, present = m.JGet("d", "$.a")
	if present {
		t.Fatal("a still there")
	}
}

func TestStoreJSetTooLarge(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.JSet("d", "$", []byte(`{"a":1}`), 1, 0, 8)
	if !ok || too {
		t.Fatal("seed")
	}
	before, _ := m.Stats().Bytes, m.Stats()
	_ = before
	costBefore := m.Stats().Bytes
	ok, too = m.JSet("d", "$.b", []byte(`"xxxxxxxxxxxxxxxxxxxx"`), 1, 0, 8)
	if ok || !too {
		t.Fatalf("want tooLarge: ok=%v too=%v", ok, too)
	}
	v, present := m.JGet("d", "$")
	if !present || !jsonx.Equal(v, []byte(`{"a":1}`)) {
		t.Fatalf("mutated: %s", v)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
}

func TestStoreJDelMissingName(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, mut := m.JDel("nope", "$.a", 1, 0)
	if !ok || mut {
		t.Fatal(ok, mut)
	}
	if m.HasJSON("nope") {
		t.Fatal("created")
	}
}

func TestStoreJSetStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.JSet("d", "$", []byte(`{}`), 99, 0, 0)
	if !ok {
		t.Fatal("create")
	}
	ent, present := m.Peek("d")
	if !present || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	ok, _ = m.JSet("d", "$.a", []byte(`1`), 99, 0, 0)
	if !ok {
		t.Fatal("second")
	}
	ent, _ = m.Peek("d")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	ok, _ = m.JSet("d", "$.b", []byte(`2`), 99, 0, 0)
	if !ok {
		t.Fatal("third")
	}
	ent, _ = m.Peek("d")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
	okDel, mut := m.JDel("d", "$.a", 99, 0)
	if !okDel || !mut {
		t.Fatal("del")
	}
	ent, _ = m.Peek("d")
	if ent.Version != 4 {
		t.Fatalf("del ver=%d", ent.Version)
	}
}

func TestStoreJInstallVersionGate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	_ = m.JInstall("d", []byte(`{"a":1}`), 2, 0)
	if m.JInstall("d", []byte(`{"a":2}`), 2, 0) {
		t.Fatal("equal")
	}
	if m.JInstall("d", []byte(`{"a":0}`), 1, 0) {
		t.Fatal("lower")
	}
	if !m.JInstall("d", []byte(`{"a":3}`), 3, 0) {
		t.Fatal("higher")
	}
	v, _ := m.JGet("d", "$.a")
	if !jsonx.Equal(v, []byte("3")) {
		t.Fatal(string(v))
	}
	if !m.DeleteIfVersion("d", 4) {
		t.Fatal("tomb")
	}
	if m.JInstall("d", []byte(`{"a":1}`), 4, 0) {
		t.Fatal("tombstone equal")
	}
	if m.HasJSON("d") {
		t.Fatal("resurrected")
	}
}

func TestStoreLiveNull(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.JSet("d", "$", []byte("null"), 1, 0, 0)
	if !ok {
		t.Fatal("set null")
	}
	if !m.HasJSON("d") {
		t.Fatal("HasJSON")
	}
	v, present := m.JGet("d", "$")
	if !present || string(v) != "null" {
		t.Fatalf("%q %v", v, present)
	}
	ok, _ = m.JSet("d", "$.a", []byte("1"), 1, 0, 0)
	if ok {
		t.Fatal("set on null")
	}
	v, present = m.JGet("d", "$")
	if !present || string(v) != "null" {
		t.Fatal("still null")
	}
	ok, _ = m.JSet("missing", "$[0]", []byte("1"), 1, 0, 0)
	if ok || m.HasJSON("missing") {
		t.Fatal("index-first create")
	}
}

func TestStoreJDelNoOpVersionTTL(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.JSet("d", "$", []byte(`{"a":1}`), 1, 100, 0)
	if !ok {
		t.Fatal("set")
	}
	ent, _ := m.Peek("d")
	ver, exp := ent.Version, ent.ExpireAt
	okDel, mut := m.JDel("d", "$.nope", 1, 999)
	if !okDel || mut {
		t.Fatal(okDel, mut)
	}
	ent, _ = m.Peek("d")
	if ent.Version != ver || ent.ExpireAt != exp {
		t.Fatalf("version/ttl slid: %+v", ent)
	}
}

func TestStoreConcurrentJDelVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	_ = firstOK(m.JSet("d", "$", []byte(`{"x":1,"y":2}`), 1, 0, 0))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.JDel("d", "$.x", 1, 0)
	}()
	go func() {
		defer wg.Done()
		m.JDel("d", "$.y", 1, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("d")
	if ent.Version < 3 {
		t.Fatalf("want distinct versions >= 3 got %d", ent.Version)
	}
	_, x := m.JGet("d", "$.x")
	_, y := m.JGet("d", "$.y")
	if x || y {
		t.Fatalf("both deletes should apply: x=%v y=%v", x, y)
	}
}

func firstOK(ok, _ bool) bool { return ok }

func TestStoreFlushJSONZeroAllocOnKV(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.Set("k", Entry{Value: []byte("v"), Version: 1}) {
		t.Fatal("set")
	}
	// warm
	m.Get("k")
	allocs := testing.AllocsPerRun(100, func() {
		m.Get("k")
	})
	if allocs > 1 {
		// Get itself clones Value (1 alloc). Flush must not add more.
		t.Fatalf("Get-hit allocs=%v (flush must be 0-alloc on KV)", allocs)
	}
}
