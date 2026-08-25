package store

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/counter"
)

func TestStoreCIncrCreateAdd(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	v, ok, ov := m.CIncr("c", 5, 1, 0)
	if !ok || ov || v != 5 {
		t.Fatalf("create %d %v %v", v, ok, ov)
	}
	v, ok, ov = m.CIncr("c", 1, 2, 0)
	if !ok || ov || v != 6 {
		t.Fatalf("add %d %v %v", v, ok, ov)
	}
	v, ok, ov = m.CIncr("z", 0, 1, 0)
	if !ok || ov || v != 0 {
		t.Fatalf("incr0 %d", v)
	}
	ent, hit := m.Peek("c")
	if !hit || !ent.IsCounter() || ent.Version != 2 || len(ent.Value) != 8 {
		t.Fatalf("envelope %+v %v", ent, hit)
	}
}

func TestStoreCIncrOverflowLeavesValue(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	_, ok, _ := m.CIncr("c", math.MaxInt64, 1, 0)
	if !ok {
		t.Fatal("set max")
	}
	before := m.Stats().Bytes
	v, ok, ov := m.CIncr("c", 1, 2, 0)
	if ok || !ov || v != math.MaxInt64 {
		t.Fatalf("overflow %d ok=%v ov=%v", v, ok, ov)
	}
	got, present := m.CGet("c")
	if !present || got != math.MaxInt64 {
		t.Fatal(got, present)
	}
	if m.Stats().Bytes != before {
		t.Fatalf("cost changed %d → %d", before, m.Stats().Bytes)
	}
}

func TestStoreCIncrAfterExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	m := NewMemory(1<<20, WithClock(func() time.Time { return now }))
	defer m.Close()
	exp := now.Add(time.Second).UnixNano()
	_, ok, _ := m.CIncr("c", 9, 1, exp)
	if !ok {
		t.Fatal("create")
	}
	now = now.Add(2 * time.Second)
	v, ok, ov := m.CIncr("c", 5, 2, 0)
	if !ok || ov || v != 5 {
		t.Fatalf("after expire want 5 got %d ok=%v ov=%v", v, ok, ov)
	}
}

func TestStoreCGetMissingVsZero(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if v, ok := m.CGet("nope"); ok || v != 0 {
		t.Fatal(v, ok)
	}
	_, applied, _ := m.CIncr("c", 0, 1, 0)
	if !applied {
		t.Fatal("create 0")
	}
	v, ok := m.CGet("c")
	if !ok || v != 0 {
		t.Fatal(v, ok)
	}
}

func TestStoreCInstallVersionGate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	blob := counter.Encode(1)
	if !m.CInstall("c", blob, 2, 0) {
		t.Fatal("install")
	}
	if m.CInstall("c", counter.Encode(9), 2, 0) {
		t.Fatal("equal")
	}
	if m.CInstall("c", counter.Encode(9), 1, 0) {
		t.Fatal("lower")
	}
	if !m.DeleteIfVersion("c", 3) {
		t.Fatal("tomb")
	}
	if m.CInstall("c", blob, 3, 0) {
		t.Fatal("tombstone")
	}
	if m.HasCounter("c") {
		t.Fatal("has")
	}
}

func TestStoreCIncrStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if _, ok, _ := m.CIncr("c", 1, 99, 0); !ok {
		t.Fatal("create")
	}
	ent, present := m.Peek("c")
	if !present || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	if _, ok, _ := m.CIncr("c", 1, 99, 0); !ok {
		t.Fatal("second")
	}
	ent, _ = m.Peek("c")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	if _, ok, _ := m.CIncr("c", 1, 99, 0); !ok {
		t.Fatal("third")
	}
	ent, _ = m.Peek("c")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
}

func TestStoreConcurrentCIncrVersions(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.CIncr("c", 1, 1, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.CIncr("c", 1, 1, 0)
	}()
	go func() {
		defer wg.Done()
		m.CIncr("c", 1, 1, 0)
	}()
	wg.Wait()
	ent, _ := m.Peek("c")
	if ent.Version < 3 {
		t.Fatalf("ver=%d", ent.Version)
	}
	v, ok := m.CGet("c")
	if !ok || v != 3 {
		t.Fatalf("value=%d ok=%v", v, ok)
	}
}
