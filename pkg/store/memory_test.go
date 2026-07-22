package store

import (
	"fmt"
	"testing"
	"time"
)

func TestMemorySetGetDelete(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()

	if !m.Set("a", Entry{Value: []byte("1"), Version: 1}) {
		t.Fatal("set failed")
	}
	e, ok := m.Get("a")
	if !ok || string(e.Value) != "1" || e.Version != 1 {
		t.Fatalf("get: ok=%v entry=%+v", ok, e)
	}
	if !m.Delete("a") {
		t.Fatal("delete")
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("expected miss after delete")
	}
}

func TestMemoryTTLExpiry(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()
	exp := time.Now().Add(-time.Second).UnixNano()
	m.Set("x", Entry{Value: []byte("v"), Version: 1, ExpireAt: exp})
	if _, ok := m.Get("x"); ok {
		t.Fatal("expected expired miss")
	}
}

func TestMemoryAcceptIfNewer(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()
	m.Set("k", Entry{Value: []byte("old"), Version: 5})
	if m.AcceptIfNewer("k", Entry{Value: []byte("stale"), Version: 4}) {
		t.Fatal("should reject stale")
	}
	if !m.AcceptIfNewer("k", Entry{Value: []byte("new"), Version: 6}) {
		t.Fatal("should accept newer")
	}
	e, _ := m.Get("k")
	if string(e.Value) != "new" {
		t.Fatalf("got %s", e.Value)
	}
	st := m.Stats()
	if st.StaleSkip < 1 {
		t.Fatalf("expected stale skip, stats=%+v", st)
	}
}

func TestMemoryMaxBytesEviction(t *testing.T) {
	// small budget: force eviction
	m := NewMemory(200)
	defer m.Close()
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%02d", i)
		m.Set(k, Entry{Value: make([]byte, 40), Version: uint64(i + 1)})
	}
	st := m.Stats()
	if st.Bytes > 200 {
		t.Fatalf("bytes %d > max", st.Bytes)
	}
	if st.Evictions == 0 {
		t.Fatal("expected evictions")
	}
}

func TestMemoryDeleteIfVersion(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()
	m.Set("k", Entry{Value: []byte("v"), Version: 3})
	if m.DeleteIfVersion("k", 2) {
		// returns false when not deleted due to higher local version
		t.Fatal("should not delete with lower delete version")
	}
	// wait - DeleteIfVersion returns false when version is higher, true when deleted
	// local version 3, deleteVersion 2: 2 >= 3 is false, so return false - good
	if !m.DeleteIfVersion("k", 3) {
		t.Fatal("should delete")
	}
	if _, ok := m.Get("k"); ok {
		t.Fatal("gone")
	}
}

func TestNegativeFlag(t *testing.T) {
	e := Entry{Flags: FlagNegative, Version: 1}
	if !e.IsNegative() {
		t.Fatal("expected negative")
	}
}

func TestAcceptNegativeNeverClobbersPositive(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()
	m.Set("k", Entry{Value: []byte("pos"), Version: 1})
	if m.AcceptNegative("k", Entry{Version: 99, Flags: FlagNegative}) {
		t.Fatal("must not replace positive")
	}
	e, ok := m.Get("k")
	if !ok || e.IsNegative() || string(e.Value) != "pos" {
		t.Fatalf("got %+v ok=%v", e, ok)
	}
}
