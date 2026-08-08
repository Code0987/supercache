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

// Delayed ApplyPut after Delete must not resurrect a key (tombstone / delete-version gate).
func TestMemoryNoResurrectionAfterDelete(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()

	if !m.Set("k", Entry{Value: []byte("v1"), Version: 5}) {
		t.Fatal("set")
	}
	if !m.DeleteIfVersion("k", 6) {
		t.Fatal("delete should succeed")
	}
	if _, ok := m.Get("k"); ok {
		t.Fatal("Get must miss after delete")
	}
	// Stale fan-out from before the delete.
	if m.AcceptIfNewer("k", Entry{Value: []byte("stale"), Version: 5}) {
		t.Fatal("delayed ApplyPut with version <= delete version must be rejected")
	}
	if _, ok := m.Get("k"); ok {
		t.Fatal("key must not be resurrected")
	}
	// Newer write after delete is still allowed.
	if !m.AcceptIfNewer("k", Entry{Value: []byte("v2"), Version: 7}) {
		t.Fatal("version > delete tombstone must apply")
	}
	e, ok := m.Get("k")
	if !ok || string(e.Value) != "v2" || e.Version != 7 {
		t.Fatalf("got ok=%v entry=%+v", ok, e)
	}
}

// ApplyDelete on a key that was never present must still block stale Puts.
func TestMemoryDeleteTombstoneWhenMissing(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()

	if !m.DeleteIfVersion("missing", 10) {
		t.Fatal("delete of missing key should succeed (install tombstone)")
	}
	if m.AcceptIfNewer("missing", Entry{Value: []byte("late"), Version: 9}) {
		t.Fatal("stale put after cluster delete must not install")
	}
	if _, ok := m.Get("missing"); ok {
		t.Fatal("must remain absent")
	}
	if !m.AcceptIfNewer("missing", Entry{Value: []byte("ok"), Version: 11}) {
		t.Fatal("newer put should win over tombstone")
	}
}

func TestMemoryRangeSkipsTombstonesAndExpired(t *testing.T) {
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	now := base
	m := NewMemory(0, WithClock(func() time.Time { return now }))
	defer m.Close()

	_ = m.Set("live", Entry{Value: []byte("v"), Version: 1})
	_ = m.Set("neg", Entry{Version: 2, Flags: FlagNegative, ExpireAt: base.Add(time.Hour).UnixNano()})
	_ = m.Set("exp", Entry{Value: []byte("old"), Version: 1, ExpireAt: base.Add(-time.Second).UnixNano()})
	_ = m.DeleteIfVersion("dead", 3)

	var keys []string
	m.Range(func(k string, e Entry) bool {
		keys = append(keys, k)
		if k == "neg" && !e.IsNegative() {
			t.Fatalf("neg entry flags: %+v", e)
		}
		return true
	})
	// exp may be purged on Peek inside Range; tombstone "dead" must be skipped.
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	if !seen["live"] || !seen["neg"] {
		t.Fatalf("want live+neg, got %v", keys)
	}
	if seen["dead"] || seen["exp"] {
		t.Fatalf("tombstone/expired should be skipped: %v", keys)
	}
}

// Tombstones expire so they do not pin memory forever.
func TestMemoryTombstoneExpiry(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	m := NewMemory(0, WithClock(func() time.Time { return now }))
	defer m.Close()

	m.Set("k", Entry{Value: []byte("v"), Version: 1, ExpireAt: base.Add(time.Hour).UnixNano()})
	// Delete with short tombstone TTL via ExpireAt on the delete path is store-internal;
	// here we simulate an expired tombstone then accept an older version (allowed after expiry).
	if !m.DeleteIfVersion("k", 2) {
		t.Fatal("delete")
	}
	// Advance past default tombstone lifetime if the store uses clock+TTL;
	// after tombstone is gone, even version 1 may apply (no durable delete log).
	now = base.Add(24 * time.Hour)
	// Force purge of expired tombstone via Get miss path.
	_, _ = m.Get("k")
	if !m.AcceptIfNewer("k", Entry{Value: []byte("old"), Version: 1}) {
		t.Fatal("after tombstone expiry, store behaves as empty for LWW")
	}
}
