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

	var all []string
	m.RangeAll(func(k string, e Entry) bool {
		all = append(all, k)
		if k == "dead" && !e.IsTombstone() {
			t.Fatalf("dead should be tombstone: %+v", e)
		}
		return true
	})
	seenAll := map[string]bool{}
	for _, k := range all {
		seenAll[k] = true
	}
	if !seenAll["live"] || !seenAll["neg"] || !seenAll["dead"] {
		t.Fatalf("RangeAll want live+neg+tombstone, got %v", all)
	}
}

func TestMemoryTombstoneRequiredUnderMaxBytes(t *testing.T) {
	// Budget smaller than a tombstone envelope (key + ~64 B). Old path
	// skipped the marker and left a resurrection hole.
	m := NewMemory(40)
	defer m.Close()

	if !m.DeleteIfVersion("k", 6) {
		t.Fatal("delete must install tombstone even when cost > MaxBytes")
	}
	ent, ok := m.Peek("k")
	if !ok || !ent.IsTombstone() || ent.Version != 6 {
		t.Fatalf("want tombstone v6, peek ok=%v %+v", ok, ent)
	}
	if m.AcceptIfNewer("k", Entry{Value: []byte("stale"), Version: 5}) {
		t.Fatal("stale ApplyPut must not resurrect when tombstone overshoots MaxBytes")
	}
}

func TestMemoryTombstoneSurvivesLRUPressure(t *testing.T) {
	m := NewMemory(400)
	defer m.Close()

	if !m.Set("keep", Entry{Value: []byte("v"), Version: 1}) {
		t.Fatal("set keep")
	}
	if !m.DeleteIfVersion("keep", 2) {
		t.Fatal("tombstone keep")
	}
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("n-%02d", i)
		_ = m.Set(k, Entry{Value: make([]byte, 40), Version: 1})
	}
	ent, ok := m.Peek("keep")
	if !ok || !ent.IsTombstone() {
		t.Fatalf("LRU must not evict live tombstone, peek ok=%v %+v", ok, ent)
	}
	if m.AcceptIfNewer("keep", Entry{Value: []byte("stale"), Version: 1}) {
		t.Fatal("eviction must not open a resurrection hole")
	}
}

func TestAcceptNegativeBranches(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()

	// Fresh negative.
	if !m.AcceptNegative("k", Entry{Version: 1, Flags: FlagNegative}) {
		t.Fatal("install negative")
	}
	if m.AcceptNegative("k", Entry{Version: 1, Flags: FlagNegative}) {
		t.Fatal("equal-version negative is stale")
	}
	if !m.AcceptNegative("k", Entry{Version: 2, Flags: FlagNegative}) {
		t.Fatal("higher negative should win")
	}

	// Tombstone blocks lower/equal negative.
	if !m.DeleteIfVersion("k", 5) {
		t.Fatal("tombstone")
	}
	if m.AcceptNegative("k", Entry{Version: 5, Flags: FlagNegative}) {
		t.Fatal("negative must not beat equal tombstone")
	}
	if m.AcceptNegative("k", Entry{Version: 4, Flags: FlagNegative}) {
		t.Fatal("negative must not beat higher tombstone")
	}
	// Higher version may replace tombstone with a negative (miss path after delete).
	if !m.AcceptNegative("k", Entry{Version: 6, Flags: FlagNegative}) {
		t.Fatal("negative after tombstone with higher version")
	}
	ent, ok := m.Peek("k")
	if !ok || !ent.IsNegative() || ent.Version != 6 {
		t.Fatalf("want negative v6, got ok=%v %+v", ok, ent)
	}
}

func TestBloomMergeAndFlags(t *testing.T) {
	const bits, k = 2048, 4
	m := NewMemory(0)
	defer m.Close()

	if !m.BloomAdd("f", []byte("a"), bits, k, 1, 0) {
		t.Fatal("add a")
	}
	// Build a second filter with only "b", then merge into store.
	m2 := NewMemory(0)
	defer m2.Close()
	if !m2.BloomAdd("f", []byte("b"), bits, k, 1, 0) {
		t.Fatal("add b on m2")
	}
	ent2, ok := m2.Peek("f")
	if !ok || !ent2.IsBloom() {
		t.Fatal("m2 bloom")
	}
	if !m.BloomMerge("f", ent2.Value, bits, k, 1, 0) {
		t.Fatal("merge")
	}
	if !m.BloomTest("f", []byte("a"), bits, k) || !m.BloomTest("f", []byte("b"), bits, k) {
		t.Fatal("merge must keep both items")
	}
	// Non-bloom live entry blocks bloom ops.
	_ = m.Set("plain", Entry{Value: []byte("x"), Version: 1})
	if m.BloomAdd("plain", []byte("z"), bits, k, 2, 0) {
		t.Fatal("must not bloom-add over plain entry")
	}
	if m.BloomMerge("plain", ent2.Value, bits, k, 2, 0) {
		t.Fatal("must not bloom-merge over plain entry")
	}
	add := Entry{Flags: FlagBloomAdd}
	empty := Entry{}
	if !add.IsBloomAdd() || empty.IsBloomAdd() {
		t.Fatal("IsBloomAdd")
	}
}

func TestBloomSurvivesLRUPressure(t *testing.T) {
	const bits, k = 256, 3 // small bitset
	m := NewMemory(500)
	defer m.Close()
	if !m.BloomAdd("keep", []byte("item"), bits, k, 1, 0) {
		t.Fatal("bloom")
	}
	for i := 0; i < 40; i++ {
		_ = m.Set(fmt.Sprintf("n-%02d", i), Entry{Value: make([]byte, 40), Version: 1})
	}
	if !m.BloomTest("keep", []byte("item"), bits, k) {
		t.Fatal("LRU must not evict live bloom")
	}
}

func TestWithTombstoneTTLNeverExpires(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	m := NewMemory(0, WithClock(func() time.Time { return now }), WithTombstoneTTL(0))
	defer m.Close()
	_ = m.Set("k", Entry{Value: []byte("v"), Version: 1})
	if !m.DeleteIfVersion("k", 2) {
		t.Fatal("delete")
	}
	now = base.Add(100 * 24 * time.Hour)
	if m.AcceptIfNewer("k", Entry{Value: []byte("old"), Version: 1}) {
		t.Fatal("tombstone with TTL=0 must not expire")
	}
}

func TestSetReplaceAndRejectTooLarge(t *testing.T) {
	m := NewMemory(100)
	defer m.Close()
	// Entire entry larger than MaxBytes is rejected.
	if m.Set("big", Entry{Value: make([]byte, 200), Version: 1}) {
		t.Fatal("oversized set must fail")
	}
	if !m.Set("k", Entry{Value: []byte("a"), Version: 1}) {
		t.Fatal("set")
	}
	if !m.Set("k", Entry{Value: []byte("bb"), Version: 2}) {
		t.Fatal("replace")
	}
	e, ok := m.Get("k")
	if !ok || string(e.Value) != "bb" || e.Version != 2 {
		t.Fatalf("got %+v ok=%v", e, ok)
	}
	if m.Delete("missing") {
		t.Fatal("delete missing")
	}
}

func TestBloomTestMissPaths(t *testing.T) {
	const bits, k = 1024, 3
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	now := base
	m := NewMemory(0, WithClock(func() time.Time { return now }))
	defer m.Close()

	if m.BloomTest("nope", []byte("x"), bits, k) {
		t.Fatal("missing filter")
	}
	_ = m.DeleteIfVersion("tomb", 1)
	if m.BloomTest("tomb", []byte("x"), bits, k) {
		t.Fatal("tombstone is not a bloom")
	}
	_ = m.Set("plain", Entry{Value: []byte("v"), Version: 1})
	if m.BloomTest("plain", []byte("x"), bits, k) {
		t.Fatal("plain kv is not a bloom")
	}
	// Expired bloom is purged and tests false.
	if !m.BloomAdd("exp", []byte("a"), bits, k, 1, base.Add(time.Second).UnixNano()) {
		t.Fatal("add exp")
	}
	now = base.Add(2 * time.Second)
	if m.BloomTest("exp", []byte("a"), bits, k) {
		t.Fatal("expired bloom")
	}
	// Wrong bit length Open fails → Test false.
	if !m.BloomAdd("f", []byte("a"), bits, k, 1, 0) {
		t.Fatal("add f")
	}
	if m.BloomTest("f", []byte("a"), bits+64, k) {
		t.Fatal("mismatched mBits must not report true")
	}
	// Stale bloom add after tombstone.
	_ = m.DeleteIfVersion("f", 5)
	if m.BloomAdd("f", []byte("z"), bits, k, 4, 0) {
		t.Fatal("stale version must not replace tombstone")
	}
	// Invalid merge bit length.
	if m.BloomMerge("g", []byte{1, 2}, bits, k, 1, 0) {
		t.Fatal("short bitset merge must fail")
	}
	// BloomAdd after higher tombstone version.
	if !m.BloomAdd("f", []byte("z"), bits, k, 6, 0) {
		t.Fatal("higher version after tombstone")
	}
	if !m.BloomTest("f", []byte("z"), bits, k) {
		t.Fatal("want z present")
	}
}

func TestRangeStopEarlyAndNil(t *testing.T) {
	m := NewMemory(0)
	defer m.Close()
	_ = m.Set("a", Entry{Value: []byte("1"), Version: 1})
	_ = m.Set("b", Entry{Value: []byte("2"), Version: 1})
	n := 0
	m.Range(func(string, Entry) bool {
		n++
		return false
	})
	if n != 1 {
		t.Fatalf("Range stop early: n=%d", n)
	}
	n = 0
	m.RangeAll(func(string, Entry) bool {
		n++
		return false
	})
	if n != 1 {
		t.Fatalf("RangeAll stop early: n=%d", n)
	}
	// nil receiver / fn must not panic
	var nilM *Memory
	nilM.Range(nil)
	nilM.RangeAll(nil)
	m.Range(nil)
	m.RangeAll(nil)
}

func TestPeekExpiredAndAcceptIfNewerTooLarge(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	now := base
	m := NewMemory(80, WithClock(func() time.Time { return now }))
	defer m.Close()
	_ = m.Set("e", Entry{Value: []byte("v"), Version: 1, ExpireAt: base.Add(time.Second).UnixNano()})
	now = base.Add(2 * time.Second)
	if _, ok := m.Peek("e"); ok {
		t.Fatal("Peek must drop expired")
	}
	_ = m.Set("k", Entry{Value: []byte("a"), Version: 1})
	// Replace with a value that exceeds MaxBytes → reject, keep old.
	if m.AcceptIfNewer("k", Entry{Value: make([]byte, 200), Version: 2}) {
		t.Fatal("oversized AcceptIfNewer must fail")
	}
	e, ok := m.Get("k")
	if !ok || string(e.Value) != "a" {
		t.Fatalf("old value must remain: %+v ok=%v", e, ok)
	}
	// New key oversized.
	if m.AcceptIfNewer("n", Entry{Value: make([]byte, 200), Version: 1}) {
		t.Fatal("new oversized key")
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
