package store

import (
	"testing"

	"github.com/Code0987/supercache/pkg/set"
)

func TestStoreSetAddContainsRemove(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.SetAdd("s", []byte("a"), 1, 0) {
		t.Fatal("add a")
	}
	if !m.SetAdd("s", []byte("b"), 1, 0) {
		t.Fatal("add b")
	}
	if !m.SetContains("s", []byte("a")) || !m.SetContains("s", []byte("b")) {
		t.Fatal("contains")
	}
	if m.SetCard("s") != 2 {
		t.Fatalf("card=%d", m.SetCard("s"))
	}
	mem := m.SetMembers("s")
	if len(mem) != 2 {
		t.Fatalf("members=%v", mem)
	}
	if !m.SetRemove("s", []byte("a"), 2, 0) {
		t.Fatal("remove a")
	}
	if m.SetContains("s", []byte("a")) || !m.SetContains("s", []byte("b")) {
		t.Fatal("after remove")
	}
	if m.SetCard("s") != 1 {
		t.Fatalf("card after remove=%d", m.SetCard("s"))
	}
}

func TestStoreSetNoClobberTwoItems(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.SetAdd("s", []byte("a"), 1, 0) || !m.SetAdd("s", []byte("b"), 1, 0) {
		t.Fatal("adds")
	}
	if m.SetCard("s") != 2 {
		t.Fatal(m.SetCard("s"))
	}
}

func TestStoreSetTombstoneBlocksStale(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	_ = m.SetAdd("s", []byte("a"), 1, 0)
	_ = m.DeleteIfVersion("s", 5)
	if m.SetAdd("s", []byte("b"), 3, 0) {
		t.Fatal("stale add must not beat tombstone v5")
	}
	if !m.SetAdd("s", []byte("b"), 6, 0) {
		t.Fatal("higher version recreate")
	}
	if !m.SetContains("s", []byte("b")) {
		t.Fatal("contains after recreate")
	}
}

func TestStoreSetInstallSnapshot(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	_ = m.SetAdd("s", []byte("old"), 1, 0)
	// Snapshot with higher version replaces membership.
	blob := mustEncode([][]byte{[]byte("new")})
	if !m.SetInstall("s", blob, 3, 0) {
		t.Fatal("install")
	}
	if m.SetContains("s", []byte("old")) || !m.SetContains("s", []byte("new")) {
		t.Fatal("snapshot replace")
	}
	if m.SetInstall("s", blob, 2, 0) {
		t.Fatal("older snapshot ignored")
	}
}

func mustEncode(members [][]byte) []byte {
	return set.Encode(members)
}
