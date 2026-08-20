package store

import (
	"testing"

	"github.com/Code0987/supercache/pkg/hashx"
)

func TestStoreHSetGetDel(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.HSet("user", []byte("b"), []byte("2"), 1, 0) {
		t.Fatal("set b")
	}
	if !m.HSet("user", []byte("a"), []byte("1"), 2, 0) {
		t.Fatal("set a")
	}
	if !m.HSet("user", []byte("a"), []byte("1x"), 3, 0) {
		t.Fatal("overwrite a")
	}
	v, ok := m.HGet("user", []byte("a"))
	if !ok || string(v) != "1x" {
		t.Fatalf("%q %v", v, ok)
	}
	if m.HLen("user") != 2 {
		t.Fatal(m.HLen("user"))
	}
	all := m.HGetAll("user")
	if len(all) != 2 || string(all[0].Field) != "a" || string(all[1].Field) != "b" {
		t.Fatalf("%+v", all)
	}
	if !m.HDel("user", []byte("b"), 4, 0) {
		t.Fatal("del")
	}
	if m.HExists("user", []byte("b")) {
		t.Fatal("exists after del")
	}
	if m.HLen("user") != 1 {
		t.Fatal(m.HLen("user"))
	}
}

func TestStoreHDelMissingNoCreate(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.HDel("nope", []byte("f"), 1, 0) {
		t.Fatal("missing del")
	}
	if m.HasHash("nope") {
		t.Fatal("created")
	}
}

func TestStoreTwoFields(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.HSet("h", []byte("x"), []byte("1"), 1, 0) || !m.HSet("h", []byte("y"), []byte("2"), 2, 0) {
		t.Fatal("sets")
	}
	if !m.HExists("h", []byte("x")) || !m.HExists("h", []byte("y")) {
		t.Fatal("both")
	}
}

func TestStoreHInstallTombstone(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.HSet("h", []byte("a"), []byte("1"), 2, 0) {
		t.Fatal("set")
	}
	if !m.DeleteIfVersion("h", 3) {
		t.Fatal("tomb")
	}
	blob := hashx.New()
	blob.Set([]byte("a"), []byte("1"))
	if m.HInstall("h", blob.Encode(), 1, 0) {
		t.Fatal("stale snapshot")
	}
	if m.HasHash("h") {
		t.Fatal("resurrected")
	}
}
