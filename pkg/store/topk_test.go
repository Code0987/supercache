package store

import "testing"

func TestStoreTopKAddList(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.TopKAdd("hot", []byte("t001"), 1, 0, 10, 0)
	if !ok || too {
		t.Fatal(ok, too)
	}
	ok, too = m.TopKAdd("hot", []byte("t002"), 1, 0, 10, 0)
	if !ok || too {
		t.Fatal("second")
	}
	ok, too = m.TopKAdd("hot", []byte("t001"), 1, 0, 10, 0)
	if !ok || too {
		t.Fatal("dup")
	}
	rows, present := m.TopKList("hot", 10)
	if !present || len(rows) != 2 {
		t.Fatalf("list %v present=%v", rows, present)
	}
	if string(rows[0].Item) != "t001" || rows[0].Count != 2 {
		t.Fatalf("head %+v", rows[0])
	}
	if string(rows[1].Item) != "t002" || rows[1].Count != 1 {
		t.Fatalf("tail %+v", rows[1])
	}
}

func TestStoreTopKAddEmptyItem(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, too := m.TopKAdd("hot", nil, 1, 0, 10, 0)
	if ok || too {
		t.Fatalf("nil: ok=%v too=%v", ok, too)
	}
	ok, too = m.TopKAdd("hot", []byte{}, 1, 0, 10, 0)
	if ok || too {
		t.Fatalf("empty: ok=%v too=%v", ok, too)
	}
	if m.HasTopK("hot") {
		t.Fatal("created")
	}
}

func TestStoreTopKAddStoredVersion(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	ok, _ := m.TopKAdd("hot", []byte("a"), 99, 0, 10, 0)
	if !ok {
		t.Fatal("create")
	}
	ent, present := m.Peek("hot")
	if !present || ent.Version != 1 {
		t.Fatalf("create ver=%d", ent.Version)
	}
	ok, _ = m.TopKAdd("hot", []byte("b"), 99, 0, 10, 0)
	if !ok {
		t.Fatal("second")
	}
	ent, _ = m.Peek("hot")
	if ent.Version != 2 {
		t.Fatalf("second ver=%d", ent.Version)
	}
	ok, _ = m.TopKAdd("hot", []byte("c"), 99, 0, 10, 0)
	if !ok {
		t.Fatal("third")
	}
	ent, _ = m.Peek("hot")
	if ent.Version != 3 {
		t.Fatalf("gate not written: ver=%d", ent.Version)
	}
}

func TestStoreTopKAddTouch(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	m.TopKAdd("hot", []byte("a"), 1, 0, 10, 0)
	m.TopKAdd("hot", []byte("a"), 1, 0, 10, 0)
	ent, _ := m.Peek("hot")
	if ent.Version != 2 {
		t.Fatalf("touch ver=%d", ent.Version)
	}
}

func TestStoreTopKAddTooLarge(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	costBefore := m.Stats().Bytes
	ok, too := m.TopKAdd("hot", []byte("a"), 1, 0, 10, 1)
	if ok || !too {
		t.Fatalf("want tooLarge: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
	if m.HasTopK("hot") {
		t.Fatal("inserted")
	}
}

func TestStoreTopKAddTooLargeLeavesPrior(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if ok, _ := m.TopKAdd("hot", []byte("a"), 1, 0, 10, 0); !ok {
		t.Fatal("seed")
	}
	before, _ := m.Peek("hot")
	costBefore := m.Stats().Bytes
	// One slot is tiny; a tiny maxValue still rejects the re-encode of a+b.
	ok, too := m.TopKAdd("hot", []byte("bbbb"), 1, 0, 10, len(before.Value))
	if ok || !too {
		t.Fatalf("want tooLarge: ok=%v too=%v", ok, too)
	}
	if m.Stats().Bytes != costBefore {
		t.Fatalf("cost %d → %d", costBefore, m.Stats().Bytes)
	}
	after, _ := m.Peek("hot")
	if after.Version != before.Version || string(after.Value) != string(before.Value) {
		t.Fatal("mutated")
	}
}
