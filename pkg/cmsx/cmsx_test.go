package cmsx

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestQueryEmpty(t *testing.T) {
	if Query(New(), []byte("x")) != 0 {
		t.Fatal(Query(New(), []byte("x")))
	}
	if Query(New(), nil) != 0 {
		t.Fatal("nil item")
	}
	if Query(New(), []byte{}) != 0 {
		t.Fatal("empty item")
	}
}

func TestOneItem(t *testing.T) {
	s := New()
	if err := Incr(s, []byte("alice"), 1); err != nil {
		t.Fatal(err)
	}
	if n := Query(s, []byte("alice")); n < 1 {
		t.Fatalf("one item: %d", n)
	}
}

func TestMonotonic(t *testing.T) {
	s := New()
	if err := Incr(s, []byte("alice"), 1); err != nil {
		t.Fatal(err)
	}
	a := Query(s, []byte("alice"))
	if err := Incr(s, []byte("alice"), 1); err != nil {
		t.Fatal(err)
	}
	if Query(s, []byte("alice")) != a+1 {
		t.Fatalf("want %d got %d", a+1, Query(s, []byte("alice")))
	}
}

func TestOverestimate(t *testing.T) {
	s := New()
	const n = 50
	for i := 0; i < n; i++ {
		if err := Incr(s, []byte("only"), 1); err != nil {
			t.Fatal(err)
		}
	}
	if q := Query(s, []byte("only")); q < n {
		t.Fatalf("Query=%d < true=%d", q, n)
	}
}

func TestManyDistinct(t *testing.T) {
	s := New()
	var buf [8]byte
	const n = 1000
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(i))
		if err := Incr(s, buf[:], 1); err != nil {
			t.Fatal(err)
		}
	}
	var maxOver uint64
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(i))
		q := Query(s, buf[:])
		if q < 1 {
			t.Fatalf("item %d Query=%d", i, q)
		}
		if q-1 > maxOver {
			maxOver = q - 1
		}
	}
	if maxOver > 32 {
		t.Fatalf("max overestimate %d > 32", maxOver)
	}
}

func TestErrorBound(t *testing.T) {
	s := New()
	var buf [8]byte
	const items = 100
	const each = 100
	trueN := make([]uint64, items)
	for i := 0; i < items*each; i++ {
		id := uint64(i % items)
		trueN[id]++
		binary.LittleEndian.PutUint64(buf[:], id)
		if err := Incr(s, buf[:], 1); err != nil {
			t.Fatal(err)
		}
	}
	for id := 0; id < items; id++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(id))
		q := Query(s, buf[:])
		if q < trueN[id] {
			t.Fatalf("item %d Query=%d < true=%d", id, q, trueN[id])
		}
		if q-trueN[id] > 32 {
			t.Fatalf("item %d overestimate %d > 32 (ε·10000≈13.3)", id, q-trueN[id])
		}
	}
}

func TestSize(t *testing.T) {
	if len(New()) != DenseSize {
		t.Fatalf("len=%d want %d", len(New()), DenseSize)
	}
	if err := Incr(make([]byte, 8), []byte("a"), 1); err != ErrSize {
		t.Fatalf("wrong size: %v", err)
	}
}

func TestOverflow(t *testing.T) {
	s := New()
	item := []byte("a")
	setCell(s, item, 0, math.MaxUint64)
	before := append([]byte(nil), s...)
	if err := Incr(s, item, 1); err != ErrOverflow {
		t.Fatalf("want ErrOverflow got %v", err)
	}
	if string(s) != string(before) {
		t.Fatal("overflow mutated")
	}
}

func TestIncr(t *testing.T) {
	s := New()
	if err := Incr(s, []byte("t"), 5); err != nil {
		t.Fatal(err)
	}
	q := Query(s, []byte("t"))
	if q < 5 {
		t.Fatalf("Query=%d", q)
	}
	if err := Incr(s, []byte("t"), 1); err != nil {
		t.Fatal(err)
	}
	if Query(s, []byte("t")) != q+1 {
		t.Fatalf("after +1: %d want %d", Query(s, []byte("t")), q+1)
	}
}

func TestIncrNZero(t *testing.T) {
	s := New()
	if err := Incr(s, []byte("t"), 0); err != nil {
		t.Fatal(err)
	}
	if Query(s, []byte("t")) != Query(func() []byte {
		o := New()
		_ = Incr(o, []byte("t"), 1)
		return o
	}(), []byte("t")) {
		t.Fatal("n=0 != n=1")
	}
}

func TestIncrOverflowPartial(t *testing.T) {
	s := New()
	item := []byte("a")
	setCell(s, item, 0, math.MaxUint64-3)
	before := append([]byte(nil), s...)
	if err := Incr(s, item, 4); err != ErrOverflow {
		t.Fatalf("want ErrOverflow got %v", err)
	}
	if string(s) != string(before) {
		t.Fatal("overflow mutated")
	}
	if err := Incr(s, item, 3); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyItem(t *testing.T) {
	s := New()
	if err := Incr(s, nil, 1); err != ErrEmpty {
		t.Fatalf("nil: %v", err)
	}
	if err := Incr(s, []byte{}, 2); err != ErrEmpty {
		t.Fatalf("empty: %v", err)
	}
}

func TestInboxCodec(t *testing.T) {
	b, err := EncodeInbox(7, []byte("t003"))
	if err != nil {
		t.Fatal(err)
	}
	n, item, err := DecodeInbox(b)
	if err != nil || n != 7 || string(item) != "t003" {
		t.Fatalf("n=%d item=%q err=%v", n, item, err)
	}
	b0, err := EncodeInbox(0, []byte("t003"))
	if err != nil {
		t.Fatal(err)
	}
	n0, _, err := DecodeInbox(b0)
	if err != nil || n0 != 1 {
		t.Fatalf("encode 0 wrote n=%d err=%v", n0, err)
	}
	if _, err := EncodeInbox(1, nil); err == nil {
		t.Fatal("empty item")
	}
	if _, _, err := DecodeInbox([]byte{1}); err == nil {
		t.Fatal("item-less inbox")
	}
}

func TestHashStability(t *testing.T) {
	if Hash64([]byte("a")) != 0xaf63dc4c8601ec8c {
		t.Fatalf("Hash64(a)=%#x", Hash64([]byte("a")))
	}
	if mix64(Hash64([]byte("a"))) != 0x02c0bdbf481420f8 {
		t.Fatalf("mix64=%#x", mix64(Hash64([]byte("a"))))
	}
}

func setCell(s, item []byte, row int, v uint64) {
	x := mix64(Hash64(item))
	h1 := x >> 32
	h2 := (x & 0xffffffff) | 1
	col := (h1 + uint64(row)*h2) & (Width - 1)
	off := (uint64(row)*Width + col) * 8
	binary.LittleEndian.PutUint64(s[off:], v)
}
