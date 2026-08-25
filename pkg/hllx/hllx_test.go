package hllx

import (
	"encoding/binary"
	"testing"
)

func TestCountEmpty(t *testing.T) {
	if Count(New()) != 0 {
		t.Fatal(Count(New()))
	}
}

func TestCountOneItem(t *testing.T) {
	r := New()
	Add(r, []byte("alice"))
	n := Count(r)
	if n != 1 && n != 2 {
		t.Fatalf("one item: %d", n)
	}
}

func TestAddIdempotent(t *testing.T) {
	r := New()
	Add(r, []byte("alice"))
	a := Count(r)
	Add(r, []byte("alice"))
	if Count(r) != a {
		t.Fatalf("duplicate grew %d → %d", a, Count(r))
	}
}

func TestManyDistinctGrow(t *testing.T) {
	small := New()
	big := New()
	var buf [8]byte
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(i))
		Add(small, buf[:])
	}
	for i := 0; i < 1000; i++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(i))
		Add(big, buf[:])
	}
	if Count(big) <= Count(small) {
		t.Fatalf("grow: 10=%d 1000=%d", Count(small), Count(big))
	}
}

func TestErrorBound10k(t *testing.T) {
	r := New()
	var buf [8]byte
	const n = 10_000
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(buf[:], uint64(i))
		Add(r, buf[:])
	}
	est := Count(r)
	rel := absRel(est, n)
	if rel >= 0.03 {
		t.Fatalf("est=%d n=%d rel=%f", est, n, rel)
	}
}

func absRel(est uint64, n int) float64 {
	d := float64(est) - float64(n)
	if d < 0 {
		d = -d
	}
	return d / float64(n)
}

func TestPackingRoundTrip(t *testing.T) {
	r := New()
	SetReg(r, 0, 0)
	SetReg(r, 1, 1)
	SetReg(r, 2, 63)
	if GetReg(r, 0) != 0 || GetReg(r, 1) != 1 || GetReg(r, 2) != 63 {
		t.Fatalf("%d %d %d", GetReg(r, 0), GetReg(r, 1), GetReg(r, 2))
	}
	SetReg(r, 0, 63)
	if GetReg(r, 1) != 1 {
		t.Fatal("clobber adj 1", GetReg(r, 1))
	}
	SetReg(r, 1, 63)
	if GetReg(r, 0) != 63 || GetReg(r, 2) != 63 {
		t.Fatalf("clobber adj 0/2: %d %d", GetReg(r, 0), GetReg(r, 2))
	}
}

func TestNewSize(t *testing.T) {
	if len(New()) != DenseSize || DenseSize != 12288 {
		t.Fatal(len(New()), DenseSize)
	}
	if err := Merge(New(), []byte{1}); err != ErrSize {
		t.Fatal(err)
	}
}

func TestMergeMax(t *testing.T) {
	a, b := New(), New()
	Add(a, []byte("alice"))
	Add(b, []byte("bob"))
	if err := Merge(a, b); err != nil {
		t.Fatal(err)
	}
	n := Count(a)
	if n < 1 || n > 4 {
		t.Fatalf("merged count %d", n)
	}
}

func TestHash64Golden(t *testing.T) {
	// FNV-1a 64 of "a" (stdlib hash/fnv.New64a).
	const want = uint64(0xaf63dc4c8601ec8c)
	if got := Hash64([]byte("a")); got != want {
		t.Fatalf("%#x want %#x", got, want)
	}
}

func TestAddNoPanicBad(t *testing.T) {
	Add(nil, []byte("x"))
	Add(New(), nil)
	Add(New()[:8], []byte("x"))
	if Count(nil) != 0 {
		t.Fatal("bad size")
	}
}
