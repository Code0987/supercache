package zset

import (
	"bytes"
	"math"
	"testing"
)

func TestEncodeRoundTripOrder(t *testing.T) {
	z := New()
	_ = z.Add([]byte("b"), 2)
	_ = z.Add([]byte("a"), 1)
	_ = z.Add([]byte("c"), 1) // equal score: a before c
	enc := z.Encode()
	z2, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	r := z2.Range(0, -1)
	if len(r) != 3 {
		t.Fatalf("len=%d", len(r))
	}
	if string(r[0].Member) != "a" || r[0].Score != 1 {
		t.Fatalf("0: %+v", r[0])
	}
	if string(r[1].Member) != "c" || r[1].Score != 1 {
		t.Fatalf("1: %+v", r[1])
	}
	if string(r[2].Member) != "b" || r[2].Score != 2 {
		t.Fatalf("2: %+v", r[2])
	}
	if !bytes.Equal(z2.Encode(), enc) {
		t.Fatal("encode not stable")
	}
}

func TestScoreUpdateAndRem(t *testing.T) {
	z := New()
	_ = z.Add([]byte("m"), 1)
	_ = z.Add([]byte("m"), 9)
	sc, ok := z.Score([]byte("m"))
	if !ok || sc != 9 {
		t.Fatalf("score=%v ok=%v", sc, ok)
	}
	if z.Card() != 1 {
		t.Fatal(z.Card())
	}
	z.Rem([]byte("m"))
	if _, ok := z.Score([]byte("m")); ok {
		t.Fatal("rem")
	}
}

func TestRangeByScore(t *testing.T) {
	z := New()
	_ = z.Add([]byte("a"), 1)
	_ = z.Add([]byte("b"), 5)
	_ = z.Add([]byte("c"), 10)
	r := z.RangeByScore(5, 10)
	if len(r) != 2 || string(r[0].Member) != "b" || string(r[1].Member) != "c" {
		t.Fatalf("%+v", r)
	}
}

func TestNaNRejected(t *testing.T) {
	z := New()
	if err := z.Add([]byte("x"), math.NaN()); err == nil {
		t.Fatal("want NaN error")
	}
}

func TestNegativeRank(t *testing.T) {
	z := New()
	_ = z.Add([]byte("a"), 1)
	_ = z.Add([]byte("b"), 2)
	_ = z.Add([]byte("c"), 3)
	r := z.Range(-1, -1)
	if len(r) != 1 || string(r[0].Member) != "c" {
		t.Fatalf("%+v", r)
	}
}
