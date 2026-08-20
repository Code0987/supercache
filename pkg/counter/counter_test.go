package counter

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 42} {
		b := Encode(v)
		if len(b) != 8 {
			t.Fatalf("%d: len %d", v, len(b))
		}
		got, err := Decode(b)
		if err != nil || got != v {
			t.Fatalf("%d: got %d %v", v, got, err)
		}
	}
	if _, err := Decode(nil); err == nil {
		t.Fatal("empty")
	}
	if _, err := Decode(make([]byte, 7)); err == nil {
		t.Fatal("7")
	}
	if _, err := Decode(make([]byte, 9)); err == nil {
		t.Fatal("9")
	}
	b := Encode(-1)
	if binary.LittleEndian.Uint64(b) != math.MaxUint64 {
		t.Fatal("twos complement -1")
	}
}

func TestAddOverflow(t *testing.T) {
	if _, err := Add(math.MaxInt64, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("max+1: %v", err)
	}
	if _, err := Add(math.MinInt64, -1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("min-1: %v", err)
	}
	if v, err := Add(0, 7); err != nil || v != 7 {
		t.Fatal(v, err)
	}
	if v, err := Add(7, 0); err != nil || v != 7 {
		t.Fatal(v, err)
	}
}
