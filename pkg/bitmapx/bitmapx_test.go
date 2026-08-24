package bitmapx

import (
	"errors"
	"math"
	"testing"
)

func TestBitOrder(t *testing.T) {
	var buf []byte
	buf = Set(buf, 0, true)
	if len(buf) != 1 || buf[0]&0x80 == 0 || buf[0] != 0x80 {
		t.Fatalf("offset 0: %x", buf)
	}
	buf = Set(nil, 7, true)
	if len(buf) != 1 || buf[0] != 0x01 {
		t.Fatalf("offset 7: %x", buf)
	}
	buf = Set(nil, 8, true)
	if len(buf) != 2 || buf[0] != 0 || buf[1] != 0x80 {
		t.Fatalf("offset 8: %x", buf)
	}
	// Bloom LSB-first would set 1<<(0%8)=0x01 for offset 0
	if Get([]byte{0x01}, 0) {
		t.Fatal("LSB packing must not look like bit 0")
	}
	if !Get([]byte{0x80}, 0) {
		t.Fatal("MSB is bit 0")
	}
}

func TestSetGrowZeroFill(t *testing.T) {
	buf := Set(nil, 8, true)
	if len(buf) != 2 || buf[0] != 0x00 || buf[1] != 0x80 {
		t.Fatalf("%x", buf)
	}
}

func TestGetPastEnd(t *testing.T) {
	if Get([]byte{0xff}, 8) {
		t.Fatal("past end")
	}
}

func TestCountWindow(t *testing.T) {
	var buf []byte
	buf = Set(buf, 0, true)
	buf = Set(buf, 8, true)
	if Count(buf, 0, -1) != 2 {
		t.Fatal(Count(buf, 0, -1))
	}
	if Count(buf, 0, 0) != 1 {
		t.Fatal("byte 0")
	}
	if Count(buf, 1, 1) != 1 {
		t.Fatal("byte 1")
	}
	if Count(buf, 2, 2) != 0 {
		t.Fatal("beyond")
	}
	if Count(buf, -1, -1) != 1 {
		t.Fatal("last byte")
	}
}

func TestCountEmpty(t *testing.T) {
	if Count(nil, 0, -1) != 0 {
		t.Fatal("empty")
	}
	if Count([]byte{0xff}, 1, 0) != 0 {
		t.Fatal("inverted")
	}
}

func TestPos(t *testing.T) {
	var buf []byte
	buf = Set(buf, 0, true)
	buf = Set(buf, 8, true)
	pos, ok := Pos(buf, true, 0, -1)
	if !ok || pos != 0 {
		t.Fatalf("first 1: %d %v", pos, ok)
	}
	pos, ok = Pos(buf, true, 1, 1)
	if !ok || pos != 8 {
		t.Fatalf("byte 1: %d %v", pos, ok)
	}
	_, ok = Pos([]byte{0x00, 0x00}, true, 0, -1)
	if ok {
		t.Fatal("all-zero looking for 1")
	}
	_, ok = Pos([]byte{0xff, 0xff}, false, 0, -1)
	if ok {
		t.Fatal("all-1s looking for 0 must not invent a tail bit")
	}
}

func TestEncodeDecodeSet(t *testing.T) {
	b := EncodeSet(0, false)
	off, bit, err := DecodeSet(b)
	if err != nil || off != 0 || bit {
		t.Fatalf("%d %v %v", off, bit, err)
	}
	b = EncodeSet(8, true)
	off, bit, err = DecodeSet(b)
	if err != nil || off != 8 || !bit {
		t.Fatalf("%d %v %v", off, bit, err)
	}
	if _, _, err := DecodeSet([]byte{0x80}); !errors.Is(err, ErrInbox) {
		t.Fatalf("truncated: %v", err)
	}
	if _, _, err := DecodeSet(append(EncodeSet(1, true), 0)); !errors.Is(err, ErrInbox) {
		t.Fatal("trailing")
	}
	bad := EncodeSet(1, true)
	bad[len(bad)-1] = 2
	if _, _, err := DecodeSet(bad); !errors.Is(err, ErrInbox) {
		t.Fatal("bit byte")
	}
}

func TestEncodedLen(t *testing.T) {
	n, ok := EncodedLen(0)
	if !ok || n != 1 {
		t.Fatal(n, ok)
	}
	n, ok = EncodedLen(7)
	if !ok || n != 1 {
		t.Fatal(n, ok)
	}
	n, ok = EncodedLen(8)
	if !ok || n != 2 {
		t.Fatal(n, ok)
	}
	if math.MaxInt < math.MaxInt64 {
		// Runtime multiply: uint64(MaxInt)*8 is a compile-time overflow on 64-bit.
		off := uint64(math.MaxInt)
		off = off*8 + 8
		_, ok = EncodedLen(off)
		if ok {
			t.Fatal("want overflow")
		}
	}
}
