package topkx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestDecodeReject(t *testing.T) {
	if _, err := Decode([]byte{1, 1, 'x'}, 0); err != ErrK {
		t.Fatalf("k<1: %v", err)
	}
	if _, err := Decode([]byte{1}, 2); err != ErrDecode {
		t.Fatalf("trunc: %v", err)
	}
	var buf []byte
	buf = appendUvarint(buf, 0)
	buf = appendUvarint(buf, 1)
	buf = append(buf, 'a')
	if _, err := Decode(buf, 2); err != ErrDecode {
		t.Fatalf("count0: %v", err)
	}
	buf = nil
	buf = appendUvarint(buf, 1)
	buf = appendUvarint(buf, 0)
	if _, err := Decode(buf, 2); err != ErrDecode {
		t.Fatalf("empty item: %v", err)
	}
	tab := New(4)
	_ = tab.Add([]byte("a"))
	enc := tab.Encode()
	enc = append(enc, enc...)
	if _, err := Decode(enc, 4); err != ErrDecode {
		t.Fatalf("dup: %v", err)
	}
	tab = New(3)
	_ = tab.Add([]byte("a"))
	_ = tab.Add([]byte("b"))
	_ = tab.Add([]byte("c"))
	if _, err := Decode(tab.Encode(), 1); err != ErrDecode {
		t.Fatalf("over k: %v", err)
	}
}

func appendUvarint(b []byte, x uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(b, tmp[:n]...)
}

func TestEncodeRoundTrip(t *testing.T) {
	tab := New(10)
	ingestHonestStream(t, tab)
	dec, err := Decode(tab.Encode(), 10)
	if err != nil {
		t.Fatal(err)
	}
	want, got := tab.List(), dec.List()
	if len(want) != len(got) {
		t.Fatalf("len %d vs %d", len(want), len(got))
	}
	for i := range want {
		if want[i].Count != got[i].Count || !bytes.Equal(want[i].Item, got[i].Item) {
			t.Fatalf("rank %d %s vs %s", i, formatList(want), formatList(got))
		}
	}
}

func TestWorstEncodedSize(t *testing.T) {
	if n := WorstEncodedSize(100, 512); n != 52400 {
		t.Fatalf("worst %d", n)
	}
	tab := New(100)
	for i := 0; i < 100; i++ {
		if err := tab.Add([]byte(fmt.Sprintf("t%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(tab.Encode()); n >= WorstEncodedSize(100, 512) {
		t.Fatalf("typical 4-byte encode %d not under 52400", n)
	}
}
