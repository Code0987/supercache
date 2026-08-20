package hashx

import (
	"bytes"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	h := New()
	h.Set([]byte("b"), []byte("2"))
	h.Set([]byte("a"), []byte("1"))
	enc := h.Encode()
	h2, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Len() != 2 {
		t.Fatal(h2.Len())
	}
	v, ok := h2.Get([]byte("a"))
	if !ok || string(v) != "1" {
		t.Fatalf("%q %v", v, ok)
	}
	if !bytes.Equal(h2.Encode(), enc) {
		t.Fatal("encode not stable")
	}
	all := h2.All()
	if len(all) != 2 || string(all[0].Field) != "a" || string(all[1].Field) != "b" {
		t.Fatalf("order %+v", all)
	}
}

func TestEncodeEmptyAndBinary(t *testing.T) {
	h := New()
	if len(h.Encode()) != 0 {
		t.Fatal("empty encode")
	}
	h.Set([]byte{0, 1, 2}, []byte{3, 0, 4})
	h2, err := Decode(h.Encode())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := h2.Get([]byte{0, 1, 2})
	if !ok || !bytes.Equal(v, []byte{3, 0, 4}) {
		t.Fatalf("%q %v", v, ok)
	}
}

func TestDecodeDupsLastWins(t *testing.T) {
	// two records for field "a"
	blob := append(EncodeSet([]byte("a"), []byte("1")), EncodeSet([]byte("a"), []byte("2"))...)
	h, err := Decode(blob)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := h.Get([]byte("a"))
	if !ok || string(v) != "2" || h.Len() != 1 {
		t.Fatalf("%q %v len=%d", v, ok, h.Len())
	}
}

func TestDecodeTruncated(t *testing.T) {
	if _, err := Decode([]byte{0x80}); err == nil {
		t.Fatal("truncated uvarint")
	}
	if _, err := Decode([]byte{0x02, 'a'}); err == nil {
		t.Fatal("short field")
	}
}

func TestEncodeDecodeSet(t *testing.T) {
	b := EncodeSet([]byte("f"), []byte("v"))
	f, v, err := DecodeSet(b)
	if err != nil || string(f) != "f" || string(v) != "v" {
		t.Fatalf("%q %q %v", f, v, err)
	}
	if _, _, err := DecodeSet(nil); err == nil {
		t.Fatal("empty")
	}
	if _, _, err := DecodeSet(append(b, 0)); err == nil {
		t.Fatal("leftover")
	}
}

func TestEmptyStoredValue(t *testing.T) {
	h := New()
	h.Set([]byte("e"), nil)
	v, ok := h.Get([]byte("e"))
	if !ok || v == nil || len(v) != 0 {
		t.Fatalf("%#v %v", v, ok)
	}
	h.Set([]byte("e"), []byte{})
	v, ok = h.Get([]byte("e"))
	if !ok || v == nil || len(v) != 0 {
		t.Fatalf("empty slice %#v %v", v, ok)
	}
}

func TestGetCopyIsolation(t *testing.T) {
	h := New()
	h.Set([]byte("f"), []byte("abc"))
	v, _ := h.Get([]byte("f"))
	v[0] = 'Z'
	v2, ok := h.Get([]byte("f"))
	if !ok || string(v2) != "abc" {
		t.Fatalf("%q", v2)
	}
}

func TestSetOverwriteAndDel(t *testing.T) {
	h := New()
	h.Set([]byte("f"), []byte("1"))
	h.Set([]byte("f"), []byte("2"))
	v, ok := h.Get([]byte("f"))
	if !ok || string(v) != "2" {
		t.Fatal(string(v), ok)
	}
	h.Del([]byte("f"))
	if h.Exists([]byte("f")) || h.Len() != 0 {
		t.Fatal("del")
	}
}
