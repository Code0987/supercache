package set

import (
	"bytes"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	in := [][]byte{[]byte("b"), []byte("a"), []byte("a")} // unsorted + dup
	enc := Encode(in)
	out, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	// Canonical: sorted unique
	if len(out) != 2 || !bytes.Equal(out[0], []byte("a")) || !bytes.Equal(out[1], []byte("b")) {
		t.Fatalf("out=%v", out)
	}
	// Re-encode stable
	if !bytes.Equal(Encode(out), enc) {
		t.Fatal("encode not canonical")
	}
}

func TestEmptyAndContains(t *testing.T) {
	if Encode(nil) == nil {
		// empty encode may be empty slice non-nil
	}
	enc := Encode(nil)
	out, err := Decode(enc)
	if err != nil || len(out) != 0 {
		t.Fatalf("empty: %v err=%v", out, err)
	}
	s := FromMembers([][]byte{[]byte("x"), []byte("y")})
	if !s.Contains([]byte("x")) || s.Contains([]byte("z")) {
		t.Fatal("contains")
	}
	if s.Len() != 2 {
		t.Fatalf("len=%d", s.Len())
	}
	s.Add([]byte("x"))
	if s.Len() != 2 {
		t.Fatal("idempotent add")
	}
	s.Remove([]byte("x"))
	if s.Contains([]byte("x")) || s.Len() != 1 {
		t.Fatal("remove")
	}
	s.Remove([]byte("x"))
	if s.Len() != 1 {
		t.Fatal("idempotent remove")
	}
}
