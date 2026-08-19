package listx

import (
	"bytes"
	"testing"
)

func TestLPushRPushOrder(t *testing.T) {
	l := New()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))
	l.LPush([]byte("z"))
	// z, a, b
	r := l.Range(0, -1)
	if len(r) != 3 || string(r[0]) != "z" || string(r[1]) != "a" || string(r[2]) != "b" {
		t.Fatalf("%q", r)
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	l := New()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))
	enc := l.Encode()
	l2, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(l2.Encode(), enc) {
		t.Fatal("unstable")
	}
	r := l2.Range(0, -1)
	if len(r) != 2 || string(r[0]) != "a" || string(r[1]) != "b" {
		t.Fatalf("%q", r)
	}
}

func TestPopsAndEmpty(t *testing.T) {
	l := New()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))
	it, ok := l.LPop()
	if !ok || string(it) != "a" {
		t.Fatal(string(it), ok)
	}
	it, ok = l.RPop()
	if !ok || string(it) != "b" {
		t.Fatal(string(it), ok)
	}
	if _, ok := l.LPop(); ok {
		t.Fatal("empty")
	}
	if l.Len() != 0 {
		t.Fatal(l.Len())
	}
}

func TestIndexAndRangeNegatives(t *testing.T) {
	l := New()
	l.RPush([]byte("a"))
	l.RPush([]byte("b"))
	l.RPush([]byte("c"))
	it, ok := l.Index(-1)
	if !ok || string(it) != "c" {
		t.Fatal(string(it), ok)
	}
	if _, ok := l.Index(9); ok {
		t.Fatal("oob")
	}
	r := l.Range(-2, -1)
	if len(r) != 2 || string(r[0]) != "b" || string(r[1]) != "c" {
		t.Fatalf("%q", r)
	}
}
