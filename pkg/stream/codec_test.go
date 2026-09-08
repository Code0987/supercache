package stream

import "testing"

func TestCodecRoundTrip(t *testing.T) {
	lg := New()
	id, err := lg.Append([]byte("hi"), 100, 0)
	if err != nil || id != "100-0" {
		t.Fatal(id, err)
	}
	blob := lg.Encode()
	got, err := DecodeSnapshot(blob)
	if err != nil || len(got.Entries) != 1 || string(got.Entries[0].Payload) != "hi" {
		t.Fatal(err, got)
	}
}

func TestAppendHardCap(t *testing.T) {
	lg := New()
	for i := 0; i < HardCap; i++ {
		if _, err := lg.Append([]byte("x"), uint64(i+1), 0); err != nil {
			t.Fatal(i, err)
		}
	}
	if _, err := lg.Append([]byte("y"), 10000, 0); err != ErrFull {
		t.Fatalf("got %v", err)
	}
}

func TestParseBoundExclusive(t *testing.T) {
	b, err := ParseBound("(10-0", true)
	if err != nil || !b.Exclusive || b.Ms != 10 {
		t.Fatal(b, err)
	}
	if _, err := ParseBound("(10-0", false); err == nil {
		t.Fatal("end exclusive should fail")
	}
}
