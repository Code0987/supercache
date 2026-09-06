package vecset

import (
	"bytes"
	"math"
	"testing"
)

func TestCodecRoundTrip(t *testing.T) {
	s := NewSet(2)
	if err := s.Add([]byte("a"), []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := s.Add([]byte("b"), []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	blob := s.Encode()
	got, err := DecodeSnapshot(blob)
	if err != nil {
		t.Fatal(err)
	}
	if got.Dim != 2 || got.Card() != 2 {
		t.Fatalf("dim=%d card=%d", got.Dim, got.Card())
	}
	va, ok := got.Emb([]byte("a"))
	if !ok || va[0] != 1 || va[1] != 0 {
		t.Fatalf("a=%v", va)
	}
}

func TestCodecRejectsNaNAndLeftover(t *testing.T) {
	s := NewSet(2)
	_ = s.Add([]byte("a"), []float32{1, 0})
	blob := s.Encode()
	// corrupt a float to NaN
	bad := append([]byte(nil), blob...)
	binaryPutNaN := func(b []byte) {
		u := math.Float32bits(float32(math.NaN()))
		b[len(b)-4] = byte(u)
		b[len(b)-3] = byte(u >> 8)
		b[len(b)-2] = byte(u >> 16)
		b[len(b)-1] = byte(u >> 24)
	}
	binaryPutNaN(bad)
	if _, err := DecodeSnapshot(bad); err == nil {
		t.Fatal("NaN")
	}
	if _, err := DecodeSnapshot(append(blob, 0)); err == nil {
		t.Fatal("leftover")
	}
	if _, err := DecodeSnapshot([]byte("nope")); err == nil {
		t.Fatal("junk")
	}
}

func TestEmptySnapshotKeepsDim(t *testing.T) {
	s := NewSet(4)
	blob := s.Encode()
	got, err := DecodeSnapshot(blob)
	if err != nil || got.Dim != 4 || got.Card() != 0 {
		t.Fatalf("%v dim=%d card=%d", err, got.Dim, got.Card())
	}
}

func TestInboxRoundTrip(t *testing.T) {
	add, err := EncodeInboxAdd([]byte("m"), []float32{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	mem, vec, err := DecodeInboxAdd(add)
	if err != nil || !bytes.Equal(mem, []byte("m")) || len(vec) != 4 || vec[3] != 4 {
		t.Fatalf("%v %q %v", err, mem, vec)
	}
	rem, err := EncodeInboxRem([]byte("m"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeInboxRem(rem)
	if err != nil || !bytes.Equal(got, []byte("m")) {
		t.Fatalf("%v %q", err, got)
	}
}

func TestCosineOrderAndTies(t *testing.T) {
	s := NewSet(2)
	_ = s.Add([]byte("far"), []float32{0, 1})
	_ = s.Add([]byte("near"), []float32{1, 0.1})
	hits, err := s.Sim([]float32{1, 0}, 2, MetricCosine)
	if err != nil || len(hits) != 2 || string(hits[0].Member) != "near" {
		t.Fatalf("%v %+v", err, hits)
	}
	if hits[0].Score <= hits[1].Score {
		t.Fatalf("scores %v %v", hits[0].Score, hits[1].Score)
	}
	s2 := NewSet(2)
	_ = s2.Add([]byte("aa"), []float32{1, 0})
	_ = s2.Add([]byte("bb"), []float32{1, 0})
	ties, err := s2.Sim([]float32{1, 0}, 2, MetricCosine)
	if err != nil || string(ties[0].Member) != "aa" || string(ties[1].Member) != "bb" {
		t.Fatalf("tie order %+v", ties)
	}
}

func TestL2AndIPOrder(t *testing.T) {
	s := NewSet(2)
	if err := s.Add([]byte("a"), []float32{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := s.Add([]byte("b"), []float32{10, 0}); err != nil {
		t.Fatal(err)
	}
	l2, err := s.Sim([]float32{1, 0}, 1, MetricL2)
	if err != nil || len(l2) != 1 || string(l2[0].Member) != "a" || l2[0].Score < 0 {
		t.Fatalf("l2 %+v %v", l2, err)
	}
	ip, err := s.Sim([]float32{1, 0}, 1, MetricIP)
	if err != nil || len(ip) != 1 || string(ip[0].Member) != "b" {
		t.Fatalf("ip %+v %v", ip, err)
	}
}

func TestKGreaterThanCard(t *testing.T) {
	s := NewSet(2)
	_ = s.Add([]byte("a"), []float32{1, 0})
	hits, err := s.Sim([]float32{1, 0}, 50, MetricCosine)
	if err != nil || len(hits) != 1 {
		t.Fatalf("%d %v", len(hits), err)
	}
}

func TestFullAndMemberLen(t *testing.T) {
	s := NewSet(2)
	for i := 0; i < MaxMembers; i++ {
		id := []byte{byte(i >> 8), byte(i)}
		if err := s.Add(id, []float32{1, 0}); err != nil {
			t.Fatal(i, err)
		}
	}
	if err := s.Add([]byte("zz"), []float32{1, 0}); err != errFull {
		t.Fatalf("full: %v", err)
	}
	// replace existing
	id0 := []byte{0, 0}
	if err := s.Add(id0, []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	if s.Card() != MaxMembers {
		t.Fatal(s.Card())
	}
	if err := s.Add(make([]byte, 256), []float32{1, 0}); err != errBadMember {
		t.Fatalf("256: %v", err)
	}
}

func TestWorstEncodedSizeFits1MiB(t *testing.T) {
	n := WorstEncodedSize(MaxDim, MaxMembers, MaxMemberLen)
	if n != 5+512*(1+255+1024) {
		t.Fatal(n)
	}
	if n > 1<<20 {
		t.Fatal(n)
	}
}
