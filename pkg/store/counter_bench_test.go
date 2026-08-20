package store

import "testing"

func BenchmarkStoreCIncr(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, _ := m.CIncr("c", 1, uint64(i+1), 0); !ok {
			b.Fatal("CIncr")
		}
	}
}

func BenchmarkStoreCGetHit(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	_, _, _ = m.CIncr("c", 1, 1, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := m.CGet("c"); !ok {
			b.Fatal("miss")
		}
	}
}
