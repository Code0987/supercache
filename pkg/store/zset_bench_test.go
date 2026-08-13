package store

import (
	"fmt"
	"testing"
)

func BenchmarkStoreZAdd(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := []byte(fmt.Sprintf("i%d", i%n))
		if !m.ZAdd("lb", member, float64(i%n), uint64(i+1), 0) {
			b.Fatal("ZAdd")
		}
	}
}

func BenchmarkStoreZScoreHit(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.ZAdd("lb", []byte(fmt.Sprintf("i%d", i)), float64(i), uint64(i+1), 0)
	}
	member := []byte("i0")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc, ok := m.ZScore("lb", member)
		if !ok || sc != 0 {
			b.Fatal("miss", sc, ok)
		}
	}
}

func BenchmarkStoreZScoreMiss(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.ZAdd("lb", []byte(fmt.Sprintf("i%d", i)), float64(i), uint64(i+1), 0)
	}
	member := []byte("never")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := m.ZScore("lb", member); ok {
			b.Fatal("hit")
		}
	}
}
