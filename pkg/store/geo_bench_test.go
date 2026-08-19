package store

import (
	"fmt"
	"testing"
)

func BenchmarkStoreGeoAdd(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := []byte(fmt.Sprintf("i%d", i%n))
		if !m.GeoAdd("g", member, float64(i%180), float64(i%90), uint64(i+1), 0) {
			b.Fatal("GeoAdd")
		}
	}
}

func BenchmarkStoreGeoPosHit(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.GeoAdd("g", []byte(fmt.Sprintf("i%d", i)), 0, 0, uint64(i+1), 0)
	}
	member := []byte("i0")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := m.GeoPos("g", member); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkStoreGeoPosMiss(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.GeoAdd("g", []byte(fmt.Sprintf("i%d", i)), 0, 0, uint64(i+1), 0)
	}
	member := []byte("never")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := m.GeoPos("g", member); ok {
			b.Fatal("hit")
		}
	}
}
