package store

import (
	"fmt"
	"testing"
)

func BenchmarkStoreHSet(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !m.HSet("h", []byte(fmt.Sprintf("f%d", i%n)), []byte("v"), uint64(i+1), 0) {
			b.Fatal("HSet")
		}
	}
}

func BenchmarkStoreHGetHit(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.HSet("h", []byte(fmt.Sprintf("f%d", i)), []byte("v"), uint64(i+1), 0)
	}
	field := []byte("f0")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := m.HGet("h", field); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkStoreHGetMiss(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.HSet("h", []byte(fmt.Sprintf("f%d", i)), []byte("v"), uint64(i+1), 0)
	}
	field := []byte("never")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := m.HGet("h", field); ok {
			b.Fatal("hit")
		}
	}
}
