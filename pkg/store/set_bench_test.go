package store

import (
	"fmt"
	"testing"
)

func BenchmarkStoreSetAdd(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := []byte(fmt.Sprintf("i%d", i%n))
		if !m.SetAdd("s", item, uint64(i+1), 0) {
			b.Fatal("SetAdd")
		}
	}
}

func BenchmarkStoreSetContainsHit(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.SetAdd("s", []byte(fmt.Sprintf("i%d", i)), uint64(i+1), 0)
	}
	item := []byte("i0")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !m.SetContains("s", item) {
			b.Fatal("miss")
		}
	}
}

func BenchmarkStoreSetContainsMiss(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = m.SetAdd("s", []byte(fmt.Sprintf("i%d", i)), uint64(i+1), 0)
	}
	item := []byte("never")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if m.SetContains("s", item) {
			b.Fatal("hit")
		}
	}
}
