package store

import "testing"

// BenchmarkStoreTopKAdd is a first-merge baseline (re-encodes K slots; not a Get-hit cell).
func BenchmarkStoreTopKAdd(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	const k = 100
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, too := m.TopKAdd("hot", items[i%len(items)], uint64(i+1), 0, k, 0)
		if !ok || too {
			b.Fatal(ok, too)
		}
	}
}
