package store

import "testing"

func BenchmarkStoreHLLAdd(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, too := m.HLLAdd("h", items[i%len(items)], uint64(i+1), 0, 0)
		if !ok || too {
			b.Fatal(ok, too)
		}
	}
}
