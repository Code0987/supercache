package store

import "testing"

func BenchmarkStoreBitSet(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, too := m.BSet("b", uint64(i%64), true, uint64(i+1), 0, 0)
		if !ok || too {
			b.Fatal(ok, too)
		}
	}
}
