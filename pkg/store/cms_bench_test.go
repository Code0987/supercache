package store

import "testing"

func BenchmarkStoreCMSIncr(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, too := m.CMSIncr("c", items[i%len(items)], 1, uint64(i+1), 0, 0)
		if !ok || too {
			b.Fatal(ok, too)
		}
	}
}

func BenchmarkStoreCMSIncrN(b *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, too := m.CMSIncr("c", items[i%len(items)], 100, uint64(i+1), 0, 0)
		if !ok || too {
			b.Fatal(ok, too)
		}
	}
}
