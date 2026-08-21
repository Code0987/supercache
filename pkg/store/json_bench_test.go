package store

import (
	"fmt"
	"testing"
)

func BenchmarkStoreJsonSet(t *testing.B) {
	m := NewMemory(64 << 20)
	defer m.Close()
	ok, _ := m.JSet("d", "$", []byte(`{}`), 1, 0, 0)
	if !ok {
		t.Fatal("seed")
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		ok, too := m.JSet("d", fmt.Sprintf("$.k%d", i%64), []byte("1"), uint64(i+2), 0, 0)
		if !ok || too {
			t.Fatal(ok, too)
		}
	}
}
