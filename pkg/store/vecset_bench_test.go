package store

import (
	"fmt"
	"testing"

	"github.com/Code0987/supercache/pkg/vecset"
)

func BenchmarkStoreVAdd(b *testing.B) {
	m := NewMemory(8 << 20)
	defer m.Close()
	vec := make([]float32, 64)
	vec[0] = 1
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := []byte(fmt.Sprintf("%d", i%128))
		m.VAdd("v", id, vec, 1, 0, 0)
	}
}

func BenchmarkStoreVSim(b *testing.B) {
	m := NewMemory(8 << 20)
	defer m.Close()
	q := make([]float32, 64)
	q[0] = 1
	for i := 0; i < 128; i++ {
		v := make([]float32, 64)
		v[i%64] = 1
		m.VAdd("v", []byte(fmt.Sprintf("%d", i)), v, 1, 0, 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.VSim("v", q, 10, int(vecset.MetricCosine))
	}
}
