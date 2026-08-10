package store

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/Code0987/supercache/internal/benchmetrics"
)

const (
	benchKeys      = 10_000
	benchValueSize = 256
)

func benchValue() []byte {
	v := make([]byte, benchValueSize)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

func filledMemory(b *testing.B) *Memory {
	b.Helper()
	m := NewMemory(64 << 20)
	val := benchValue()
	for i := 0; i < benchKeys; i++ {
		ok := m.Set(fmt.Sprintf("k%d", i), Entry{Value: val, Version: 1})
		if !ok {
			b.Fatal("set rejected")
		}
	}
	return m
}

func BenchmarkStoreGetHit(b *testing.B) {
	m := filledMemory(b)
	defer m.Close()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok := m.Get(fmt.Sprintf("k%d", i%benchKeys))
		if !ok {
			b.Fatal("miss")
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkStoreGetHitParallel(b *testing.B) {
	m := filledMemory(b)
	defer m.Close()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := m.Get(fmt.Sprintf("k%d", i%benchKeys))
			if !ok {
				b.Error("miss")
				return
			}
			i++
		}
	})
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkStorePut(b *testing.B) {
	m := filledMemory(b)
	defer m.Close()
	val := benchValue()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	var ver uint64 = 1
	for i := 0; i < b.N; i++ {
		ver++
		ok := m.AcceptIfNewer(fmt.Sprintf("k%d", i%benchKeys), Entry{Value: val, Version: ver})
		if !ok {
			b.Fatal("reject")
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkStorePutParallel(b *testing.B) {
	m := filledMemory(b)
	defer m.Close()
	val := benchValue()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	var ver atomic.Uint64
	ver.Store(1)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v := ver.Add(1)
			_ = m.AcceptIfNewer(fmt.Sprintf("k%d", i%benchKeys), Entry{Value: val, Version: v})
			i++
		}
	})
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}
