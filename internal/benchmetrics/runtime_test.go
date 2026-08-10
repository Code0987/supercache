package benchmetrics

import (
	"runtime"
	"testing"
	"time"
)

func TestReadHasCPUOrGCKind(t *testing.T) {
	s := Read()
	// At least one of the cumulative counters should be readable on Go 1.22.
	if s.CPUUserSeconds == 0 && s.HeapAllocs == 0 && s.GCCycles == 0 {
		t.Fatalf("Read returned all-zero snapshot; metrics names may be wrong for %s", runtime.Version())
	}
}

func TestDeltaMonotonicAllocs(t *testing.T) {
	before := Read()
	sink := make([][]byte, 0, 256)
	for i := 0; i < 256; i++ {
		sink = append(sink, make([]byte, 1024))
	}
	runtime.KeepAlive(sink)
	after := Read()
	r := Delta(before, after, 10*time.Millisecond, 256)
	if after.HeapAllocs < before.HeapAllocs {
		t.Fatalf("allocs went backwards: %d -> %d", before.HeapAllocs, after.HeapAllocs)
	}
	if r.Ops != 256 || r.WallNs != (10 * time.Millisecond).Nanoseconds() {
		t.Fatalf("ops/wall: %+v", r)
	}
	if r.AllocsPerOp < 0 || r.BytesPerOp < 0 {
		t.Fatalf("negative per-op: %+v", r)
	}
}

func TestDeltaZeroOpsNoDivZero(t *testing.T) {
	a := Read()
	r := Delta(a, a, time.Second, 0)
	if r.CPUUserNsPerOp != 0 || r.AllocsPerOp != 0 || r.BytesPerOp != 0 {
		t.Fatalf("zero ops should leave per-op at 0: %+v", r)
	}
}

func TestPercentileNs(t *testing.T) {
	secs := []float64{1e-6, 2e-6, 3e-6, 4e-6}
	p50 := percentileNs(secs, 0.50)
	if p50 < 1e3 || p50 > 4e3 {
		t.Fatalf("p50 ns=%v", p50)
	}
	if percentileNs(nil, 0.99) != 0 {
		t.Fatal("empty")
	}
}
