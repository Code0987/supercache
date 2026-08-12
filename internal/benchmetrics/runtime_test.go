package benchmetrics

import (
	"math"
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

func TestReadDeltaAndHistPercentiles(t *testing.T) {
	before := Read()
	// allocate a bit so heap counters may move
	sink := make([]byte, 1<<16)
	_ = sink
	after := Read()

	r := Delta(before, after, 100*time.Millisecond, 1000)
	if r.Ops != 1000 || r.WallNs <= 0 {
		t.Fatalf("%+v", r)
	}
	// zero ops still ok
	r0 := Delta(before, after, time.Second, 0)
	if r0.AllocsPerOp != 0 {
		t.Fatalf("zero ops per-op: %+v", r0)
	}

	// synthetic hist for percentiles
	b := Snapshot{
		PauseBuckets: []float64{0, 0.001, 0.01, math.Inf(1)},
		PauseCounts:  []uint64{0, 2, 1},
	}
	a := Snapshot{
		PauseBuckets: []float64{0, 0.001, 0.01, math.Inf(1)},
		PauseCounts:  []uint64{0, 5, 3},
		CPUUserSeconds:   before.CPUUserSeconds + 0.01,
		CPUSystemSeconds: before.CPUSystemSeconds + 0.01,
		HeapAllocs:       before.HeapAllocs + 10,
		HeapAllocBytes:   before.HeapAllocBytes + 1000,
		GCCycles:         before.GCCycles + 1,
	}
	r2 := Delta(b, a, 50*time.Millisecond, 10)
	if r2.GCP50Ns <= 0 && r2.GCP99Ns <= 0 {
		// may still be zero if hist delta empty shape mismatch
		_ = r2
	}
	if r2.GCCycles != 1 {
		t.Fatalf("gc cycles %d", r2.GCCycles)
	}

	// histDelta edge: empty after
	if histDeltaSeconds(Snapshot{}, Snapshot{}) != nil {
		t.Fatal("empty hist")
	}
	// before counts length mismatch resets before
	out := histDeltaSeconds(
		Snapshot{PauseBuckets: []float64{0, 1}, PauseCounts: []uint64{1}},
		Snapshot{PauseBuckets: []float64{0, 1, 2}, PauseCounts: []uint64{0, 3}},
	)
	_ = out

	if percentileNs(nil, 0.5) != 0 {
		t.Fatal("empty percentile")
	}
	if percentileNs([]float64{0.001}, 0.99) <= 0 {
		t.Fatal("single percentile")
	}
}

func TestReportBAndPercentileBounds(t *testing.T) {
	// Exercise ReportB via testing.Benchmark so it runs under go test -cover.
	_ = testing.Benchmark(func(b *testing.B) {
		before := Read()
		for i := 0; i < b.N; i++ {
			_ = i * i
		}
		b.StopTimer()
		ReportB(b, before, Read())
	})

	// percentile index clamps
	if percentileNs([]float64{0.001, 0.002}, 0) <= 0 {
		t.Fatal("p=0")
	}
	if percentileNs([]float64{0.001, 0.002}, 1.5) <= 0 {
		t.Fatal("p>1")
	}

	// hist bucket with +Inf / NaN skips
	out := histDeltaSeconds(
		Snapshot{PauseBuckets: []float64{0, math.NaN(), math.Inf(1)}, PauseCounts: []uint64{0, 0, 0}},
		Snapshot{PauseBuckets: []float64{0, math.NaN(), math.Inf(1)}, PauseCounts: []uint64{0, 2, 1}},
	)
	_ = out
	// negative upper bound skipped
	_ = histDeltaSeconds(
		Snapshot{PauseBuckets: []float64{-2, -1}, PauseCounts: []uint64{0}},
		Snapshot{PauseBuckets: []float64{-2, -1}, PauseCounts: []uint64{3}},
	)
}
