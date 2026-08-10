// Package benchmetrics samples process CPU, heap, and GC via runtime/metrics.
// These are process-level deltas, not testing.B allocs/op.
package benchmetrics

import (
	"math"
	"runtime/metrics"
	"sort"
	"time"
)

// Snapshot is a point-in-time read of process runtime/metrics.
type Snapshot struct {
	CPUUserSeconds   float64
	CPUSystemSeconds float64
	HeapObjectsBytes uint64
	HeapAllocs       uint64
	HeapAllocBytes   uint64
	GCCycles         uint64
	// PauseHist is a copy of the cumulative /gc/pauses:seconds histogram.
	PauseBuckets []float64
	PauseCounts  []uint64
}

// Report is a Delta over a measure window. JSON names use the proc_ prefix
// so they are never confused with testing.B allocs/op / B/op.
type Report struct {
	CPUUserNsPerOp   float64 `json:"proc_cpu_user_ns_per_op"`
	CPUSystemNsPerOp float64 `json:"proc_cpu_system_ns_per_op"`
	AllocsPerOp      float64 `json:"proc_allocs_per_op"`
	BytesPerOp       float64 `json:"proc_bytes_per_op"`
	GCP50Ns          float64 `json:"proc_gc_p50_ns"`
	GCP99Ns          float64 `json:"proc_gc_p99_ns"`
	GCFrac           float64 `json:"proc_gc_frac"`
	GCCycles         uint64  `json:"proc_gc_cycles"`
	WallNs           int64   `json:"proc_wall_ns"`
	Ops              int64   `json:"proc_ops"`
}

var metricNames = []string{
	"/cpu/classes/user:cpu-seconds",
	"/cpu/classes/system:cpu-seconds",
	"/memory/classes/heap/objects:bytes",
	"/gc/heap/allocs:objects",
	"/gc/heap/allocs:bytes",
	"/gc/cycles/total:gc-cycles",
	"/gc/pauses:seconds",
}

// Read captures current runtime/metrics. Missing names are left zero.
func Read() Snapshot {
	samples := make([]metrics.Sample, len(metricNames))
	for i, n := range metricNames {
		samples[i].Name = n
	}
	metrics.Read(samples)

	var s Snapshot
	for _, sm := range samples {
		switch sm.Name {
		case "/cpu/classes/user:cpu-seconds":
			if sm.Value.Kind() == metrics.KindFloat64 {
				s.CPUUserSeconds = sm.Value.Float64()
			}
		case "/cpu/classes/system:cpu-seconds":
			if sm.Value.Kind() == metrics.KindFloat64 {
				s.CPUSystemSeconds = sm.Value.Float64()
			}
		case "/memory/classes/heap/objects:bytes":
			if sm.Value.Kind() == metrics.KindUint64 {
				s.HeapObjectsBytes = sm.Value.Uint64()
			}
		case "/gc/heap/allocs:objects":
			if sm.Value.Kind() == metrics.KindUint64 {
				s.HeapAllocs = sm.Value.Uint64()
			}
		case "/gc/heap/allocs:bytes":
			if sm.Value.Kind() == metrics.KindUint64 {
				s.HeapAllocBytes = sm.Value.Uint64()
			}
		case "/gc/cycles/total:gc-cycles":
			if sm.Value.Kind() == metrics.KindUint64 {
				s.GCCycles = sm.Value.Uint64()
			}
		case "/gc/pauses:seconds":
			if sm.Value.Kind() == metrics.KindFloat64Histogram {
				h := sm.Value.Float64Histogram()
				s.PauseBuckets = append([]float64(nil), h.Buckets...)
				s.PauseCounts = append([]uint64(nil), h.Counts...)
			}
		}
	}
	return s
}

// Delta subtracts before from after over wall and ops.
// Zero ops still fills GC percentiles and GCFrac; per-op fields stay 0.
func Delta(before, after Snapshot, wall time.Duration, ops int64) Report {
	r := Report{WallNs: wall.Nanoseconds(), Ops: ops}
	if after.CPUUserSeconds >= before.CPUUserSeconds {
		du := after.CPUUserSeconds - before.CPUUserSeconds
		if ops > 0 {
			r.CPUUserNsPerOp = du * 1e9 / float64(ops)
		}
	}
	if after.CPUSystemSeconds >= before.CPUSystemSeconds {
		ds := after.CPUSystemSeconds - before.CPUSystemSeconds
		if ops > 0 {
			r.CPUSystemNsPerOp = ds * 1e9 / float64(ops)
		}
	}
	if after.HeapAllocs >= before.HeapAllocs && ops > 0 {
		r.AllocsPerOp = float64(after.HeapAllocs-before.HeapAllocs) / float64(ops)
	}
	if after.HeapAllocBytes >= before.HeapAllocBytes && ops > 0 {
		r.BytesPerOp = float64(after.HeapAllocBytes-before.HeapAllocBytes) / float64(ops)
	}
	if after.GCCycles >= before.GCCycles {
		r.GCCycles = after.GCCycles - before.GCCycles
	}

	pauses := histDeltaSeconds(before, after)
	if len(pauses) == 0 {
		return r
	}
	r.GCP50Ns = percentileNs(pauses, 0.50)
	r.GCP99Ns = percentileNs(pauses, 0.99)
	var pauseTotal float64
	for _, s := range pauses {
		pauseTotal += s
	}
	if wall > 0 {
		r.GCFrac = pauseTotal / wall.Seconds()
	}
	return r
}

func histDeltaSeconds(before, after Snapshot) []float64 {
	if len(after.PauseCounts) == 0 || len(after.PauseBuckets) == 0 {
		return nil
	}
	n := len(after.PauseCounts)
	if len(before.PauseCounts) != n || len(before.PauseBuckets) != len(after.PauseBuckets) {
		before.PauseCounts = make([]uint64, n)
	}
	var out []float64
	for i := 0; i < n; i++ {
		var d uint64
		if after.PauseCounts[i] >= before.PauseCounts[i] {
			d = after.PauseCounts[i] - before.PauseCounts[i]
		}
		if d == 0 {
			continue
		}
		// Use bucket upper bound (Buckets[i+1]); last bucket is +Inf.
		sec := math.NaN()
		if i+1 < len(after.PauseBuckets) && !math.IsInf(after.PauseBuckets[i+1], 0) {
			sec = after.PauseBuckets[i+1]
		} else if i < len(after.PauseBuckets) && !math.IsInf(after.PauseBuckets[i], 0) {
			sec = after.PauseBuckets[i]
		}
		if math.IsNaN(sec) || sec < 0 {
			continue
		}
		for k := uint64(0); k < d; k++ {
			out = append(out, sec)
		}
	}
	return out
}

func percentileNs(secs []float64, p float64) float64 {
	if len(secs) == 0 {
		return 0
	}
	cp := append([]float64(nil), secs...)
	sort.Float64s(cp)
	idx := int(math.Ceil(p*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx] * 1e9
}
