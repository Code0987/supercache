package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
)

type runRecord struct {
	Backend     string        `json:"backend"`
	Addr        string        `json:"addr"`
	Op          string        `json:"op"`
	Keys        int           `json:"keys"`
	ValueBytes  int           `json:"value_bytes"`
	Concurrency int           `json:"concurrency"`
	Dist        string        `json:"dist"`
	Trials      []trialResult `json:"trials"`
	// Aggregates across trials (median of each metric)
	MedianOpsPerSec float64              `json:"median_ops_per_sec"`
	MedianP50       time.Duration        `json:"median_p50_ns"`
	MedianP95       time.Duration        `json:"median_p95_ns"`
	MedianP99       time.Duration        `json:"median_p99_ns"`
	MedianP999      time.Duration        `json:"median_p999_ns"`
	MinOpsPerSec    float64              `json:"min_ops_per_sec"`
	MaxOpsPerSec    float64              `json:"max_ops_per_sec"`
	MedianProc      *benchmetrics.Report `json:"median_proc,omitempty"`
	Nodes           int                  `json:"nodes,omitempty"`
	Conns           int                  `json:"conns,omitempty"`
	Sticky          bool                 `json:"sticky,omitempty"`
	Path            string               `json:"path,omitempty"`
	Embed           bool                 `json:"embed,omitempty"`
	FanoutErrors    uint64               `json:"fanout_errors,omitempty"`
	FanoutDropped   uint64               `json:"fanout_dropped,omitempty"`
	HintsFlushed    uint64               `json:"hints_flushed,omitempty"`
	HintsDropped    uint64               `json:"hints_dropped,omitempty"`
}

type suiteReport struct {
	SchemaVersion int         `json:"schema_version"`
	GeneratedAt   string      `json:"generated_at"`
	GitSHA        string      `json:"git_sha,omitempty"`
	GOMAXPROCS    int         `json:"gomaxprocs"`
	GoVersion     string      `json:"go_version"`
	NumCPU        int         `json:"num_cpu"`
	Note          string      `json:"note"`
	Runs          []runRecord `json:"runs"`
}

func latencyStats(ds []time.Duration) (p50, p95, p99, p999, mean time.Duration) {
	if len(ds) == 0 {
		return 0, 0, 0, 0, 0
	}
	cp := append([]time.Duration(nil), ds...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	pct := func(p float64) time.Duration {
		if len(cp) == 1 {
			return cp[0]
		}
		idx := int(math.Ceil(p*float64(len(cp)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		return cp[idx]
	}
	var sum time.Duration
	for _, d := range cp {
		sum += d
	}
	mean = sum / time.Duration(len(cp))
	return pct(0.50), pct(0.95), pct(0.99), pct(0.999), mean
}

type trialAgg struct {
	medOps, minOps, maxOps          float64
	medP50, medP95, medP99, medP999 time.Duration
	medProc                         *benchmetrics.Report
}

func aggregateTrials(trials []trialResult) trialAgg {
	var out trialAgg
	if len(trials) == 0 {
		return out
	}
	ops := make([]float64, len(trials))
	p50s := make([]time.Duration, len(trials))
	p95s := make([]time.Duration, len(trials))
	p99s := make([]time.Duration, len(trials))
	p999s := make([]time.Duration, len(trials))
	var procs []benchmetrics.Report
	for i, t := range trials {
		ops[i] = t.OpsPerSec
		p50s[i] = t.P50
		p95s[i] = t.P95
		p99s[i] = t.P99
		p999s[i] = t.P999
		if t.Proc != nil {
			procs = append(procs, *t.Proc)
		}
	}
	sort.Float64s(ops)
	sort.Slice(p50s, func(i, j int) bool { return p50s[i] < p50s[j] })
	sort.Slice(p95s, func(i, j int) bool { return p95s[i] < p95s[j] })
	sort.Slice(p99s, func(i, j int) bool { return p99s[i] < p99s[j] })
	sort.Slice(p999s, func(i, j int) bool { return p999s[i] < p999s[j] })
	med := func(xs []float64) float64 {
		n := len(xs)
		if n%2 == 1 {
			return xs[n/2]
		}
		return (xs[n/2-1] + xs[n/2]) / 2
	}
	medD := func(xs []time.Duration) time.Duration {
		n := len(xs)
		if n%2 == 1 {
			return xs[n/2]
		}
		return (xs[n/2-1] + xs[n/2]) / 2
	}
	out.medOps, out.minOps, out.maxOps = med(ops), ops[0], ops[len(ops)-1]
	out.medP50, out.medP95, out.medP99, out.medP999 = medD(p50s), medD(p95s), medD(p99s), medD(p999s)
	if len(procs) == len(trials) && len(procs) > 0 {
		out.medProc = medianProc(procs)
	}
	return out
}

func medianProc(ps []benchmetrics.Report) *benchmetrics.Report {
	pick := func(get func(benchmetrics.Report) float64) float64 {
		xs := make([]float64, len(ps))
		for i, p := range ps {
			xs[i] = get(p)
		}
		sort.Float64s(xs)
		n := len(xs)
		if n%2 == 1 {
			return xs[n/2]
		}
		return (xs[n/2-1] + xs[n/2]) / 2
	}
	cycles := make([]uint64, len(ps))
	for i, p := range ps {
		cycles[i] = p.GCCycles
	}
	sort.Slice(cycles, func(i, j int) bool { return cycles[i] < cycles[j] })
	mid := cycles[len(cycles)/2]
	r := benchmetrics.Report{
		CPUUserNsPerOp:   pick(func(p benchmetrics.Report) float64 { return p.CPUUserNsPerOp }),
		CPUSystemNsPerOp: pick(func(p benchmetrics.Report) float64 { return p.CPUSystemNsPerOp }),
		AllocsPerOp:      pick(func(p benchmetrics.Report) float64 { return p.AllocsPerOp }),
		BytesPerOp:       pick(func(p benchmetrics.Report) float64 { return p.BytesPerOp }),
		GCP50Ns:          pick(func(p benchmetrics.Report) float64 { return p.GCP50Ns }),
		GCP99Ns:          pick(func(p benchmetrics.Report) float64 { return p.GCP99Ns }),
		GCFrac:           pick(func(p benchmetrics.Report) float64 { return p.GCFrac }),
		GCCycles:         mid,
		WallNs:           int64(pick(func(p benchmetrics.Report) float64 { return float64(p.WallNs) })),
		Ops:              int64(pick(func(p benchmetrics.Report) float64 { return float64(p.Ops) })),
	}
	return &r
}

func printTrial(backend, op string, trial, nTrials int, r trialResult) {
	fmt.Printf("  trial %d/%d  ops/s=%.0f  err=%d  p50=%s p95=%s p99=%s p999=%s mean=%s samples=%d\n",
		trial, nTrials, r.OpsPerSec, r.Errors,
		r.P50.Round(time.Microsecond), r.P95.Round(time.Microsecond),
		r.P99.Round(time.Microsecond), r.P999.Round(time.Microsecond),
		r.Mean.Round(time.Microsecond), r.Samples)
	_ = backend
	_ = op
}

func printRunSummary(rec runRecord) {
	fmt.Println()
	fmt.Printf("SUMMARY backend=%-10s op=%-5s trials=%d  median_ops/s=%.0f  (min=%.0f max=%.0f)\n",
		rec.Backend, rec.Op, len(rec.Trials), rec.MedianOpsPerSec, rec.MinOpsPerSec, rec.MaxOpsPerSec)
	fmt.Printf("         latency median-of-trials  p50=%s  p95=%s  p99=%s  p999=%s\n",
		rec.MedianP50.Round(time.Microsecond),
		rec.MedianP95.Round(time.Microsecond),
		rec.MedianP99.Round(time.Microsecond),
		rec.MedianP999.Round(time.Microsecond))
	if rec.MedianProc != nil {
		p := rec.MedianProc
		fmt.Printf("         proc  cpu_user_ns/op=%.0f  proc_allocs/op=%.2f  proc_B/op=%.0f  gc_p50=%s  gc_p99=%s  gc_frac=%.4f\n",
			p.CPUUserNsPerOp, p.AllocsPerOp, p.BytesPerOp,
			time.Duration(p.GCP50Ns).Round(time.Microsecond),
			time.Duration(p.GCP99Ns).Round(time.Microsecond),
			p.GCFrac)
	}
	if rec.Embed {
		fmt.Printf("         embed nodes=%d path=%s sticky=%v fanout_err=%d fanout_drop=%d hints_drop=%d\n",
			rec.Nodes, rec.Path, rec.Sticky, rec.FanoutErrors, rec.FanoutDropped, rec.HintsDropped)
	}
	fmt.Println()
}

func printCompareTable(runs []runRecord) {
	// Group by op+params key
	fmt.Println("══════════════════════════════════════════════════════════════")
	fmt.Println(" COMPARISON (median of trials)")
	fmt.Println("══════════════════════════════════════════════════════════════")
	fmt.Printf("%-10s %-6s %10s %10s %10s %10s %10s\n", "backend", "op", "ops/s", "p50", "p95", "p99", "p999")
	fmt.Println("──────────────────────────────────────────────────────────────")
	for _, r := range runs {
		fmt.Printf("%-10s %-6s %10.0f %10s %10s %10s %10s\n",
			r.Backend, r.Op, r.MedianOpsPerSec,
			r.MedianP50.Round(time.Microsecond),
			r.MedianP95.Round(time.Microsecond),
			r.MedianP99.Round(time.Microsecond),
			r.MedianP999.Round(time.Microsecond))
	}
	fmt.Println("══════════════════════════════════════════════════════════════")
	// Ratio when both present for same op
	byOp := map[string][]runRecord{}
	for _, r := range runs {
		byOp[r.Op] = append(byOp[r.Op], r)
	}
	for op, list := range byOp {
		var sc, rd *runRecord
		for i := range list {
			switch list[i].Backend {
			case "supercache":
				sc = &list[i]
			case "redis":
				rd = &list[i]
			}
		}
		if sc != nil && rd != nil && rd.MedianOpsPerSec > 0 {
			ratio := sc.MedianOpsPerSec / rd.MedianOpsPerSec
			fmt.Printf("op=%s  supercache/redis throughput ratio = %.2fx  ( >1 means SuperCache higher ops/s )\n", op, ratio)
		}
	}
	fmt.Println()
}

func writeJSON(path string, report suiteReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func envNote() suiteReport {
	return suiteReport{
		SchemaVersion: 2,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		GitSHA:        gitSHA(),
		GOMAXPROCS:    runtime.GOMAXPROCS(0),
		GoVersion:     runtime.Version(),
		NumCPU:        runtime.NumCPU(),
		Note:          "Single-node localhost comparison. Prefer native redis-server over Docker. Median of trials is the headline number. proc_* fields are process-level, not testing.B.",
	}
}

func gitSHA() string {
	if s := os.Getenv("GITHUB_SHA"); s != "" {
		return s
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, st := range bi.Settings {
			if st.Key == "vcs.revision" {
				return st.Value
			}
		}
	}
	return ""
}
