package benchmetrics

import "testing"

// ReportB publishes process-level CPU/GC metrics on a testing.B.
// Call after StopTimer. Uses b.Elapsed() and b.N.
// Zero GC pauses in a short window (e.g. -benchtime=200ms) report as 0.
func ReportB(b *testing.B, before, after Snapshot) {
	b.Helper()
	r := Delta(before, after, b.Elapsed(), int64(b.N))
	b.ReportMetric(r.CPUUserNsPerOp, "cpu-ns/op")
	b.ReportMetric(r.GCP50Ns, "gc-p50-ns")
	b.ReportMetric(r.GCP99Ns, "gc-p99-ns")
	b.ReportMetric(r.GCFrac, "gc-frac")
}
