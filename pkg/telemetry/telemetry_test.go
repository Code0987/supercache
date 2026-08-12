package telemetry

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestMetricsRecordAndSnapshot(t *testing.T) {
	// nil receiver safety
	var nilM *Metrics
	_ = nilM.Snapshot()
	nilM.RecordRingGenMismatch()
	nilM.RecordGet("ks", "hit")
	nilM.RecordPut("ks")
	nilM.RecordDelete("ks")
	nilM.RecordLoad("ks", nil)
	nilM.RecordUnavailable("ks")
	nilM.RecordOwnerFallback("ks")
	nilM.SetFanoutStats(1, 2)
	ctx, sp := nilM.StartSpan(context.Background(), "x")
	_ = ctx
	_ = sp

	m := New()
	m.RecordGet("ks", "hit")
	m.RecordGet("ks", "miss")
	m.RecordGet("ks", "negative")
	m.RecordGet("ks", "other") // default branch: only Gets
	m.RecordPut("ks")
	m.RecordDelete("ks")
	m.RecordLoad("ks", nil)
	m.RecordLoad("ks", errors.New("down"))
	m.RecordUnavailable("ks")
	m.RecordOwnerFallback("ks")
	m.RecordRingGenMismatch()
	m.SetFanoutStats(3, 4)

	s := m.Snapshot()
	if s.Gets < 4 || s.Hits != 1 || s.Misses < 2 || s.Negative != 1 {
		t.Fatalf("get stats: %+v", s)
	}
	if s.Puts != 1 || s.Deletes != 1 || s.Loads != 2 || s.LoadErrors != 1 {
		t.Fatalf("put/load: %+v", s)
	}
	if s.Unavailable != 1 || s.OwnerFallback != 1 || s.RingGenMismatch != 1 {
		t.Fatalf("misc: %+v", s)
	}
	if s.FanoutErrors != 3 || s.FanoutDropped != 4 {
		t.Fatalf("fanout: %+v", s)
	}

	ctx2, span := m.StartSpan(context.Background(), "engine.Get", attribute.String("k", "v"))
	span.End()
	_ = ctx2

	// add with nil counter is no-op (already exercised via otel counters)
	m.add(nil, 1)
}
