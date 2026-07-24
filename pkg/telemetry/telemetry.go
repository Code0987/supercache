package telemetry

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/Code0987/supercache"

// Metrics holds process-local counters and optional OTel instruments.
type Metrics struct {
	Gets       atomic.Uint64
	Hits       atomic.Uint64
	Misses     atomic.Uint64
	Negative   atomic.Uint64
	Puts       atomic.Uint64
	Deletes    atomic.Uint64
	Loads      atomic.Uint64
	LoadErrors atomic.Uint64
	Unavailable    atomic.Uint64
	OwnerFallback  atomic.Uint64
	FanoutErrors  atomic.Uint64
	FanoutDropped atomic.Uint64
	// RingGenMismatch counts ApplyPut/ApplyDelete whose wire ring_generation
	// differs from the local ring generation (diagnostic; LWW still applies).
	RingGenMismatch atomic.Uint64

	tracer trace.Tracer

	// otel counters (nil-safe if registration fails)
	otelGets   metric.Int64Counter
	otelHits   metric.Int64Counter
	otelMisses metric.Int64Counter
	otelPuts   metric.Int64Counter
	otelLoads  metric.Int64Counter
	otelLoadE  metric.Int64Counter
}

// New creates Metrics wired to the global MeterProvider / TracerProvider.
func New() *Metrics {
	m := &Metrics{
		tracer: otel.Tracer(instrumentationName),
	}
	meter := otel.Meter(instrumentationName)
	m.otelGets, _ = meter.Int64Counter("supercache.gets")
	m.otelHits, _ = meter.Int64Counter("supercache.hits")
	m.otelMisses, _ = meter.Int64Counter("supercache.misses")
	m.otelPuts, _ = meter.Int64Counter("supercache.puts")
	m.otelLoads, _ = meter.Int64Counter("supercache.loads")
	m.otelLoadE, _ = meter.Int64Counter("supercache.load_errors")
	return m
}

// Snapshot is a point-in-time copy of counters.
type Snapshot struct {
	Gets        uint64 `json:"gets"`
	Hits        uint64 `json:"hits"`
	Misses      uint64 `json:"misses"`
	Negative    uint64 `json:"negative_hits"`
	Puts        uint64 `json:"puts"`
	Deletes     uint64 `json:"deletes"`
	Loads       uint64 `json:"loads"`
	LoadErrors  uint64 `json:"load_errors"`
	Unavailable    uint64 `json:"unavailable"`
	OwnerFallback  uint64 `json:"owner_fallback_local"`
	FanoutErrors    uint64 `json:"put_fanout_errors"`
	FanoutDropped   uint64 `json:"fanout_dropped"`
	RingGenMismatch uint64 `json:"ring_gen_mismatch"`
}

// Snapshot returns current counters.
func (m *Metrics) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{
		Gets:        m.Gets.Load(),
		Hits:        m.Hits.Load(),
		Misses:      m.Misses.Load(),
		Negative:    m.Negative.Load(),
		Puts:        m.Puts.Load(),
		Deletes:     m.Deletes.Load(),
		Loads:       m.Loads.Load(),
		LoadErrors:  m.LoadErrors.Load(),
		Unavailable:    m.Unavailable.Load(),
		OwnerFallback:  m.OwnerFallback.Load(),
		FanoutErrors:    m.FanoutErrors.Load(),
		FanoutDropped:   m.FanoutDropped.Load(),
		RingGenMismatch: m.RingGenMismatch.Load(),
	}
}

// RecordRingGenMismatch increments the apply ring-generation mismatch counter.
func (m *Metrics) RecordRingGenMismatch() {
	if m == nil {
		return
	}
	m.RingGenMismatch.Add(1)
}

func (m *Metrics) add(c metric.Int64Counter, n int64, attrs ...attribute.KeyValue) {
	if m == nil || c == nil {
		return
	}
	c.Add(context.Background(), n, metric.WithAttributes(attrs...))
}

// RecordGet records a Get outcome: hit | miss | negative.
func (m *Metrics) RecordGet(keyspace, outcome string) {
	if m == nil {
		return
	}
	m.Gets.Add(1)
	m.add(m.otelGets, 1, attribute.String("keyspace", keyspace), attribute.String("outcome", outcome))
	switch outcome {
	case "hit":
		m.Hits.Add(1)
		m.add(m.otelHits, 1, attribute.String("keyspace", keyspace))
	case "miss":
		m.Misses.Add(1)
		m.add(m.otelMisses, 1, attribute.String("keyspace", keyspace))
	case "negative":
		m.Negative.Add(1)
		m.Misses.Add(1)
		m.add(m.otelMisses, 1, attribute.String("keyspace", keyspace), attribute.String("kind", "negative"))
	}
}

// RecordPut increments put counters.
func (m *Metrics) RecordPut(keyspace string) {
	if m == nil {
		return
	}
	m.Puts.Add(1)
	m.add(m.otelPuts, 1, attribute.String("keyspace", keyspace))
}

// RecordDelete increments delete counters.
func (m *Metrics) RecordDelete(keyspace string) {
	if m == nil {
		return
	}
	m.Deletes.Add(1)
}

// RecordLoad records a DataSource load attempt.
func (m *Metrics) RecordLoad(keyspace string, err error) {
	if m == nil {
		return
	}
	m.Loads.Add(1)
	m.add(m.otelLoads, 1, attribute.String("keyspace", keyspace))
	if err != nil {
		m.LoadErrors.Add(1)
		m.add(m.otelLoadE, 1, attribute.String("keyspace", keyspace))
	}
}

// RecordUnavailable increments unavailable (rate limit / breaker) counters.
func (m *Metrics) RecordUnavailable(keyspace string) {
	if m == nil {
		return
	}
	m.Unavailable.Add(1)
}

// RecordOwnerFallback increments owner-down local fill counter.
func (m *Metrics) RecordOwnerFallback(keyspace string) {
	if m == nil {
		return
	}
	m.OwnerFallback.Add(1)
}

// StartSpan starts an engine span (no-op if no TracerProvider configured).
func (m *Metrics) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if m == nil || m.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return m.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// SetFanoutStats updates fan-out counters from the transport (process snapshot).
func (m *Metrics) SetFanoutStats(errors, dropped uint64) {
	if m == nil {
		return
	}
	m.FanoutErrors.Store(errors)
	m.FanoutDropped.Store(dropped)
}
