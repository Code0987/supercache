package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/telemetry"
)

func TestMetricsCounters(t *testing.T) {
	m := telemetry.New()
	e := engine.New(engine.WithMetrics(m))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name:       "lt",
		Mode:       keyspace.ModeLoadThrough,
		MaxBytes:   1 << 20,
		TTL:        time.Minute,
		DataSource: datasource.Map{"a": []byte("1")},
	})
	ctx := context.Background()
	_, _ = e.Get(ctx, "lt", "a") // miss+load
	_, _ = e.Get(ctx, "lt", "a") // hit
	_ = e.Put(ctx, "lt", "b", []byte("2"))
	_ = e.Delete(ctx, "lt", "b")

	snap := e.Metrics()
	if snap.Hits < 1 || snap.Misses < 1 || snap.Puts < 1 || snap.Deletes < 1 || snap.Loads < 1 {
		t.Fatalf("unexpected metrics: %+v", snap)
	}
	snaps := e.KeySpaceSnapshots()
	if len(snaps) != 1 || snaps[0].Name != "lt" {
		t.Fatalf("%+v", snaps)
	}
	if !e.Ready() {
		t.Fatal("should be ready")
	}
	e.Close()
	if e.Ready() {
		t.Fatal("closed not ready")
	}
}
