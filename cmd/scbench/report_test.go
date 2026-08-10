package main

import (
	"testing"
	"time"
)

func TestAggregateTrialsIncludesP999(t *testing.T) {
	trials := []trialResult{
		{OpsPerSec: 100, P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 3 * time.Millisecond, P999: 10 * time.Millisecond},
		{OpsPerSec: 200, P50: 2 * time.Millisecond, P95: 3 * time.Millisecond, P99: 4 * time.Millisecond, P999: 20 * time.Millisecond},
		{OpsPerSec: 300, P50: 3 * time.Millisecond, P95: 4 * time.Millisecond, P99: 5 * time.Millisecond, P999: 30 * time.Millisecond},
	}
	agg := aggregateTrials(trials)
	if agg.medOps != 200 {
		t.Fatalf("med ops %v", agg.medOps)
	}
	if agg.medP999 != 20*time.Millisecond {
		t.Fatalf("med p999 %v", agg.medP999)
	}
	if agg.minOps != 100 || agg.maxOps != 300 {
		t.Fatalf("min/max %v %v", agg.minOps, agg.maxOps)
	}
}
