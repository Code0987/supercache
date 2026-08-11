package keyspace

import (
	"testing"
	"time"
)

func TestEffectiveReplication(t *testing.T) {
	cases := []struct {
		rf, n, want int
	}{
		{0, 10, DefaultReplicationFactor},
		{0, 2, 2},
		{1, 10, 1},
		{3, 10, 3},
		{99, 4, 4},
		{ReplicationAll, 7, 7},
		{-5, 3, 3},
		{3, 0, 1},
	}
	for _, tc := range cases {
		got := Config{ReplicationFactor: tc.rf}.EffectiveReplication(tc.n)
		if got != tc.want {
			t.Fatalf("rf=%d n=%d: got %d want %d", tc.rf, tc.n, got, tc.want)
		}
	}
}

func TestEffectiveTombstoneTTL(t *testing.T) {
	if got := (Config{}).EffectiveTombstoneTTL(); got != DefaultTombstoneTTL {
		t.Fatalf("zero: got %v want %v", got, DefaultTombstoneTTL)
	}
	if got := (Config{TombstoneTTL: time.Second}).EffectiveTombstoneTTL(); got != time.Second {
		t.Fatalf("explicit: got %v", got)
	}
	if got := (Config{TombstoneTTL: TombstoneTTLNever}).EffectiveTombstoneTTL(); got != 0 {
		t.Fatalf("never: got %v want 0", got)
	}
}

func TestConfigHashIncludesTombstoneTTL(t *testing.T) {
	a := Config{Name: "k", Mode: ModeCacheOnly, MaxBytes: 1}
	b := a
	b.TombstoneTTL = time.Second
	if a.ConfigHash() == b.ConfigHash() {
		t.Fatal("TombstoneTTL must change config hash")
	}
}

func TestConfigHashIncludesReplication(t *testing.T) {
	a := Config{Name: "k", Mode: ModeCacheOnly, MaxBytes: 1}
	b := a
	b.ReplicationFactor = 1
	if a.ConfigHash() == b.ConfigHash() {
		t.Fatal("ReplicationFactor must change config hash")
	}
}
