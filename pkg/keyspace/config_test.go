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

func TestModeString(t *testing.T) {
	if ModeLoadThrough.String() != "LoadThrough" {
		t.Fatal(ModeLoadThrough.String())
	}
	if ModeCacheOnly.String() != "CacheOnly" {
		t.Fatal(ModeCacheOnly.String())
	}
	if ModeBloom.String() != "Bloom" {
		t.Fatal(ModeBloom.String())
	}
	if ModeSet.String() != "Set" {
		t.Fatal(ModeSet.String())
	}
	if ModeZSet.String() != "ZSet" {
		t.Fatal(ModeZSet.String())
	}
	if ModeGeo.String() != "Geo" {
		t.Fatal(ModeGeo.String())
	}
	if ModeList.String() != "List" {
		t.Fatal(ModeList.String())
	}
	if Mode(99).String() != "Mode(99)" {
		t.Fatal(Mode(99).String())
	}
}

func TestValidate(t *testing.T) {
	if err := (Config{}).Validate(); err == nil {
		t.Fatal("empty name")
	}
	if err := (Config{Name: "lt", Mode: ModeLoadThrough}).Validate(); err == nil {
		t.Fatal("LoadThrough needs DataSource")
	}
	if err := (Config{Name: "c", Mode: ModeCacheOnly, MaxBytes: -1}).Validate(); err == nil {
		t.Fatal("negative MaxBytes")
	}
	// Bloom: bits too small via explicit BloomBits
	if err := (Config{Name: "bf", Mode: ModeBloom, BloomBits: 32, BloomHashes: 3}).Validate(); err == nil {
		t.Fatal("BloomBits < 64")
	}
	// BloomHashes via EffectiveBloomHashes: 0 is OK (defaults to 7); force invalid with Bits ok but hashes path
	// BloomBits >= 64 but BloomHashes forced: set BloomHashes -1 → Effective returns default 7 → ok
	// Exceed MaxBytes: bitset larger than MaxBytes
	if err := (Config{
		Name: "bf", Mode: ModeBloom, BloomBits: 1024, BloomHashes: 3, MaxBytes: 10,
	}).Validate(); err == nil {
		t.Fatal("bitset exceeds MaxBytes")
	}
	// Happy paths
	if err := (Config{Name: "c", Mode: ModeCacheOnly, MaxBytes: 1 << 20}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Config{
		Name: "bf", Mode: ModeBloom, BloomBits: 1024, BloomHashes: 3, MaxBytes: 1 << 20,
	}).Validate(); err != nil {
		t.Fatal(err)
	}
	// MaxBytes 0 allows bloom (no size check when MaxBytes==0)
	if err := (Config{Name: "bf", Mode: ModeBloom, BloomBits: 1024, BloomHashes: 1}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEffectiveBloomDefaults(t *testing.T) {
	c := Config{}
	if c.EffectiveBloomBits() != DefaultBloomBits {
		t.Fatalf("bits default %d", c.EffectiveBloomBits())
	}
	if c.EffectiveBloomHashes() != DefaultBloomHashes {
		t.Fatalf("hashes default %d", c.EffectiveBloomHashes())
	}
	c.BloomBits = 256
	c.BloomHashes = 4
	if c.EffectiveBloomBits() != 256 || c.EffectiveBloomHashes() != 4 {
		t.Fatal("explicit bloom params")
	}
}

func TestConfigHashIncludesBloom(t *testing.T) {
	a := Config{Name: "k", Mode: ModeBloom, MaxBytes: 1 << 20, BloomBits: 1024, BloomHashes: 3}
	b := a
	b.BloomBits = 2048
	if a.ConfigHash() == b.ConfigHash() {
		t.Fatal("BloomBits must change hash")
	}
}
