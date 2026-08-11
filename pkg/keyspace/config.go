package keyspace

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/protect"
)

// Mode selects miss behavior for a keyspace.
type Mode int

const (
	// ModeLoadThrough loads from DataSource on miss.
	ModeLoadThrough Mode = iota
	// ModeCacheOnly never calls DataSource; miss = not found.
	ModeCacheOnly
	// ModeBloom is a named Bloom filter (BloomAdd / BloomTest).
	ModeBloom
)

func (m Mode) String() string {
	switch m {
	case ModeLoadThrough:
		return "LoadThrough"
	case ModeCacheOnly:
		return "CacheOnly"
	case ModeBloom:
		return "Bloom"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// Default limits from PLAN §14.
const (
	DefaultMaxKeyLen    = 512
	DefaultMaxValueSize = 1 << 20 // 1 MiB
	DefaultMaxBatch     = 100
	// DefaultReplicationFactor is used when Config.ReplicationFactor is 0.
	DefaultReplicationFactor = 3
	// ReplicationAll stores each key on every peer (legacy full mesh).
	ReplicationAll = -1
	// DefaultTombstoneTTL is used when Config.TombstoneTTL is 0.
	DefaultTombstoneTTL = 5 * time.Minute
	// TombstoneTTLNever keeps delete markers until they are replaced
	// (Config.TombstoneTTL < 0).
	TombstoneTTLNever = time.Duration(-1)
	// DefaultBloomBits is m when Config.BloomBits is 0 (1 MiB bitset).
	DefaultBloomBits = 1 << 20
	// DefaultBloomHashes is k when Config.BloomHashes is 0.
	DefaultBloomHashes = 7
)

// EffectiveReplication returns how many peers should store each key given
// the current ring size.
func (c Config) EffectiveReplication(peerCount int) int {
	if peerCount <= 0 {
		return 1
	}
	rf := c.ReplicationFactor
	if rf < 0 {
		return peerCount
	}
	if rf == 0 {
		rf = DefaultReplicationFactor
	}
	if rf > peerCount {
		return peerCount
	}
	return rf
}

// EffectiveTombstoneTTL is how long a delete marker is retained.
// 0 → DefaultTombstoneTTL; negative → never expire (0 for the store).
func (c Config) EffectiveTombstoneTTL() time.Duration {
	if c.TombstoneTTL < 0 {
		return 0
	}
	if c.TombstoneTTL == 0 {
		return DefaultTombstoneTTL
	}
	return c.TombstoneTTL
}

// Config is a keyspace definition (local to a node).
type Config struct {
	Name            string
	Mode            Mode
	TTL             time.Duration
	NegativeTTL     time.Duration // 0 = disabled
	MaxBytes        int64
	LoadTimeout     time.Duration
	PeerTimeout     time.Duration
	WarmKeys        []string
	RefreshInterval time.Duration
	// RateLimitRPS 0 = no per-keyspace limit (global may still apply).
	RateLimitRPS float64
	// CircuitBreaker zero value disables breaker for this keyspace guard.
	CircuitBreaker protect.Config
	DataSource     datasource.DataSource

	// MaxKeyLen / MaxValueSize override engine defaults when > 0.
	MaxKeyLen    int
	MaxValueSize int

	// ReplicationFactor is how many ring members store each key (owner plus
	// clockwise successors). 0 means DefaultReplicationFactor (3). Negative
	// means every peer (legacy full-mesh). Always capped at cluster size.
	ReplicationFactor int

	// TombstoneTTL is how long a versioned delete marker is kept so a delayed
	// ApplyPut cannot resurrect the key. 0 means DefaultTombstoneTTL (5m).
	// Negative means never expire (TombstoneTTLNever).
	TombstoneTTL time.Duration

	// BloomBits / BloomHashes size a ModeBloom filter (0 → defaults).
	BloomBits   int
	BloomHashes int
}

// Validate checks config invariants.
func (c Config) Validate() error {
	if c.Name == "" {
		return errors.New("keyspace: name is required")
	}
	if c.Mode == ModeLoadThrough && c.DataSource == nil {
		return errors.New("keyspace: DataSource required for LoadThrough mode")
	}
	if c.MaxBytes < 0 {
		return errors.New("keyspace: MaxBytes must be >= 0")
	}
	if c.Mode == ModeBloom {
		bits := c.EffectiveBloomBits()
		if bits < 64 || c.EffectiveBloomHashes() < 1 {
			return errors.New("keyspace: BloomBits must be >= 64 and BloomHashes >= 1")
		}
		need := int64((bits + 7) / 8)
		if c.MaxBytes > 0 && need > c.MaxBytes {
			return fmt.Errorf("keyspace: Bloom bitset %d bytes exceeds MaxBytes %d", need, c.MaxBytes)
		}
	}
	return nil
}

// EffectiveBloomBits is m for ModeBloom.
func (c Config) EffectiveBloomBits() int {
	if c.BloomBits <= 0 {
		return DefaultBloomBits
	}
	return c.BloomBits
}

// EffectiveBloomHashes is k for ModeBloom.
func (c Config) EffectiveBloomHashes() int {
	if c.BloomHashes <= 0 {
		return DefaultBloomHashes
	}
	return c.BloomHashes
}

// ConfigHash is a stable hash of non-function config fields for drift detection.
func (c Config) ConfigHash() string {
	type wire struct {
		Name              string
		Mode              int
		TTL               time.Duration
		NegativeTTL       time.Duration
		MaxBytes          int64
		LoadTimeout       time.Duration
		PeerTimeout       time.Duration
		WarmKeys          []string
		RefreshInterval   time.Duration
		RateLimitRPS      float64
		BreakerRPS        float64
		BreakerBurst      int
		BreakerThreshold  int
		BreakerOpen       time.Duration
		MaxKeyLen         int
		MaxValueSize      int
		ReplicationFactor int
		TombstoneTTL      time.Duration
		BloomBits         int
		BloomHashes       int
	}
	b, _ := json.Marshal(wire{
		Name:              c.Name,
		Mode:              int(c.Mode),
		TTL:               c.TTL,
		NegativeTTL:       c.NegativeTTL,
		MaxBytes:          c.MaxBytes,
		LoadTimeout:       c.LoadTimeout,
		PeerTimeout:       c.PeerTimeout,
		WarmKeys:          c.WarmKeys,
		RefreshInterval:   c.RefreshInterval,
		RateLimitRPS:      c.RateLimitRPS,
		BreakerRPS:        c.CircuitBreaker.RateLimitRPS,
		BreakerBurst:      c.CircuitBreaker.Burst,
		BreakerThreshold:  c.CircuitBreaker.FailureThreshold,
		BreakerOpen:       c.CircuitBreaker.OpenTimeout,
		MaxKeyLen:         c.MaxKeyLen,
		MaxValueSize:      c.MaxValueSize,
		ReplicationFactor: c.ReplicationFactor,
		TombstoneTTL:      c.TombstoneTTL,
		BloomBits:         c.BloomBits,
		BloomHashes:       c.BloomHashes,
	})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}
