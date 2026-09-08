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
	"github.com/Code0987/supercache/pkg/topkx"
	"github.com/Code0987/supercache/pkg/vecset"
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
	// ModeSet is a named exact set (SetAdd / SetRemove / SetContains / …).
	ModeSet
	// ModeZSet is a named sorted set (ZAdd / ZRem / ZScore / ZRange / …).
	ModeZSet
	// ModeGeo is a named geospatial point index (GeoAdd / GeoPos / GeoRadius / …).
	ModeGeo
	// ModeList is a named ordered list (LPush / LPop / LRange / …).
	ModeList
	// ModeHash is a named field map (HSet / HGet / HDel / …).
	ModeHash
	// ModeCounter is a named int64 counter (Incr / CounterGet).
	ModeCounter
	// ModeJSON is a named nested JSON document (JsonSet / JsonGet / JsonDel).
	ModeJSON
	// ModeBitmap is a named packed bit vector (BitSet / BitGet / BitCount / BitPos).
	ModeBitmap
	// ModeHLL is a named HyperLogLog sketch (HLLAdd / HLLCount).
	ModeHLL
	// ModeTopK is a named Space-Saving heavy-hitter table (TopKAdd / TopKList).
	ModeTopK
	// ModeCMS is a named Count-Min Sketch (CMSIncr / CMSQuery).
	ModeCMS
	// ModeVectorSet is a named embedding set (VAdd / VSim / VRem / …).
	ModeVectorSet
	// ModeStream is a named append-only log (XAdd / XRange / …).
	ModeStream
)

func (m Mode) String() string {
	switch m {
	case ModeLoadThrough:
		return "LoadThrough"
	case ModeCacheOnly:
		return "CacheOnly"
	case ModeBloom:
		return "Bloom"
	case ModeSet:
		return "Set"
	case ModeZSet:
		return "ZSet"
	case ModeGeo:
		return "Geo"
	case ModeList:
		return "List"
	case ModeHash:
		return "Hash"
	case ModeCounter:
		return "Counter"
	case ModeJSON:
		return "JSON"
	case ModeBitmap:
		return "Bitmap"
	case ModeHLL:
		return "HLL"
	case ModeTopK:
		return "TopK"
	case ModeCMS:
		return "CMS"
	case ModeVectorSet:
		return "VectorSet"
	case ModeStream:
		return "Stream"
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
	// DefaultTopKSize is K when Config.TopKSize is 0 (billboard-sized).
	DefaultTopKSize = 100
	// StreamHardCap is the max entries per name even when StreamMaxLen is 0.
	StreamHardCap = 4096
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

	// TopKSize is K for ModeTopK (0 → DefaultTopKSize). Per-name RESERVE is not v1.
	TopKSize int

	// VectorDim is the locked dim for ModeVectorSet (0 = first VAdd locks). Range 2..256.
	VectorDim int
	// VectorMetric is cosine (0), L2, or IP for ModeVectorSet.
	VectorMetric VectorMetric

	// StreamMaxLen auto-trims oldest entries after XAdd (0 = no auto-trim).
	// Capped at StreamHardCap (4096).
	StreamMaxLen int
}

// VectorMetric is the keyspace K-NN formula.
type VectorMetric int

const (
	VectorMetricCosine VectorMetric = iota
	VectorMetricL2
	VectorMetricIP
)

func (m VectorMetric) String() string {
	switch m {
	case VectorMetricL2:
		return "l2"
	case VectorMetricIP:
		return "ip"
	default:
		return "cosine"
	}
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
	if c.Mode == ModeHLL && c.MaxValueSize > 0 && c.MaxValueSize < 12288 {
		return fmt.Errorf("keyspace: ModeHLL MaxValueSize %d < 12288", c.MaxValueSize)
	}
	if c.Mode == ModeCMS && c.MaxValueSize > 0 && c.MaxValueSize < 65536 {
		return fmt.Errorf("keyspace: ModeCMS MaxValueSize %d < 65536", c.MaxValueSize)
	}
	if c.Mode == ModeTopK {
		if c.TopKSize < 0 {
			return errors.New("keyspace: TopKSize must be >= 0")
		}
		maxItem := DefaultMaxKeyLen
		if c.MaxKeyLen > 0 {
			maxItem = c.MaxKeyLen
		}
		maxVal := DefaultMaxValueSize
		if c.MaxValueSize > 0 {
			maxVal = c.MaxValueSize
		}
		need := topkx.WorstEncodedSize(c.EffectiveTopKSize(), maxItem)
		if maxVal > 0 && need > maxVal {
			return fmt.Errorf("keyspace: ModeTopK WorstEncodedSize %d > MaxValueSize %d", need, maxVal)
		}
	}
	if c.Mode == ModeVectorSet {
		if c.VectorDim < 0 || c.VectorDim == 1 || c.VectorDim > vecset.MaxDim {
			return fmt.Errorf("keyspace: VectorDim %d out of range", c.VectorDim)
		}
		switch c.VectorMetric {
		case VectorMetricCosine, VectorMetricL2, VectorMetricIP:
		default:
			return fmt.Errorf("keyspace: unknown VectorMetric %d", int(c.VectorMetric))
		}
		dim := c.VectorDim
		if dim == 0 {
			dim = vecset.MaxDim
		}
		maxItem := DefaultMaxKeyLen
		if c.MaxKeyLen > 0 {
			maxItem = c.MaxKeyLen
		}
		if maxItem > vecset.MaxMemberLen {
			maxItem = vecset.MaxMemberLen
		}
		maxVal := DefaultMaxValueSize
		if c.MaxValueSize > 0 {
			maxVal = c.MaxValueSize
		}
		need := vecset.WorstEncodedSize(dim, vecset.MaxMembers, maxItem)
		if maxVal > 0 && need > maxVal {
			return fmt.Errorf("keyspace: ModeVectorSet WorstEncodedSize %d > MaxValueSize %d", need, maxVal)
		}
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

// EffectiveTopKSize is K for ModeTopK (0 → DefaultTopKSize).
func (c Config) EffectiveTopKSize() int {
	if c.TopKSize <= 0 {
		return DefaultTopKSize
	}
	return c.TopKSize
}

// EffectiveStreamMaxLen is auto-trim after XAdd (0 = none). Values above StreamHardCap clamp.
func (c Config) EffectiveStreamMaxLen() int {
	if c.StreamMaxLen <= 0 {
		return 0
	}
	if c.StreamMaxLen > StreamHardCap {
		return StreamHardCap
	}
	return c.StreamMaxLen
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
		TopKSize          int
		VectorDim         int
		VectorMetric      int
		StreamMaxLen      int
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
		TopKSize:          c.TopKSize,
		VectorDim:         c.VectorDim,
		VectorMetric:      int(c.VectorMetric),
		StreamMaxLen:      c.StreamMaxLen,
	})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}
