// Package vecset is the ModeVectorSet codec (snapshot + owner inbox).
package vecset

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
)

const (
	PrefixSnapshot byte = 'S'
	PrefixAdd      byte = 'A'
	PrefixRem      byte = 'R'

	MinDim         = 2
	MaxDim         = 256
	MaxMembers     = 512
	MaxMemberLen   = 255
	DefaultK       = 10
	MaxK           = 50
	snapshotHeader = 5
)

// Metric is the keyspace K-NN formula (mirrors keyspace.VectorMetric).
type Metric int

const (
	MetricCosine Metric = iota
	MetricL2
	MetricIP
)

// Hit is one VSim row.
type Hit struct {
	Member []byte
	Score  float32
}

var (
	errBadBlob   = errors.New("vecset: bad blob")
	errBadMember = errors.New("vecset: bad member")
	errBadVec    = errors.New("vecset: bad vector")
	errFull      = errors.New("vecset: full")
)

// WorstEncodedSize is the snapshot size for Validate (header + count records).
func WorstEncodedSize(dim, count, idLen int) int {
	if dim < MinDim {
		dim = MaxDim
	}
	if idLen < 1 {
		idLen = MaxMemberLen
	}
	if count < 0 {
		count = MaxMembers
	}
	return snapshotHeader + count*(1+idLen+4*dim)
}

// ClampK maps k<=0 → 10 and k>50 → 50.
func ClampK(k int) int {
	if k <= 0 {
		return DefaultK
	}
	if k > MaxK {
		return MaxK
	}
	return k
}

// ValidDim reports whether dim is in 2..256.
func ValidDim(dim int) bool { return dim >= MinDim && dim <= MaxDim }

// IsZero reports an all-zero vector (cosine-undefined).
func IsZero(vec []float32) bool {
	for _, x := range vec {
		if x != 0 {
			return false
		}
	}
	return len(vec) > 0
}

// Finite reports no NaN/Inf.
func Finite(vec []float32) bool {
	for _, x := range vec {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return false
		}
	}
	return true
}

// Set is a decoded vector set (dim locked even when empty).
type Set struct {
	Dim  int
	vals map[string][]float32
}

// NewSet creates an empty set with a locked dim.
func NewSet(dim int) *Set {
	return &Set{Dim: dim, vals: make(map[string][]float32)}
}

func (s *Set) Card() int {
	if s == nil {
		return 0
	}
	return len(s.vals)
}

func (s *Set) Emb(member []byte) ([]float32, bool) {
	if s == nil {
		return nil, false
	}
	v, ok := s.vals[string(member)]
	if !ok {
		return nil, false
	}
	out := make([]float32, len(v))
	copy(out, v)
	return out, true
}

func (s *Set) Add(member []byte, vec []float32) error {
	if s == nil || !ValidDim(s.Dim) || len(vec) != s.Dim || !Finite(vec) {
		return errBadVec
	}
	if len(member) < 1 || len(member) > MaxMemberLen {
		return errBadMember
	}
	k := string(member)
	if _, exists := s.vals[k]; !exists && len(s.vals) >= MaxMembers {
		return errFull
	}
	cp := make([]float32, len(vec))
	copy(cp, vec)
	if s.vals == nil {
		s.vals = make(map[string][]float32)
	}
	s.vals[k] = cp
	return nil
}

func (s *Set) Rem(member []byte) {
	if s == nil || s.vals == nil {
		return
	}
	delete(s.vals, string(member))
}

// Encode writes a snapshot blob ('S' | u16 dim | u16 count | records).
func (s *Set) Encode() []byte {
	if s == nil {
		return nil
	}
	keys := make([]string, 0, len(s.vals))
	for k := range s.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	n := snapshotHeader
	for _, k := range keys {
		n += 1 + len(k) + 4*s.Dim
	}
	buf := make([]byte, n)
	buf[0] = PrefixSnapshot
	binary.LittleEndian.PutUint16(buf[1:3], uint16(s.Dim))
	binary.LittleEndian.PutUint16(buf[3:5], uint16(len(keys)))
	off := snapshotHeader
	tmp := make([]byte, 4)
	for _, k := range keys {
		buf[off] = byte(len(k))
		off++
		off += copy(buf[off:], k)
		for _, x := range s.vals[k] {
			binary.LittleEndian.PutUint32(tmp, math.Float32bits(x))
			off += copy(buf[off:], tmp)
		}
	}
	return buf
}

// DecodeSnapshot parses a FlagVectorSet snapshot. Empty count is valid.
func DecodeSnapshot(blob []byte) (*Set, error) {
	if len(blob) < snapshotHeader || blob[0] != PrefixSnapshot {
		return nil, errBadBlob
	}
	dim := int(binary.LittleEndian.Uint16(blob[1:3]))
	count := int(binary.LittleEndian.Uint16(blob[3:5]))
	if !ValidDim(dim) || count < 0 || count > MaxMembers {
		return nil, errBadBlob
	}
	s := NewSet(dim)
	off := snapshotHeader
	for i := 0; i < count; i++ {
		if off >= len(blob) {
			return nil, errBadBlob
		}
		idLen := int(blob[off])
		off++
		if idLen < 1 || idLen > MaxMemberLen || off+idLen+4*dim > len(blob) {
			return nil, errBadBlob
		}
		id := blob[off : off+idLen]
		off += idLen
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(blob[off : off+4]))
			off += 4
			if math.IsNaN(float64(vec[j])) || math.IsInf(float64(vec[j]), 0) {
				return nil, errBadBlob
			}
		}
		if err := s.Add(append([]byte(nil), id...), vec); err != nil {
			return nil, err
		}
	}
	if off != len(blob) {
		return nil, errBadBlob
	}
	return s, nil
}

// EncodeInboxAdd is owner-inbox 'A' | u8 | member | floats.
func EncodeInboxAdd(member []byte, vec []float32) ([]byte, error) {
	if len(member) < 1 || len(member) > MaxMemberLen || !ValidDim(len(vec)) || !Finite(vec) {
		return nil, errBadVec
	}
	buf := make([]byte, 2+len(member)+4*len(vec))
	buf[0] = PrefixAdd
	buf[1] = byte(len(member))
	copy(buf[2:], member)
	off := 2 + len(member)
	for _, x := range vec {
		binary.LittleEndian.PutUint32(buf[off:off+4], math.Float32bits(x))
		off += 4
	}
	return buf, nil
}

// EncodeInboxRem is owner-inbox 'R' | member.
func EncodeInboxRem(member []byte) ([]byte, error) {
	if len(member) < 1 || len(member) > MaxMemberLen {
		return nil, errBadMember
	}
	buf := make([]byte, 1+len(member))
	buf[0] = PrefixRem
	copy(buf[1:], member)
	return buf, nil
}

// DecodeInboxAdd parses an 'A' inbox.
func DecodeInboxAdd(blob []byte) (member []byte, vec []float32, err error) {
	if len(blob) < 2 || blob[0] != PrefixAdd {
		return nil, nil, errBadBlob
	}
	idLen := int(blob[1])
	if idLen < 1 || idLen > MaxMemberLen || 2+idLen >= len(blob) {
		return nil, nil, errBadBlob
	}
	rest := len(blob) - 2 - idLen
	if rest%4 != 0 {
		return nil, nil, errBadBlob
	}
	dim := rest / 4
	if !ValidDim(dim) {
		return nil, nil, errBadBlob
	}
	member = append([]byte(nil), blob[2:2+idLen]...)
	vec = make([]float32, dim)
	off := 2 + idLen
	for i := 0; i < dim; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(blob[off : off+4]))
		off += 4
		if math.IsNaN(float64(vec[i])) || math.IsInf(float64(vec[i]), 0) {
			return nil, nil, errBadBlob
		}
	}
	return member, vec, nil
}

// DecodeInboxRem parses an 'R' inbox (remainder is the member).
func DecodeInboxRem(blob []byte) ([]byte, error) {
	if len(blob) < 2 || blob[0] != PrefixRem {
		return nil, errBadBlob
	}
	member := blob[1:]
	if len(member) < 1 || len(member) > MaxMemberLen {
		return nil, errBadBlob
	}
	return append([]byte(nil), member...), nil
}

// Score one pair. Cosine of a zero vector is 0 (caller should reject).
func Score(a, b []float32, m Metric) float32 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	switch m {
	case MetricL2:
		var s float64
		for i := range a {
			d := float64(a[i]) - float64(b[i])
			s += d * d
		}
		return float32(math.Sqrt(s))
	case MetricIP:
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return float32(dot)
	default: // cosine
		var dot, na, nb float64
		for i := range a {
			fa, fb := float64(a[i]), float64(b[i])
			dot += fa * fb
			na += fa * fa
			nb += fb * fb
		}
		if na == 0 || nb == 0 {
			return 0
		}
		return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
	}
}

func better(scoreA, scoreB float32, m Metric) bool {
	if m == MetricL2 {
		return scoreA < scoreB
	}
	return scoreA > scoreB
}

// Sim ranks members in a snapshot against query.
func Sim(blob []byte, query []float32, k int, m Metric) ([]Hit, error) {
	s, err := DecodeSnapshot(blob)
	if err != nil {
		return nil, err
	}
	return s.Sim(query, k, m)
}

func (s *Set) Sim(query []float32, k int, m Metric) ([]Hit, error) {
	if s == nil || len(query) != s.Dim || !Finite(query) {
		return nil, errBadVec
	}
	k = ClampK(k)
	type row struct {
		id    string
		score float32
	}
	rows := make([]row, 0, len(s.vals))
	for id, vec := range s.vals {
		rows = append(rows, row{id: id, score: Score(query, vec, m)})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].score != rows[j].score {
			return better(rows[i].score, rows[j].score, m)
		}
		return rows[i].id < rows[j].id
	})
	if k > len(rows) {
		k = len(rows)
	}
	out := make([]Hit, k)
	for i := 0; i < k; i++ {
		out[i] = Hit{Member: []byte(rows[i].id), Score: rows[i].score}
	}
	return out, nil
}
