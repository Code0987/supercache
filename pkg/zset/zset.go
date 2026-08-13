// Package zset encodes sorted sets (score + member) for ModeZSet.
package zset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Member is one scored element.
type Member struct {
	Member []byte
	Score  float64
}

// ZSet is an in-memory sorted set.
type ZSet struct {
	scores map[string]float64
}

// New returns an empty zset.
func New() *ZSet {
	return &ZSet{scores: make(map[string]float64)}
}

// Add inserts or updates member's score. Rejects NaN.
func (z *ZSet) Add(member []byte, score float64) error {
	if math.IsNaN(score) {
		return fmt.Errorf("zset: NaN score")
	}
	if z.scores == nil {
		z.scores = make(map[string]float64)
	}
	z.scores[string(member)] = score
	return nil
}

// Rem removes member if present.
func (z *ZSet) Rem(member []byte) {
	if z == nil || z.scores == nil {
		return
	}
	delete(z.scores, string(member))
}

// Score returns the score if present.
func (z *ZSet) Score(member []byte) (float64, bool) {
	if z == nil || z.scores == nil {
		return 0, false
	}
	s, ok := z.scores[string(member)]
	return s, ok
}

// Card is the number of members.
func (z *ZSet) Card() int {
	if z == nil || z.scores == nil {
		return 0
	}
	return len(z.scores)
}

// ordered returns members sorted by score asc, then member bytes.
func (z *ZSet) ordered() []Member {
	if z == nil || len(z.scores) == 0 {
		return nil
	}
	out := make([]Member, 0, len(z.scores))
	for k, sc := range z.scores {
		out = append(out, Member{Member: []byte(k), Score: sc})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score < out[j].Score
		}
		return bytes.Compare(out[i].Member, out[j].Member) < 0
	})
	return out
}

// Range returns members by rank [start, stop] inclusive (Redis-style negatives).
func (z *ZSet) Range(start, stop int) []Member {
	all := z.ordered()
	n := len(all)
	if n == 0 {
		return nil
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return nil
	}
	out := make([]Member, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, Member{
			Member: append([]byte(nil), all[i].Member...),
			Score:  all[i].Score,
		})
	}
	return out
}

// RangeByScore returns members with min <= score <= max.
func (z *ZSet) RangeByScore(min, max float64) []Member {
	all := z.ordered()
	var out []Member
	for _, m := range all {
		if m.Score < min {
			continue
		}
		if m.Score > max {
			break
		}
		out = append(out, Member{
			Member: append([]byte(nil), m.Member...),
			Score:  m.Score,
		})
	}
	return out
}

// Encode packs sorted records: float64 LE score + uvarint len + member.
func (z *ZSet) Encode() []byte {
	all := z.ordered()
	if len(all) == 0 {
		return []byte{}
	}
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	for _, m := range all {
		var fb [8]byte
		binary.LittleEndian.PutUint64(fb[:], math.Float64bits(m.Score))
		buf.Write(fb[:])
		n := binary.PutUvarint(scratch[:], uint64(len(m.Member)))
		buf.Write(scratch[:n])
		buf.Write(m.Member)
	}
	return buf.Bytes()
}

// ApproxWireBytes estimates encoded size without allocating.
func (z *ZSet) ApproxWireBytes() int64 {
	if z == nil || len(z.scores) == 0 {
		return 0
	}
	var n int64
	for k := range z.scores {
		n += 8 + int64(uvarintSize(uint64(len(k)))) + int64(len(k))
	}
	return n
}

func uvarintSize(x uint64) int {
	c := 1
	for x >= 0x80 {
		x >>= 7
		c++
	}
	return c
}

// Decode rebuilds a ZSet from Encode output.
func Decode(b []byte) (*ZSet, error) {
	z := New()
	if len(b) == 0 {
		return z, nil
	}
	for len(b) > 0 {
		if len(b) < 8 {
			return nil, fmt.Errorf("zset: truncated score")
		}
		bits := binary.LittleEndian.Uint64(b[:8])
		score := math.Float64frombits(bits)
		b = b[8:]
		n, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("zset: bad uvarint")
		}
		b = b[w:]
		if uint64(len(b)) < n {
			return nil, fmt.Errorf("zset: truncated member")
		}
		mem := append([]byte(nil), b[:n]...)
		b = b[n:]
		if err := z.Add(mem, score); err != nil {
			return nil, err
		}
	}
	return z, nil
}

// EncodeAdd packs a single add payload (score + member) for fan-out.
func EncodeAdd(member []byte, score float64) []byte {
	z := New()
	_ = z.Add(member, score)
	return z.Encode()
}

// DecodeAdd unpacks a single-member encode blob.
func DecodeAdd(b []byte) (member []byte, score float64, err error) {
	z, err := Decode(b)
	if err != nil {
		return nil, 0, err
	}
	all := z.ordered()
	if len(all) != 1 {
		return nil, 0, fmt.Errorf("zset: add payload want 1 member got %d", len(all))
	}
	return all[0].Member, all[0].Score, nil
}
