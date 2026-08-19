// Package geo encodes geospatial point indexes for ModeGeo.
package geo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// EarthRadiusM is the mean Earth radius used for haversine (meters).
const EarthRadiusM = 6371008.8

// Point is a WGS84 coordinate.
type Point struct {
	Lon, Lat float64
}

// Member is a point plus optional distance from a query.
type Member struct {
	Member []byte
	Lon    float64
	Lat    float64
	Dist   float64
}

// Index is an in-memory named point set.
type Index struct {
	pts map[string]Point
}

// New returns an empty index.
func New() *Index {
	return &Index{pts: make(map[string]Point)}
}

// ValidCoord reports whether lon/lat are finite WGS84 values.
func ValidCoord(lon, lat float64) bool {
	if math.IsNaN(lon) || math.IsNaN(lat) || math.IsInf(lon, 0) || math.IsInf(lat, 0) {
		return false
	}
	return lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90
}

// Add inserts or updates member position.
func (g *Index) Add(member []byte, lon, lat float64) error {
	if !ValidCoord(lon, lat) {
		return fmt.Errorf("geo: invalid lon/lat")
	}
	if g.pts == nil {
		g.pts = make(map[string]Point)
	}
	g.pts[string(member)] = Point{Lon: lon, Lat: lat}
	return nil
}

// Rem removes member if present.
func (g *Index) Rem(member []byte) {
	if g == nil || g.pts == nil {
		return
	}
	delete(g.pts, string(member))
}

// Pos returns the point if present.
func (g *Index) Pos(member []byte) (Point, bool) {
	if g == nil || g.pts == nil {
		return Point{}, false
	}
	p, ok := g.pts[string(member)]
	return p, ok
}

// Card is the number of members.
func (g *Index) Card() int {
	if g == nil || g.pts == nil {
		return 0
	}
	return len(g.pts)
}

// Dist is haversine meters between two members.
func (g *Index) Dist(a, b []byte) (float64, bool) {
	pa, oka := g.Pos(a)
	pb, okb := g.Pos(b)
	if !oka || !okb {
		return 0, false
	}
	return Haversine(pa.Lon, pa.Lat, pb.Lon, pb.Lat), true
}

// Haversine returns great-circle distance in meters.
func Haversine(lon1, lat1, lon2, lat2 float64) float64 {
	φ1 := lat1 * math.Pi / 180
	φ2 := lat2 * math.Pi / 180
	Δφ := (lat2 - lat1) * math.Pi / 180
	Δλ := (lon2 - lon1) * math.Pi / 180
	sinΔφ := math.Sin(Δφ / 2)
	sinΔλ := math.Sin(Δλ / 2)
	h := sinΔφ*sinΔφ + math.Cos(φ1)*math.Cos(φ2)*sinΔλ*sinΔλ
	return 2 * EarthRadiusM * math.Asin(math.Min(1, math.Sqrt(h)))
}

// Radius returns members within radiusM of (lon,lat), nearest first.
// limit <= 0 means no cap.
func (g *Index) Radius(lon, lat, radiusM float64, limit int) []Member {
	if g == nil || len(g.pts) == 0 {
		return nil
	}
	out := make([]Member, 0)
	for k, p := range g.pts {
		d := Haversine(lon, lat, p.Lon, p.Lat)
		if d > radiusM {
			continue
		}
		out = append(out, Member{
			Member: append([]byte(nil), k...),
			Lon:    p.Lon,
			Lat:    p.Lat,
			Dist:   d,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Dist != out[j].Dist {
			return out[i].Dist < out[j].Dist
		}
		return bytes.Compare(out[i].Member, out[j].Member) < 0
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (g *Index) orderedMembers() []Member {
	if g == nil || len(g.pts) == 0 {
		return nil
	}
	out := make([]Member, 0, len(g.pts))
	for k, p := range g.pts {
		out = append(out, Member{Member: []byte(k), Lon: p.Lon, Lat: p.Lat})
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Member, out[j].Member) < 0
	})
	return out
}

// Encode packs records: lon LE + lat LE + uvarint len + member (member-byte order).
func (g *Index) Encode() []byte {
	all := g.orderedMembers()
	if len(all) == 0 {
		return []byte{}
	}
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	for _, m := range all {
		var fb [8]byte
		binary.LittleEndian.PutUint64(fb[:], math.Float64bits(m.Lon))
		buf.Write(fb[:])
		binary.LittleEndian.PutUint64(fb[:], math.Float64bits(m.Lat))
		buf.Write(fb[:])
		n := binary.PutUvarint(scratch[:], uint64(len(m.Member)))
		buf.Write(scratch[:n])
		buf.Write(m.Member)
	}
	return buf.Bytes()
}

// ApproxWireBytes estimates encoded size without allocating.
func (g *Index) ApproxWireBytes() int64 {
	if g == nil || len(g.pts) == 0 {
		return 0
	}
	var n int64
	for k := range g.pts {
		n += 16 + int64(uvarintSize(uint64(len(k)))) + int64(len(k))
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

// Decode rebuilds an Index from Encode output.
func Decode(b []byte) (*Index, error) {
	g := New()
	if len(b) == 0 {
		return g, nil
	}
	for len(b) > 0 {
		if len(b) < 16 {
			return nil, fmt.Errorf("geo: truncated coord")
		}
		lon := math.Float64frombits(binary.LittleEndian.Uint64(b[:8]))
		lat := math.Float64frombits(binary.LittleEndian.Uint64(b[8:16]))
		b = b[16:]
		n, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("geo: bad uvarint")
		}
		b = b[w:]
		if uint64(len(b)) < n {
			return nil, fmt.Errorf("geo: truncated member")
		}
		mem := append([]byte(nil), b[:n]...)
		b = b[n:]
		if err := g.Add(mem, lon, lat); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// EncodeAdd packs a single add payload.
func EncodeAdd(member []byte, lon, lat float64) []byte {
	g := New()
	_ = g.Add(member, lon, lat)
	return g.Encode()
}

// DecodeAdd unpacks a single-member encode blob.
func DecodeAdd(b []byte) (member []byte, lon, lat float64, err error) {
	g, err := Decode(b)
	if err != nil {
		return nil, 0, 0, err
	}
	all := g.orderedMembers()
	if len(all) != 1 {
		return nil, 0, 0, fmt.Errorf("geo: add payload want 1 member got %d", len(all))
	}
	return all[0].Member, all[0].Lon, all[0].Lat, nil
}
