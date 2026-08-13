// Package set encodes exact set membership for ModeSet store entries.
package set

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// Set is an in-memory exact set of opaque items (map keys are string(item)).
type Set struct {
	m map[string]struct{}
}

// New returns an empty set.
func New() *Set {
	return &Set{m: make(map[string]struct{})}
}

// FromMembers builds a set from items (deduped).
func FromMembers(items [][]byte) *Set {
	s := New()
	for _, it := range items {
		s.Add(it)
	}
	return s
}

// Add inserts item. Idempotent.
func (s *Set) Add(item []byte) {
	if s.m == nil {
		s.m = make(map[string]struct{})
	}
	s.m[string(item)] = struct{}{}
}

// Remove deletes item if present. Idempotent.
func (s *Set) Remove(item []byte) {
	if s.m == nil {
		return
	}
	delete(s.m, string(item))
}

// Contains reports membership.
func (s *Set) Contains(item []byte) bool {
	if s == nil || s.m == nil {
		return false
	}
	_, ok := s.m[string(item)]
	return ok
}

// Len is the number of elements.
func (s *Set) Len() int {
	if s == nil || s.m == nil {
		return 0
	}
	return len(s.m)
}

// Members returns sorted unique items (each a defensive copy).
func (s *Set) Members() [][]byte {
	if s == nil || len(s.m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = append([]byte(nil), k...)
	}
	return out
}

// Encode packs members as sorted uvarint-length-prefixed items.
func Encode(items [][]byte) []byte {
	s := FromMembers(items)
	return s.Encode()
}

// Encode packs this set canonically.
func (s *Set) Encode() []byte {
	mem := s.Members()
	if len(mem) == 0 {
		return []byte{}
	}
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	for _, it := range mem {
		n := binary.PutUvarint(scratch[:], uint64(len(it)))
		buf.Write(scratch[:n])
		buf.Write(it)
	}
	return buf.Bytes()
}

// ApproxWireBytes estimates encoded size without allocating the blob
// (uvarint length prefix + payload per member).
func (s *Set) ApproxWireBytes() int64 {
	if s == nil || len(s.m) == 0 {
		return 0
	}
	var n int64
	for k := range s.m {
		n += int64(uvarintSize(uint64(len(k)))) + int64(len(k))
	}
	return n
}

func uvarintSize(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// Decode unpacks an Encode blob into sorted unique members.
func Decode(b []byte) ([][]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var out [][]byte
	for len(b) > 0 {
		n, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("set: bad uvarint")
		}
		b = b[w:]
		if uint64(len(b)) < n {
			return nil, fmt.Errorf("set: truncated item")
		}
		item := append([]byte(nil), b[:n]...)
		b = b[n:]
		out = append(out, item)
	}
	// Normalize via set in case of bad order/dups on the wire.
	return FromMembers(out).Members(), nil
}

// DecodeSet returns a Set from an encoded blob.
func DecodeSet(b []byte) (*Set, error) {
	mem, err := Decode(b)
	if err != nil {
		return nil, err
	}
	return FromMembers(mem), nil
}
