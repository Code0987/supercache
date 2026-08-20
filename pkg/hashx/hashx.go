// Package hashx encodes named field maps for ModeHash.
package hashx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// Field is one hash field/value pair.
type Field struct {
	Field []byte
	Value []byte
}

// Hash is an in-memory field map (field bytes as Go string keys).
type Hash struct {
	m map[string][]byte
}

// New returns an empty hash.
func New() *Hash {
	return &Hash{m: make(map[string][]byte)}
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Set upserts field. Nil value is stored as empty. Copies the value in.
func (h *Hash) Set(field, value []byte) {
	if h.m == nil {
		h.m = make(map[string][]byte)
	}
	h.m[string(field)] = copyBytes(value)
}

// Del removes field if present.
func (h *Hash) Del(field []byte) {
	if h == nil || h.m == nil {
		return
	}
	delete(h.m, string(field))
}

// Get returns a copy of the value. A stored empty value is a non-nil empty slice + ok.
func (h *Hash) Get(field []byte) ([]byte, bool) {
	if h == nil || h.m == nil {
		return nil, false
	}
	v, ok := h.m[string(field)]
	if !ok {
		return nil, false
	}
	return copyBytes(v), true
}

// Exists reports whether field is present.
func (h *Hash) Exists(field []byte) bool {
	if h == nil || h.m == nil {
		return false
	}
	_, ok := h.m[string(field)]
	return ok
}

// Len is the number of fields.
func (h *Hash) Len() int {
	if h == nil || h.m == nil {
		return 0
	}
	return len(h.m)
}

// All returns copies of every pair in raw field-byte order.
func (h *Hash) All() []Field {
	if h == nil || len(h.m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(h.m))
	for k := range h.m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	out := make([]Field, len(keys))
	for i, k := range keys {
		out[i] = Field{Field: copyBytes([]byte(k)), Value: copyBytes(h.m[k])}
	}
	return out
}

// Encode packs records: uvarint(len field)+field+uvarint(len value)+value (field-byte order).
func (h *Hash) Encode() []byte {
	all := h.All()
	if len(all) == 0 {
		return []byte{}
	}
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	for _, f := range all {
		n := binary.PutUvarint(scratch[:], uint64(len(f.Field)))
		buf.Write(scratch[:n])
		buf.Write(f.Field)
		n = binary.PutUvarint(scratch[:], uint64(len(f.Value)))
		buf.Write(scratch[:n])
		buf.Write(f.Value)
	}
	return buf.Bytes()
}

// ApproxWireBytes estimates encoded size without allocating.
func (h *Hash) ApproxWireBytes() int64 {
	if h == nil || len(h.m) == 0 {
		return 0
	}
	var n int64
	for k, v := range h.m {
		n += int64(uvarintSize(uint64(len(k)))) + int64(len(k))
		n += int64(uvarintSize(uint64(len(v)))) + int64(len(v))
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

func consumePair(b []byte) (field, value, rest []byte, err error) {
	n, w := binary.Uvarint(b)
	if w <= 0 {
		return nil, nil, nil, fmt.Errorf("hashx: bad field uvarint")
	}
	b = b[w:]
	if uint64(len(b)) < n {
		return nil, nil, nil, fmt.Errorf("hashx: truncated field")
	}
	field = append([]byte(nil), b[:n]...)
	b = b[n:]
	n, w = binary.Uvarint(b)
	if w <= 0 {
		return nil, nil, nil, fmt.Errorf("hashx: bad value uvarint")
	}
	b = b[w:]
	if uint64(len(b)) < n {
		return nil, nil, nil, fmt.Errorf("hashx: truncated value")
	}
	value = append([]byte(nil), b[:n]...)
	return field, value, b[n:], nil
}

// Decode rebuilds a Hash from Encode output. Duplicate fields last-win.
func Decode(b []byte) (*Hash, error) {
	h := New()
	if len(b) == 0 {
		return h, nil
	}
	for len(b) > 0 {
		field, value, rest, err := consumePair(b)
		if err != nil {
			return nil, err
		}
		h.Set(field, value)
		b = rest
	}
	return h, nil
}

// EncodeSet packs exactly one field/value record (no sort).
func EncodeSet(field, value []byte) []byte {
	if value == nil {
		value = []byte{}
	}
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(field)))
	buf.Write(scratch[:n])
	buf.Write(field)
	n = binary.PutUvarint(scratch[:], uint64(len(value)))
	buf.Write(scratch[:n])
	buf.Write(value)
	return buf.Bytes()
}

// DecodeSet unpacks exactly one pair. Errors on 0 records or leftover bytes.
func DecodeSet(b []byte) (field, value []byte, err error) {
	if len(b) == 0 {
		return nil, nil, fmt.Errorf("hashx: empty set payload")
	}
	field, value, rest, err := consumePair(b)
	if err != nil {
		return nil, nil, err
	}
	if len(rest) != 0 {
		return nil, nil, fmt.Errorf("hashx: leftover after one pair")
	}
	return field, value, nil
}
