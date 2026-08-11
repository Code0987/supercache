// Package bloom is a standard bitset Bloom filter (adds and tests only).
package bloom

import (
	"fmt"
	"hash/fnv"
)

// Filter is an in-memory Bloom filter. Test never false-negatives for items
// that were Add-ed to this instance (or OR-merged in).
type Filter struct {
	bits []byte
	m    int // bit count
	k    int
}

// New allocates a zeroed filter with mBits bits and k hashes.
func New(mBits, k int) *Filter {
	if mBits < 8 {
		mBits = 8
	}
	if k < 1 {
		k = 1
	}
	return &Filter{bits: make([]byte, (mBits+7)/8), m: mBits, k: k}
}

// Open wraps an existing bitset. bits is not copied; the caller must not
// resize it. Used for in-place Test/Add on store values.
func Open(mBits, k int, bits []byte) (*Filter, error) {
	if mBits < 8 || k < 1 {
		return nil, fmt.Errorf("bloom: invalid params m=%d k=%d", mBits, k)
	}
	need := (mBits + 7) / 8
	if len(bits) != need {
		return nil, fmt.Errorf("bloom: bitset len %d want %d", len(bits), need)
	}
	return &Filter{bits: bits, m: mBits, k: k}, nil
}

// Add sets the k bits for item. Safe to call twice.
func (f *Filter) Add(item []byte) {
	if f == nil {
		return
	}
	h1, h2 := hash2(item)
	m := uint64(f.m)
	for i := 0; i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % m
		f.bits[idx/8] |= 1 << (idx % 8)
	}
}

// Test reports whether item may be in the set. false means definitely not.
func (f *Filter) Test(item []byte) bool {
	if f == nil || len(f.bits) == 0 {
		return false
	}
	h1, h2 := hash2(item)
	m := uint64(f.m)
	for i := 0; i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % m
		if f.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// Merge ORs other into f. Both must share m and k.
func (f *Filter) Merge(other *Filter) error {
	if f == nil || other == nil {
		return fmt.Errorf("bloom: nil merge")
	}
	if f.m != other.m || f.k != other.k || len(f.bits) != len(other.bits) {
		return fmt.Errorf("bloom: merge shape mismatch")
	}
	for i := range f.bits {
		f.bits[i] |= other.bits[i]
	}
	return nil
}

// Bytes returns the packed bitset (not a copy).
func (f *Filter) Bytes() []byte {
	if f == nil {
		return nil
	}
	return f.bits
}

func hash2(item []byte) (h1, h2 uint64) {
	a := fnv.New64a()
	_, _ = a.Write(item)
	h1 = a.Sum64()
	b := fnv.New64()
	_, _ = b.Write(item)
	h2 = b.Sum64()
	if h2&1 == 0 {
		h2++
	}
	return h1, h2
}
