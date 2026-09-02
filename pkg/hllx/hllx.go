// Package hllx is a dense HyperLogLog sketch (p=14, 6-bit registers).
//
// Estimates do not match Redis PFCOUNT (different hash, no sparse/bias tables).
package hllx

import (
	"errors"
	"math"
	"math/bits"
)

const (
	// Precision is Redis-class p.
	Precision = 14
	// Registers is 1<<Precision.
	Registers = 1 << Precision
	// RegBits is the packed register width.
	RegBits = 6
	// DenseSize is Registers * RegBits / 8 (12288).
	DenseSize = Registers * RegBits / 8
)

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

var (
	// ErrSize is a register blob that is not DenseSize.
	ErrSize = errors.New("hllx: bad register blob size")
	// ErrEmpty is an empty item (engine rejects this first).
	ErrEmpty = errors.New("hllx: empty item")
)

// New returns a zeroed dense sketch.
func New() []byte {
	return make([]byte, DenseSize)
}

// mix64 is a splitmix64 finalizer so FNV-1a high bits can be used as an HLL index.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Hash64 is alloc-free FNV-1a 64.
func Hash64(item []byte) uint64 {
	h := uint64(offset64)
	for _, c := range item {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

// Add hashes item into regs in place. No-op if size or item is invalid.
func Add(regs []byte, item []byte) {
	if len(regs) != DenseSize || len(item) == 0 {
		return
	}
	// FNV-1a high bits avalanche poorly; mix so the Flajolet split is uniform.
	x := mix64(Hash64(item))
	idx := int(x >> (64 - Precision))
	w := x << Precision
	var rho int
	if w == 0 {
		rho = (64 - Precision) + 1
	} else {
		rho = bits.LeadingZeros64(w) + 1
	}
	if rho > 63 {
		rho = 63
	}
	if uint8(rho) > GetReg(regs, idx) {
		SetReg(regs, idx, uint8(rho))
	}
}

// Count is the corrected HLL estimate. All-zero → 0. Bad size → 0.
func Count(regs []byte) uint64 {
	if len(regs) != DenseSize {
		return 0
	}
	const m = float64(Registers)
	var sum float64
	zeros := 0
	for i := 0; i < Registers; i++ {
		r := GetReg(regs, i)
		if r == 0 {
			zeros++
		}
		sum += math.Ldexp(1, -int(r))
	}
	alpha := 0.7213 / (1 + 1.079/m)
	e := alpha * m * m / sum
	if e <= 2.5*m && zeros > 0 {
		e = m * math.Log(m/float64(zeros))
	}
	return uint64(math.Floor(e + 0.5))
}

// merge writes per-register max(dst, src) into dst.
// Not used on install (handoff is LWW replace, not max-merge).
func merge(dst, src []byte) error {
	if len(dst) != DenseSize || len(src) != DenseSize {
		return ErrSize
	}
	for i := 0; i < Registers; i++ {
		a, b := GetReg(dst, i), GetReg(src, i)
		if b > a {
			SetReg(dst, i, b)
		}
	}
	return nil
}

// GetReg returns register i (0 if out of range).
func GetReg(regs []byte, i int) uint8 {
	if i < 0 || i >= Registers || len(regs) != DenseSize {
		return 0
	}
	off := (i * RegBits) / 8
	fb := (i * RegBits) % 8
	b0 := uint16(regs[off])
	b1 := uint16(0)
	if off+1 < len(regs) {
		b1 = uint16(regs[off+1])
	}
	return uint8(((b0 >> fb) | (b1 << (8 - fb))) & 0x3f)
}

// SetReg writes the low 6 bits of v into register i.
func SetReg(regs []byte, i int, v uint8) {
	if i < 0 || i >= Registers || len(regs) != DenseSize {
		return
	}
	v &= 0x3f
	off := (i * RegBits) / 8
	fb := (i * RegBits) % 8
	fb8 := 8 - fb
	regs[off] &^= 0x3f << fb
	regs[off] |= v << fb
	if off+1 < len(regs) {
		regs[off+1] &^= byte(0x3f) >> fb8
		regs[off+1] |= v >> fb8
	}
}
