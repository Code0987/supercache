// Package cmsx is a dense Count-Min Sketch (d=4, w=2048, uint64 cells).
//
// Estimates do not match Redis CMS.QUERY (different hash, locked dims).
package cmsx

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	// Depth is the number of hash rows.
	Depth = 4
	// Width is the number of cells per row.
	Width = 2048
	// CellBytes is the on-wire cell width.
	CellBytes = 8
	// DenseSize is Depth * Width * CellBytes (65536).
	DenseSize = Depth * Width * CellBytes
)

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

var (
	// ErrSize is a sketch blob that is not DenseSize.
	ErrSize = errors.New("cmsx: bad sketch blob size")
	// ErrEmpty is an empty item (engine rejects this first).
	ErrEmpty = errors.New("cmsx: empty item")
	// ErrOverflow is a cell that would wrap uint64.
	ErrOverflow = errors.New("cmsx: counter overflow")
	// ErrInbox is a truncated or empty FlagCMSIncr payload.
	ErrInbox = errors.New("cmsx: bad inbox")
)

// New returns a zeroed dense sketch (DenseSize bytes).
func New() []byte {
	return make([]byte, DenseSize)
}

// mix64 is the splitmix64 finalizer copied from pkg/hllx/hllx.go.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Hash64 is alloc-free FNV-1a 64 (same as hllx).
func Hash64(item []byte) uint64 {
	h := uint64(offset64)
	for _, c := range item {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

func delta(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	return n
}

func columns(item []byte) [Depth]uint64 {
	x := mix64(Hash64(item))
	h1 := x >> 32
	h2 := (x & 0xffffffff) | 1
	var cols [Depth]uint64
	for i := 0; i < Depth; i++ {
		cols[i] = (h1 + uint64(i)*h2) & (Width - 1)
	}
	return cols
}

func cellOff(row int, col uint64) int {
	return int((uint64(row)*Width + col) * CellBytes)
}

// Incr adds n to the d hashed cells in place. n==0 means 1.
func Incr(sketch []byte, item []byte, n uint64) error {
	if len(sketch) != DenseSize {
		return ErrSize
	}
	if len(item) == 0 {
		return ErrEmpty
	}
	d := delta(n)
	cols := columns(item)
	for i := 0; i < Depth; i++ {
		v := binary.LittleEndian.Uint64(sketch[cellOff(i, cols[i]):])
		if v > math.MaxUint64-d {
			return ErrOverflow
		}
	}
	for i := 0; i < Depth; i++ {
		off := cellOff(i, cols[i])
		v := binary.LittleEndian.Uint64(sketch[off:])
		binary.LittleEndian.PutUint64(sketch[off:], v+d)
	}
	return nil
}

// Query is min of d cells. All-zero / unseen / empty item / bad size → 0.
func Query(sketch []byte, item []byte) uint64 {
	if len(sketch) != DenseSize || len(item) == 0 {
		return 0
	}
	cols := columns(item)
	min := uint64(math.MaxUint64)
	for i := 0; i < Depth; i++ {
		v := binary.LittleEndian.Uint64(sketch[cellOff(i, cols[i]):])
		if v < min {
			min = v
		}
	}
	return min
}

// EncodeInbox writes uvarint(delta)||item. n==0 encodes as 1. Empty item → error.
func EncodeInbox(n uint64, item []byte) ([]byte, error) {
	if len(item) == 0 {
		return nil, ErrEmpty
	}
	d := delta(n)
	var buf [binary.MaxVarintLen64]byte
	k := binary.PutUvarint(buf[:], d)
	out := make([]byte, k+len(item))
	copy(out, buf[:k])
	copy(out[k:], item)
	return out, nil
}

// DecodeInbox reads uvarint(n)||item. Empty item / truncated → error.
func DecodeInbox(b []byte) (n uint64, item []byte, err error) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return 0, nil, ErrInbox
	}
	item = b[k:]
	if len(item) == 0 {
		return 0, nil, ErrEmpty
	}
	return n, item, nil
}
