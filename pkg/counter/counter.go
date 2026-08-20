// Package counter encodes ModeCounter int64 snapshots.
package counter

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrOverflow is returned when an add would wrap int64.
var ErrOverflow = errors.New("counter overflow")

var errBad = errors.New("counter: need 8-byte value")

// Encode writes v as 8-byte two's-complement little-endian.
func Encode(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

// Decode reads Encode output. Error unless len == 8.
func Decode(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, errBad
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

// Add returns cur+delta or ErrOverflow (no wrap).
func Add(cur, delta int64) (int64, error) {
	if delta > 0 && cur > math.MaxInt64-delta {
		return 0, ErrOverflow
	}
	if delta < 0 && cur < math.MinInt64-delta {
		return 0, ErrOverflow
	}
	return cur + delta, nil
}
