// Package bitmapx encodes packed Redis-order bit strings for ModeBitmap.
package bitmapx

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

var ErrInbox = errors.New("bitmapx: invalid inbox")

// EncodedLen is the byte length needed to store offset (offset/8 + 1).
// ok=false if the length does not fit in int.
func EncodedLen(offset uint64) (n int, ok bool) {
	need := offset/8 + 1
	if need > uint64(math.MaxInt) {
		return 0, false
	}
	return int(need), true
}

func bitMask(offset uint64) (byteIdx int, mask byte) {
	byteIdx = int(offset / 8)
	shift := 7 - int(offset%8) // MSB first (Redis SETBIT)
	return byteIdx, 1 << shift
}

// Get reports the bit at offset. Past len(buf)*8 → false (implicit zero).
func Get(buf []byte, offset uint64) bool {
	need, ok := EncodedLen(offset)
	if !ok {
		return false
	}
	i := need - 1
	if i < 0 || i >= len(buf) {
		return false
	}
	_, mask := bitMask(offset)
	return buf[i]&mask != 0
}

// Set writes bit at offset, zero-filling / growing as needed.
func Set(buf []byte, offset uint64, bit bool) []byte {
	need, ok := EncodedLen(offset)
	if !ok {
		return buf
	}
	if len(buf) < need {
		n := make([]byte, need)
		copy(n, buf)
		buf = n
	}
	i, mask := bitMask(offset)
	if bit {
		buf[i] |= mask
	} else {
		buf[i] &^= mask
	}
	return buf
}

// NormalizeByteRange is the Redis / listx.Range clamp.
func NormalizeByteRange(n, start, end int) (lo, hi int, empty bool) {
	if n == 0 {
		return 0, 0, true
	}
	if start < 0 {
		start = n + start
	}
	if end < 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end || start >= n {
		return 0, 0, true
	}
	return start, end, false
}

// Count is Redis BITCOUNT over an inclusive byte window.
func Count(buf []byte, start, end int) int64 {
	lo, hi, empty := NormalizeByteRange(len(buf), start, end)
	if empty {
		return 0
	}
	var n int64
	for i := lo; i <= hi; i++ {
		n += int64(bits.OnesCount8(buf[i]))
	}
	return n
}

// Pos is Redis BITPOS over an inclusive stored-byte window.
func Pos(buf []byte, bit bool, start, end int) (pos int64, found bool) {
	lo, hi, empty := NormalizeByteRange(len(buf), start, end)
	if empty {
		return 0, false
	}
	want := byte(0)
	if bit {
		want = 1
	}
	for i := lo; i <= hi; i++ {
		b := buf[i]
		for s := 0; s < 8; s++ {
			mask := byte(1 << (7 - s))
			set := b&mask != 0
			if set == (want == 1) {
				return int64(i*8 + s), true
			}
		}
	}
	return 0, false
}

// EncodeSet packs uvarint(offset) + 1 byte {0,1}.
func EncodeSet(offset uint64, bit bool) []byte {
	var scratch [binary.MaxVarintLen64 + 1]byte
	n := binary.PutUvarint(scratch[:], offset)
	if bit {
		scratch[n] = 1
	}
	return append([]byte(nil), scratch[:n+1]...)
}

// DecodeSet unpacks one inbox set payload. w<=0 is the only uvarint failure.
func DecodeSet(b []byte) (uint64, bool, error) {
	v, w := binary.Uvarint(b)
	if w <= 0 {
		return 0, false, ErrInbox
	}
	rest := b[w:]
	if len(rest) != 1 {
		return 0, false, ErrInbox
	}
	switch rest[0] {
	case 0:
		return v, false, nil
	case 1:
		return v, true, nil
	default:
		return 0, false, ErrInbox
	}
}
