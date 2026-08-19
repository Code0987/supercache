// Package listx encodes ordered lists for ModeList.
package listx

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// List is an in-memory sequence (index 0 = head).
type List struct {
	items [][]byte
}

// New returns an empty list.
func New() *List {
	return &List{}
}

// LPush prepends a copy of item.
func (l *List) LPush(item []byte) {
	cp := append([]byte(nil), item...)
	l.items = append([][]byte{cp}, l.items...)
}

// RPush appends a copy of item.
func (l *List) RPush(item []byte) {
	l.items = append(l.items, append([]byte(nil), item...))
}

// LPop removes and returns the head.
func (l *List) LPop() ([]byte, bool) {
	if l == nil || len(l.items) == 0 {
		return nil, false
	}
	it := l.items[0]
	l.items = l.items[1:]
	return it, true
}

// RPop removes and returns the tail.
func (l *List) RPop() ([]byte, bool) {
	if l == nil || len(l.items) == 0 {
		return nil, false
	}
	n := len(l.items)
	it := l.items[n-1]
	l.items = l.items[:n-1]
	return it, true
}

// Len is the number of elements.
func (l *List) Len() int {
	if l == nil {
		return 0
	}
	return len(l.items)
}

func (l *List) resolve(idx int) (int, bool) {
	n := l.Len()
	if n == 0 {
		return 0, false
	}
	if idx < 0 {
		idx = n + idx
	}
	if idx < 0 || idx >= n {
		return 0, false
	}
	return idx, true
}

// Index returns a copy of the element at idx (Redis negatives).
func (l *List) Index(idx int) ([]byte, bool) {
	i, ok := l.resolve(idx)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), l.items[i]...), true
}

// Range returns copies of [start, stop] inclusive (Redis negatives).
func (l *List) Range(start, stop int) [][]byte {
	n := l.Len()
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
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, append([]byte(nil), l.items[i]...))
	}
	return out
}

// Encode packs left-to-right uvarint(len)+bytes.
func (l *List) Encode() []byte {
	if l == nil || len(l.items) == 0 {
		return []byte{}
	}
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	for _, it := range l.items {
		n := binary.PutUvarint(scratch[:], uint64(len(it)))
		buf.Write(scratch[:n])
		buf.Write(it)
	}
	return buf.Bytes()
}

// ApproxWireBytes estimates encoded size.
func (l *List) ApproxWireBytes() int64 {
	if l == nil {
		return 0
	}
	var n int64
	for _, it := range l.items {
		n += int64(uvarintSize(uint64(len(it)))) + int64(len(it))
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

// Decode rebuilds a List from Encode output.
func Decode(b []byte) (*List, error) {
	l := New()
	if len(b) == 0 {
		return l, nil
	}
	for len(b) > 0 {
		n, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("listx: bad uvarint")
		}
		b = b[w:]
		if uint64(len(b)) < n {
			return nil, fmt.Errorf("listx: truncated item")
		}
		l.items = append(l.items, append([]byte(nil), b[:n]...))
		b = b[n:]
	}
	return l, nil
}
