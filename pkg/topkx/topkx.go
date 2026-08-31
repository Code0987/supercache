// Package topkx is a Space-Saving top-K table (Metwally et al.).
//
// Estimates do not match Redis TOPK (HeavyKeeper). Handoff is LWW replace,
// not a merge of two tables.
package topkx

import "errors"

var (
	// ErrEmpty is an empty item (engine rejects this first).
	ErrEmpty = errors.New("topkx: empty item")
	// ErrK is Decode with k < 1. New returns nil instead.
	ErrK = errors.New("topkx: K must be >= 1")
	// ErrOverflow is a uint64 increment that would wrap.
	ErrOverflow = errors.New("topkx: count overflow")
	// ErrDecode is a truncated, empty-slot, duplicate, or over-capacity blob.
	ErrDecode = errors.New("topkx: bad table blob")
)

// Entry is one chart row.
type Entry struct {
	Item  []byte
	Count uint64
}

// Table is a Space-Saving filter of at most k slots.
type Table struct {
	k     int
	slots []Entry
	idx   map[string]int
}

// New returns an empty table of capacity k.
// k < 1 → nil; Decode reports ErrK instead. The engine validates K first.
func New(k int) *Table {
	if k < 1 {
		return nil
	}
	return &Table{k: k, idx: make(map[string]int)}
}

// Occupied is how many slots are filled (always ≤ k).
func (t *Table) Occupied() int {
	if t == nil {
		return 0
	}
	return len(t.slots)
}

// copyBytes returns an independent copy so callers cannot alias table memory.
func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
