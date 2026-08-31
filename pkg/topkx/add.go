package topkx

import (
	"bytes"
	"math"
)

// Add records one observation of item (+1).
//
// Already in the table: increment that slot (reject if the count would wrap).
// Table not full: insert at count 1.
// Table full: evict the current min-count slot (tie = smallest item bytes)
// and insert item at min+1. That overestimates — Space-Saving, not exact.
// Empty item / overflow leave the table unchanged.
func (t *Table) Add(item []byte) error {
	if t == nil || len(item) == 0 {
		return ErrEmpty
	}
	key := string(item)
	if i, ok := t.idx[key]; ok {
		if t.slots[i].Count == math.MaxUint64 {
			return ErrOverflow
		}
		t.slots[i].Count++
		return nil
	}
	if len(t.slots) < t.k {
		t.idx[key] = len(t.slots)
		t.slots = append(t.slots, Entry{Item: copyBytes(item), Count: 1})
		return nil
	}
	min := t.minSlot()
	c := t.slots[min].Count
	if c == math.MaxUint64 {
		return ErrOverflow
	}
	delete(t.idx, string(t.slots[min].Item))
	t.slots[min] = Entry{Item: copyBytes(item), Count: c + 1}
	t.idx[key] = min
	return nil
}

// minSlot is the victim for replace-min: lowest count, then smallest item bytes.
func (t *Table) minSlot() int {
	min := 0
	for i := 1; i < len(t.slots); i++ {
		c, mc := t.slots[i].Count, t.slots[min].Count
		if c < mc || (c == mc && bytes.Compare(t.slots[i].Item, t.slots[min].Item) < 0) {
			min = i
		}
	}
	return min
}
