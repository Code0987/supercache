package topkx

import (
	"bytes"
	"sort"
)

// List is the current chart: a copy sorted by count descending, then item bytes.
// Encode uses this order so replica Peek bytes are comparable in tests.
func (t *Table) List() []Entry {
	if t == nil || len(t.slots) == 0 {
		return nil
	}
	out := make([]Entry, len(t.slots))
	for i, e := range t.slots {
		out[i] = Entry{Item: copyBytes(e.Item), Count: e.Count}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return bytes.Compare(out[i].Item, out[j].Item) < 0
	})
	return out
}
