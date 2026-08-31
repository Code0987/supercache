package topkx

import "encoding/binary"

// Encode writes occupied slots in List order as
//
//	uvarint(count) uvarint(len) item[len]
//
// repeating. K is not in the blob (it is keyspace config). Empty table → nil.
func (t *Table) Encode() []byte {
	rows := t.List()
	if len(rows) == 0 {
		return nil
	}
	var buf []byte
	var tmp [binary.MaxVarintLen64]byte
	for _, e := range rows {
		n := binary.PutUvarint(tmp[:], e.Count)
		buf = append(buf, tmp[:n]...)
		n = binary.PutUvarint(tmp[:], uint64(len(e.Item)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, e.Item...)
	}
	return buf
}

// Decode rebuilds a table from Encode output.
// Empty/nil blob is a valid empty table. Rejects k<1, truncated input,
// count 0, empty items, duplicates, and occupied > k (shrink-K / corrupt).
func Decode(b []byte, k int) (*Table, error) {
	if k < 1 {
		return nil, ErrK
	}
	t := New(k)
	if len(b) == 0 {
		return t, nil
	}
	for len(b) > 0 {
		count, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, ErrDecode
		}
		b = b[n:]
		if count == 0 {
			return nil, ErrDecode
		}
		ln, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, ErrDecode
		}
		b = b[n:]
		if ln == 0 || uint64(len(b)) < ln {
			return nil, ErrDecode
		}
		item := copyBytes(b[:ln])
		b = b[ln:]
		key := string(item)
		if _, dup := t.idx[key]; dup {
			return nil, ErrDecode
		}
		if len(t.slots) >= t.k {
			return nil, ErrDecode
		}
		t.idx[key] = len(t.slots)
		t.slots = append(t.slots, Entry{Item: item, Count: count})
	}
	return t, nil
}

// WorstEncodedSize is the Validate / topkNeed DoS bound:
// k slots × (max uvarint count + uvarint(maxItem) + maxItem bytes).
func WorstEncodedSize(k, maxItem int) int {
	if k < 1 || maxItem < 0 {
		return 0
	}
	return k * (10 + uvarintSize(uint64(maxItem)) + maxItem)
}

func uvarintSize(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
